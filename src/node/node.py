import asyncio
import logging
import random
import time
from typing import Dict, Tuple

from src.node.membership import MembershipTable, MemberInfo, NodeStatus
from src.network.udp import UDPTransport
from src.protocol.failure_detector import FailureDetector, FDConfig

logger = logging.getLogger(__name__)


class Node:
    def __init__(
        self,
        node_id: str,
        bind_host: str,
        bind_port: int,
        peers: Dict[str, Tuple[str, int]],
        gossip_interval: float = 1.0,
        fanout: int = 1,
    ):
        self.node_id = node_id
        self.bind_host = bind_host
        self.bind_port = bind_port
        self.known_peers = peers  # node_id -> (host, port)

        self.gossip_interval = gossip_interval
        self.fanout = fanout

        self.membership = MembershipTable(node_id)
        self.fd = FailureDetector(
            self.membership,
            FDConfig(suspect_timeout=6.0, dead_timeout=12.0),
            )

        self.net = UDPTransport(bind_host, bind_port)

        # pending local membership updates to disseminate (node_id -> dict with fields + 'ttl')
        self.pending_updates: Dict[str, dict] = {}

        # number of gossip rounds to retransmit each important update
        self.gossip_repeat: int = 3  # retransmit each update for N gossip rounds

        # Metrics
        self.metrics = {
            "gossip_sent": 0,
            "gossip_received": 0,
            "suspect_events": 0,
            "dead_events": 0,
        }

        self._running = False

    async def start(self):
        logger.info(f"Node {self.node_id} starting")
        self._running = True

        self.membership.revive_self()
        await self.net.start(self.on_message)

        tasks = [
            asyncio.create_task(self.gossip_loop(), name=f"{self.node_id}-gossip"),
            asyncio.create_task(self.receive_loop(), name=f"{self.node_id}-receive"),
            asyncio.create_task(self.failure_detector_loop(), name=f"{self.node_id}-fd"),
            asyncio.create_task(self.metrics_loop(), name=f"{self.node_id}-metrics"),
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info(f"Node {self.node_id} cancelled")
            raise
        finally:
            await self.stop()

    async def stop(self):
        if not self._running:
            return

        logger.info(f"Node {self.node_id} stopping")
        self._running = False

        if hasattr(self.net, "close"):
            result = self.net.close()
            if asyncio.iscoroutine(result):
                await result

        logger.info(f"Node {self.node_id} stopped")

    async def gossip_loop(self):
        while self._running:
            await asyncio.sleep(self.gossip_interval)
            self.membership.increment_heartbeat()

            candidate_peers = [
                pid for pid in self.known_peers.keys()
                if not self.membership.is_dead(pid)
            ]
            logger.debug(f"[{self.node_id}] Gossip tick — peers={len(candidate_peers)}")

            if not candidate_peers:
                continue

            k = min(self.fanout, len(candidate_peers))
            targets = random.sample(candidate_peers, k=k)

            for target_id in targets:
                await self.send_gossip(target_id)

    async def send_gossip(self, peer_id: str):
        if peer_id not in self.known_peers:
            return
        host, port = self.known_peers[peer_id]

        # send a membership snapshot (no timestamps)
        payload = {
            "type": "GOSSIP",
            "from": self.node_id,
            "members": {
                mid: {
                    "heartbeat": m.heartbeat,
                    "incarnation": m.incarnation,
                    "status": m.status.value,
                }
                for mid, m in self.membership.members.items()
            },
            "updates": self._collect_updates(),
        }

        self.metrics["gossip_sent"] += 1    
        self.net.send(payload, host, port)
        logger.debug(f"[{self.node_id}] Sent gossip to {peer_id} ({host}:{port})")

    def on_message(self, msg: Dict, addr):
        if msg.get("type") != "GOSSIP":
            return

        sender = msg.get("from", "?")
        sender_hb = msg.get("members", {}).get(sender, {}).get("heartbeat", 0)
        self.membership.mark_seen(sender, int(sender_hb))
        members = msg.get("members", {})
        # Apply prioritized failure updates 
        updates = msg.get("updates", {})
        for nid, upd in updates.items():
            try:
                inc = int(upd.get("incarnation", 0))
                st = NodeStatus(upd["status"])

                current = self.membership.members.get(nid)
                if current is None:
                    # unknown node: create it 
                    self.membership.members[nid] = MemberInfo(
                        node_id=nid,
                        heartbeat=0,
                        incarnation=inc,
                        status=st,
                        last_seen=time.time(),
                    )
                else:
                    # accept if newer incarnation, or same incarnation with "worse" status
                    if inc > current.incarnation:
                        current.incarnation = inc
                        current.status = st
                        current.last_seen = time.time()
                    elif inc == current.incarnation:
                        order = {NodeStatus.ALIVE: 0, NodeStatus.SUSPECT: 1, NodeStatus.DEAD: 2}
                        if order[st] > order[current.status]:
                            current.status = st
                            current.last_seen = time.time()

            except Exception:
                continue
        logger.debug(f"[{self.node_id}] Received gossip from {sender} @ {addr}")
        self.metrics["gossip_received"] += 1 #count received gossip messages

        # convert back to MemberInfo and merge
        incoming = {}
        for mid, data in members.items():
            try:
                # Do NOT trust remote timestamps for last_seen/last_update.
                # last_seen as a local-only timestamp (merge will refresh it).
                incoming[mid] = MemberInfo(
                    node_id=mid,
                    heartbeat=int(data["heartbeat"]),
                    incarnation=int(data.get("incarnation", 0)),
                    status=NodeStatus(data["status"]),
                    last_seen=0.0,
                )
            except Exception:
                continue

        # Self-defense: if someone gossips that I'm SUSPECT/DEAD with the same
        # incarnation, I must refute by increasing my incarnation and marking ALIVE.
        me = incoming.get(self.node_id)
        if me and me.status in (NodeStatus.SUSPECT, NodeStatus.DEAD):
            local_me = self.membership.members[self.node_id]
            if me.incarnation == local_me.incarnation:
                local_me.incarnation += 1
                local_me.status = NodeStatus.ALIVE
                logger.info(f"[{self.node_id}] Refuting {me.status.value}: increase incarnation -> {local_me.incarnation}")

        self.membership.merge(incoming)

    async def receive_loop(self):
        # UDP callbacks already handle receiving; keep loop idle
        while self._running:
            await asyncio.sleep(1.0)

    # Failure detector loop 
    async def failure_detector_loop(self):
        while self._running:
            await asyncio.sleep(0.5)

            events = self.fd.tick()  # <-- tick() must return a list of events
            for node_id, status, incarnation in events:
                if status == NodeStatus.SUSPECT:
                    self.metrics["suspect_events"] += 1
                if status == NodeStatus.DEAD:
                    self.metrics["dead_events"] += 1
                    
                self.pending_updates[node_id] = {
                    "status": status.value,
                    "incarnation": int(incarnation),
                    "ttl": self.gossip_repeat,
                }
    
    async def metrics_loop(self):
        while self._running:
            await asyncio.sleep(5)

            logger.info(
                f"[METRICS] node={self.node_id} "
                f"sent={self.metrics['gossip_sent']} "
                f"recv={self.metrics['gossip_received']} "
                f"suspect={self.metrics['suspect_events']} "
                f"dead={self.metrics['dead_events']}"
            )

            logger.info("[MEMBERSHIP]")
            for m in self.membership.members.values():
                logger.info(
                    f"  {m.node_id} "
                    f"hb={m.heartbeat} "
                    f"inc={m.incarnation} "
                    f"status={m.status.value}"
                )

    def _collect_updates(self) -> Dict[str, dict]:
        out: Dict[str, dict] = {}
        to_delete = []

        for nid, upd in self.pending_updates.items():
            out[nid] = {
                "status": upd["status"],
                "incarnation": upd["incarnation"],
            }

            upd["ttl"] -= 1
            if upd["ttl"] <= 0:
                to_delete.append(nid)

        for nid in to_delete:
            del self.pending_updates[nid]

        return out