import asyncio
import logging
import random
import time
import os
from typing import Dict, Tuple, Set

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
        self.known_peers = peers

        self.gossip_interval = gossip_interval
        self.fanout = fanout

        self.membership = MembershipTable(node_id)
        self.fd = FailureDetector(
            self.membership,
            FDConfig(suspect_timeout=6.0, dead_timeout=12.0),
        )

        now = time.time()
        for pid in self.known_peers.keys():
            if pid == self.node_id:
                continue
            self.membership.members.setdefault(
                pid,
                MemberInfo(
                    node_id=pid,
                    heartbeat=0,
                    incarnation=0,
                    status=NodeStatus.ALIVE,
                    last_seen=now,
                ),
            )

        self.net = UDPTransport(bind_host, bind_port)

        # Failure dissemination
        self.pending_updates: Dict[str, dict] = {}
        self.gossip_repeat: int = 3

        # Gossip Arena: rumor storage
        self.rumors: Set[str] = set()
        self.rumor_repeat: int = 5
        self.pending_rumors: Dict[str, int] = {}

        # Optional startup rumor
        startup_rumor = os.environ.get("STARTUP_RUMOR", "").strip()
        if startup_rumor:
            self.rumors.add(startup_rumor)
            self.pending_rumors[startup_rumor] = self.rumor_repeat

        self.metrics = {
            "gossip_sent": 0,
            "gossip_received": 0,
            "suspect_events": 0,
            "dead_events": 0,
            "rumors_generated": 1 if startup_rumor else 0,
            "rumors_received": 0,
            "rumors_forwarded": 0,
        }

        self._running = False

    async def start(self):
        logger.info(f"Node {self.node_id} starting")
        self._running = True

        self.membership.revive_self()

        await self.net.start(self.on_message)

        await asyncio.gather(
            self.gossip_loop(),
            self.receive_loop(),
            self.failure_detector_loop(),
            self.metrics_loop(),
        )

    async def stop(self):
        self._running = False

    async def gossip_loop(self):
        while self._running:
            await asyncio.sleep(self.gossip_interval)
            self.membership.increment_heartbeat()

            peers = self.membership.get_alive_peers()
            if not peers:
                continue

            k = min(self.fanout, len(peers))
            targets = random.sample(peers, k=k)

            for target_id in targets:
                await self.send_gossip(target_id)

    async def send_gossip(self, peer_id: str):
        if peer_id not in self.known_peers:
            return

        host, port = self.known_peers[peer_id]

        rumors_to_send = self._collect_rumors()

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
            "rumors": rumors_to_send,
        }

        self.metrics["gossip_sent"] += 1
        if rumors_to_send:
            self.metrics["rumors_forwarded"] += len(rumors_to_send)

        self.net.send(payload, host, port)

    def on_message(self, msg: Dict, addr):
        if msg.get("type") != "GOSSIP":
            return

        sender = msg.get("from", "?")
        sender_hb = msg.get("members", {}).get(sender, {}).get("heartbeat", 0)
        self.membership.mark_seen(sender, int(sender_hb))

        members = msg.get("members", {})
        updates = msg.get("updates", {})

        for nid, upd in updates.items():
            try:
                inc = int(upd.get("incarnation", 0))
                st = NodeStatus(upd["status"])

                current = self.membership.members.get(nid)
                if current is None:
                    self.membership.members[nid] = MemberInfo(
                        node_id=nid,
                        heartbeat=0,
                        incarnation=inc,
                        status=st,
                        last_seen=time.time(),
                    )
                else:
                    if inc > current.incarnation:
                        current.incarnation = inc
                        current.status = st
                        current.last_seen = time.time()
                    elif inc == current.incarnation:
                        order = {
                            NodeStatus.ALIVE: 0,
                            NodeStatus.SUSPECT: 1,
                            NodeStatus.DEAD: 2,
                        }
                        if order[st] > order[current.status]:
                            current.status = st
                            current.last_seen = time.time()
            except Exception:
                continue

        self.metrics["gossip_received"] += 1

        incoming = {}
        for mid, data in members.items():
            try:
                incoming[mid] = MemberInfo(
                    node_id=mid,
                    heartbeat=int(data["heartbeat"]),
                    incarnation=int(data.get("incarnation", 0)),
                    status=NodeStatus(data["status"]),
                    last_seen=0.0,
                )
            except Exception:
                continue

        me = incoming.get(self.node_id)
        if me and me.status in (NodeStatus.SUSPECT, NodeStatus.DEAD):
            local_me = self.membership.members[self.node_id]
            if me.incarnation == local_me.incarnation:
                local_me.incarnation += 1
                local_me.status = NodeStatus.ALIVE
                logger.info(
                    f"[{self.node_id}] Refuting {me.status.value}: "
                    f"increase incarnation -> {local_me.incarnation}"
                )

        self.membership.merge(incoming)

        # Gossip Arena: rumor reception
        incoming_rumors = msg.get("rumors", [])
        for rumor_id in incoming_rumors:
            if rumor_id not in self.rumors:
                self.rumors.add(rumor_id)
                self.pending_rumors[rumor_id] = self.rumor_repeat
                self.metrics["rumors_received"] += 1
                logger.info(f"[RUMOR] node={self.node_id} learned={rumor_id} from={sender}")

    async def receive_loop(self):
        while self._running:
            await asyncio.sleep(1.0)

    async def failure_detector_loop(self):
        while self._running:
            await asyncio.sleep(0.5)

            events = self.fd.tick()
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
                f"dead={self.metrics['dead_events']} "
                f"rumors_generated={self.metrics['rumors_generated']} "
                f"rumors_received={self.metrics['rumors_received']} "
                f"rumors_forwarded={self.metrics['rumors_forwarded']}"
            )

            logger.info("[MEMBERSHIP]")
            for m in self.membership.members.values():
                logger.info(
                    f"  {m.node_id} "
                    f"hb={m.heartbeat} "
                    f"inc={m.incarnation} "
                    f"status={m.status.value}"
                )

            logger.info(f"[RUMORS] node={self.node_id} rumors={sorted(self.rumors)}")

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

    def _collect_rumors(self):
        rumors_out = []
        to_delete = []

        for rumor_id, ttl in self.pending_rumors.items():
            rumors_out.append(rumor_id)
            self.pending_rumors[rumor_id] -= 1
            if self.pending_rumors[rumor_id] <= 0:
                to_delete.append(rumor_id)

        for rumor_id in to_delete:
            del self.pending_rumors[rumor_id]

        return rumors_out