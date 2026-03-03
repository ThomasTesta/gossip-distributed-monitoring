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
        # pre-load known peers into membership (ALIVE, heartbeat=0)
        now = time.time()
        for pid in self.known_peers.keys():
            if pid == self.node_id:
                continue
            self.membership.members.setdefault(
                pid,
                MemberInfo(node_id=pid, heartbeat=0, incarnation=0, status=NodeStatus.ALIVE, last_seen=now),
            )

        self.net = UDPTransport(bind_host, bind_port)

        self._running = False

    async def start(self):
        logger.info(f"Node {self.node_id} starting")
        self._running = True

        # revive self on startup (increment incarnation and mark alive)
        self.membership.revive_self()

        await self.net.start(self.on_message)

        await asyncio.gather(
            self.gossip_loop(),
            self.receive_loop(),
            self.failure_detector_loop(),
        )

    async def stop(self):
        self._running = False

    async def gossip_loop(self):
        while self._running:
            await asyncio.sleep(self.gossip_interval)
            self.membership.increment_heartbeat()

            peers = self.membership.get_alive_peers()
            logger.debug(f"[{self.node_id}] Gossip tick — peers={len(peers)}")

            if not peers:
                continue

            # choose up to fanout peers
            k = min(self.fanout, len(peers))
            targets = random.sample(peers, k=k)

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
        }

        self.net.send(payload, host, port)
        logger.debug(f"[{self.node_id}] Sent gossip to {peer_id} ({host}:{port})")

    def on_message(self, msg: Dict, addr):
        if msg.get("type") != "GOSSIP":
            return

        sender = msg.get("from", "?")
        sender_hb = msg.get("members", {}).get(sender, {}).get("heartbeat", 0)
        self.membership.mark_seen(sender, int(sender_hb))
        members = msg.get("members", {})
        logger.debug(f"[{self.node_id}] Received gossip from {sender} @ {addr}")

        # convert back to MemberInfo and merge
        incoming = {}
        for mid, data in members.items():
            try:
                # Do NOT trust remote timestamps for last_seen/last_update.
                # We'll keep last_seen as a local-only timestamp (merge will refresh it).
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
            self.fd.tick()