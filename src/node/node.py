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
            FDConfig(suspect_timeout=3.0, dead_timeout=6.0),
            )
        # pre-load known peers into membership (ALIVE, heartbeat=0)
        now = time.time()
        for pid in self.known_peers.keys():
            if pid == self.node_id:
                continue
            self.membership.members.setdefault(
                pid,
                MemberInfo(node_id=pid, heartbeat=0, status=NodeStatus.ALIVE, last_update=now),
            )

        self.net = UDPTransport(bind_host, bind_port)

        self._running = False

    async def start(self):
        logger.info(f"Node {self.node_id} starting")
        self._running = True

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

        # send a membership snapshot
        payload = {
            "type": "GOSSIP",
            "from": self.node_id,
            "members": {
                mid: {
                    "heartbeat": m.heartbeat,
                    "status": m.status.value,
                    "last_update": m.last_update,
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
                incoming[mid] = MemberInfo(
                    node_id=mid,
                    heartbeat=int(data["heartbeat"]),
                    status=NodeStatus(data["status"]),
                    last_update=float(data["last_update"]),
                )
            except Exception:
                continue

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