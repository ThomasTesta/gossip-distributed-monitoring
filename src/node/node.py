import asyncio
import logging
import random
from typing import List

from src.node.membership import MembershipTable


logger = logging.getLogger(__name__)


class Node:
    def __init__(self, node_id: str, gossip_interval: float = 1.0):
        self.node_id = node_id
        self.gossip_interval = gossip_interval

        self.membership = MembershipTable(node_id)

        self._running = False

    # ======================
    # Lifecycle
    # ======================

    async def start(self):
        logger.info(f"Node {self.node_id} starting")

        self._running = True

        await asyncio.gather(
            self.gossip_loop(),
            self.receive_loop(),
        )

    async def stop(self):
        self._running = False

    # ======================
    # Gossip Loop
    # ======================

    async def gossip_loop(self):
        while self._running:
            await asyncio.sleep(self.gossip_interval)

            # increment own heartbeat
            self.membership.increment_heartbeat()

            peers = self.membership.get_alive_peers()

            logger.debug(
                f"[{self.node_id}] Gossip tick — peers={len(peers)}"
            )

            if not peers:
                continue

            # placeholder peer selection
            target = random.choice(peers)

            await self.send_gossip(target)

    async def send_gossip(self, peer_id: str):
        """
        Stub — network will come later.
        """
        logger.debug(f"[{self.node_id}] Would gossip to {peer_id}")

    # ======================
    # Receive Loop
    # ======================

    async def receive_loop(self):
        while self._running:
            # placeholder for UDP receive
            await asyncio.sleep(0.1)