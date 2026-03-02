import logging
import time
from dataclasses import dataclass
from typing import List

from src.node.membership import MembershipTable, NodeStatus

logger = logging.getLogger(__name__)


@dataclass
class FDConfig:
    suspect_timeout: float
    dead_timeout: float


class FailureDetector:
    def __init__(self, membership: MembershipTable, cfg: FDConfig):
        self.membership = membership
        self.cfg = cfg

    def tick(self) -> List[str]:
        """
        Evaluate timeouts and update member states.
        Returns list of node_ids whose status changed.
        """
        now = time.time()
        changed: List[str] = []

        for node_id, info in list(self.membership.members.items()):
            if node_id == self.membership.self_id:
                continue
            if info.status == NodeStatus.DEAD:
                continue

            elapsed = now - info.last_update

            # ALIVE -> SUSPECT
            if info.status == NodeStatus.ALIVE and elapsed >= self.cfg.suspect_timeout:
                self.membership.set_status(node_id, NodeStatus.SUSPECT)
                changed.append(node_id)
                logger.info(f"[FD] {node_id} -> SUSPECT (elapsed={elapsed:.2f}s)")

            # SUSPECT -> DEAD
            if info.status == NodeStatus.SUSPECT and elapsed >= self.cfg.dead_timeout:
                self.membership.set_status(node_id, NodeStatus.DEAD)
                changed.append(node_id)
                logger.info(f"[FD] {node_id} -> DEAD (elapsed={elapsed:.2f}s)")

        return changed