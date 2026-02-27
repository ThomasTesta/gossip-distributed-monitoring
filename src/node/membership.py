from dataclasses import dataclass
from enum import Enum
from typing import Dict
import time


class NodeStatus(str, Enum):
    ALIVE = "ALIVE"
    SUSPECT = "SUSPECT"
    DEAD = "DEAD"


@dataclass
class MemberInfo:
    node_id: str
    heartbeat: int
    status: NodeStatus
    last_update: float


class MembershipTable:
    def __init__(self, self_id: str):
        self.self_id = self_id
        self.members: Dict[str, MemberInfo] = {}

        # register self
        self.members[self_id] = MemberInfo(
            node_id=self_id,
            heartbeat=0,
            status=NodeStatus.ALIVE,
            last_update=time.time(),
        )

    def increment_heartbeat(self):
        me = self.members[self.self_id]
        me.heartbeat += 1
        me.last_update = time.time()

    def get_alive_peers(self):
        return [
            m.node_id
            for m in self.members.values()
            if m.node_id != self.self_id and m.status != NodeStatus.DEAD
        ]

    def merge(self, incoming: Dict[str, MemberInfo]):
        """
        Placeholder merge logic (we improve later).
        """
        for node_id, info in incoming.items():
            local = self.members.get(node_id)

            if local is None or info.heartbeat > local.heartbeat:
                self.members[node_id] = info