from dataclasses import dataclass
from enum import Enum
from typing import Dict
import time


class NodeStatus(str, Enum):
    ALIVE = "ALIVE"
    SUSPECT = "SUSPECT" #3 sec
    DEAD = "DEAD" #6sec


@dataclass
class MemberInfo:
    node_id: str
    heartbeat: int #logical clock
    incarnation: int #increment for reborn and restart nodes
    status: NodeStatus
    last_seen: float # timestamp local: when I last heard from this node

# members: Dict[str, MemberInfo]
class MembershipTable:
    def __init__(self, self_id: str):
        self.self_id = self_id
        self.members: Dict[str, MemberInfo] = {}

        # register self
        self.members[self_id] = MemberInfo(
            node_id=self_id,
            heartbeat=0,
            incarnation=0,
            status=NodeStatus.ALIVE,
            last_seen=time.time(),
        )

    def increment_heartbeat(self):
        me = self.members[self.self_id]
        me.heartbeat += 1
        me.last_seen = time.time()

    def get_alive_peers(self):
        return [
            m.node_id
            for m in self.members.values()
            if m.node_id != self.self_id and m.status != NodeStatus.DEAD
        ]

    def _status_rank(self, s: NodeStatus) -> int:
        return {NodeStatus.ALIVE: 0, NodeStatus.SUSPECT: 1, NodeStatus.DEAD: 2}[s]
    
    def merge(self, incoming: Dict[str, MemberInfo]):
        """ Merge rules (SWIM-lite):
        1) Higher incarnation wins.
        2) If same incarnation: higher heartbeat wins.
        3) If tie: higher status rank wins (DEAD > SUSPECT > ALIVE).
        When we accept newer info we refresh `last_seen` locally (do not trust remote timestamps).
        """
        
        for node_id, inc in incoming.items():
            loc = self.members.get(node_id)
            
            if loc is None:
                # store incoming info but refresh last_seen to local time
                inc.last_seen = time.time()
                self.members[node_id] = inc
                continue
            newer = False
            
            if inc.incarnation > loc.incarnation:
                newer = True
            elif inc.incarnation == loc.incarnation:
                if inc.heartbeat > loc.heartbeat:
                    newer = True
                elif inc.heartbeat == loc.heartbeat:
                    if self._status_rank(inc.status) > self._status_rank(loc.status):
                        newer = True
                
            if newer:
                # update existing local record fields and refresh local last_seen
                loc.heartbeat = inc.heartbeat
                loc.incarnation = inc.incarnation
                loc.status = inc.status
                loc.last_seen = time.time()

    # Mark a node as seen (update heartbeat and timestamp)      
    def mark_seen(self, node_id: str, heartbeat: int = 0):
        now = time.time()
        m = self.members.get(node_id)

        if m is None:
            self.members[node_id] = MemberInfo(
                node_id=node_id,
                heartbeat=heartbeat,
                incarnation=0,
                status=NodeStatus.ALIVE,
                last_seen=now,
            )
            return

        m.last_seen = now
        if heartbeat > m.heartbeat:
            m.heartbeat = heartbeat

        # always mark seen nodes as ALIVE when receive a valid message
        m.status = NodeStatus.ALIVE

    # Update node status
    def set_status(self, node_id: str, status: NodeStatus):
        m = self.members.get(node_id)
        if m:
            m.status = status
    #Calling at the beginning 
    def revive_self(self):
        """Call on startup for reborn."""
        me = self.members[self.self_id]
        me.incarnation += 1
        me.status = NodeStatus.ALIVE
        me.last_seen = time.time()