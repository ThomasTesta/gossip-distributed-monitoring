import asyncio
import logging
import random
import time
import os
import uvicorn

from src.dashboard.api import create_app
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
        gossip_mode: str = "push",
    ):
        self.node_id = node_id
        self.bind_host = bind_host
        self.bind_port = bind_port
        self.known_peers = peers

        self.gossip_interval = gossip_interval
        self.fanout = fanout

        self.gossip_mode = gossip_mode.lower()
        logger.info(f"[GOSSIP] mode={self.gossip_mode}")

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

        # Gossip Arena: structured rumors
        # rumor_id -> {"rumor_id", "origin", "created_at", "hop_count"}
        self.rumors: Dict[str, dict] = {}
        self.rumor_repeat: int = 5
        self.pending_rumors: Dict[str, int] = {}

        startup_rumors_raw = os.environ.get("STARTUP_RUMORS", "").strip()
        startup_rumors = [
            r.strip()
            for r in startup_rumors_raw.split(",")
            if r.strip()
        ]

        for rumor_id in startup_rumors:
            rumor = {
                "rumor_id": rumor_id,
                "origin": self.node_id,
                "created_at": time.time(),
                "hop_count": 0,
            }
            self.rumors[rumor_id] = rumor
            self.pending_rumors[rumor_id] = self.rumor_repeat

        self.metrics = {
            "gossip_sent": 0,
            "gossip_received": 0,
            "suspect_events": 0,
            "dead_events": 0,
            "rumors_generated": len(startup_rumors),
            "rumors_received": 0,
            "rumors_forwarded": 0,
        }

        self.api_host = os.environ.get("API_HOST", "0.0.0.0")
        self.api_port = int(os.environ.get("API_PORT", "8000"))

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
            self.api_loop(),
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
                if self.gossip_mode == "push":
                    await self.send_gossip(target_id)
                elif self.gossip_mode == "push-pull":
                    await self.send_gossip(target_id)
                    await self.send_pull_request(target_id)

    async def send_gossip(self, peer_id: str):
        if peer_id not in self.known_peers:
            return

        host, port = self.known_peers[peer_id]
        payload = self.build_gossip_payload()

        self.metrics["gossip_sent"] += 1
        if payload.get("rumors"):
            self.metrics["rumors_forwarded"] += len(payload["rumors"])

        self.net.send(payload, host, port)

    async def send_pull_request(self, peer_id: str):
        if peer_id not in self.known_peers:
            return

        host, port = self.known_peers[peer_id]

        payload = {
            "type": "PULL_REQUEST",
            "from": self.node_id,
        }

        self.net.send(payload, host, port)

    def build_gossip_payload(self) -> Dict:
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

        return payload

    def on_message(self, msg: Dict, addr):
        msg_type = msg.get("type")

        if msg_type == "GOSSIP":
            self.handle_gossip(msg, addr)
        elif msg_type == "PULL_REQUEST":
            self.handle_pull_request(msg, addr)
        elif msg_type == "PULL_RESPONSE":
            # Pull response contains gossip-like payload
            self.handle_gossip(msg, addr)

    def handle_gossip(self, msg: Dict, addr):
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

        # Structured rumor reception
        incoming_rumors = msg.get("rumors", [])
        for rumor in incoming_rumors:
            try:
                rumor_id = rumor["rumor_id"]
                if rumor_id not in self.rumors:
                    stored = {
                        "rumor_id": rumor_id,
                        "origin": rumor.get("origin", sender),
                        "created_at": float(rumor.get("created_at", time.time())),
                        "hop_count": int(rumor.get("hop_count", 0)),
                    }
                    self.rumors[rumor_id] = stored
                    self.pending_rumors[rumor_id] = self.rumor_repeat
                    self.metrics["rumors_received"] += 1

                    logger.info(
                        f"[RUMOR] node={self.node_id} "
                        f"learned={rumor_id} "
                        f"origin={stored['origin']} "
                        f"hop_count={stored['hop_count']} "
                        f"from={sender}"
                    )
            except Exception:
                continue

    def handle_pull_request(self, msg: Dict, addr):
        sender = msg.get("from")

        if sender not in self.known_peers:
            return

        host, port = self.known_peers[sender]

        payload = self.build_gossip_payload()
        payload["type"] = "PULL_RESPONSE"

        self.net.send(payload, host, port)

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

            rumor_summary = [
                f"{rid}(origin={r['origin']},hop={r['hop_count']})"
                for rid, r in sorted(self.rumors.items())
            ]
            logger.info(f"[RUMORS] node={self.node_id} rumors={rumor_summary}")

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
            rumor = self.rumors[rumor_id]

            rumors_out.append(
                {
                    "rumor_id": rumor["rumor_id"],
                    "origin": rumor["origin"],
                    "created_at": rumor["created_at"],
                    "hop_count": rumor["hop_count"] + 1,
                }
            )

            self.pending_rumors[rumor_id] -= 1
            if self.pending_rumors[rumor_id] <= 0:
                to_delete.append(rumor_id)

        for rumor_id in to_delete:
            del self.pending_rumors[rumor_id]

        return rumors_out

    async def api_loop(self):
        app = create_app(self)

        config = uvicorn.Config(
            app,
            host=self.api_host,
            port=self.api_port,
            log_level="warning",
        )

        server = uvicorn.Server(config)
        await server.serve()