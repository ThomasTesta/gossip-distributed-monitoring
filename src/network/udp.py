import asyncio
import json
import logging
import os
import random
from typing import Any, Callable, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class UDPProtocol(asyncio.DatagramProtocol):
    def __init__(self, on_datagram: Callable[[bytes, Tuple[str, int]], None]):
        self.on_datagram = on_datagram
        self.transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore
        sockname = self.transport.get_extra_info("sockname")
        logger.info(f"UDP listening on {sockname}")

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        self.on_datagram(data, addr)

    def error_received(self, exc: Exception) -> None:
        logger.warning(f"UDP error_received: {exc}")


class UDPTransport:
    def __init__(self, bind_host: str, bind_port: int):
        self.bind_host = bind_host
        self.bind_port = bind_port
        self._transport: Optional[asyncio.DatagramTransport] = None

        # Network config (env vars)
        # - NET_DROP_RATE: float in [0.0, 1.0]
        # - NET_DELAY_MIN_MS / NET_DELAY_MAX_MS: int milliseconds (0 = no delay)
        self.drop_rate = float(os.getenv("NET_DROP_RATE", "0.0"))
        self.delay_min_ms = int(os.getenv("NET_DELAY_MIN_MS", os.getenv("NET_DELAY_MS_MIN", "0")))
        self.delay_max_ms = int(os.getenv("NET_DELAY_MAX_MS", os.getenv("NET_DELAY_MS_MAX", "0")))

        # sanitize
        if self.drop_rate < 0.0:
            self.drop_rate = 0.0
        if self.drop_rate > 1.0:
            self.drop_rate = 1.0
        if self.delay_min_ms < 0:
            self.delay_min_ms = 0
        if self.delay_max_ms < 0:
            self.delay_max_ms = 0
        if self.delay_max_ms < self.delay_min_ms:
            self.delay_max_ms = self.delay_min_ms

        logger.info(
            f"[NET] impairment config: drop_rate={self.drop_rate}, "
            f"delay_ms=[{self.delay_min_ms},{self.delay_max_ms}]"
        )

    async def start(self, on_message: Callable[[Dict[str, Any], Tuple[str, int]], None]) -> None:
        loop = asyncio.get_running_loop()

        def _on_datagram(data: bytes, addr: Tuple[str, int]) -> None:
            try:
                msg = json.loads(data.decode("utf-8"))
                on_message(msg, addr)
            except Exception as e:
                logger.warning(f"Failed to decode datagram from {addr}: {e}")

        protocol = UDPProtocol(_on_datagram)
        transport, _ = await loop.create_datagram_endpoint(
            lambda: protocol,
            local_addr=(self.bind_host, self.bind_port),
        )
        self._transport = transport  # type: ignore

    def send(self, msg: Dict[str, Any], host: str, port: int) -> None:
        if not self._transport:
            raise RuntimeError("UDPTransport not started")

        # Packet loss
        if self.drop_rate > 0.0 and random.random() < self.drop_rate:
            logger.debug(f"[NET] DROP -> {(host, port)} type={msg.get('type')}")
            return

        data = json.dumps(msg).encode("utf-8")

        # Delay 
        delay_ms = 0
        if self.delay_max_ms > 0:
            delay_ms = random.randint(self.delay_min_ms, self.delay_max_ms)

        if delay_ms <= 0:
            self._transport.sendto(data, (host, port))
        else:
            loop = asyncio.get_running_loop()
            loop.call_later(
                delay_ms / 1000.0,
                lambda: self._transport and self._transport.sendto(data, (host, port)),
            )