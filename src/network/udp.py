import asyncio
import json
import logging
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
        data = json.dumps(msg).encode("utf-8")
        self._transport.sendto(data, (host, port))