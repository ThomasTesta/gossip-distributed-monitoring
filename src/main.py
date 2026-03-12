import asyncio
import logging
import os
from typing import Dict, Tuple

from src.node.node import Node
from typing import Optional


def parse_peers(peers_raw: str) -> Dict[str, Tuple[str, int]]:
    """
    Parse PEERS in the format:
    node-1@node-1:5001,node-2@node-2:5002,node-3@node-3:5003

    Returns:
        {
            "node-1": ("node-1", 5001),
            "node-2": ("node-2", 5002),
            etc...
        }
    """
    peers: Dict[str, Tuple[str, int]] = {}

    if not peers_raw.strip():
        raise ValueError("PEERS is empty")

    entries = [entry.strip() for entry in peers_raw.split(",") if entry.strip()]
    if not entries:
        raise ValueError("PEERS contains no valid entries")

    for entry in entries:
        try:
            node_id_part, address_part = entry.split("@", 1)
            host_part, port_part = address_part.rsplit(":", 1)

            node_id = node_id_part.strip()
            host = host_part.strip()
            port = int(port_part.strip())

            if not node_id:
                raise ValueError("missing node_id")
            if not host:
                raise ValueError("missing host")

            peers[node_id] = (host, port)

        except Exception as exc:
            raise ValueError(
                f"Invalid PEERS entry '{entry}'. Expected format NODE_ID@HOST:PORT"
            ) from exc

    return peers


def get_env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and (value is None or value.strip() == ""):
        raise ValueError(f"Missing required environment variable: {name}")
    if value is None:
        raise ValueError(f"Missing environment variable: {name}")
    return value.strip()


def get_env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be an integer") from exc


def get_env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, str(default))
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be a float") from exc


def configure_logging() -> None:
    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


async def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)

    node_id = get_env("NODE_ID", required=True)
    bind_host = get_env("BIND_HOST", default="0.0.0.0")
    bind_port = get_env_int("BIND_PORT", 5001)
    peers_raw = get_env("PEERS", required=True)

    gossip_interval = get_env_float("GOSSIP_INTERVAL", 1.0)
    fanout = get_env_int("FANOUT", 2)

    all_peers = parse_peers(peers_raw)

    if node_id not in all_peers:
        raise ValueError(
            f"NODE_ID '{node_id}' is not present in PEERS. "
            f"Add an entry like {node_id}@<host>:<port>"
        )

    # Optional check between PEERS and explicit bind port
    advertised_host, advertised_port = all_peers[node_id]
    if advertised_port != bind_port:
        logger.warning(
            "BIND_PORT (%s) differs from the port declared in PEERS for %s (%s). "
            "Using BIND_PORT for local bind and PEERS for remote addressing.",
            bind_port,
            node_id,
            advertised_port,
        )

    known_peers = {nid: addr for nid, addr in all_peers.items() if nid != node_id}

    logger.info(
        "[CONFIG] node_id=%s bind=%s:%s gossip_interval=%s fanout=%s peers=%s",
        node_id,
        bind_host,
        bind_port,
        gossip_interval,
        fanout,
        list(known_peers.keys()),
    )

    node = Node(
        node_id=node_id,
        bind_host=bind_host,
        bind_port=bind_port,
        peers=known_peers,
        gossip_interval=gossip_interval,
        fanout=fanout,
    )

    await node.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user")