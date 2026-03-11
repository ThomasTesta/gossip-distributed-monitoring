import asyncio
import logging
import os
from src.node.node import Node

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

PEERS = {
    "node-1": ("127.0.0.1", 5001),
    "node-2": ("127.0.0.1", 5002),
    "node-3": ("127.0.0.1", 5003),
}


async def main():
    node_id = os.environ.get("NODE_ID", "node-1")
    host, port = PEERS[node_id]

    node = Node(
        node_id=node_id,
        bind_host=host,
        bind_port=port,
        peers={k: v for k, v in PEERS.items() if k != node_id},
        gossip_interval=1.0,
        fanout=2,
    )
    await node.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user")