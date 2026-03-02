import asyncio
import logging
import os
from src.node.node import Node

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# simple static config for now
PEERS = {
    "node-1": ("127.0.0.1", 5001),
    "node-2": ("127.0.0.1", 5002),
}


async def main():
    node_id = os.environ.get("NODE_ID", "node-1")
    host, port = PEERS[node_id]

    # each node knows the others
    node = Node(
        node_id=node_id,
        bind_host=host,
        bind_port=port,
        peers={k: v for k, v in PEERS.items() if k != node_id},
        gossip_interval=1.0,
        fanout=1,
    )
    await node.start()


if __name__ == "__main__":
    asyncio.run(main())