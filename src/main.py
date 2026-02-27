import asyncio
import logging
from src.node.node import Node

#
#logging.basicConfig(
#    level=logging.INFO,
#    format="%(asctime)s [%(levelname)s] %(message)s",
#)
#
logging.basicConfig(
    level=logging.DEBUG, 
    format="%(asctime)s [%(levelname)s] %(message)s",
)

async def main():
    node = Node(node_id="node-1")
    await node.start()


if __name__ == "__main__":
    asyncio.run(main())