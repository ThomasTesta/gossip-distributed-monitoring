# gossip-distributed-monitoring
Gossip-based distributed membership and failure detection (course project).

# Gossip-Based Distributed Monitoring System

Course project for Distributed Systems.

## Goal
A fully decentralized monitoring system based on epidemic (gossip) protocols to:
- disseminate cluster membership,
- detect failures (ALIVE → SUSPECT → DEAD),
- analyze convergence and overhead under loss/delay.

## Tech
- Python + asyncio
- UDP (or lightweight sockets)
- Docker & Docker Compose
- FastAPI dashboard

## Status
Work in progress.
