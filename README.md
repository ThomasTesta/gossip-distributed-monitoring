# Gossip-Based Distributed Monitoring System

A fully decentralized monitoring system implementing epidemic (gossip-based)
membership dissemination and failure detection.

Developed as a Distributed Systems course project.

---

## Overview

This project implements a distributed monitoring service composed of multiple
independent nodes communicating through gossip protocols over UDP.

Each node maintains a partial local view of the cluster and periodically
exchanges membership information with randomly selected peers. Through repeated
epidemic dissemination rounds, nodes progressively converge toward a globally
consistent membership view without relying on any centralized coordinator.

The system supports:

- decentralized membership dissemination;
- heartbeat-based failure detection;
- probabilistic gossip communication;
- configurable network impairments;
- eventual consistency;
- experimental dissemination analysis through the Gossip Arena module.

---

## Main Features

- Gossip-based membership dissemination
- Failure detection (`ALIVE → SUSPECT → DEAD`)
- UDP asynchronous communication
- Configurable gossip fanout
- Packet loss and artificial delay simulation
- Dockerized multi-node deployment
- Experimental validation environment
- Rumor dissemination experiments (Gossip Arena)

---

## Architecture

Each node executes three main activities:

1. Incrementing its local heartbeat
2. Disseminating membership updates through gossip rounds
3. Detecting failures through timeout-based state transitions

Nodes communicate exclusively through UDP message passing and maintain all state
in-memory.

The architecture is fully decentralized:
- no leader,
- no master node,
- no centralized registry.

---

## Technologies

- Python 3
- asyncio
- UDP sockets
- Docker
- Docker Compose
- FastAPI (optional dashboard / arena APIs)

---

## Project Structure

```text
src/
├── config/        # Configuration loading
├── dashboard/     # FastAPI APIs and dashboard
├── network/       # UDP transport layer
├── node/          # Membership and node logic
├── protocol/      # Gossip protocol messages
└── main.py        # Application entrypoint

tests/
├── test_failure_detector.py
└── test_membership.py
````

---

## Running Locally

### Start node 1

```bash
NODE_ID=node-1 \
BIND_PORT=5001 \
FANOUT=2 \
PEERS=node-1@127.0.0.1:5001,node-2@127.0.0.1:5002,node-3@127.0.0.1:5003 \
python3 -m src.main
```

### Start node 2

```bash
NODE_ID=node-2 \
BIND_PORT=5002 \
FANOUT=2 \
PEERS=node-1@127.0.0.1:5001,node-2@127.0.0.1:5002,node-3@127.0.0.1:5003 \
python3 -m src.main
```

### Start node 3

```bash
NODE_ID=node-3 \
BIND_PORT=5003 \
FANOUT=2 \
PEERS=node-1@127.0.0.1:5001,node-2@127.0.0.1:5002,node-3@127.0.0.1:5003 \
python3 -m src.main
```

---

## Docker Deployment

Build and start the cluster:

```bash
docker compose up --build
```

Run a larger cluster:

```bash
docker compose up --build --scale node=5
```

---

## Failure Detection

Nodes periodically exchange heartbeat information.

If a node stops responding:

```text
ALIVE -> SUSPECT -> DEAD
```

State transitions are controlled through configurable timeout thresholds.

---

## Network Impairment Simulation

The transport layer supports configurable network impairments:

```bash
NET_DROP_RATE=0.3
NET_DELAY_MIN_MS=100
NET_DELAY_MAX_MS=500
```

This enables experimental evaluation under unreliable network conditions.

---

## Example Logs

```text
[METRICS] node=node-1 sent=118 recv=110 suspect=0 dead=0

[MEMBERSHIP]
node-1 hb=59 status=ALIVE
node-2 hb=57 status=ALIVE
node-3 hb=53 status=ALIVE
```

---

## Experimental Goals

The project investigates:

* epidemic dissemination dynamics;
* scalability tradeoffs;
* convergence speed;
* communication overhead;
* robustness under packet loss;
* decentralized failure detection.

---

## Related Distributed Systems Concepts

* Gossip Protocols
* Epidemic Dissemination
* Eventual Consistency
* Failure Detection
* Decentralized Coordination
* CAP Theorem
* SWIM-inspired Membership

---

## Author

Thomas Testa


Distributed Systems Project — University of Bologna, Cesena's Campus

---

Experimental Branches
The main branch contains the stable implementation used for the final report and experimental validation.
An additional experimental branch (feature/push-pull-gossip) extends the system with push-pull epidemic dissemination and bidirectional anti-entropy synchronization mechanisms for further experimentation and research-oriented evaluation.


## REST API

Quando il nodo espone l'API integrata (FastAPI + Uvicorn), sono disponibili i seguenti endpoint utili per monitorare e interagire con il nodo:

- **GET** `/membership` — restituisce la membership conosciuta dal nodo.
- **GET** `/metrics` — restituisce le metriche locali del nodo (gossip inviati/ricevuti, eventi suspect/dead, ecc.).
- **GET** `/rumors` — restituisce i rumors presenti nell'arena del nodo.
- **POST** `/arena/inject` — inietta un rumor nella Gossip Arena; body JSON: `{ "rumor_id": "<id>" }`.

Esempi `curl`:

```bash
# Legge la membership (esempio node-1 su porta 8001)
curl http://localhost:8001/membership

# Legge le metriche
curl http://localhost:8001/metrics

# Legge i rumors
curl http://localhost:8001/rumors

# Inietta un rumor
curl -X POST http://localhost:8001/arena/inject \
	-H "Content-Type: application/json" \
	-d '{"rumor_id":"r42"}'
```

Nota: le porte API sono configurate tramite la variabile d'ambiente `API_PORT` (default `8000`). Nel `docker-compose.yml` i servizi `node-1`, `node-2` e `node-3` espongono rispettivamente le porte `8001`, `8002`, `8003`.

