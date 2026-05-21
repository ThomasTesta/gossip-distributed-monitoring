import time
from fastapi import FastAPI
from pydantic import BaseModel


class RumorRequest(BaseModel):
    rumor_id: str


def create_app(node) -> FastAPI:
    app = FastAPI(title=f"Gossip Monitor API - {node.node_id}")

    @app.get("/membership")
    async def membership():
        return {
            "node": node.node_id,
            "members": {
                node_id: {
                    "heartbeat": m.heartbeat,
                    "incarnation": m.incarnation,
                    "status": m.status.value,
                    "last_seen": m.last_seen,
                }
                for node_id, m in node.membership.members.items()
            },
        }

    @app.get("/metrics")
    async def metrics():
        return {
            "node": node.node_id,
            "metrics": node.metrics,
        }

    @app.get("/rumors")
    async def rumors():
        return {
            "node": node.node_id,
            "rumors": node.rumors,
        }

    @app.post("/arena/inject")
    async def inject_rumor(req: RumorRequest):
        rumor = {
            "rumor_id": req.rumor_id,
            "origin": node.node_id,
            "created_at": time.time(),
            "hop_count": 0,
        }

        node.rumors[req.rumor_id] = rumor
        node.pending_rumors[req.rumor_id] = node.rumor_repeat
        node.metrics["rumors_generated"] += 1

        return {
            "status": "injected",
            "node": node.node_id,
            "rumor": rumor,
        }

    return app