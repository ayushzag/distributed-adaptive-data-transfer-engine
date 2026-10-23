"""Small FastAPI control plane for node registration and transfer status tracking.

The data plane (the TCP sender/receiver) does not depend on this service. This
control plane is intentionally lightweight and keeps its demo registry in memory.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Literal

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

app = FastAPI(
    title="Adaptive Transfer Control Plane",
    description="Register transfer nodes and track transfer lifecycle metadata.",
    version="1.0.0",
)

NODES: dict[str, dict] = {}
TRANSFERS: dict[str, dict] = {}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class NodeRegistration(BaseModel):
    node_id: str = Field(min_length=1, max_length=100)
    host: str = Field(min_length=1, max_length=253)
    port: int = Field(default=5050, ge=1, le=65535)
    role: Literal["sender", "receiver", "both"] = "both"
    labels: dict[str, str] = Field(default_factory=dict)


class TransferRegistration(BaseModel):
    transfer_id: str = Field(min_length=8, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")
    filename: str = Field(min_length=1, max_length=255)
    source_node: str = Field(min_length=1, max_length=100)
    destination_node: str = Field(min_length=1, max_length=100)
    size_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[a-fA-F0-9]{64}$")


class TransferUpdate(BaseModel):
    status: Literal["pending", "running", "completed", "failed"]
    throughput_mbps: float | None = Field(default=None, ge=0)
    error: str | None = Field(default=None, max_length=1000)


@app.get("/healthz")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/nodes/register", status_code=status.HTTP_201_CREATED)
def register_node(request: NodeRegistration) -> dict:
    record = request.model_dump()
    record["registered_at"] = utc_now()
    NODES[request.node_id] = record
    return record


@app.get("/nodes")
def list_nodes() -> dict:
    return {"nodes": list(NODES.values()), "count": len(NODES)}


@app.post("/transfers", status_code=status.HTTP_201_CREATED)
def register_transfer(request: TransferRegistration) -> dict:
    if request.transfer_id in TRANSFERS:
        raise HTTPException(status_code=409, detail="transfer_id already exists")
    record = request.model_dump()
    record.update({"status": "pending", "throughput_mbps": None, "error": None, "created_at": utc_now(), "updated_at": utc_now()})
    TRANSFERS[request.transfer_id] = record
    return record


@app.get("/transfers")
def list_transfers() -> dict:
    return {"transfers": list(TRANSFERS.values()), "count": len(TRANSFERS)}


@app.get("/transfers/{transfer_id}")
def get_transfer(transfer_id: str) -> dict:
    record = TRANSFERS.get(transfer_id)
    if record is None:
        raise HTTPException(status_code=404, detail="transfer not found")
    return record


@app.patch("/transfers/{transfer_id}")
def update_transfer(transfer_id: str, request: TransferUpdate) -> dict:
    record = TRANSFERS.get(transfer_id)
    if record is None:
        raise HTTPException(status_code=404, detail="transfer not found")
    record.update(request.model_dump())
    record["updated_at"] = utc_now()
    return record


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the transfer control-plane API")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    import uvicorn

    uvicorn.run("control_plane.app:app", host=args.host, port=args.port, reload=False)


if __name__ == "__main__":
    main()
