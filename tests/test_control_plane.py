import asyncio

import httpx
import pytest

from control_plane import app as control_module


def _request(method: str, path: str, **kwargs):
    async def run():
        transport = httpx.ASGITransport(app=control_module.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.request(method, path, **kwargs)

    return asyncio.run(run())


@pytest.fixture(autouse=True)
def reset_store():
    control_module.NODES.clear()
    control_module.TRANSFERS.clear()
    yield
    control_module.NODES.clear()
    control_module.TRANSFERS.clear()


def test_health_endpoint():
    response = _request("GET", "/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@pytest.mark.parametrize("role", ["sender", "receiver", "both"])
def test_node_registration(role):
    response = _request(
        "POST", "/nodes/register", json={"node_id": "node-01", "host": "127.0.0.1", "port": 5050, "role": role}
    )
    assert response.status_code == 201
    assert response.json()["role"] == role


def test_node_registration_upserts_existing_id():
    _request("POST", "/nodes/register", json={"node_id": "node-a", "host": "127.0.0.1"})
    response = _request("POST", "/nodes/register", json={"node_id": "node-a", "host": "10.0.0.1"})
    assert response.status_code == 201
    assert response.json()["host"] == "10.0.0.1"


def test_list_nodes():
    _request("POST", "/nodes/register", json={"node_id": "node-a", "host": "127.0.0.1"})
    response = _request("GET", "/nodes")
    assert response.status_code == 200
    assert response.json()["count"] == 1


def _transfer_payload():
    return {
        "transfer_id": "control-test-0001",
        "filename": "data.bin",
        "source_node": "node-a",
        "destination_node": "node-b",
        "size_bytes": 1024,
        "sha256": "a" * 64,
    }


def test_register_transfer_and_list_it():
    response = _request("POST", "/transfers", json=_transfer_payload())
    assert response.status_code == 201
    assert response.json()["status"] == "pending"
    listed = _request("GET", "/transfers")
    assert listed.json()["count"] == 1


def test_duplicate_transfer_id_returns_conflict():
    _request("POST", "/transfers", json=_transfer_payload())
    response = _request("POST", "/transfers", json=_transfer_payload())
    assert response.status_code == 409


def test_get_transfer_by_id():
    _request("POST", "/transfers", json=_transfer_payload())
    response = _request("GET", "/transfers/control-test-0001")
    assert response.status_code == 200
    assert response.json()["filename"] == "data.bin"


def test_update_transfer_status_and_metrics():
    _request("POST", "/transfers", json=_transfer_payload())
    response = _request(
        "PATCH", "/transfers/control-test-0001", json={"status": "completed", "throughput_mbps": 12.5}
    )
    assert response.status_code == 200
    assert response.json()["status"] == "completed"
    assert response.json()["throughput_mbps"] == 12.5


@pytest.mark.parametrize("status", ["pending", "running", "completed", "failed"])
def test_all_supported_transfer_statuses(status):
    _request("POST", "/transfers", json=_transfer_payload())
    response = _request("PATCH", "/transfers/control-test-0001", json={"status": status})
    assert response.status_code == 200
    assert response.json()["status"] == status


def test_missing_transfer_get_returns_404():
    response = _request("GET", "/transfers/missing-transfer")
    assert response.status_code == 404


def test_missing_transfer_patch_returns_404():
    response = _request("PATCH", "/transfers/missing-transfer", json={"status": "failed"})
    assert response.status_code == 404


def test_invalid_transfer_hash_is_rejected():
    payload = _transfer_payload()
    payload["sha256"] = "not-a-hash"
    response = _request("POST", "/transfers", json=payload)
    assert response.status_code == 422


def test_control_plane_main_passes_host_and_port_to_uvicorn(monkeypatch):
    import sys
    import uvicorn

    calls = []
    monkeypatch.setattr(sys, "argv", ["control-plane", "--host", "127.0.0.1", "--port", "8123"])
    monkeypatch.setattr(uvicorn, "run", lambda *args, **kwargs: calls.append((args, kwargs)))
    control_module.main()
    assert calls == [(("control_plane.app:app",), {"host": "127.0.0.1", "port": 8123, "reload": False})]
