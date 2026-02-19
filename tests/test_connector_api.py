"""Tests for the connector API endpoints."""

from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def project(tmp_path):
    """Create a minimal test project."""
    (tmp_path / "project.yml").write_text(
        "name: test\n"
        "database:\n"
        "  path: warehouse.duckdb\n"
        "connections: {}\n"
        "streams:\n"
        "  full-refresh:\n"
        "    description: test\n"
        "    steps:\n"
        "      - ingest: [all]\n"
    )
    (tmp_path / "transform" / "bronze").mkdir(parents=True)
    (tmp_path / "ingest").mkdir()
    (tmp_path / ".env").write_text("")

    conn = duckdb.connect(str(tmp_path / "warehouse.duckdb"))
    conn.execute("CREATE SCHEMA IF NOT EXISTS landing")
    conn.close()

    return tmp_path


@pytest.fixture
def client(project):
    import dp.server.app as server_app

    server_app.PROJECT_DIR = project
    server_app.AUTH_ENABLED = False
    return TestClient(server_app.app)


def test_list_available_connectors(client):
    """GET /api/connectors/available should return all connector types."""
    resp = client.get("/api/connectors/available")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    names = {c["name"] for c in data}
    assert "postgres" in names
    assert "csv" in names
    assert "stripe" in names
    assert "webhook" in names
    assert len(names) >= 10


def test_list_configured_empty(client):
    """GET /api/connectors should return empty list for fresh project."""
    resp = client.get("/api/connectors")
    assert resp.status_code == 200
    assert resp.json() == []


def test_test_connector_csv(client, project):
    """POST /api/connectors/test should validate CSV file exists."""
    csv_file = project / "data.csv"
    csv_file.write_text("id,name\n1,Alice\n")

    resp = client.post("/api/connectors/test", json={
        "connector_type": "csv",
        "config": {"path": str(csv_file)},
    })
    assert resp.status_code == 200
    assert resp.json()["success"] is True


def test_test_connector_csv_missing(client):
    """POST /api/connectors/test should fail for missing file."""
    resp = client.post("/api/connectors/test", json={
        "connector_type": "csv",
        "config": {"path": "/nonexistent/file.csv"},
    })
    assert resp.status_code == 200
    assert resp.json()["success"] is False


def test_test_connector_unknown_type(client):
    """POST /api/connectors/test should return 400 for unknown type."""
    resp = client.post("/api/connectors/test", json={
        "connector_type": "nonexistent",
        "config": {},
    })
    assert resp.status_code == 400


def test_discover_connector(client):
    """POST /api/connectors/discover should return resources."""
    resp = client.post("/api/connectors/discover", json={
        "connector_type": "webhook",
        "config": {"table_name": "events"},
    })
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["name"] == "events"


def test_setup_connector(client, project):
    """POST /api/connectors/setup should create a connector."""
    csv_file = project / "test.csv"
    csv_file.write_text("x,y\n1,2\n")

    resp = client.post("/api/connectors/setup", json={
        "connector_type": "csv",
        "connection_name": "test_csv",
        "config": {"path": str(csv_file)},
        "target_schema": "landing",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "success"
    assert data["connection_name"] == "test_csv"

    # Verify script was created
    assert (project / "ingest" / "connector_test_csv.py").exists()

    # Verify it shows up in configured list
    resp = client.get("/api/connectors")
    assert resp.status_code == 200
    names = [c["name"] for c in resp.json()]
    assert "test_csv" in names


def test_remove_connector(client, project):
    """DELETE /api/connectors/{name} should remove a connector."""
    csv_file = project / "rm.csv"
    csv_file.write_text("a\n1\n")

    client.post("/api/connectors/setup", json={
        "connector_type": "csv",
        "connection_name": "removable",
        "config": {"path": str(csv_file)},
    })

    resp = client.delete("/api/connectors/removable")
    assert resp.status_code == 200
    assert resp.json()["status"] == "success"
    assert not (project / "ingest" / "connector_removable.py").exists()


def test_remove_nonexistent(client):
    """DELETE /api/connectors/{name} should return 404."""
    resp = client.delete("/api/connectors/nonexistent")
    assert resp.status_code == 404


def test_webhook_endpoint(client, project):
    """POST /api/webhook/{name} should store data."""
    resp = client.post(
        "/api/webhook/test_events",
        json={"event": "signup", "user_id": 42},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "received"
    assert "test_events_inbox" in data["table"]

    # Verify data was stored
    conn = duckdb.connect(str(project / "warehouse.duckdb"))
    count = conn.execute(
        "SELECT COUNT(*) FROM landing.test_events_inbox"
    ).fetchone()[0]
    assert count == 1
    conn.close()


def test_webhook_rejects_bad_name(client):
    """POST /api/webhook/{name} should reject names with injection attempts."""
    resp = client.post(
        "/api/webhook/; DROP TABLE--",
        json={"event": "test"},
    )
    assert resp.status_code == 400
