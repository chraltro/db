"""Tests for the FastAPI backend."""

from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def project(tmp_path):
    """Create a minimal test project."""
    (tmp_path / "project.yml").write_text("""
name: test
database:
  path: warehouse.duckdb
streams:
  test-stream:
    description: "Test"
    steps:
      - transform: [all]
""")
    # Create dirs
    (tmp_path / "transform" / "bronze").mkdir(parents=True)
    (tmp_path / "ingest").mkdir()
    (tmp_path / "export").mkdir()

    # Create a model
    (tmp_path / "transform" / "bronze" / "test.sql").write_text(
        "-- config: materialized=view, schema=bronze\n\n"
        "SELECT 1 AS id, 'hello' AS msg\n"
    )

    # Create warehouse with some data
    conn = duckdb.connect(str(tmp_path / "warehouse.duckdb"))
    conn.execute("CREATE SCHEMA IF NOT EXISTS landing")
    conn.execute("CREATE TABLE landing.data AS SELECT 1 AS x")
    conn.close()

    return tmp_path


@pytest.fixture
def client(project):
    import dp.server.app as server_app

    server_app.PROJECT_DIR = project
    return TestClient(server_app.app)


def test_list_files(client):
    resp = client.get("/api/files")
    assert resp.status_code == 200
    data = resp.json()
    names = [f["name"] for f in data]
    assert "transform" in names
    assert "project.yml" in names


def test_list_models(client):
    resp = client.get("/api/models")
    assert resp.status_code == 200
    models = resp.json()
    assert len(models) == 1
    assert models[0]["full_name"] == "bronze.test"


def test_get_dag(client):
    resp = client.get("/api/dag")
    assert resp.status_code == 200
    dag = resp.json()
    assert "nodes" in dag
    assert "edges" in dag
    assert any(n["id"] == "bronze.test" for n in dag["nodes"])


def test_run_query(client):
    resp = client.post("/api/query", json={"sql": "SELECT 42 AS answer"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["columns"] == ["answer"]
    assert data["rows"] == [[42]]


def test_run_query_invalid_sql(client):
    resp = client.post("/api/query", json={"sql": "INVALID SQL FOOBAR"})
    assert resp.status_code == 400


def test_run_query_empty_rejected(client):
    resp = client.post("/api/query", json={"sql": ""})
    assert resp.status_code == 422  # pydantic min_length=1


def test_list_tables(client):
    resp = client.get("/api/tables")
    assert resp.status_code == 200
    tables = resp.json()
    assert any(t["name"] == "data" and t["schema"] == "landing" for t in tables)


def test_list_tables_with_schema_filter(client):
    resp = client.get("/api/tables?schema=landing")
    assert resp.status_code == 200
    tables = resp.json()
    assert all(t["schema"] == "landing" for t in tables)

    resp = client.get("/api/tables?schema=nonexistent")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_streams(client):
    resp = client.get("/api/streams")
    assert resp.status_code == 200
    data = resp.json()
    assert "test-stream" in data


def test_docs_endpoint(client):
    resp = client.get("/api/docs/markdown")
    assert resp.status_code == 200
    data = resp.json()
    assert "markdown" in data
    assert "landing.data" in data["markdown"]


def test_scheduler_endpoint(client):
    resp = client.get("/api/scheduler")
    assert resp.status_code == 200
    data = resp.json()
    assert "scheduled_streams" in data


def test_overview_endpoint(client):
    resp = client.get("/api/overview")
    assert resp.status_code == 200
    data = resp.json()
    # Should have all expected keys
    assert "recent_runs" in data
    assert "schemas" in data
    assert "total_tables" in data
    assert "total_rows" in data
    assert "connectors" in data
    assert "has_data" in data
    assert "streams" in data
    # The test project has a landing.data table
    assert data["has_data"] is True
    assert data["total_tables"] >= 1
    # Should have a landing schema
    schema_names = [s["name"] for s in data["schemas"]]
    assert "landing" in schema_names
    # Should include streams from project.yml
    assert "test-stream" in data["streams"]
