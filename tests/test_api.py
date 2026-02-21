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


# --- Tests for warehouse existence handling ---


@pytest.fixture
def no_warehouse_project(tmp_path):
    """Create a project with no warehouse database."""
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
    (tmp_path / "transform" / "bronze").mkdir(parents=True)
    (tmp_path / "ingest").mkdir()
    (tmp_path / "export").mkdir()
    return tmp_path


@pytest.fixture
def no_db_client(no_warehouse_project):
    import dp.server.app as server_app
    server_app.PROJECT_DIR = no_warehouse_project
    return TestClient(server_app.app)


def test_query_no_warehouse(no_db_client):
    resp = no_db_client.post("/api/query", json={"sql": "SELECT 1"})
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()


def test_describe_table_no_warehouse(no_db_client):
    resp = no_db_client.get("/api/tables/landing/data")
    assert resp.status_code == 404


def test_sample_table_no_warehouse(no_db_client):
    resp = no_db_client.get("/api/tables/landing/data/sample")
    assert resp.status_code == 404


def test_profile_table_no_warehouse(no_db_client):
    resp = no_db_client.get("/api/tables/landing/data/profile")
    assert resp.status_code == 404


# --- Tests for upload path safety ---


def test_upload_rejects_path_traversal(client):
    import io
    # Simulate a file upload with path traversal name
    resp = client.post(
        "/api/upload",
        files={"file": ("../../../etc/passwd", io.BytesIO(b"evil"), "text/plain")},
    )
    # Should either reject the name or strip the path components
    if resp.status_code == 200:
        data = resp.json()
        assert ".." not in data["name"]
        assert "/" not in data["name"]
    else:
        assert resp.status_code == 400


def test_upload_rejects_dotfile(client):
    import io
    resp = client.post(
        "/api/upload",
        files={"file": (".env", io.BytesIO(b"SECRET=x"), "text/plain")},
    )
    assert resp.status_code == 400


# --- Notebook API endpoint tests ---


def test_run_sql_cell_endpoint(client):
    """Run a SQL cell via the API."""
    resp = client.post(
        "/api/notebooks/run-cell/test_nb",
        json={"source": "SELECT 42 AS answer", "cell_type": "sql"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["outputs"]) == 1
    assert data["outputs"][0]["type"] == "table"
    assert data["outputs"][0]["rows"] == [[42]]
    assert "duration_ms" in data


def test_run_code_cell_endpoint(client):
    """Run a Python code cell via the API."""
    resp = client.post(
        "/api/notebooks/run-cell/test_nb",
        json={"source": "1 + 1", "cell_type": "code"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["outputs"]) == 1
    assert "2" in data["outputs"][0]["text"]


def test_run_cell_namespace_persistence(client):
    """Variables persist across code cells in the same notebook."""
    # Set a variable
    resp1 = client.post(
        "/api/notebooks/run-cell/ns_test",
        json={"source": "x = 42", "cell_type": "code", "reset": True},
    )
    assert resp1.status_code == 200

    # Read it back
    resp2 = client.post(
        "/api/notebooks/run-cell/ns_test",
        json={"source": "x", "cell_type": "code"},
    )
    assert resp2.status_code == 200
    assert "42" in resp2.json()["outputs"][0]["text"]


def test_run_cell_reset_namespace(client):
    """Reset flag clears the namespace."""
    # Set a variable
    client.post(
        "/api/notebooks/run-cell/reset_test",
        json={"source": "y = 99", "cell_type": "code"},
    )

    # Reset and try to read
    resp = client.post(
        "/api/notebooks/run-cell/reset_test",
        json={"source": "y", "cell_type": "code", "reset": True},
    )
    assert resp.status_code == 200
    assert any(o["type"] == "error" for o in resp.json()["outputs"])


def test_run_sql_cell_error_endpoint(client):
    """SQL cell errors are returned, not raised as HTTP errors."""
    resp = client.post(
        "/api/notebooks/run-cell/test_nb",
        json={"source": "SELECT * FROM nonexistent_xyzzy", "cell_type": "sql"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert any(o["type"] == "error" for o in data["outputs"])


def test_promote_to_model_endpoint(client, project):
    """Promote SQL to model via the API."""
    resp = client.post(
        "/api/notebooks/promote-to-model",
        json={
            "sql_source": "SELECT * FROM landing.data",
            "model_name": "clean_data",
            "target_schema": "bronze",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "created"
    assert data["full_name"] == "bronze.clean_data"
    assert "path" in data

    # Verify the file was created
    model_file = project / data["path"]
    assert model_file.exists()
    content = model_file.read_text()
    assert "SELECT * FROM landing.data" in content
    assert "-- config:" in content


def test_promote_to_model_conflict(client, project):
    """Promote returns 409 when model already exists."""
    # Create first
    client.post(
        "/api/notebooks/promote-to-model",
        json={
            "sql_source": "SELECT 1",
            "model_name": "conflict_model",
            "target_schema": "bronze",
        },
    )
    # Try again — should get 409
    resp = client.post(
        "/api/notebooks/promote-to-model",
        json={
            "sql_source": "SELECT 2",
            "model_name": "conflict_model",
            "target_schema": "bronze",
        },
    )
    assert resp.status_code == 409

    # With overwrite — should succeed
    resp = client.post(
        "/api/notebooks/promote-to-model",
        json={
            "sql_source": "SELECT 2",
            "model_name": "conflict_model",
            "target_schema": "bronze",
            "overwrite": True,
        },
    )
    assert resp.status_code == 200


def test_promote_validates_identifiers(client):
    """Promote rejects invalid model names and schemas."""
    resp = client.post(
        "/api/notebooks/promote-to-model",
        json={
            "sql_source": "SELECT 1",
            "model_name": "DROP TABLE users--",
            "target_schema": "bronze",
        },
    )
    assert resp.status_code == 422  # Pydantic pattern validation


def test_model_to_notebook_endpoint(client, project):
    """Create notebook from model via the API."""
    resp = client.post("/api/notebooks/model-to-notebook/bronze.test")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "created"
    assert "notebook" in data
    assert data["notebook"]["title"] == "Debug: bronze.test"


def test_model_to_notebook_not_found(client):
    """Model-to-notebook returns 404 for nonexistent model."""
    resp = client.post("/api/notebooks/model-to-notebook/nonexistent.model")
    assert resp.status_code == 404


def test_debug_notebook_endpoint(client, project):
    """Generate debug notebook via the API."""
    resp = client.post("/api/notebooks/debug/bronze.test")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "created"
    assert "notebook" in data
    assert "Debug" in data["notebook"]["title"]


def test_debug_notebook_not_found(client):
    """Debug notebook returns 404 for nonexistent model."""
    resp = client.post("/api/notebooks/debug/nonexistent.model")
    assert resp.status_code == 404


def test_list_notebooks(client, project):
    """List notebooks endpoint."""
    # Create a notebook
    (project / "notebooks").mkdir(exist_ok=True)
    import json
    nb = {"title": "Test NB", "cells": []}
    (project / "notebooks" / "test.dpnb").write_text(json.dumps(nb))

    resp = client.get("/api/notebooks")
    assert resp.status_code == 200
    data = resp.json()
    assert any(n["name"] == "test" for n in data)


# --- Notebook path traversal tests ---


def test_resolve_notebook_rejects_path_traversal(project):
    """_resolve_notebook rejects paths that escape the project directory."""
    from fastapi import HTTPException
    import dp.server.app as server_app

    with pytest.raises(HTTPException) as exc_info:
        server_app._resolve_notebook(project, "../../../etc/passwd")
    assert exc_info.value.status_code == 400

    with pytest.raises(HTTPException) as exc_info:
        server_app._resolve_notebook(project, "notebooks/../../../etc/passwd.dpnb")
    assert exc_info.value.status_code == 400


def test_run_cell_ingest_rejects_injection(client):
    """Ingest cell via API rejects SQL injection in identifiers."""
    import json as _json
    resp = client.post(
        "/api/notebooks/run-cell/test_nb",
        json={
            "source": _json.dumps({
                "source_type": "csv",
                "source_path": "/data/test.csv",
                "target_schema": "landing; DROP TABLE--",
                "target_table": "data",
            }),
            "cell_type": "ingest",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert any(o["type"] == "error" for o in data["outputs"])
    assert any("Invalid" in o.get("text", "") for o in data["outputs"])
