"""Connector management, data import, file upload, and CDC endpoints."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from dp.server.deps import (
    DbConn,
    DbConnReadOnly,
    _get_project_dir,
    _require_permission,
    _validate_identifier,
)

logger = logging.getLogger("dp.server")

router = APIRouter()


# --- Pydantic models ---


class ImportFileRequest(BaseModel):
    file_path: str = Field(..., min_length=1, max_length=1000)
    target_schema: str = Field(default="landing", min_length=1, max_length=100)
    target_table: str | None = Field(default=None, max_length=100)


class TestConnectionRequest(BaseModel):
    connection_type: str = Field(..., min_length=1, max_length=50)
    params: dict


class ImportFromConnectionRequest(BaseModel):
    connection_type: str = Field(..., min_length=1, max_length=50)
    params: dict
    source_table: str = Field(..., min_length=1, max_length=500)
    target_schema: str = Field(default="landing", min_length=1, max_length=100)
    target_table: str | None = Field(default=None, max_length=100)


class ConnectorSetupRequest(BaseModel):
    connector_type: str = Field(..., min_length=1, max_length=50)
    connection_name: str = Field(
        ..., min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9_-]+$"
    )
    config: dict
    tables: list[str] | None = None
    target_schema: str = Field(default="landing", min_length=1, max_length=100)
    schedule: str | None = None


class ConnectorTestRequest(BaseModel):
    connector_type: str = Field(..., min_length=1, max_length=50)
    config: dict


class ConnectorDiscoverRequest(BaseModel):
    connector_type: str = Field(..., min_length=1, max_length=50)
    config: dict


# --- Import endpoints ---


@router.post("/api/import/preview-file")
def preview_file_endpoint(request: Request, req: ImportFileRequest) -> dict:
    """Preview data from a file before importing."""
    _require_permission(request, "execute")
    from dp.engine.importer import preview_file

    try:
        return preview_file(req.file_path)
    except Exception as e:
        raise HTTPException(400, str(e))


@router.post("/api/import/file")
def import_file_endpoint(
    request: Request, req: ImportFileRequest, conn: DbConn
) -> dict:
    """Import a file into the warehouse."""
    _require_permission(request, "execute")
    from dp.engine.importer import import_file

    return import_file(conn, req.file_path, req.target_schema, req.target_table)


@router.post("/api/import/test-connection")
def test_connection_endpoint(
    request: Request, req: TestConnectionRequest
) -> dict:
    """Test a database connection."""
    _require_permission(request, "execute")
    from dp.engine.importer import test_connection

    return test_connection(req.connection_type, req.params)


@router.post("/api/import/from-connection")
def import_from_connection_endpoint(
    request: Request, req: ImportFromConnectionRequest, conn: DbConn
) -> dict:
    """Import from an external database."""
    _require_permission(request, "execute")
    from dp.engine.importer import import_from_connection

    return import_from_connection(
        conn,
        req.connection_type,
        req.params,
        req.source_table,
        req.target_schema,
        req.target_table,
    )


# --- Upload ---


@router.post("/api/upload")
async def upload_file(request: Request) -> dict:
    """Upload a file for data import."""
    _require_permission(request, "execute")

    form = await request.form()
    file = form.get("file")
    if not file:
        raise HTTPException(400, "No file uploaded")

    data_dir = _get_project_dir() / "data"
    data_dir.mkdir(exist_ok=True)
    safe_name = Path(file.filename).name
    if not safe_name or safe_name.startswith("."):
        raise HTTPException(400, "Invalid filename")
    file_path = data_dir / safe_name
    if not file_path.resolve().is_relative_to(data_dir.resolve()):
        raise HTTPException(400, "Invalid filename")

    content = await file.read()
    file_path.write_bytes(content)

    return {"path": str(file_path), "name": safe_name, "size": len(content)}


# --- Connector endpoints ---


@router.get("/api/connectors/available")
def list_available_connectors(request: Request) -> list[dict]:
    """List all available connector types."""
    _require_permission(request, "read")
    import dp.connectors  # noqa: F401
    from dp.engine.connector import list_connectors

    return list_connectors()


@router.get("/api/connectors")
def list_configured_connectors_endpoint(request: Request) -> list[dict]:
    """List connectors configured in this project."""
    _require_permission(request, "read")
    import dp.connectors  # noqa: F401
    from dp.engine.connector import list_configured_connectors

    return list_configured_connectors(_get_project_dir())


@router.post("/api/connectors/test")
def test_connector_endpoint(
    request: Request, req: ConnectorTestRequest
) -> dict:
    """Test a connector without setting it up."""
    _require_permission(request, "execute")
    import dp.connectors  # noqa: F401
    from dp.engine.connector import test_connector

    try:
        return test_connector(req.connector_type, req.config)
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/api/connectors/discover")
def discover_connector_endpoint(
    request: Request, req: ConnectorDiscoverRequest
) -> list[dict]:
    """Discover available resources for a connector."""
    _require_permission(request, "execute")
    import dp.connectors  # noqa: F401
    from dp.engine.connector import discover_connector

    try:
        return discover_connector(req.connector_type, req.config)
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/api/connectors/setup")
def setup_connector_endpoint(
    request: Request, req: ConnectorSetupRequest
) -> dict:
    """Set up a new connector: test, generate script, update config."""
    _require_permission(request, "execute")
    import dp.connectors  # noqa: F401
    from dp.engine.connector import setup_connector

    try:
        result = setup_connector(
            project_dir=_get_project_dir(),
            connector_type=req.connector_type,
            connection_name=req.connection_name,
            config=req.config,
            tables=req.tables,
            target_schema=req.target_schema,
            schedule=req.schedule,
        )
        if result["status"] == "error":
            raise HTTPException(400, result.get("error", "Setup failed"))
        return result
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/api/connectors/regenerate/{connection_name}")
def regenerate_connector_endpoint(
    request: Request, connection_name: str, body: dict = {}
) -> dict:
    """Regenerate the ingest script for an existing connector."""
    _require_permission(request, "execute")
    _validate_identifier(connection_name, "connection name")
    import dp.connectors  # noqa: F401
    from dp.engine.connector import regenerate_connector

    result = regenerate_connector(_get_project_dir(), connection_name, body or None)
    if result["status"] == "error":
        raise HTTPException(400, result.get("error", "Regeneration failed"))
    return result


@router.post("/api/connectors/sync/{connection_name}")
def sync_connector_endpoint(request: Request, connection_name: str) -> dict:
    """Run sync for a configured connector."""
    _require_permission(request, "execute")
    _validate_identifier(connection_name, "connection name")
    import dp.connectors  # noqa: F401
    from dp.engine.connector import sync_connector

    result = sync_connector(_get_project_dir(), connection_name)
    if result.get("status") == "error":
        raise HTTPException(400, result.get("error", "Sync failed"))
    return result


@router.delete("/api/connectors/{connection_name}")
def remove_connector_endpoint(request: Request, connection_name: str) -> dict:
    """Remove a configured connector."""
    _require_permission(request, "write")
    _validate_identifier(connection_name, "connection name")
    import dp.connectors  # noqa: F401
    from dp.engine.connector import remove_connector

    result = remove_connector(_get_project_dir(), connection_name)
    if result["status"] == "error":
        raise HTTPException(404, result.get("error", "Not found"))
    return result


@router.get("/api/connectors/health")
def connector_health_endpoint(
    request: Request, conn: DbConnReadOnly
) -> list:
    """Get last sync status for each connector from run_log."""
    _require_permission(request, "read")
    try:
        rows = conn.execute(
            """
            SELECT target, status, started_at, duration_ms, error
            FROM _dp_internal.run_log
            WHERE run_type = 'script' AND target LIKE 'ingest/%'
            ORDER BY started_at DESC
            """
        ).fetchall()
    except Exception:
        return []

    seen: dict[str, dict] = {}
    for target, status, started_at, duration_ms, error in rows:
        if target not in seen:
            seen[target] = {
                "target": target,
                "status": status,
                "started_at": str(started_at) if started_at else None,
                "duration_ms": duration_ms,
                "error": error,
            }
    return list(seen.values())


# --- Webhook receive ---


@router.post("/api/webhook/{webhook_name}")
async def receive_webhook(
    request: Request, webhook_name: str, conn: DbConn
) -> dict:
    """Receive webhook data and store it in the inbox table."""
    _validate_identifier(webhook_name, "webhook name")

    body = await request.body()
    try:
        payload = json.loads(body)
    except Exception:
        raise HTTPException(400, "Invalid JSON payload")

    table = f"landing.{webhook_name}_inbox"
    conn.execute("CREATE SCHEMA IF NOT EXISTS landing")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id VARCHAR DEFAULT gen_random_uuid()::VARCHAR,
            received_at TIMESTAMP DEFAULT current_timestamp,
            payload JSON
        )
    """
    )
    conn.execute(
        f"INSERT INTO {table} (payload) VALUES (?::JSON)",
        [json.dumps(payload)],
    )
    return {"status": "received", "table": table}


# --- CDC Status ---


@router.get("/api/cdc")
def get_cdc_status_endpoint(
    request: Request, conn: DbConnReadOnly
) -> list[dict]:
    """Get CDC state for all tracked connectors."""
    _require_permission(request, "read")
    from dp.engine.cdc import get_cdc_status

    return get_cdc_status(conn)


@router.get("/api/cdc/{connector_name}")
def get_cdc_connector_status(
    request: Request, connector_name: str, conn: DbConnReadOnly
) -> list[dict]:
    """Get CDC state for a specific connector."""
    _require_permission(request, "read")
    from dp.engine.cdc import get_cdc_status

    return get_cdc_status(conn, connector_name)


@router.post("/api/cdc/{connector_name}/reset")
def reset_cdc_state(
    request: Request, connector_name: str, conn: DbConn
) -> dict:
    """Reset CDC watermarks for a connector."""
    _require_permission(request, "write")
    from dp.engine.cdc import reset_watermark

    reset_watermark(conn, connector_name)
    return {"status": "reset", "connector": connector_name}
