"""Ingest cell execution for notebooks."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

import duckdb

from dp.engine.utils import validate_identifier as _validate_identifier

from .formatting import _serialize

logger = logging.getLogger("dp.notebook")


def execute_ingest_cell(
    conn: duckdb.DuckDBPyConnection,
    source: str,
    project_dir: Path | None = None,
) -> dict:
    """Execute a structured ingest cell.

    The cell source is JSON with fields:
        - source_type: "csv", "parquet", "json", "database", "url"
        - source_path: file path, URL, or query
        - target_schema: target schema (default "landing")
        - target_table: target table name
        - connection: optional connection name from project.yml
        - options: optional dict of extra options

    Returns dict with:
        - outputs: list of output items
        - duration_ms: execution time
    """
    outputs: list[dict] = []
    start = time.perf_counter()

    try:
        spec = json.loads(source)
    except json.JSONDecodeError as e:
        outputs.append({"type": "error", "text": f"Invalid ingest cell JSON: {e}"})
        duration_ms = int((time.perf_counter() - start) * 1000)
        return {"outputs": outputs, "duration_ms": duration_ms}

    source_type = spec.get("source_type", "").lower()
    source_path = spec.get("source_path", "")
    target_schema = spec.get("target_schema", "landing")
    target_table = spec.get("target_table", "")
    connection_name = spec.get("connection")
    options = spec.get("options", {})

    # Validate required fields
    if not source_type:
        return _ingest_error("Missing 'source_type' in ingest cell.", start)
    if not target_table:
        return _ingest_error("Missing 'target_table' in ingest cell.", start)

    # Validate identifiers to prevent SQL injection
    try:
        _validate_identifier(target_schema, "target_schema")
        _validate_identifier(target_table, "target_table")
    except ValueError as e:
        return _ingest_error(str(e), start)

    try:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
        full_table = f"{target_schema}.{target_table}"

        if source_type == "csv":
            resolved = _resolve_path(source_path, project_dir)
            conn.execute(
                f"CREATE OR REPLACE TABLE {full_table} AS "
                f"SELECT * FROM read_csv_auto('{resolved}')"
            )
        elif source_type == "parquet":
            resolved = _resolve_path(source_path, project_dir)
            conn.execute(
                f"CREATE OR REPLACE TABLE {full_table} AS "
                f"SELECT * FROM read_parquet('{resolved}')"
            )
        elif source_type == "json":
            resolved = _resolve_path(source_path, project_dir)
            conn.execute(
                f"CREATE OR REPLACE TABLE {full_table} AS "
                f"SELECT * FROM read_json_auto('{resolved}')"
            )
        elif source_type == "url":
            # DuckDB's httpfs extension for remote files
            conn.execute("INSTALL httpfs; LOAD httpfs;")
            if source_path.endswith(".parquet"):
                conn.execute(
                    f"CREATE OR REPLACE TABLE {full_table} AS "
                    f"SELECT * FROM read_parquet('{source_path}')"
                )
            elif source_path.endswith(".json"):
                conn.execute(
                    f"CREATE OR REPLACE TABLE {full_table} AS "
                    f"SELECT * FROM read_json_auto('{source_path}')"
                )
            else:
                conn.execute(
                    f"CREATE OR REPLACE TABLE {full_table} AS "
                    f"SELECT * FROM read_csv_auto('{source_path}')"
                )
        elif source_type == "database":
            # Use connection from project.yml
            if not connection_name:
                return _ingest_error("Database source requires 'connection' name.", start)
            if not project_dir:
                return _ingest_error("Database ingest requires project context.", start)

            from dp.config import load_project
            config = load_project(project_dir)
            conn_config = config.connections.get(connection_name)
            if not conn_config:
                return _ingest_error(f"Connection '{connection_name}' not found in project.yml.", start)
            from dp.engine.importer import import_from_connection
            result = import_from_connection(
                conn, conn_config.type, conn_config.__dict__,
                source_path, target_schema, target_table,
            )
            if result.get("error"):
                return _ingest_error(result["error"], start)
        else:
            return _ingest_error(f"Unsupported source_type: {source_type}", start)

        # Show preview of loaded data
        row_count = conn.execute(f"SELECT COUNT(*) FROM {full_table}").fetchone()[0]
        preview = conn.execute(f"SELECT * FROM {full_table} LIMIT 10")
        columns = [desc[0] for desc in preview.description]
        rows = preview.fetchall()

        outputs.append({
            "type": "text",
            "text": f"Loaded {row_count:,} rows into {full_table}",
        })
        outputs.append({
            "type": "table",
            "columns": columns,
            "rows": [[_serialize(v) for v in row] for row in rows],
            "total_rows": row_count,
        })

        # Log to run history
        from dp.engine.database import ensure_meta_table, log_run
        try:
            ensure_meta_table(conn)
            log_run(conn, "ingest", full_table, "success",
                    int((time.perf_counter() - start) * 1000), row_count)
        except Exception as e:
            logger.debug("SQL cell logging error: %s", e)

    except Exception as e:
        outputs.append({"type": "error", "text": str(e)})

    duration_ms = int((time.perf_counter() - start) * 1000)
    return {"outputs": outputs, "duration_ms": duration_ms}


def _ingest_error(msg: str, start: float) -> dict:
    """Build an ingest cell error response."""
    return {
        "outputs": [{"type": "error", "text": msg}],
        "duration_ms": int((time.perf_counter() - start) * 1000),
    }


def _resolve_path(source_path: str, project_dir: Path | None) -> str:
    """Resolve a relative path against the project directory.

    Validates that the resolved path stays within the project directory
    to prevent path traversal attacks.
    """
    p = Path(source_path)
    if not p.is_absolute() and project_dir:
        p = project_dir / p
    resolved = p.resolve()
    # Prevent path traversal: resolved path must stay within project_dir
    if project_dir and not str(resolved).startswith(str(project_dir.resolve())):
        raise ValueError(f"Path traversal detected: {source_path!r} resolves outside project directory")
    return str(resolved)
