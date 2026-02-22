"""Change Data Capture (CDC) engine for external source connectors.

Tracks high-watermark columns for database sources and last-modified timestamps
for file sources, enabling incremental extraction from external systems.

CDC modes:
- **high_watermark**: Track the max value of a column (e.g. updated_at) and only
  fetch rows where the column exceeds the stored watermark.
- **file_tracking**: Track last-modified times for file sources (CSV, Parquet)
  and only re-ingest when files have changed.
- **full_refresh**: Always fetch everything (no CDC).

State is stored in ``_dp_internal.cdc_state``.

Usage in project.yml::

    connectors:
      prod_users:
        type: postgres
        connection: prod_postgres
        target_schema: landing
        tables:
          - name: users
            cdc_mode: high_watermark
            cdc_column: updated_at
          - name: roles
            cdc_mode: full_refresh
        schedule: "*/30 * * * *"
"""

from __future__ import annotations

import logging
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from dp.engine.database import ensure_meta_table, log_run
from dp.engine.utils import validate_identifier

logger = logging.getLogger("dp.cdc")


@dataclass
class CDCTableConfig:
    """Configuration for a single table's CDC behavior."""

    name: str
    cdc_mode: str = "full_refresh"  # "high_watermark", "file_tracking", "full_refresh"
    cdc_column: str | None = None  # Column to track for high_watermark mode
    source_query: str | None = None  # Custom query (default: SELECT * FROM table)


@dataclass
class CDCConnectorConfig:
    """Configuration for a CDC-enabled connector."""

    name: str
    connection_name: str  # Reference to connections in project.yml
    connector_type: str  # e.g. "postgres", "mysql", "csv"
    target_schema: str = "landing"
    tables: list[CDCTableConfig] = field(default_factory=list)
    schedule: str | None = None


@dataclass
class CDCSyncResult:
    """Result of syncing a single table."""

    table: str
    status: str  # "success", "skipped", "error"
    rows_synced: int = 0
    duration_ms: int = 0
    cdc_mode: str = "full_refresh"
    watermark_before: str | None = None
    watermark_after: str | None = None
    error: str | None = None


def ensure_cdc_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the CDC state tracking table (no-op on read-only connections)."""
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS _dp_internal")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS _dp_internal.cdc_state (
                connector_name  VARCHAR NOT NULL,
                table_name      VARCHAR NOT NULL,
                cdc_mode        VARCHAR NOT NULL DEFAULT 'full_refresh',
                watermark_value VARCHAR,
                file_mtime      DOUBLE,
                last_sync_at    TIMESTAMP DEFAULT current_timestamp,
                rows_synced     BIGINT DEFAULT 0,
                PRIMARY KEY (connector_name, table_name)
            )
        """)
    except Exception:
        pass  # Read-only connection — table may already exist


def get_watermark(
    conn: duckdb.DuckDBPyConnection,
    connector_name: str,
    table_name: str,
) -> str | None:
    """Get the current high-watermark value for a table."""
    ensure_cdc_table(conn)
    row = conn.execute(
        "SELECT watermark_value FROM _dp_internal.cdc_state "
        "WHERE connector_name = ? AND table_name = ?",
        [connector_name, table_name],
    ).fetchone()
    return row[0] if row else None


def update_watermark(
    conn: duckdb.DuckDBPyConnection,
    connector_name: str,
    table_name: str,
    cdc_mode: str,
    watermark_value: str | None = None,
    file_mtime: float | None = None,
    rows_synced: int = 0,
) -> None:
    """Update the CDC state after a successful sync."""
    ensure_cdc_table(conn)
    conn.execute(
        """
        INSERT OR REPLACE INTO _dp_internal.cdc_state
            (connector_name, table_name, cdc_mode, watermark_value, file_mtime, last_sync_at, rows_synced)
        VALUES (?, ?, ?, ?, ?, current_timestamp, ?)
        """,
        [connector_name, table_name, cdc_mode, watermark_value, file_mtime, rows_synced],
    )


def should_sync_file(
    conn: duckdb.DuckDBPyConnection,
    connector_name: str,
    table_name: str,
    file_path: Path,
) -> bool:
    """Check if a file source needs re-syncing based on modification time."""
    ensure_cdc_table(conn)
    if not file_path.exists():
        return False
    current_mtime = file_path.stat().st_mtime
    row = conn.execute(
        "SELECT file_mtime FROM _dp_internal.cdc_state "
        "WHERE connector_name = ? AND table_name = ?",
        [connector_name, table_name],
    ).fetchone()
    if row is None:
        return True  # Never synced
    stored_mtime = row[0]
    return stored_mtime is None or current_mtime > stored_mtime


def sync_table_high_watermark(
    conn: duckdb.DuckDBPyConnection,
    connector_name: str,
    table_config: CDCTableConfig,
    target_schema: str,
    source_conn_str: str,
    source_type: str = "postgres",
) -> CDCSyncResult:
    """Sync a single table using high-watermark CDC.

    Attaches the external DB, queries rows above the stored watermark,
    and appends them to the target table.
    """
    start = time.perf_counter()
    ensure_cdc_table(conn)

    table_name = table_config.name
    cdc_column = table_config.cdc_column
    if not cdc_column:
        return CDCSyncResult(
            table=table_name,
            status="error",
            cdc_mode="high_watermark",
            error="cdc_column is required for high_watermark mode",
        )

    # Validate all identifiers to prevent SQL injection
    try:
        validate_identifier(connector_name, "connector name")
        validate_identifier(table_name, "table name")
        validate_identifier(cdc_column, "cdc_column")
        validate_identifier(target_schema, "target schema")
        validate_identifier(source_type, "source type")
    except ValueError as e:
        return CDCSyncResult(
            table=table_name,
            status="error",
            cdc_mode="high_watermark",
            error=str(e),
        )

    # Get current watermark
    watermark_before = get_watermark(conn, connector_name, table_name)

    try:
        # Attach external database — escape single quotes in connection string
        attach_alias = f"_cdc_src_{connector_name}"
        safe_conn_str = source_conn_str.replace("'", "''")
        conn.execute(
            f"ATTACH '{safe_conn_str}' AS {attach_alias} (TYPE {source_type}, READ_ONLY)"
        )

        try:
            # Build query with watermark filter
            base_query = table_config.source_query or f'SELECT * FROM {attach_alias}."{table_name}"'
            if watermark_before:
                # Use parameterized comparison via a subquery to avoid string interpolation
                # of the watermark value directly into SQL
                safe_wm = watermark_before.replace("'", "''")
                query = f"{base_query} WHERE \"{cdc_column}\" > '{safe_wm}'"
            else:
                query = base_query

            # Create target table if it doesn't exist
            target_table = f'"{target_schema}"."{table_name}"'
            conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{target_schema}"')

            exists = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
                [target_schema, table_name],
            ).fetchone()[0] > 0

            if not exists:
                conn.execute(f"CREATE TABLE {target_table} AS {query}")
            else:
                conn.execute(f"INSERT INTO {target_table} {query}")

            # Get new watermark
            new_watermark_row = conn.execute(
                f'SELECT MAX("{cdc_column}")::VARCHAR FROM {target_table}'
            ).fetchone()
            watermark_after = new_watermark_row[0] if new_watermark_row else watermark_before

            # Count rows synced
            rows_synced = conn.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]

            # Update state
            update_watermark(
                conn, connector_name, table_name,
                "high_watermark", watermark_after, rows_synced=rows_synced,
            )

            duration_ms = int((time.perf_counter() - start) * 1000)
            return CDCSyncResult(
                table=table_name,
                status="success",
                rows_synced=rows_synced,
                duration_ms=duration_ms,
                cdc_mode="high_watermark",
                watermark_before=watermark_before,
                watermark_after=watermark_after,
            )

        finally:
            try:
                conn.execute(f"DETACH {attach_alias}")
            except Exception:
                pass

    except Exception as e:
        duration_ms = int((time.perf_counter() - start) * 1000)
        return CDCSyncResult(
            table=table_name,
            status="error",
            duration_ms=duration_ms,
            cdc_mode="high_watermark",
            error=str(e),
        )


def sync_table_file(
    conn: duckdb.DuckDBPyConnection,
    connector_name: str,
    table_config: CDCTableConfig,
    target_schema: str,
    file_path: Path,
) -> CDCSyncResult:
    """Sync a file source with change tracking based on modification time."""
    start = time.perf_counter()
    ensure_cdc_table(conn)

    table_name = table_config.name

    if not file_path.exists():
        return CDCSyncResult(
            table=table_name,
            status="error",
            cdc_mode="file_tracking",
            error=f"File not found: {file_path}",
        )

    # Check if file has changed
    if not should_sync_file(conn, connector_name, table_name, file_path):
        return CDCSyncResult(
            table=table_name,
            status="skipped",
            cdc_mode="file_tracking",
        )

    # Validate identifiers
    try:
        validate_identifier(target_schema, "target schema")
        validate_identifier(table_name, "table name")
    except ValueError as e:
        return CDCSyncResult(
            table=table_name,
            status="error",
            cdc_mode="file_tracking",
            error=str(e),
        )

    try:
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{target_schema}"')
        target_table = f'"{target_schema}"."{table_name}"'

        # Determine reader based on file extension — escape single quotes in path
        ext = file_path.suffix.lower()
        safe_path = str(file_path).replace("'", "''")
        if ext == ".csv":
            reader = f"read_csv('{safe_path}', auto_detect=true)"
        elif ext in (".parquet", ".pq"):
            reader = f"read_parquet('{safe_path}')"
        elif ext in (".json", ".jsonl", ".ndjson"):
            reader = f"read_json('{safe_path}', auto_detect=true)"
        else:
            return CDCSyncResult(
                table=table_name,
                status="error",
                cdc_mode="file_tracking",
                error=f"Unsupported file type: {ext}",
            )

        # Full replace for file sources
        conn.execute(f"CREATE OR REPLACE TABLE {target_table} AS SELECT * FROM {reader}")
        rows_synced = conn.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]

        # Update CDC state
        update_watermark(
            conn, connector_name, table_name,
            "file_tracking", file_mtime=file_path.stat().st_mtime,
            rows_synced=rows_synced,
        )

        duration_ms = int((time.perf_counter() - start) * 1000)
        return CDCSyncResult(
            table=table_name,
            status="success",
            rows_synced=rows_synced,
            duration_ms=duration_ms,
            cdc_mode="file_tracking",
        )

    except Exception as e:
        duration_ms = int((time.perf_counter() - start) * 1000)
        return CDCSyncResult(
            table=table_name,
            status="error",
            duration_ms=duration_ms,
            cdc_mode="file_tracking",
            error=str(e),
        )


def get_cdc_status(
    conn: duckdb.DuckDBPyConnection,
    connector_name: str | None = None,
) -> list[dict]:
    """Get CDC state for all tracked tables (or a specific connector)."""
    ensure_cdc_table(conn)
    try:
        if connector_name:
            rows = conn.execute(
                "SELECT connector_name, table_name, cdc_mode, watermark_value, "
                "file_mtime, last_sync_at, rows_synced "
                "FROM _dp_internal.cdc_state WHERE connector_name = ? "
                "ORDER BY table_name",
                [connector_name],
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT connector_name, table_name, cdc_mode, watermark_value, "
                "file_mtime, last_sync_at, rows_synced "
                "FROM _dp_internal.cdc_state ORDER BY connector_name, table_name"
            ).fetchall()
    except Exception:
        return []  # Table doesn't exist yet

    return [
        {
            "connector": r[0],
            "table": r[1],
            "cdc_mode": r[2],
            "watermark": r[3],
            "file_mtime": r[4],
            "last_sync_at": str(r[5]) if r[5] else None,
            "rows_synced": r[6],
        }
        for r in rows
    ]


def reset_watermark(
    conn: duckdb.DuckDBPyConnection,
    connector_name: str,
    table_name: str | None = None,
) -> int:
    """Reset CDC state for a connector (or a specific table).

    Returns the number of rows deleted.
    """
    ensure_cdc_table(conn)
    if table_name:
        result = conn.execute(
            "DELETE FROM _dp_internal.cdc_state "
            "WHERE connector_name = ? AND table_name = ?",
            [connector_name, table_name],
        )
    else:
        result = conn.execute(
            "DELETE FROM _dp_internal.cdc_state WHERE connector_name = ?",
            [connector_name],
        )
    return result.fetchone()[0] if result.description else 0


def parse_cdc_config(raw: dict) -> list[CDCConnectorConfig]:
    """Parse the ``connectors:`` section from project.yml.

    Example YAML::

        connectors:
          prod_users:
            type: postgres
            connection: prod_postgres
            target_schema: landing
            tables:
              - name: users
                cdc_mode: high_watermark
                cdc_column: updated_at
              - name: roles
            schedule: "*/30 * * * *"
    """
    configs: list[CDCConnectorConfig] = []
    connectors_raw = raw.get("connectors", {})
    if not isinstance(connectors_raw, dict):
        return configs

    for name, conf in connectors_raw.items():
        tables = []
        for t in conf.get("tables", []):
            if isinstance(t, str):
                tables.append(CDCTableConfig(name=t))
            elif isinstance(t, dict):
                tables.append(CDCTableConfig(
                    name=t.get("name", ""),
                    cdc_mode=t.get("cdc_mode", "full_refresh"),
                    cdc_column=t.get("cdc_column"),
                    source_query=t.get("source_query"),
                ))

        configs.append(CDCConnectorConfig(
            name=name,
            connection_name=conf.get("connection", ""),
            connector_type=conf.get("type", ""),
            target_schema=conf.get("target_schema", "landing"),
            tables=tables,
            schedule=conf.get("schedule"),
        ))

    return configs
