"""Data import wizard engine.

Supports importing data from:
- CSV/Parquet/JSON files
- PostgreSQL, MySQL, SQLite connections
- HTTP/API endpoints

Workflow: test connection -> preview data -> land into table.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from dp.engine.database import connect, ensure_meta_table, log_run


def preview_file(file_path: str, limit: int = 100) -> dict:
    """Preview data from a local file (CSV, Parquet, JSON)."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    conn = duckdb.connect(":memory:")
    try:
        ext = path.suffix.lower()
        if ext == ".csv":
            query = f"SELECT * FROM read_csv('{file_path}', auto_detect=true) LIMIT {limit}"
        elif ext in (".parquet", ".pq"):
            query = f"SELECT * FROM read_parquet('{file_path}') LIMIT {limit}"
        elif ext in (".json", ".jsonl", ".ndjson"):
            query = f"SELECT * FROM read_json('{file_path}', auto_detect=true) LIMIT {limit}"
        elif ext in (".xlsx", ".xls"):
            query = f"SELECT * FROM st_read('{file_path}') LIMIT {limit}"
        else:
            raise ValueError(f"Unsupported file type: {ext}")

        result = conn.execute(query)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()

        # Get column types
        col_types = []
        type_result = conn.execute(
            f"SELECT column_name, column_type FROM (DESCRIBE {query.replace(f' LIMIT {limit}', ' LIMIT 1')})"
        )
        for row in type_result.fetchall():
            col_types.append({"name": row[0], "type": row[1]})

        return {
            "columns": columns,
            "column_types": col_types,
            "rows": [[_serialize(v) for v in row] for row in rows],
            "total_preview": len(rows),
            "source": file_path,
            "source_type": ext.lstrip("."),
        }
    finally:
        conn.close()


def preview_query(connection_string: str, query: str, limit: int = 100) -> dict:
    """Preview data from an external database using DuckDB's extension system."""
    conn = duckdb.connect(":memory:")
    try:
        # Install and load extensions as needed
        if "postgres" in connection_string.lower() or "postgresql" in connection_string.lower():
            conn.execute("INSTALL postgres; LOAD postgres;")
            conn.execute(f"ATTACH '{connection_string}' AS ext_db (TYPE POSTGRES, READ_ONLY)")
        elif "mysql" in connection_string.lower():
            conn.execute("INSTALL mysql; LOAD mysql;")
            conn.execute(f"ATTACH '{connection_string}' AS ext_db (TYPE MYSQL, READ_ONLY)")
        elif "sqlite" in connection_string.lower() or connection_string.endswith(".db"):
            conn.execute("INSTALL sqlite; LOAD sqlite;")
            conn.execute(f"ATTACH '{connection_string}' AS ext_db (TYPE SQLITE, READ_ONLY)")

        limited_query = f"SELECT * FROM ({query}) sub LIMIT {limit}"
        result = conn.execute(limited_query)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()

        return {
            "columns": columns,
            "rows": [[_serialize(v) for v in row] for row in rows],
            "total_preview": len(rows),
            "source": "external_query",
            "source_type": "database",
        }
    finally:
        conn.close()


def test_connection(connection_type: str, params: dict) -> dict:
    """Test a database connection and return available tables."""
    conn = duckdb.connect(":memory:")
    try:
        conn_string = _build_connection_string(connection_type, params)

        if connection_type == "postgres":
            conn.execute("INSTALL postgres; LOAD postgres;")
            conn.execute(f"ATTACH '{conn_string}' AS ext_db (TYPE POSTGRES, READ_ONLY)")
        elif connection_type == "mysql":
            conn.execute("INSTALL mysql; LOAD mysql;")
            conn.execute(f"ATTACH '{conn_string}' AS ext_db (TYPE MYSQL, READ_ONLY)")
        elif connection_type == "sqlite":
            conn.execute("INSTALL sqlite; LOAD sqlite;")
            conn.execute(f"ATTACH '{conn_string}' AS ext_db (TYPE SQLITE, READ_ONLY)")
        else:
            return {"success": False, "error": f"Unsupported connection type: {connection_type}"}

        # List tables
        tables = conn.execute(
            "SELECT table_schema, table_name FROM information_schema.tables WHERE table_catalog = 'ext_db'"
        ).fetchall()

        return {
            "success": True,
            "tables": [{"schema": t[0], "name": t[1]} for t in tables],
        }
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        conn.close()


def import_file(
    db_conn: duckdb.DuckDBPyConnection,
    file_path: str,
    target_schema: str = "landing",
    target_table: str | None = None,
) -> dict:
    """Import a file into the warehouse as a landing table."""
    import time

    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    table_name = target_table or path.stem.replace("-", "_").replace(" ", "_")
    full_name = f"{target_schema}.{table_name}"

    ensure_meta_table(db_conn)
    db_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")

    ext = path.suffix.lower()
    start = time.perf_counter()

    try:
        if ext == ".csv":
            db_conn.execute(
                f"CREATE OR REPLACE TABLE {full_name} AS "
                f"SELECT * FROM read_csv('{file_path}', auto_detect=true)"
            )
        elif ext in (".parquet", ".pq"):
            db_conn.execute(
                f"CREATE OR REPLACE TABLE {full_name} AS "
                f"SELECT * FROM read_parquet('{file_path}')"
            )
        elif ext in (".json", ".jsonl", ".ndjson"):
            db_conn.execute(
                f"CREATE OR REPLACE TABLE {full_name} AS "
                f"SELECT * FROM read_json('{file_path}', auto_detect=true)"
            )
        else:
            raise ValueError(f"Unsupported file type: {ext}")

        row_count = db_conn.execute(f"SELECT COUNT(*) FROM {full_name}").fetchone()[0]
        duration_ms = int((time.perf_counter() - start) * 1000)

        log_run(db_conn, "import", full_name, "success", duration_ms, rows_affected=row_count, log_output=path.name)

        return {
            "status": "success",
            "table": full_name,
            "rows": row_count,
            "duration_ms": duration_ms,
        }
    except Exception as e:
        duration_ms = int((time.perf_counter() - start) * 1000)
        log_run(db_conn, "import", full_name, "error", duration_ms, error=str(e), log_output=path.name)
        return {"status": "error", "table": full_name, "error": str(e)}


def import_from_connection(
    db_conn: duckdb.DuckDBPyConnection,
    connection_type: str,
    params: dict,
    source_table: str,
    target_schema: str = "landing",
    target_table: str | None = None,
) -> dict:
    """Import a table from an external database into the warehouse."""
    import time

    table_name = target_table or source_table.split(".")[-1]
    full_name = f"{target_schema}.{table_name}"

    ensure_meta_table(db_conn)
    db_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")

    conn_string = _build_connection_string(connection_type, params)
    start = time.perf_counter()

    try:
        if connection_type == "postgres":
            db_conn.execute("INSTALL postgres; LOAD postgres;")
            db_conn.execute(f"ATTACH '{conn_string}' AS _import_src (TYPE POSTGRES, READ_ONLY)")
        elif connection_type == "mysql":
            db_conn.execute("INSTALL mysql; LOAD mysql;")
            db_conn.execute(f"ATTACH '{conn_string}' AS _import_src (TYPE MYSQL, READ_ONLY)")
        elif connection_type == "sqlite":
            db_conn.execute("INSTALL sqlite; LOAD sqlite;")
            db_conn.execute(f"ATTACH '{conn_string}' AS _import_src (TYPE SQLITE, READ_ONLY)")

        db_conn.execute(
            f"CREATE OR REPLACE TABLE {full_name} AS "
            f"SELECT * FROM _import_src.{source_table}"
        )

        row_count = db_conn.execute(f"SELECT COUNT(*) FROM {full_name}").fetchone()[0]
        duration_ms = int((time.perf_counter() - start) * 1000)

        # Detach
        try:
            db_conn.execute("DETACH _import_src")
        except Exception:
            pass

        log_run(db_conn, "import", full_name, "success", duration_ms, rows_affected=row_count)

        return {
            "status": "success",
            "table": full_name,
            "rows": row_count,
            "duration_ms": duration_ms,
        }
    except Exception as e:
        duration_ms = int((time.perf_counter() - start) * 1000)
        try:
            db_conn.execute("DETACH _import_src")
        except Exception:
            pass
        log_run(db_conn, "import", full_name, "error", duration_ms, error=str(e))
        return {"status": "error", "table": full_name, "error": str(e)}


def _build_connection_string(connection_type: str, params: dict) -> str:
    """Build a connection string from type and params."""
    if connection_type == "postgres":
        host = params.get("host", "localhost")
        port = params.get("port", 5432)
        database = params.get("database", "postgres")
        user = params.get("user", "postgres")
        password = params.get("password", "")
        return f"host={host} port={port} dbname={database} user={user} password={password}"
    elif connection_type == "mysql":
        host = params.get("host", "localhost")
        port = params.get("port", 3306)
        database = params.get("database", "")
        user = params.get("user", "root")
        password = params.get("password", "")
        return f"host={host} port={port} database={database} user={user} password={password}"
    elif connection_type == "sqlite":
        return params.get("path", params.get("database", ""))
    return ""


def _serialize(value: Any) -> Any:
    """Make values JSON-serializable."""
    if value is None:
        return None
    if isinstance(value, (int, float, str, bool)):
        return value
    return str(value)
