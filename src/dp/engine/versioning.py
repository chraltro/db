"""Versioned warehouse with time travel.

Stores table data as Parquet snapshots and enables point-in-time queries,
schema/data diffs between versions, and full or table-level restores.

Snapshot storage layout::

    _snapshots/
      run-42/
        gold.customers.parquet
        gold.orders.parquet
        _manifest.json            # {tables, created_at, run_id, row_counts}
      run-43/
        ...

State is tracked in ``_dp_internal.version_history``.
"""

from __future__ import annotations

import json
import logging
import re
import shutil
import time
from datetime import datetime
from pathlib import Path

import duckdb

from dp.engine.database import ensure_meta_table
from dp.engine.utils import validate_identifier

logger = logging.getLogger("dp.versioning")


def _ensure_version_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the version history metadata table (no-op on read-only connections)."""
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS _dp_internal")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS _dp_internal.version_history (
                version_id      VARCHAR PRIMARY KEY,
                created_at      TIMESTAMP DEFAULT current_timestamp,
                description     VARCHAR DEFAULT '',
                tables_snapshot JSON,
                trigger         VARCHAR DEFAULT 'manual'
            )
        """)
    except Exception:
        pass  # Read-only connection — table may already exist


def create_version(
    conn: duckdb.DuckDBPyConnection,
    project_dir: Path,
    description: str = "",
    tables: list[str] | None = None,
    trigger: str = "manual",
) -> dict:
    """Snapshot current table data as Parquet files.

    Args:
        conn: DuckDB connection.
        project_dir: Project root (for ``_snapshots/`` directory).
        description: Human-readable description of why this version was created.
        tables: List of ``schema.table`` to snapshot (None = all user tables).
        trigger: What triggered this version (``manual``, ``transform``, ``restore``).

    Returns:
        Dict with version_id, tables snapshotted, and file count.
    """
    _ensure_version_tables(conn)

    # Generate version ID from existing version count + timestamp
    try:
        ver_count = conn.execute(
            "SELECT COUNT(*) FROM _dp_internal.version_history"
        ).fetchone()[0]
    except Exception:
        ver_count = 0
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    version_id = f"v{ver_count + 1}-{ts}"

    # Discover tables to snapshot
    if tables:
        table_list = []
        for t in tables:
            parts = t.split(".")
            if len(parts) == 2:
                table_list.append((parts[0], parts[1]))
    else:
        table_list = _get_user_tables(conn)

    if not table_list:
        return {"version_id": version_id, "tables": [], "error": "No tables found"}

    # Validate version_id is safe for filesystem use (alphanumeric, hyphens, dots)
    if not re.match(r"^[A-Za-z0-9._-]+$", version_id):
        return {"version_id": version_id, "tables": [], "error": "Invalid version ID"}

    # Create snapshot directory
    snap_dir = project_dir / "_snapshots" / version_id
    snap_dir.mkdir(parents=True, exist_ok=True)

    # Export each table to Parquet
    tables_info: dict[str, dict] = {}
    for schema, table in table_list:
        full_name = f"{schema}.{table}"
        parquet_path = snap_dir / f"{full_name}.parquet"
        try:
            row_count = conn.execute(
                f'SELECT COUNT(*) FROM "{schema}"."{table}"'
            ).fetchone()[0]
            safe_parquet_path = str(parquet_path).replace("'", "''")
            conn.execute(
                f"COPY (SELECT * FROM \"{schema}\".\"{table}\") "
                f"TO '{safe_parquet_path}' (FORMAT PARQUET)"
            )
            # Get schema info
            cols = conn.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
                [schema, table],
            ).fetchall()

            tables_info[full_name] = {
                "row_count": row_count,
                "columns": [{"name": c[0], "type": c[1]} for c in cols],
                "parquet_file": str(parquet_path.relative_to(project_dir)),
            }
        except Exception as e:
            logger.warning("Failed to snapshot %s: %s", full_name, e)
            tables_info[full_name] = {"error": str(e)}

    # Write manifest
    manifest = {
        "version_id": version_id,
        "created_at": datetime.now().isoformat(),
        "description": description,
        "trigger": trigger,
        "tables": tables_info,
    }
    (snap_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2))

    # Record in metadata
    conn.execute(
        """
        INSERT OR REPLACE INTO _dp_internal.version_history
            (version_id, description, tables_snapshot, trigger)
        VALUES (?, ?, ?::JSON, ?)
        """,
        [version_id, description, json.dumps(tables_info), trigger],
    )

    return {
        "version_id": version_id,
        "tables": list(tables_info.keys()),
        "table_count": len(tables_info),
        "description": description,
    }


def list_versions(
    conn: duckdb.DuckDBPyConnection,
    limit: int = 50,
) -> list[dict]:
    """List all versions (newest first)."""
    _ensure_version_tables(conn)
    rows = conn.execute(
        """
        SELECT version_id, created_at, description, tables_snapshot, trigger
        FROM _dp_internal.version_history
        ORDER BY created_at DESC
        LIMIT ?
        """,
        [limit],
    ).fetchall()

    results = []
    for r in rows:
        tables_info = json.loads(r[3]) if r[3] else {}
        total_rows = sum(
            t.get("row_count", 0) for t in tables_info.values()
            if isinstance(t, dict) and "error" not in t
        )
        results.append({
            "version_id": r[0],
            "created_at": str(r[1]),
            "description": r[2],
            "trigger": r[4],
            "table_count": len(tables_info),
            "total_rows": total_rows,
        })
    return results


def get_version(
    conn: duckdb.DuckDBPyConnection,
    version_id: str,
) -> dict | None:
    """Get detailed info for a specific version."""
    _ensure_version_tables(conn)
    row = conn.execute(
        "SELECT version_id, created_at, description, tables_snapshot, trigger "
        "FROM _dp_internal.version_history WHERE version_id = ?",
        [version_id],
    ).fetchone()
    if not row:
        return None
    return {
        "version_id": row[0],
        "created_at": str(row[1]),
        "description": row[2],
        "tables": json.loads(row[3]) if row[3] else {},
        "trigger": row[4],
    }


def diff_versions(
    conn: duckdb.DuckDBPyConnection,
    project_dir: Path,
    from_version: str,
    to_version: str | None = None,
) -> dict:
    """Compare two versions (or a version against current state).

    Args:
        conn: DuckDB connection.
        project_dir: Project root.
        from_version: Earlier version to compare from.
        to_version: Later version to compare to (None = current state).

    Returns:
        Dict with table-level changes (added, removed, modified with row diffs).
    """
    _ensure_version_tables(conn)

    from_info = get_version(conn, from_version)
    if not from_info:
        return {"error": f"Version '{from_version}' not found"}

    from_tables = from_info["tables"]

    if to_version:
        to_info = get_version(conn, to_version)
        if not to_info:
            return {"error": f"Version '{to_version}' not found"}
        to_tables = to_info["tables"]
    else:
        # Compare against current state
        to_tables = {}
        for schema, table in _get_user_tables(conn):
            full_name = f"{schema}.{table}"
            row_count = conn.execute(
                f'SELECT COUNT(*) FROM "{schema}"."{table}"'
            ).fetchone()[0]
            cols = conn.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
                [schema, table],
            ).fetchall()
            to_tables[full_name] = {
                "row_count": row_count,
                "columns": [{"name": c[0], "type": c[1]} for c in cols],
            }

    from_set = set(from_tables.keys())
    to_set = set(to_tables.keys())
    changes: list[dict] = []

    for t in sorted(to_set - from_set):
        changes.append({
            "table": t,
            "change": "added",
            "rows": to_tables[t].get("row_count", 0),
        })

    for t in sorted(from_set - to_set):
        changes.append({
            "table": t,
            "change": "removed",
            "rows_before": from_tables[t].get("row_count", 0),
        })

    for t in sorted(from_set & to_set):
        from_rows = from_tables[t].get("row_count", 0)
        to_rows = to_tables[t].get("row_count", 0)
        from_cols = from_tables[t].get("columns", [])
        to_cols = to_tables[t].get("columns", [])

        row_diff = to_rows - from_rows
        schema_changed = from_cols != to_cols

        if row_diff != 0 or schema_changed:
            change = {
                "table": t,
                "change": "modified",
                "rows_before": from_rows,
                "rows_after": to_rows,
                "row_diff": row_diff,
            }
            if schema_changed:
                from_col_names = {c["name"] for c in from_cols}
                to_col_names = {c["name"] for c in to_cols}
                change["columns_added"] = sorted(to_col_names - from_col_names)
                change["columns_removed"] = sorted(from_col_names - to_col_names)
            changes.append(change)

    return {
        "from_version": from_version,
        "to_version": to_version or "current",
        "changes": changes,
        "tables_compared": len(from_set | to_set),
    }


def restore_version(
    conn: duckdb.DuckDBPyConnection,
    project_dir: Path,
    version_id: str,
    tables: list[str] | None = None,
) -> dict:
    """Restore tables from a version's Parquet snapshots.

    Args:
        conn: DuckDB connection (read-write).
        project_dir: Project root.
        version_id: Version to restore from.
        tables: Specific tables to restore (None = all tables in the version).

    Returns:
        Dict with restoration results.
    """
    _ensure_version_tables(conn)
    ensure_meta_table(conn)

    version = get_version(conn, version_id)
    if not version:
        return {"error": f"Version '{version_id}' not found"}

    snap_dir = project_dir / "_snapshots" / version_id
    if not snap_dir.exists():
        return {"error": f"Snapshot directory not found: {snap_dir}"}

    # First, snapshot current state before restore
    create_version(
        conn, project_dir,
        description=f"Auto-snapshot before restore to {version_id}",
        trigger="restore",
    )

    version_tables = version["tables"]
    if tables:
        version_tables = {
            t: info for t, info in version_tables.items()
            if t in tables
        }

    restored: list[dict] = []
    for full_name, info in version_tables.items():
        if "error" in info:
            continue

        parquet_file = info.get("parquet_file", "")
        parquet_path = (project_dir / parquet_file).resolve()

        # Path traversal protection: ensure the resolved path is within project_dir
        try:
            parquet_path.relative_to(project_dir.resolve())
        except ValueError:
            restored.append({
                "table": full_name,
                "status": "error",
                "error": "Parquet path escapes project directory",
            })
            continue

        if not parquet_path.exists():
            restored.append({
                "table": full_name,
                "status": "error",
                "error": "Parquet file not found",
            })
            continue

        parts = full_name.split(".")
        if len(parts) != 2:
            continue
        schema, table = parts

        # Validate identifiers to prevent SQL injection
        try:
            validate_identifier(schema, "schema")
            validate_identifier(table, "table name")
        except ValueError as e:
            restored.append({
                "table": full_name,
                "status": "error",
                "error": str(e),
            })
            continue

        try:
            conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            safe_parquet_path = str(parquet_path).replace("'", "''")
            conn.execute(
                f'CREATE OR REPLACE TABLE "{schema}"."{table}" AS '
                f"SELECT * FROM read_parquet('{safe_parquet_path}')"
            )
            row_count = conn.execute(
                f'SELECT COUNT(*) FROM "{schema}"."{table}"'
            ).fetchone()[0]

            restored.append({
                "table": full_name,
                "status": "success",
                "rows_restored": row_count,
            })
        except Exception as e:
            restored.append({
                "table": full_name,
                "status": "error",
                "error": str(e),
            })

    return {
        "version_id": version_id,
        "tables_restored": len([r for r in restored if r["status"] == "success"]),
        "details": restored,
    }


def table_timeline(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    limit: int = 50,
) -> list[dict]:
    """Get the history of a specific table across all versions.

    Returns a timeline showing how the table changed over time.
    """
    _ensure_version_tables(conn)
    rows = conn.execute(
        """
        SELECT version_id, created_at, description, tables_snapshot, trigger
        FROM _dp_internal.version_history
        ORDER BY created_at DESC
        LIMIT ?
        """,
        [limit],
    ).fetchall()

    timeline: list[dict] = []
    for r in rows:
        tables_info = json.loads(r[3]) if r[3] else {}
        if table_name in tables_info:
            tinfo = tables_info[table_name]
            timeline.append({
                "version_id": r[0],
                "created_at": str(r[1]),
                "description": r[2],
                "trigger": r[4],
                "row_count": tinfo.get("row_count", 0),
                "columns": tinfo.get("columns", []),
                "has_error": "error" in tinfo,
            })

    return timeline


def cleanup_old_versions(
    project_dir: Path,
    conn: duckdb.DuckDBPyConnection,
    keep: int = 10,
) -> dict:
    """Remove old version snapshots, keeping the N most recent.

    Args:
        project_dir: Project root.
        conn: DuckDB connection.
        keep: Number of most recent versions to keep.

    Returns:
        Dict with count of removed versions.
    """
    _ensure_version_tables(conn)

    # Get versions ordered oldest first
    rows = conn.execute(
        "SELECT version_id FROM _dp_internal.version_history ORDER BY created_at ASC"
    ).fetchall()

    all_versions = [r[0] for r in rows]
    if len(all_versions) <= keep:
        return {"removed": 0, "kept": len(all_versions)}

    to_remove = all_versions[:-keep]
    removed = 0

    for vid in to_remove:
        snap_dir = (project_dir / "_snapshots" / vid).resolve()
        # Safety: only delete if within the project _snapshots directory
        snapshots_root = (project_dir / "_snapshots").resolve()
        if snap_dir.exists() and str(snap_dir).startswith(str(snapshots_root)):
            shutil.rmtree(snap_dir)
        conn.execute(
            "DELETE FROM _dp_internal.version_history WHERE version_id = ?",
            [vid],
        )
        removed += 1

    return {"removed": removed, "kept": keep}


def _get_user_tables(
    conn: duckdb.DuckDBPyConnection,
) -> list[tuple[str, str]]:
    """Get all user-created tables (excluding internal schemas)."""
    try:
        rows = conn.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema NOT IN ('information_schema', '_dp_internal', 'pg_catalog') "
            "AND table_type = 'BASE TABLE' "
            "ORDER BY table_schema, table_name"
        ).fetchall()
        return [(r[0], r[1]) for r in rows]
    except Exception:
        return []
