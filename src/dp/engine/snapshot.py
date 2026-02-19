"""Snapshot storage for project + data state checkpoints.

Named snapshots allow comparing current state against a previous baseline
without requiring git.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path

import duckdb


def _ensure_snapshot_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the snapshots table if it doesn't exist."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS _dp_internal")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _dp_internal.snapshots (
            name VARCHAR PRIMARY KEY,
            created_at TIMESTAMP DEFAULT now(),
            project_hash VARCHAR,
            table_signatures VARCHAR,
            file_manifest VARCHAR
        )
    """)


def _hash_file(path: Path) -> str:
    """Compute SHA256 hash of a file."""
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()[:16]


def _build_file_manifest(project_dir: Path) -> dict[str, str]:
    """Build a manifest of all project files with their content hashes."""
    manifest: dict[str, str] = {}
    dirs_to_scan = ["transform", "ingest", "export"]
    files_to_include = ["project.yml"]

    for d in dirs_to_scan:
        dir_path = project_dir / d
        if not dir_path.exists():
            continue
        for f in sorted(dir_path.rglob("*")):
            if f.is_file() and f.suffix in (".sql", ".py", ".dpnb", ".yml", ".yaml"):
                rel = str(f.relative_to(project_dir))
                manifest[rel] = _hash_file(f)

    for f in files_to_include:
        fpath = project_dir / f
        if fpath.exists():
            manifest[f] = _hash_file(fpath)

    return manifest


def _build_table_signatures(conn: duckdb.DuckDBPyConnection) -> dict[str, dict]:
    """Build a map of table signatures: {schema.table: {row_count, col_hash}}."""
    signatures: dict[str, dict] = {}
    try:
        tables = conn.execute(
            "SELECT table_schema, table_name, table_type "
            "FROM information_schema.tables "
            "WHERE table_schema NOT IN ('information_schema', '_dp_internal')"
        ).fetchall()
    except Exception:
        return signatures

    for schema, table, table_type in tables:
        full_name = f"{schema}.{table}"
        sig: dict = {"type": table_type}

        # Row count
        try:
            count = conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]
            sig["row_count"] = count
        except Exception:
            sig["row_count"] = -1

        # Column signature
        try:
            cols = conn.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
                [schema, table],
            ).fetchall()
            col_str = "|".join(f"{c[0]}:{c[1]}" for c in cols)
            sig["col_hash"] = hashlib.sha256(col_str.encode()).hexdigest()[:16]
        except Exception:
            sig["col_hash"] = ""

        signatures[full_name] = sig

    return signatures


def _compute_project_hash(manifest: dict[str, str]) -> str:
    """Compute a combined hash of all project files."""
    parts = [f"{k}:{v}" for k, v in sorted(manifest.items())]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]


def create_snapshot(
    conn: duckdb.DuckDBPyConnection,
    project_dir: Path,
    name: str | None = None,
) -> dict:
    """Create a named snapshot of the current project and data state."""
    _ensure_snapshot_table(conn)

    if not name:
        name = f"snapshot-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    manifest = _build_file_manifest(project_dir)
    signatures = _build_table_signatures(conn)
    project_hash = _compute_project_hash(manifest)

    conn.execute(
        """
        INSERT OR REPLACE INTO _dp_internal.snapshots
            (name, project_hash, table_signatures, file_manifest)
        VALUES (?, ?, ?, ?)
        """,
        [name, project_hash, json.dumps(signatures), json.dumps(manifest)],
    )

    return {
        "name": name,
        "table_count": len(signatures),
        "file_count": len(manifest),
    }


def list_snapshots(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """List all snapshots ordered by creation time (newest first)."""
    _ensure_snapshot_table(conn)
    rows = conn.execute(
        "SELECT name, created_at, project_hash, table_signatures, file_manifest "
        "FROM _dp_internal.snapshots ORDER BY created_at DESC"
    ).fetchall()
    return [
        {
            "name": r[0],
            "created_at": r[1],
            "project_hash": r[2],
            "table_signatures": json.loads(r[3]) if r[3] else {},
            "file_manifest": json.loads(r[4]) if r[4] else {},
        }
        for r in rows
    ]


def get_snapshot(conn: duckdb.DuckDBPyConnection, name: str) -> dict | None:
    """Get a single snapshot by name."""
    _ensure_snapshot_table(conn)
    row = conn.execute(
        "SELECT name, created_at, project_hash, table_signatures, file_manifest "
        "FROM _dp_internal.snapshots WHERE name = ?",
        [name],
    ).fetchone()
    if not row:
        return None
    return {
        "name": row[0],
        "created_at": row[1],
        "project_hash": row[2],
        "table_signatures": json.loads(row[3]) if row[3] else {},
        "file_manifest": json.loads(row[4]) if row[4] else {},
    }


def delete_snapshot(conn: duckdb.DuckDBPyConnection, name: str) -> bool:
    """Delete a snapshot. Returns True if found and deleted."""
    _ensure_snapshot_table(conn)
    existing = conn.execute(
        "SELECT COUNT(*) FROM _dp_internal.snapshots WHERE name = ?", [name]
    ).fetchone()
    if existing[0] == 0:
        return False
    conn.execute("DELETE FROM _dp_internal.snapshots WHERE name = ?", [name])
    return True


def diff_against_snapshot(
    conn: duckdb.DuckDBPyConnection,
    project_dir: Path,
    snapshot_name: str,
) -> dict | None:
    """Compare current state against a snapshot.

    Returns dict with file_changes and table_changes, or None if snapshot not found.
    """
    snap = get_snapshot(conn, snapshot_name)
    if not snap:
        return None

    # File comparison
    current_manifest = _build_file_manifest(project_dir)
    snap_manifest = snap["file_manifest"]

    current_files = set(current_manifest.keys())
    snap_files = set(snap_manifest.keys())

    file_changes = {
        "added": sorted(current_files - snap_files),
        "removed": sorted(snap_files - current_files),
        "modified": sorted(
            f for f in current_files & snap_files
            if current_manifest[f] != snap_manifest[f]
        ),
    }

    # Table comparison
    current_sigs = _build_table_signatures(conn)
    snap_sigs = snap["table_signatures"]

    current_tables = set(current_sigs.keys())
    snap_tables = set(snap_sigs.keys())

    table_changes: list[dict] = []

    for t in sorted(current_tables - snap_tables):
        table_changes.append({
            "table": t,
            "status": "added",
            "current_rows": current_sigs[t].get("row_count", 0),
        })

    for t in sorted(snap_tables - current_tables):
        table_changes.append({
            "table": t,
            "status": "removed",
            "snapshot_rows": snap_sigs[t].get("row_count", 0),
        })

    for t in sorted(current_tables & snap_tables):
        curr = current_sigs[t]
        prev = snap_sigs[t]
        changed = (
            curr.get("row_count") != prev.get("row_count")
            or curr.get("col_hash") != prev.get("col_hash")
        )
        if changed:
            table_changes.append({
                "table": t,
                "status": "modified",
                "snapshot_rows": prev.get("row_count", 0),
                "current_rows": curr.get("row_count", 0),
            })

    return {
        "snapshot_name": snap["name"],
        "created_at": snap["created_at"],
        "file_changes": file_changes,
        "table_changes": table_changes,
    }
