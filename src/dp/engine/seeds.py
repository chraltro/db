"""Seed data loader.

Loads CSV files from the seeds/ directory into DuckDB tables.
Supports change detection via content hashing, force reload, and
integration with the transform DAG.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import duckdb
from rich.console import Console

from dp.engine.database import ensure_meta_table, log_run

console = Console()


def _hash_file(path: Path) -> str:
    """Hash file content for change detection."""
    content = path.read_bytes()
    return hashlib.sha256(content).hexdigest()[:16]


def _has_seed_changed(
    conn: duckdb.DuckDBPyConnection,
    seed_name: str,
    content_hash: str,
) -> bool:
    """Check if a seed file has changed since last load."""
    result = conn.execute(
        "SELECT content_hash FROM _dp_internal.model_state WHERE model_path = ?",
        [seed_name],
    ).fetchone()
    if result is None:
        return True
    return result[0] != content_hash


def _update_seed_state(
    conn: duckdb.DuckDBPyConnection,
    seed_name: str,
    content_hash: str,
    row_count: int,
    duration_ms: int,
) -> None:
    """Update the model state for a seed after loading."""
    conn.execute(
        """
        INSERT OR REPLACE INTO _dp_internal.model_state
            (model_path, content_hash, upstream_hash, materialized_as, last_run_at, run_duration_ms, row_count)
        VALUES (?, ?, '', 'seed', current_timestamp, ?, ?)
        """,
        [seed_name, content_hash, duration_ms, row_count],
    )


def discover_seeds(seeds_dir: Path, schema: str = "seeds") -> list[dict]:
    """Discover all CSV files in the seeds directory.

    Returns a list of dicts with keys: path, name, full_name, schema.
    """
    if not seeds_dir.exists():
        return []
    seeds = []
    for csv_file in sorted(seeds_dir.glob("*.csv")):
        name = csv_file.stem
        seeds.append({
            "path": csv_file,
            "name": name,
            "full_name": f"{schema}.{name}",
            "schema": schema,
        })
    return seeds


def load_seed(
    conn: duckdb.DuckDBPyConnection,
    csv_path: Path,
    schema: str = "seeds",
    force: bool = False,
) -> dict:
    """Load a single CSV file into a DuckDB table.

    Handles:
    - Empty CSVs (creates empty table)
    - Headers with non-SQL-safe characters (auto-quoted by DuckDB)
    - Change detection via content hash

    Returns dict with: name, full_name, status, row_count, duration_ms.
    """
    import time

    name = csv_path.stem
    full_name = f"{schema}.{name}"
    content_hash = _hash_file(csv_path)

    ensure_meta_table(conn)

    if not force and not _has_seed_changed(conn, full_name, content_hash):
        return {
            "name": name,
            "full_name": full_name,
            "status": "skipped",
            "row_count": 0,
            "duration_ms": 0,
        }

    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    start = time.perf_counter()

    # Check if file is empty (only whitespace/newlines)
    content = csv_path.read_text(errors="replace").strip()
    if not content:
        conn.execute(f"DROP TABLE IF EXISTS {full_name}")
        conn.execute(f"CREATE TABLE {full_name} (empty_file BOOLEAN)")
        duration_ms = int((time.perf_counter() - start) * 1000)
        _update_seed_state(conn, full_name, content_hash, 0, duration_ms)
        log_run(conn, "seed", full_name, "success", duration_ms, 0)
        return {
            "name": name,
            "full_name": full_name,
            "status": "built",
            "row_count": 0,
            "duration_ms": duration_ms,
        }

    # Use DuckDB's read_csv_auto with quoting for the table name
    csv_str = str(csv_path).replace("'", "''")
    conn.execute(
        f"CREATE OR REPLACE TABLE {full_name} AS "
        f"SELECT * FROM read_csv_auto('{csv_str}', header=true, all_varchar=false)"
    )

    result = conn.execute(f"SELECT COUNT(*) FROM {full_name}").fetchone()
    row_count = result[0] if result else 0
    duration_ms = int((time.perf_counter() - start) * 1000)

    _update_seed_state(conn, full_name, content_hash, row_count, duration_ms)
    log_run(conn, "seed", full_name, "success", duration_ms, row_count)

    return {
        "name": name,
        "full_name": full_name,
        "status": "built",
        "row_count": row_count,
        "duration_ms": duration_ms,
    }


def run_seeds(
    conn: duckdb.DuckDBPyConnection,
    seeds_dir: Path,
    schema: str = "seeds",
    force: bool = False,
) -> dict[str, str]:
    """Load all CSV files from the seeds directory.

    Returns dict of seed_name -> status ("built", "skipped", "error").
    """
    ensure_meta_table(conn)
    seeds = discover_seeds(seeds_dir, schema)

    if not seeds:
        console.print("[yellow]No CSV files found in seeds/[/yellow]")
        return {}

    results: dict[str, str] = {}

    for seed in seeds:
        label = f"[bold]{seed['full_name']}[/bold]"
        try:
            result = load_seed(conn, seed["path"], schema, force)
            if result["status"] == "skipped":
                console.print(f"  [dim]skip[/dim]  {label}")
            else:
                suffix = f" ({result['row_count']:,} rows, {result['duration_ms']}ms)"
                console.print(f"  [green]done[/green]  {label}{suffix}")
            results[seed["full_name"]] = result["status"]
        except Exception as e:
            console.print(f"  [red]fail[/red]  {label}: {e}")
            log_run(conn, "seed", seed["full_name"], "error", error=str(e))
            results[seed["full_name"]] = "error"

    return results
