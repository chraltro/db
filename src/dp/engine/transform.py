"""SQL transformation engine.

Parses SQL files with config comments, builds a DAG, executes in dependency order.
Handles change detection via content hashing.
"""

from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass, field
from graphlib import TopologicalSorter
from pathlib import Path

import duckdb
from rich.console import Console

from dp.engine.database import ensure_meta_table, log_run

console = Console()

# Pattern for config comments at the top of SQL files
# -- config: materialized=view, schema=bronze
CONFIG_PATTERN = re.compile(r"^--\s*config:\s*(.+)$", re.MULTILINE)
# -- depends_on: bronze.customers, bronze.orders
DEPENDS_PATTERN = re.compile(r"^--\s*depends_on:\s*(.+)$", re.MULTILINE)


@dataclass
class SQLModel:
    """A single SQL transformation model."""

    path: Path
    name: str  # e.g. "customers"
    schema: str  # e.g. "bronze"
    full_name: str  # e.g. "bronze.customers"
    sql: str  # raw SQL content
    query: str  # SQL without config comments
    materialized: str  # "view" or "table"
    depends_on: list[str] = field(default_factory=list)
    content_hash: str = ""
    upstream_hash: str = ""

    def __post_init__(self) -> None:
        self.content_hash = _hash_content(self.query)


def _hash_content(content: str) -> str:
    """Hash SQL content for change detection. Normalizes whitespace."""
    normalized = re.sub(r"\s+", " ", content.strip())
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


def _parse_config(sql: str) -> dict[str, str]:
    """Parse -- config: key=value, key=value from SQL header."""
    match = CONFIG_PATTERN.search(sql)
    if not match:
        return {}
    config = {}
    for pair in match.group(1).split(","):
        pair = pair.strip()
        if "=" in pair:
            key, value = pair.split("=", 1)
            config[key.strip()] = value.strip()
    return config


def _parse_depends(sql: str) -> list[str]:
    """Parse -- depends_on: schema.table, schema.table from SQL header."""
    match = DEPENDS_PATTERN.search(sql)
    if not match:
        return []
    return [dep.strip() for dep in match.group(1).split(",") if dep.strip()]


def _strip_config_comments(sql: str) -> str:
    """Remove config/depends comments, return the actual query."""
    lines = sql.split("\n")
    query_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("-- config:") or stripped.startswith("-- depends_on:"):
            continue
        query_lines.append(line)
    # Strip leading blank lines
    while query_lines and not query_lines[0].strip():
        query_lines.pop(0)
    return "\n".join(query_lines)


def discover_models(transform_dir: Path) -> list[SQLModel]:
    """Discover all SQL models in the transform directory.

    Convention: folder names map to schemas.
    transform/bronze/customers.sql -> schema=bronze, name=customers
    """
    models = []
    if not transform_dir.exists():
        return models

    for sql_file in sorted(transform_dir.rglob("*.sql")):
        sql = sql_file.read_text()
        config = _parse_config(sql)
        depends = _parse_depends(sql)
        query = _strip_config_comments(sql)

        # Schema from folder name (convention) or config override
        rel = sql_file.relative_to(transform_dir)
        folder_schema = rel.parent.name if rel.parent.name else "public"
        schema = config.get("schema", folder_schema)
        name = sql_file.stem
        materialized = config.get("materialized", "view")

        models.append(
            SQLModel(
                path=sql_file,
                name=name,
                schema=schema,
                full_name=f"{schema}.{name}",
                sql=sql,
                query=query,
                materialized=materialized,
                depends_on=depends,
            )
        )

    return models


def build_dag(models: list[SQLModel]) -> list[SQLModel]:
    """Sort models in dependency order using topological sort."""
    model_map = {m.full_name: m for m in models}
    sorter: TopologicalSorter[str] = TopologicalSorter()

    for m in models:
        # Filter dependencies to only those that are known models
        # (landing.* tables won't be in the model list — that's fine)
        known_deps = [d for d in m.depends_on if d in model_map]
        sorter.add(m.full_name, *known_deps)

    ordered = list(sorter.static_order())
    return [model_map[name] for name in ordered if name in model_map]


def _compute_upstream_hash(model: SQLModel, model_map: dict[str, SQLModel]) -> str:
    """Compute a combined hash of all upstream model content hashes."""
    if not model.depends_on:
        return ""
    upstream_hashes = []
    for dep in sorted(model.depends_on):
        if dep in model_map:
            upstream_hashes.append(model_map[dep].content_hash)
    return hashlib.sha256("".join(upstream_hashes).encode()).hexdigest()[:16]


def _has_changed(
    conn: duckdb.DuckDBPyConnection,
    model: SQLModel,
) -> bool:
    """Check if a model has changed since last run."""
    result = conn.execute(
        "SELECT content_hash, upstream_hash FROM _dp_internal.model_state WHERE model_path = ?",
        [model.full_name],
    ).fetchone()
    if result is None:
        return True
    old_content_hash, old_upstream_hash = result
    return old_content_hash != model.content_hash or old_upstream_hash != model.upstream_hash


def _update_state(
    conn: duckdb.DuckDBPyConnection,
    model: SQLModel,
    duration_ms: int,
    row_count: int,
) -> None:
    """Update the model state after a successful run."""
    conn.execute(
        """
        INSERT OR REPLACE INTO _dp_internal.model_state
            (model_path, content_hash, upstream_hash, materialized_as, last_run_at, run_duration_ms, row_count)
        VALUES (?, ?, ?, ?, current_timestamp, ?, ?)
        """,
        [model.full_name, model.content_hash, model.upstream_hash, model.materialized, duration_ms, row_count],
    )


def execute_model(
    conn: duckdb.DuckDBPyConnection,
    model: SQLModel,
) -> tuple[int, int]:
    """Execute a single model. Returns (duration_ms, row_count)."""
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {model.schema}")

    start = time.perf_counter()

    if model.materialized == "view":
        ddl = f"CREATE OR REPLACE VIEW {model.full_name} AS\n{model.query}"
    elif model.materialized == "table":
        ddl = f"CREATE OR REPLACE TABLE {model.full_name} AS\n{model.query}"
    else:
        raise ValueError(f"Unknown materialization: {model.materialized}")

    conn.execute(ddl)
    duration_ms = int((time.perf_counter() - start) * 1000)

    # Get row count for tables
    row_count = 0
    if model.materialized == "table":
        result = conn.execute(f"SELECT count(*) FROM {model.full_name}").fetchone()
        row_count = result[0] if result else 0

    return duration_ms, row_count


def run_transform(
    conn: duckdb.DuckDBPyConnection,
    transform_dir: Path,
    targets: list[str] | None = None,
    force: bool = False,
) -> dict[str, str]:
    """Run the full transformation pipeline.

    Args:
        conn: DuckDB connection
        transform_dir: Path to transform/ directory
        targets: Specific models to run (None = all)
        force: Force rebuild even if unchanged

    Returns:
        Dict of model_name -> status ("built", "skipped", "error")
    """
    ensure_meta_table(conn)
    models = discover_models(transform_dir)

    if not models:
        console.print("[yellow]No SQL models found in transform/[/yellow]")
        return {}

    # Filter to targets if specified
    if targets and targets != ["all"]:
        target_set = set(targets)
        models = [m for m in models if m.full_name in target_set or m.name in target_set]

    # Build DAG and sort
    ordered = build_dag(models)
    model_map = {m.full_name: m for m in ordered}

    # Compute upstream hashes
    for model in ordered:
        model.upstream_hash = _compute_upstream_hash(model, model_map)

    results: dict[str, str] = {}

    for model in ordered:
        changed = force or _has_changed(conn, model)
        label = f"[bold]{model.full_name}[/bold] ({model.materialized})"

        if not changed:
            console.print(f"  [dim]skip[/dim]  {label}")
            results[model.full_name] = "skipped"
            continue

        try:
            duration_ms, row_count = execute_model(conn, model)
            _update_state(conn, model, duration_ms, row_count)
            log_run(conn, "transform", model.full_name, "success", duration_ms, row_count)

            suffix = f" ({row_count:,} rows, {duration_ms}ms)" if row_count else f" ({duration_ms}ms)"
            console.print(f"  [green]done[/green]  {label}{suffix}")
            results[model.full_name] = "built"

        except Exception as e:
            log_run(conn, "transform", model.full_name, "error", error=str(e))
            console.print(f"  [red]fail[/red]  {label}: {e}")
            results[model.full_name] = "error"

    return results
