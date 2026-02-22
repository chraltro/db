"""SQL transformation engine.

Parses SQL files with config comments, builds a DAG, executes in dependency order.
Handles change detection via content hashing, incremental models, data quality
assertions, auto-profiling, freshness monitoring, and parallel execution.
"""

from __future__ import annotations

import hashlib
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from graphlib import TopologicalSorter
from pathlib import Path

import duckdb
from rich.console import Console

from dp.engine.database import ensure_meta_table, log_run
from dp.engine.utils import validate_identifier

console = Console()
logger = logging.getLogger("dp.transform")

from dp.engine.sql_analysis import (
    extract_column_lineage as _extract_column_lineage_impl,
    extract_table_refs,
    parse_assertions,
    parse_column_docs,
    parse_config,
    parse_depends,
    parse_description,
    strip_config_comments,
)


@dataclass
class AssertionResult:
    """Result of a data quality assertion."""

    expression: str
    passed: bool
    detail: str = ""


@dataclass
class ProfileResult:
    """Auto-computed profile stats for a model after execution."""

    row_count: int = 0
    column_count: int = 0
    null_percentages: dict[str, float] = field(default_factory=dict)
    distinct_counts: dict[str, int] = field(default_factory=dict)


@dataclass
class SQLModel:
    """A single SQL transformation model."""

    path: Path
    name: str  # e.g. "customers"
    schema: str  # e.g. "bronze"
    full_name: str  # e.g. "bronze.customers"
    sql: str  # raw SQL content
    query: str  # SQL without config comments
    materialized: str  # "view", "table", or "incremental"
    depends_on: list[str] = field(default_factory=list)
    description: str = ""
    column_docs: dict[str, str] = field(default_factory=dict)
    content_hash: str = ""
    upstream_hash: str = ""
    assertions: list[str] = field(default_factory=list)
    unique_key: str | None = None  # For incremental models
    incremental_strategy: str = "delete+insert"  # "delete+insert", "append", or "merge"
    incremental_filter: str | None = None  # e.g. "WHERE updated_at > (SELECT MAX(updated_at) FROM {this})"
    partition_by: str | None = None  # e.g. "event_date" — enables partition-based pruning

    def __post_init__(self) -> None:
        self.content_hash = _hash_content(self.query)


@dataclass
class ModelResult:
    """Full result from executing a single model."""

    status: str  # "built", "skipped", "error"
    duration_ms: int = 0
    row_count: int = 0
    error: str | None = None
    assertions: list[AssertionResult] = field(default_factory=list)
    profile: ProfileResult | None = None


def _hash_content(content: str) -> str:
    """Hash SQL content for change detection. Normalizes whitespace."""
    normalized = re.sub(r"\s+", " ", content.strip())
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


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
        config = parse_config(sql)
        depends = parse_depends(sql)
        description = parse_description(sql)
        column_docs = parse_column_docs(sql)
        assertions = parse_assertions(sql)
        query = strip_config_comments(sql)
        if not depends:
            folder_schema_tmp = sql_file.relative_to(transform_dir).parent.name or "public"
            own_schema_tmp = config.get("schema", folder_schema_tmp)
            own_name_tmp = sql_file.stem
            depends = extract_table_refs(query, exclude=f"{own_schema_tmp}.{own_name_tmp}")

        # Schema from folder name (convention) or config override
        rel = sql_file.relative_to(transform_dir)
        folder_schema = rel.parent.name if rel.parent.name else "public"
        schema = config.get("schema", folder_schema)
        name = sql_file.stem
        # Validate identifiers at discovery time to prevent SQL injection downstream
        validate_identifier(schema, f"schema for {sql_file.name}")
        validate_identifier(name, f"model name for {sql_file.name}")
        materialized = config.get("materialized", "view")
        unique_key = config.get("unique_key")
        incremental_strategy = config.get("incremental_strategy", "delete+insert")
        incremental_filter = config.get("incremental_filter")
        partition_by = config.get("partition_by")

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
                description=description,
                column_docs=column_docs,
                assertions=assertions,
                unique_key=unique_key,
                incremental_strategy=incremental_strategy,
                incremental_filter=incremental_filter,
                partition_by=partition_by,
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


def build_dag_tiers(models: list[SQLModel]) -> list[list[SQLModel]]:
    """Build DAG and return models grouped by execution tier.

    Models within the same tier have no dependencies on each other
    and can execute in parallel.
    """
    model_map = {m.full_name: m for m in models}
    sorter: TopologicalSorter[str] = TopologicalSorter()

    for m in models:
        known_deps = [d for d in m.depends_on if d in model_map]
        sorter.add(m.full_name, *known_deps)

    sorter.prepare()
    tiers: list[list[SQLModel]] = []

    while sorter.is_active():
        ready = sorted(sorter.get_ready())
        tier = [model_map[name] for name in ready if name in model_map]
        if tier:
            tiers.append(tier)
        for name in ready:
            sorter.done(name)

    return tiers


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


# --- Incremental model support ---


def _execute_incremental(
    conn: duckdb.DuckDBPyConnection,
    model: SQLModel,
) -> tuple[int, int]:
    """Execute an incremental model.

    Strategies:
        delete+insert (default): Delete matching rows by unique_key, insert new.
        append: Always append, no deduplication.
        merge: True upsert — update existing rows, insert new ones.

    If the target table doesn't exist yet, performs a full load regardless of strategy.
    Handles schema evolution: new columns in the source query are auto-added to the target.
    Supports incremental_filter for filtering the query on incremental runs.
    Supports partition_by for partition-based pruning (deletes affected partitions before insert).
    """
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {model.schema}")
    start = time.perf_counter()

    # Check if target table exists
    exists = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        [model.schema, model.name],
    ).fetchone()[0] > 0

    # Build the query, applying incremental_filter if this is not the first run
    query = model.query
    if exists and model.incremental_filter:
        # Replace {this} with the target table name
        filter_clause = model.incremental_filter.replace("{this}", model.full_name)
        query = f"{query}\n{filter_clause}"

    strategy = model.incremental_strategy

    if not exists:
        # First run — full load
        ddl = f"CREATE TABLE {model.full_name} AS\n{query}"
        conn.execute(ddl)
    elif strategy == "append" or not model.unique_key:
        # Append-only: just insert
        conn.execute(f"INSERT INTO {model.full_name}\n{query}")
    else:
        # Strategies that need staging: delete+insert, merge
        keys = [k.strip() for k in model.unique_key.split(",")]
        staging_name = f"_dp_staging_{model.name}"

        # Create staging table with new data
        conn.execute(f"CREATE OR REPLACE TEMP TABLE {staging_name} AS\n{query}")

        # Handle schema evolution: detect new columns in staging that don't exist in target
        target_cols = {
            r[0]
            for r in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ? ",
                [model.schema, model.name],
            ).fetchall()
        }
        staging_cols = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = ? "
            "ORDER BY ordinal_position",
            [staging_name],
        ).fetchall()

        for col_name, col_type in staging_cols:
            if col_name not in target_cols:
                conn.execute(
                    f'ALTER TABLE {model.full_name} ADD COLUMN "{col_name}" {col_type}'
                )

        # Get the final column list from staging for explicit INSERT
        staging_col_names = [r[0] for r in staging_cols]
        staging_select = ", ".join(f'"{c}"' for c in staging_col_names)
        key_cols = ", ".join(f'"{k}"' for k in keys)

        if strategy == "merge":
            # True upsert: UPDATE existing rows, INSERT new ones
            non_key_cols = [c for c in staging_col_names if c not in keys]
            if non_key_cols:
                set_clause = ", ".join(
                    f'"{c}" = staging."{c}"' for c in non_key_cols
                )
                join_cond = " AND ".join(
                    f'target."{k}" = staging."{k}"' for k in keys
                )
                conn.execute(
                    f"UPDATE {model.full_name} AS target SET {set_clause} "
                    f"FROM {staging_name} AS staging WHERE {join_cond}"
                )
            # Insert rows that don't already exist
            not_exists_cond = " AND ".join(
                f'staging."{k}" = target."{k}"' for k in keys
            )
            insert_cols = ", ".join(f'"{c}"' for c in staging_col_names)
            conn.execute(
                f"INSERT INTO {model.full_name} ({insert_cols}) "
                f"SELECT {staging_select} FROM {staging_name} AS staging "
                f"WHERE NOT EXISTS (SELECT 1 FROM {model.full_name} AS target WHERE {not_exists_cond})"
            )
        elif model.partition_by:
            # Partition-based pruning: delete entire affected partitions, then insert
            part_col = model.partition_by.strip()
            conn.execute(
                f'DELETE FROM {model.full_name} '
                f'WHERE "{part_col}" IN (SELECT DISTINCT "{part_col}" FROM {staging_name})'
            )
            insert_cols = ", ".join(f'"{c}"' for c in staging_col_names)
            conn.execute(
                f"INSERT INTO {model.full_name} ({insert_cols}) SELECT {staging_select} FROM {staging_name}"
            )
        else:
            # delete+insert strategy: delete by key, insert new
            conn.execute(
                f"DELETE FROM {model.full_name} "
                f"WHERE ({key_cols}) IN (SELECT {key_cols} FROM {staging_name})"
            )
            insert_cols = ", ".join(f'"{c}"' for c in staging_col_names)
            conn.execute(
                f"INSERT INTO {model.full_name} ({insert_cols}) SELECT {staging_select} FROM {staging_name}"
            )
        conn.execute(f"DROP TABLE IF EXISTS {staging_name}")

    duration_ms = int((time.perf_counter() - start) * 1000)
    result = conn.execute(f"SELECT count(*) FROM {model.full_name}").fetchone()
    row_count = result[0] if result else 0

    return duration_ms, row_count


# --- Data quality assertions ---


def run_assertions(
    conn: duckdb.DuckDBPyConnection,
    model: SQLModel,
) -> list[AssertionResult]:
    """Run data quality assertions against a built model.

    Supported assertion forms:
        -- assert: row_count > 0
        -- assert: no_nulls(column_name)
        -- assert: unique(column_name)
        -- assert: accepted_values(column, ['a', 'b', 'c'])
        -- assert: expression_that_returns_true
    """
    results: list[AssertionResult] = []
    if not model.assertions:
        return results

    for expr in model.assertions:
        try:
            result = _evaluate_assertion(conn, model, expr)
            results.append(result)
        except Exception as e:
            results.append(AssertionResult(
                expression=expr,
                passed=False,
                detail=f"Assertion error: {e}",
            ))

    return results


def _evaluate_assertion(
    conn: duckdb.DuckDBPyConnection,
    model: SQLModel,
    expr: str,
) -> AssertionResult:
    """Evaluate a single assertion expression."""
    table = model.full_name

    # row_count > N / row_count >= N / etc.
    m = re.match(r"row_count\s*(>|>=|<|<=|=|==|!=)\s*(\d+)", expr)
    if m:
        op, val = m.group(1), int(m.group(2))
        if op == "==":
            op = "="
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        check = conn.execute(f"SELECT {count} {op} {val}").fetchone()[0]
        return AssertionResult(
            expression=expr,
            passed=bool(check),
            detail=f"row_count={count}",
        )

    # no_nulls(column)
    m = re.match(r"no_nulls\((\w+)\)", expr)
    if m:
        col = m.group(1)
        null_count = conn.execute(
            f'SELECT COUNT(*) FROM {table} WHERE "{col}" IS NULL'
        ).fetchone()[0]
        return AssertionResult(
            expression=expr,
            passed=null_count == 0,
            detail=f"null_count={null_count}",
        )

    # unique(column)
    m = re.match(r"unique\((\w+)\)", expr)
    if m:
        col = m.group(1)
        dup_count = conn.execute(
            f'SELECT COUNT(*) - COUNT(DISTINCT "{col}") FROM {table}'
        ).fetchone()[0]
        return AssertionResult(
            expression=expr,
            passed=dup_count == 0,
            detail=f"duplicate_count={dup_count}",
        )

    # accepted_values(column, ['val1', 'val2'])
    m = re.match(r"accepted_values\((\w+),\s*\[(.+)\]\)", expr)
    if m:
        col = m.group(1)
        raw_values = m.group(2)
        values = [v.strip().strip("'\"") for v in raw_values.split(",")]
        placeholders = ", ".join(f"'{v}'" for v in values)
        bad_count = conn.execute(
            f'SELECT COUNT(*) FROM {table} WHERE "{col}" IS NOT NULL AND "{col}"::VARCHAR NOT IN ({placeholders})'
        ).fetchone()[0]
        return AssertionResult(
            expression=expr,
            passed=bad_count == 0,
            detail=f"invalid_count={bad_count}",
        )

    # Generic SQL expression — wrap in SELECT and check if true
    check = conn.execute(
        f"SELECT CASE WHEN ({expr}) THEN true ELSE false END FROM {table} LIMIT 1"
    ).fetchone()
    passed = bool(check[0]) if check else False
    return AssertionResult(
        expression=expr,
        passed=passed,
        detail="",
    )


# --- Auto data profiling ---


def profile_model(
    conn: duckdb.DuckDBPyConnection,
    model: SQLModel,
) -> ProfileResult:
    """Compute profile statistics for a model after execution."""
    table = model.full_name

    row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    cols = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
        [model.schema, model.name],
    ).fetchall()
    column_names = [c[0] for c in cols]

    null_pcts: dict[str, float] = {}
    distinct_counts: dict[str, int] = {}

    if row_count > 0:
        for col_name in column_names:
            qcol = f'"{col_name}"'
            stats = conn.execute(
                f"SELECT COUNT(*) - COUNT({qcol}), COUNT(DISTINCT {qcol}) FROM {table}"
            ).fetchone()
            null_count = stats[0]
            null_pcts[col_name] = round((null_count / row_count) * 100, 1) if row_count > 0 else 0.0
            distinct_counts[col_name] = stats[1]

    return ProfileResult(
        row_count=row_count,
        column_count=len(column_names),
        null_percentages=null_pcts,
        distinct_counts=distinct_counts,
    )


def _save_profile(
    conn: duckdb.DuckDBPyConnection,
    model: SQLModel,
    profile: ProfileResult,
) -> None:
    """Save profile stats to the metadata table."""
    import json
    conn.execute(
        """
        INSERT OR REPLACE INTO _dp_internal.model_profiles
            (model_path, row_count, column_count, null_percentages, distinct_counts, profiled_at)
        VALUES (?, ?, ?, ?::JSON, ?::JSON, current_timestamp)
        """,
        [
            model.full_name,
            profile.row_count,
            profile.column_count,
            json.dumps(profile.null_percentages),
            json.dumps(profile.distinct_counts),
        ],
    )


def _save_assertions(
    conn: duckdb.DuckDBPyConnection,
    model: SQLModel,
    results: list[AssertionResult],
) -> None:
    """Save assertion results to the metadata table."""
    for ar in results:
        conn.execute(
            """
            INSERT INTO _dp_internal.assertion_results
                (model_path, expression, passed, detail, checked_at)
            VALUES (?, ?, ?, ?, current_timestamp)
            """,
            [model.full_name, ar.expression, ar.passed, ar.detail],
        )


# --- Column-level lineage ---


def extract_column_lineage(
    model: SQLModel,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> dict[str, list[dict[str, str]]]:
    """Extract column-level lineage from a SQL model using sqlglot AST parsing.

    Returns a mapping of output_column -> list of {source_table, source_column}.
    Delegates to the shared sql_analysis module for AST-based lineage tracing.
    """
    return _extract_column_lineage_impl(
        query=model.query,
        depends_on=model.depends_on,
        conn=conn,
    )


# --- Compile-time SQL validation ---


@dataclass
class ValidationError:
    """A single validation error found during compile-time check."""

    model: str
    severity: str  # "error" or "warning"
    message: str
    line: int | None = None


def validate_models(
    conn: duckdb.DuckDBPyConnection | None,
    models: list[SQLModel],
    known_tables: set[str] | None = None,
    source_columns: dict[str, set[str]] | None = None,
) -> list[ValidationError]:
    """Validate all models without executing them.

    Checks:
    - SQL parses correctly (sqlglot)
    - Referenced tables exist (in DAG, DuckDB catalog, sources.yml, or seeds)
    - Column references exist in upstream tables (when resolvable)
    - Ambiguous column references (column in multiple upstream tables without qualifier)

    Args:
        conn: DuckDB connection for catalog lookups.
        models: List of SQL models to validate.
        known_tables: Additional known table names (e.g. from seeds, sources).
        source_columns: Column sets declared in sources.yml, keyed by table name.
    """
    import sqlglot
    from sqlglot import exp

    model_names = {m.full_name for m in models}
    errors: list[ValidationError] = []

    # Build catalog of known tables (existing in DuckDB + model names + extra)
    all_known_tables: set[str] = set(model_names)
    if known_tables:
        all_known_tables.update(t.lower() for t in known_tables)
    if conn:
        try:
            rows = conn.execute(
                "SELECT table_schema || '.' || table_name FROM information_schema.tables"
            ).fetchall()
            all_known_tables.update(r[0].lower() for r in rows)
        except Exception as e:
            logger.debug("Could not get table columns from catalog: %s", e)

    # Build column catalog: table -> set of columns
    column_catalog: dict[str, set[str]] = {}
    if source_columns:
        for table_name, cols in source_columns.items():
            column_catalog.setdefault(table_name.lower(), set()).update(
                c.lower() for c in cols
            )
    if conn:
        try:
            rows = conn.execute(
                "SELECT table_schema || '.' || table_name, column_name "
                "FROM information_schema.columns"
            ).fetchall()
            for table_fqn, col_name in rows:
                table_fqn = table_fqn.lower()
                column_catalog.setdefault(table_fqn, set()).add(col_name.lower())
        except Exception as e:
            logger.debug("Could not describe table columns: %s", e)

    for model in models:
        # 1. Parse check
        try:
            parsed = sqlglot.parse_one(model.query, read="duckdb")
        except sqlglot.errors.ParseError as e:
            errors.append(ValidationError(
                model=model.full_name,
                severity="error",
                message=f"SQL parse error: {e}",
            ))
            continue

        # 2. Check referenced tables exist
        for table in parsed.find_all(exp.Table):
            db_name = table.db or ""
            table_name = table.name or ""
            if db_name and table_name:
                fqn = f"{db_name}.{table_name}".lower()
                from dp.engine.sql_analysis import SKIP_SCHEMAS
                if fqn not in all_known_tables and db_name.lower() not in SKIP_SCHEMAS:
                    errors.append(ValidationError(
                        model=model.full_name,
                        severity="error",
                        message=f"Referenced table '{fqn}' does not exist",
                    ))

        # 3. Check column references
        # Build alias map for this model
        alias_map: dict[str, str] = {}
        for table in parsed.find_all(exp.Table):
            db_name = table.db or ""
            table_name = table.name or ""
            alias = table.alias or ""
            if db_name and table_name:
                fqn = f"{db_name}.{table_name}".lower()
                if alias:
                    alias_map[alias.lower()] = fqn
                alias_map[fqn] = fqn

        for col in parsed.find_all(exp.Column):
            col_name = col.name.lower() if col.name else ""
            table_ref = col.table.lower() if col.table else ""

            if table_ref and col_name:
                resolved_table = alias_map.get(table_ref, table_ref)
                if resolved_table in column_catalog:
                    if col_name not in column_catalog[resolved_table]:
                        errors.append(ValidationError(
                            model=model.full_name,
                            severity="error",
                            message=f"Column '{col_name}' not found in table '{resolved_table}'",
                        ))
            elif col_name and not table_ref:
                # Unqualified column — check for ambiguity
                found_in: list[str] = []
                for dep in model.depends_on:
                    if dep in column_catalog and col_name in column_catalog[dep]:
                        found_in.append(dep)
                if len(found_in) > 1:
                    errors.append(ValidationError(
                        model=model.full_name,
                        severity="warning",
                        message=f"Ambiguous column '{col_name}' found in multiple tables: {', '.join(found_in)}",
                    ))

    return errors


# --- Impact analysis ---


def impact_analysis(
    models: list[SQLModel],
    target: str,
    column: str | None = None,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> dict:
    """Analyze downstream impact of changing a model or column.

    Args:
        models: All discovered models
        target: Model name (e.g. "silver.customers")
        column: Optional column name to trace
        conn: Optional connection for column-level lineage resolution

    Returns:
        Dict with downstream_models, affected_columns, impact_chain
    """
    model_map = {m.full_name: m for m in models}

    # Build reverse dependency graph: model -> list of models that depend on it
    reverse_deps: dict[str, list[str]] = {}
    for m in models:
        for dep in m.depends_on:
            reverse_deps.setdefault(dep, []).append(m.full_name)

    # BFS to find all downstream models
    downstream: list[str] = []
    visited: set[str] = set()
    queue = [target]

    while queue:
        current = queue.pop(0)
        for child in reverse_deps.get(current, []):
            if child not in visited:
                visited.add(child)
                downstream.append(child)
                queue.append(child)

    # Build impact chain (model -> its direct dependents)
    impact_chain: dict[str, list[str]] = {}
    chain_visited: set[str] = set()
    chain_queue = [target]
    while chain_queue:
        current = chain_queue.pop(0)
        if current in chain_visited:
            continue
        chain_visited.add(current)
        children = reverse_deps.get(current, [])
        if children:
            impact_chain[current] = children
            chain_queue.extend(children)

    result: dict = {
        "target": target,
        "downstream_models": downstream,
        "impact_chain": impact_chain,
    }

    # Column-level impact if a column is specified
    if column and conn:
        affected_columns: list[dict[str, str]] = []
        for ds_name in downstream:
            ds_model = model_map.get(ds_name)
            if not ds_model:
                continue
            lineage = extract_column_lineage(ds_model, conn)
            for out_col, sources in lineage.items():
                for src in sources:
                    if src["source_table"] == target and src["source_column"] == column:
                        affected_columns.append({
                            "model": ds_name,
                            "column": out_col,
                        })
        result["column"] = column
        result["affected_columns"] = affected_columns

    return result


# --- Freshness monitoring ---


def check_freshness(
    conn: duckdb.DuckDBPyConnection,
    max_age_hours: float = 24.0,
) -> list[dict]:
    """Check freshness of all models. Returns stale models.

    A model is stale if it hasn't been run within max_age_hours.
    """
    try:
        rows = conn.execute(
            """
            SELECT model_path, last_run_at, run_duration_ms, row_count,
                   EXTRACT(EPOCH FROM (current_timestamp - last_run_at)) / 3600 AS hours_since
            FROM _dp_internal.model_state
            ORDER BY last_run_at ASC
            """
        ).fetchall()
    except Exception as e:
        logger.warning("Failed to check freshness: %s", e)
        return []

    results = []
    for model_path, last_run, duration_ms, row_count, hours_since in rows:
        results.append({
            "model": model_path,
            "last_run_at": str(last_run) if last_run else None,
            "hours_since_run": round(hours_since, 1) if hours_since is not None else None,
            "is_stale": hours_since is not None and hours_since > max_age_hours,
            "row_count": row_count,
        })

    return results


def execute_model(
    conn: duckdb.DuckDBPyConnection,
    model: SQLModel,
) -> tuple[int, int]:
    """Execute a single model. Returns (duration_ms, row_count)."""
    if model.materialized == "incremental":
        return _execute_incremental(conn, model)

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


def _execute_single_model(
    db_path: str,
    model: SQLModel,
    force: bool,
    model_map: dict[str, SQLModel],
) -> tuple[str, ModelResult]:
    """Execute a single model in its own connection (for parallel execution).

    Returns (model_full_name, ModelResult).
    """
    conn = duckdb.connect(db_path)
    try:
        ensure_meta_table(conn)
        model.upstream_hash = _compute_upstream_hash(model, model_map)
        changed = force or _has_changed(conn, model)

        if not changed:
            return model.full_name, ModelResult(status="skipped")

        duration_ms, row_count = execute_model(conn, model)
        _update_state(conn, model, duration_ms, row_count)
        log_run(conn, "transform", model.full_name, "success", duration_ms, row_count)

        # Run assertions
        assertion_results: list[AssertionResult] = []
        if model.assertions:
            assertion_results = run_assertions(conn, model)
            _save_assertions(conn, model, assertion_results)

        # Auto-profile
        profile: ProfileResult | None = None
        if model.materialized in ("table", "incremental"):
            profile = profile_model(conn, model)
            _save_profile(conn, model, profile)

        return model.full_name, ModelResult(
            status="built",
            duration_ms=duration_ms,
            row_count=row_count,
            assertions=assertion_results,
            profile=profile,
        )

    except Exception as e:
        try:
            log_run(conn, "transform", model.full_name, "error", error=str(e))
        except Exception as e2:
            logger.debug("Failed to log run error: %s", e2)
        return model.full_name, ModelResult(status="error", error=str(e))
    finally:
        conn.close()


def run_transform(
    conn: duckdb.DuckDBPyConnection,
    transform_dir: Path,
    targets: list[str] | None = None,
    force: bool = False,
    parallel: bool = False,
    max_workers: int = 4,
    db_path: str | None = None,
) -> dict[str, str]:
    """Run the full transformation pipeline.

    Args:
        conn: DuckDB connection
        transform_dir: Path to transform/ directory
        targets: Specific models to run (None = all)
        force: Force rebuild even if unchanged
        parallel: Enable parallel execution of independent models
        max_workers: Max number of parallel workers
        db_path: Explicit database path (required for parallel mode)

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
        if not models:
            all_names = [m.full_name for m in discover_models(transform_dir)]
            console.print(f"[yellow]No models matched targets: {', '.join(targets)}[/yellow]")
            if all_names:
                console.print(f"[dim]Available models: {', '.join(all_names)}[/dim]")
            return {}

    if parallel:
        return _run_transform_parallel(conn, models, force, max_workers, db_path=db_path)
    return _run_transform_sequential(conn, models, force)


def _run_transform_sequential(
    conn: duckdb.DuckDBPyConnection,
    models: list[SQLModel],
    force: bool,
) -> dict[str, str]:
    """Run models sequentially (original behavior + assertions + profiling)."""
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

            # Run data quality assertions
            if model.assertions:
                assertion_results = run_assertions(conn, model)
                _save_assertions(conn, model, assertion_results)
                for ar in assertion_results:
                    if ar.passed:
                        console.print(f"         [green]pass[/green]  assert: {ar.expression}")
                    else:
                        console.print(f"         [red]FAIL[/red]  assert: {ar.expression} ({ar.detail})")

                failed = [ar for ar in assertion_results if not ar.passed]
                if failed:
                    results[model.full_name] = "assertion_failed"
                    continue

            # Auto-profile for tables
            if model.materialized in ("table", "incremental"):
                profile = profile_model(conn, model)
                _save_profile(conn, model, profile)
                null_alerts = [
                    col for col, pct in profile.null_percentages.items()
                    if pct > 50.0
                ]
                if null_alerts:
                    console.print(
                        f"         [yellow]warn[/yellow]  high nulls: "
                        f"{', '.join(f'{c}({profile.null_percentages[c]}%)' for c in null_alerts)}"
                    )

            results[model.full_name] = "built"

        except Exception as e:
            log_run(conn, "transform", model.full_name, "error", error=str(e))
            console.print(f"  [red]fail[/red]  {label}: {e}")
            results[model.full_name] = "error"

    return results


def _run_transform_parallel(
    conn: duckdb.DuckDBPyConnection,
    models: list[SQLModel],
    force: bool,
    max_workers: int,
    db_path: str | None = None,
) -> dict[str, str]:
    """Run models in parallel by DAG tiers.

    Models within the same tier are independent and can execute concurrently.
    Each tier must complete before the next one starts.
    Assertion failures in a tier block the next tier.
    """
    tiers = build_dag_tiers(models)
    model_map = {m.full_name: m for m in models}

    # Compute upstream hashes
    ordered = build_dag(models)
    for model in ordered:
        model.upstream_hash = _compute_upstream_hash(model, model_map)

    # Resolve database path explicitly
    db_path_str = db_path
    if not db_path_str:
        # Fall back to extracting from connection
        try:
            result = conn.execute("SELECT current_setting('duckdb_database_file')").fetchone()
            db_path_str = result[0] if result and result[0] else None
        except Exception as e:
            logger.debug("Could not extract db path from connection: %s", e)
    if not db_path_str:
        console.print("[yellow]Cannot determine database path, falling back to sequential[/yellow]")
        return _run_transform_sequential(conn, models, force)

    results: dict[str, str] = {}
    total_tiers = len(tiers)

    for tier_idx, tier in enumerate(tiers, 1):
        # Check if any previous tier had failures that should block this tier
        has_blocking_failure = any(
            s in ("error", "assertion_failed") for s in results.values()
        )
        if has_blocking_failure:
            for model in tier:
                console.print(f"  [dim]skip[/dim]  [bold]{model.full_name}[/bold] (upstream failure)")
                results[model.full_name] = "skipped"
            continue

        if len(tier) > 1:
            console.print(f"  [dim]tier {tier_idx}/{total_tiers}[/dim] ({len(tier)} models in parallel)")

        if len(tier) == 1:
            # Single model — run in the main connection
            model = tier[0]
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

                # Assertions
                if model.assertions:
                    ar_results = run_assertions(conn, model)
                    _save_assertions(conn, model, ar_results)
                    failed = [ar for ar in ar_results if not ar.passed]
                    if failed:
                        for ar in failed:
                            console.print(f"         [red]FAIL[/red]  assert: {ar.expression} ({ar.detail})")
                        results[model.full_name] = "assertion_failed"
                        continue

                # Profile
                if model.materialized in ("table", "incremental"):
                    profile = profile_model(conn, model)
                    _save_profile(conn, model, profile)

                suffix = f" ({row_count:,} rows, {duration_ms}ms)" if row_count else f" ({duration_ms}ms)"
                console.print(f"  [green]done[/green]  {label}{suffix}")
                results[model.full_name] = "built"

            except Exception as e:
                log_run(conn, "transform", model.full_name, "error", error=str(e))
                console.print(f"  [red]fail[/red]  {label}: {e}")
                results[model.full_name] = "error"
        else:
            # Multiple models — run in parallel with separate connections
            # Collect ALL results from all futures before reporting
            tier_results: list[tuple[str, ModelResult]] = []
            with ThreadPoolExecutor(max_workers=min(max_workers, len(tier))) as executor:
                futures = {
                    executor.submit(
                        _execute_single_model, db_path_str, model, force, model_map
                    ): model
                    for model in tier
                }
                for future in as_completed(futures):
                    tier_results.append(future.result())

            # Report all results from this tier
            for model_name, model_result in tier_results:
                label = f"[bold]{model_name}[/bold]"
                if model_result.status == "skipped":
                    console.print(f"  [dim]skip[/dim]  {label}")
                elif model_result.status == "built":
                    suffix = ""
                    if model_result.row_count:
                        suffix = f" ({model_result.row_count:,} rows, {model_result.duration_ms}ms)"
                    else:
                        suffix = f" ({model_result.duration_ms}ms)"
                    console.print(f"  [green]done[/green]  {label}{suffix}")
                elif model_result.status == "assertion_failed":
                    console.print(f"  [red]FAIL[/red]  {label}: assertion(s) failed")
                else:
                    console.print(f"  [red]fail[/red]  {label}: {model_result.error}")

                results[model_name] = model_result.status

    return results
