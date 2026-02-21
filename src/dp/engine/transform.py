"""SQL transformation engine.

Parses SQL files with config comments, builds a DAG, executes in dependency order.
Handles change detection via content hashing, incremental models, data quality
assertions, auto-profiling, freshness monitoring, and parallel execution.
"""

from __future__ import annotations

import hashlib
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
# Matches schema.table after FROM / JOIN keywords
SQL_FROM_REF_PATTERN = re.compile(
    r"\b(?:FROM|JOIN)\s+([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\b",
    re.IGNORECASE,
)
# -- description: Human-readable model description
DESCRIPTION_PATTERN = re.compile(r"^--\s*description:\s*(.+)$", re.MULTILINE)
# -- col: column_name: Column description
COL_PATTERN = re.compile(r"^--\s*col:\s*(\w+):\s*(.+)$", re.MULTILINE)
# -- assert: <assertion expression>
ASSERT_PATTERN = re.compile(r"^--\s*assert:\s*(.+)$", re.MULTILINE)


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


def _parse_assertions(sql: str) -> list[str]:
    """Parse -- assert: expression lines from SQL header."""
    return [m.group(1).strip() for m in ASSERT_PATTERN.finditer(sql)]


# Schemas that are never real upstream dependencies
_SKIP_SCHEMAS = {"information_schema", "_dp_internal", "pg_catalog", "sys"}


def _infer_depends(query: str, own_full_name: str) -> list[str]:
    """Infer upstream table dependencies from FROM/JOIN clauses in the SQL body."""
    refs = set()
    # Strip SQL line comments before scanning to avoid matching commented-out refs
    clean = re.sub(r"--[^\n]*", "", query)
    for match in SQL_FROM_REF_PATTERN.finditer(clean):
        schema, table = match.group(1).lower(), match.group(2).lower()
        if schema in _SKIP_SCHEMAS:
            continue
        ref = f"{schema}.{table}"
        if ref == own_full_name:
            continue
        refs.add(ref)
    return sorted(refs)


def _parse_description(sql: str) -> str:
    """Parse -- description: text from SQL header."""
    match = DESCRIPTION_PATTERN.search(sql)
    return match.group(1).strip() if match else ""


def _parse_column_docs(sql: str) -> dict[str, str]:
    """Parse -- col: name: description lines from SQL header."""
    return {m.group(1): m.group(2).strip() for m in COL_PATTERN.finditer(sql)}


def _strip_config_comments(sql: str) -> str:
    """Remove config/depends/description/col/assert comments, return the actual query."""
    lines = sql.split("\n")
    query_lines = []
    for line in lines:
        stripped = line.strip()
        if (
            stripped.startswith("-- config:")
            or stripped.startswith("-- depends_on:")
            or stripped.startswith("-- description:")
            or stripped.startswith("-- col:")
            or stripped.startswith("-- assert:")
        ):
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
        description = _parse_description(sql)
        column_docs = _parse_column_docs(sql)
        assertions = _parse_assertions(sql)
        query = _strip_config_comments(sql)
        if not depends:
            folder_schema_tmp = sql_file.relative_to(transform_dir).parent.name or "public"
            own_schema_tmp = config.get("schema", folder_schema_tmp)
            own_name_tmp = sql_file.stem
            depends = _infer_depends(query, f"{own_schema_tmp}.{own_name_tmp}")

        # Schema from folder name (convention) or config override
        rel = sql_file.relative_to(transform_dir)
        folder_schema = rel.parent.name if rel.parent.name else "public"
        schema = config.get("schema", folder_schema)
        name = sql_file.stem
        materialized = config.get("materialized", "view")
        unique_key = config.get("unique_key")

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
    """Execute an incremental model using MERGE/upsert logic.

    If the target table doesn't exist yet, performs a full load.
    Otherwise merges new rows using the unique_key.
    """
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {model.schema}")
    start = time.perf_counter()

    # Check if target table exists
    exists = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        [model.schema, model.name],
    ).fetchone()[0] > 0

    if not exists:
        # First run — full load
        ddl = f"CREATE TABLE {model.full_name} AS\n{model.query}"
        conn.execute(ddl)
    else:
        unique_key = model.unique_key
        if not unique_key:
            # No unique key — append-only
            conn.execute(f"INSERT INTO {model.full_name}\n{model.query}")
        else:
            # MERGE using unique key
            keys = [k.strip() for k in unique_key.split(",")]
            staging_name = f"_dp_staging_{model.name}"
            staging_full = f"{model.schema}.{staging_name}"

            # Create staging table with new data
            conn.execute(f"CREATE OR REPLACE TEMP TABLE {staging_name} AS\n{model.query}")

            # Get column list from staging
            cols_result = conn.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema = '{model.schema}' AND table_name = '{model.name}' "
                f"ORDER BY ordinal_position"
            ).fetchall()
            columns = [r[0] for r in cols_result]

            # Build MERGE statement
            on_clause = " AND ".join(f"target.\"{k}\" = source.\"{k}\"" for k in keys)
            update_cols = [c for c in columns if c not in keys]
            update_set = ", ".join(f"\"{c}\" = source.\"{c}\"" for c in update_cols)
            insert_cols = ", ".join(f"\"{c}\"" for c in columns)
            insert_vals = ", ".join(f"source.\"{c}\"" for c in columns)

            merge_sql = f"""
                DELETE FROM {model.full_name}
                WHERE ({", ".join(f'"{k}"' for k in keys)}) IN (
                    SELECT {", ".join(f'"{k}"' for k in keys)} FROM {staging_name}
                );
                INSERT INTO {model.full_name} SELECT * FROM {staging_name};
            """
            for stmt in merge_sql.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(stmt)

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


def extract_column_lineage(model: SQLModel) -> dict[str, list[dict[str, str]]]:
    """Extract column-level lineage from a SQL model.

    Returns a mapping of output_column -> list of {source_table, source_column}.
    This uses regex-based heuristics — not a full SQL parser — but handles
    the common patterns in plain SQL transforms.
    """
    query = model.query
    # Strip comments
    clean = re.sub(r"--[^\n]*", "", query)
    # Strip block comments
    clean = re.sub(r"/\*.*?\*/", "", clean, flags=re.DOTALL)

    lineage: dict[str, list[dict[str, str]]] = {}

    # Parse table aliases from FROM/JOIN clauses
    alias_map: dict[str, str] = {}
    alias_pattern = re.compile(
        r"\b(?:FROM|JOIN)\s+([a-zA-Z_]\w*\.[a-zA-Z_]\w*)\s+(?:AS\s+)?([a-zA-Z_]\w*)\b",
        re.IGNORECASE,
    )
    for m in alias_pattern.finditer(clean):
        alias_map[m.group(2).lower()] = m.group(1).lower()
    # Also match FROM table without alias
    no_alias_pattern = re.compile(
        r"\b(?:FROM|JOIN)\s+([a-zA-Z_]\w*\.[a-zA-Z_]\w*)\b",
        re.IGNORECASE,
    )
    for m in no_alias_pattern.finditer(clean):
        table_ref = m.group(1).lower()
        alias_map[table_ref] = table_ref

    # Parse SELECT columns
    # Find the outermost SELECT ... FROM
    select_match = re.search(r"\bSELECT\b(.+?)\bFROM\b", clean, re.IGNORECASE | re.DOTALL)
    if not select_match:
        return lineage

    select_body = select_match.group(1)

    # Split by commas (respecting parentheses)
    columns: list[str] = []
    depth = 0
    current = ""
    for char in select_body:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            columns.append(current.strip())
            current = ""
            continue
        current += char
    if current.strip():
        columns.append(current.strip())

    for col_expr in columns:
        # Determine output column name (alias)
        alias_m = re.search(r"\bAS\s+(\w+)\s*$", col_expr, re.IGNORECASE)
        if alias_m:
            out_col = alias_m.group(1).lower()
            expr = col_expr[:alias_m.start()].strip()
        else:
            # No alias — last identifier is the column name
            parts = re.findall(r"[a-zA-Z_]\w*", col_expr)
            out_col = parts[-1].lower() if parts else "?"
            expr = col_expr.strip()

        # Find source references in the expression (alias.column or table.column patterns)
        sources: list[dict[str, str]] = []
        ref_pattern = re.compile(r"\b([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\b")
        for ref_m in ref_pattern.finditer(expr):
            alias_or_table = ref_m.group(1).lower()
            src_col = ref_m.group(2).lower()
            src_table = alias_map.get(alias_or_table, alias_or_table)
            # Skip function calls or keywords
            if src_table in _SKIP_SCHEMAS:
                continue
            sources.append({"source_table": src_table, "source_column": src_col})

        if not sources and "." not in expr:
            # Simple column reference without table qualifier — try to infer from upstream
            simple_col = re.match(r"^\s*(\w+)\s*$", expr)
            if simple_col:
                col_name = simple_col.group(1).lower()
                for dep in model.depends_on:
                    sources.append({"source_table": dep, "source_column": col_name})
                    break

        lineage[out_col] = sources

    return lineage


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
    except Exception:
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
        except Exception:
            pass
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
) -> dict[str, str]:
    """Run the full transformation pipeline.

    Args:
        conn: DuckDB connection
        transform_dir: Path to transform/ directory
        targets: Specific models to run (None = all)
        force: Force rebuild even if unchanged
        parallel: Enable parallel execution of independent models
        max_workers: Max number of parallel workers

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
        return _run_transform_parallel(conn, models, force, max_workers)
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
) -> dict[str, str]:
    """Run models in parallel by DAG tiers.

    Models within the same tier are independent and can execute concurrently.
    Each tier must complete before the next one starts.
    """
    tiers = build_dag_tiers(models)
    model_map = {m.full_name: m for m in models}

    # Compute upstream hashes
    ordered = build_dag(models)
    for model in ordered:
        model.upstream_hash = _compute_upstream_hash(model, model_map)

    # Get database path from the connection
    db_path = conn.execute("SELECT current_setting('duckdb_database_file')").fetchone()
    if not db_path or not db_path[0]:
        # Fall back to sequential if we can't determine the db path
        console.print("[yellow]Cannot determine database path, falling back to sequential[/yellow]")
        return _run_transform_sequential(conn, models, force)
    db_path_str = db_path[0]

    results: dict[str, str] = {}
    total_tiers = len(tiers)

    for tier_idx, tier in enumerate(tiers, 1):
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
            # Close main connection temporarily so worker threads can access the database
            with ThreadPoolExecutor(max_workers=min(max_workers, len(tier))) as executor:
                futures = {
                    executor.submit(
                        _execute_single_model, db_path_str, model, force, model_map
                    ): model
                    for model in tier
                }
                for future in as_completed(futures):
                    model_name, model_result = future.result()
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
