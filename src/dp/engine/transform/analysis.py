"""Column lineage, SQL validation, impact analysis, and freshness monitoring."""

from __future__ import annotations

import logging

import duckdb

from dp.engine.sql_analysis import extract_column_lineage as _extract_column_lineage_impl

from .models import SQLModel, ValidationError

logger = logging.getLogger("dp.transform")


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
