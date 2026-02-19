"""Diff engine for comparing model SQL output against materialized tables.

Computes row-level and schema-level deltas without modifying the warehouse.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import duckdb


# -- Primary key detection patterns --

# SQL comment: -- dp:primary_key = id   or   -- dp:primary_key = id, date
PK_PATTERN = re.compile(r"^--\s*dp:primary_key\s*=\s*(.+)$", re.MULTILINE)


@dataclass
class SchemaChange:
    """A single column-level schema change."""

    column: str
    change_type: str  # "added", "removed", "type_changed"
    old_type: str | None = None
    new_type: str | None = None


@dataclass
class DiffResult:
    """Result of comparing a model's output against its materialized table."""

    model: str
    added: int = 0
    removed: int = 0
    modified: int = 0
    total_before: int = 0
    total_after: int = 0
    sample_added: list[dict] = field(default_factory=list)
    sample_removed: list[dict] = field(default_factory=list)
    sample_modified: list[dict] = field(default_factory=list)
    schema_changes: list[SchemaChange] = field(default_factory=list)
    error: str | None = None
    is_new: bool = False


def parse_primary_key_from_sql(sql: str) -> list[str] | None:
    """Parse primary key from SQL comment: -- dp:primary_key = col1, col2."""
    match = PK_PATTERN.search(sql)
    if not match:
        return None
    cols = [c.strip() for c in match.group(1).split(",") if c.strip()]
    return cols if cols else None


def get_primary_key_from_config(project_config, model_full_name: str) -> list[str] | None:
    """Get primary key from project.yml models section."""
    if project_config is None:
        return None
    raw = getattr(project_config, "_raw", None)
    if raw is None:
        return None
    models_section = raw.get("models", {})
    if not models_section or not isinstance(models_section, dict):
        return None
    model_config = models_section.get(model_full_name, {})
    if not model_config or not isinstance(model_config, dict):
        return None
    pk = model_config.get("primary_key")
    if pk is None:
        return None
    if isinstance(pk, str):
        return [pk]
    if isinstance(pk, list):
        return [str(c) for c in pk]
    return None


def get_primary_key(sql: str, project_config=None, model_full_name: str = "") -> list[str] | None:
    """Get primary key for a model. SQL comment takes precedence over project.yml."""
    pk = parse_primary_key_from_sql(sql)
    if pk is not None:
        return pk
    return get_primary_key_from_config(project_config, model_full_name)


def _get_column_info(
    conn: duckdb.DuckDBPyConnection, schema: str, table: str
) -> dict[str, str]:
    """Get column name -> type mapping for an existing table."""
    try:
        rows = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? "
            "ORDER BY ordinal_position",
            [schema, table],
        ).fetchall()
        return {row[0]: row[1] for row in rows}
    except Exception:
        return {}


def _get_temp_column_info(conn: duckdb.DuckDBPyConnection, temp_table: str) -> dict[str, str]:
    """Get column name -> type mapping for a temp table."""
    try:
        result = conn.execute(f"SELECT * FROM {temp_table} LIMIT 0")
        columns = result.description
        # For temp tables, we need to describe via PRAGMA or similar
        rows = conn.execute(
            f"SELECT column_name, column_type FROM (DESCRIBE {temp_table})"
        ).fetchall()
        return {row[0]: row[1] for row in rows}
    except Exception:
        return {}


def _compute_schema_changes(
    old_columns: dict[str, str], new_columns: dict[str, str]
) -> list[SchemaChange]:
    """Compare column signatures and report additions, removals, and type changes."""
    changes: list[SchemaChange] = []
    old_set = set(old_columns.keys())
    new_set = set(new_columns.keys())

    for col in sorted(new_set - old_set):
        changes.append(SchemaChange(column=col, change_type="added", new_type=new_columns[col]))

    for col in sorted(old_set - new_set):
        changes.append(SchemaChange(column=col, change_type="removed", old_type=old_columns[col]))

    for col in sorted(old_set & new_set):
        if old_columns[col] != new_columns[col]:
            changes.append(SchemaChange(
                column=col,
                change_type="type_changed",
                old_type=old_columns[col],
                new_type=new_columns[col],
            ))

    return changes


def _rows_to_dicts(
    conn: duckdb.DuckDBPyConnection, sql: str, limit: int = 20
) -> list[dict]:
    """Execute SQL and return results as list of dicts, capped at limit."""
    try:
        result = conn.execute(f"SELECT * FROM ({sql}) AS _q LIMIT {limit}")
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [
            {col: _serialize(val) for col, val in zip(columns, row)}
            for row in rows
        ]
    except Exception:
        return []


def _serialize(value):
    """Make a value JSON-serializable."""
    if value is None:
        return None
    if isinstance(value, (int, float, str, bool)):
        return value
    return str(value)


def _table_exists(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    """Check if a table/view exists in the warehouse."""
    try:
        result = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, table],
        ).fetchone()
        return result[0] > 0 if result else False
    except Exception:
        return False


def _quote_columns(columns: list[str]) -> str:
    """Quote column names for safe SQL usage."""
    return ", ".join(f'"{c}"' for c in columns)


def diff_model(
    conn: duckdb.DuckDBPyConnection,
    model_sql: str,
    target_schema: str,
    target_table: str,
    primary_key: list[str] | None = None,
    full: bool = False,
) -> DiffResult:
    """Compare a model's SELECT output against its currently materialized table.

    Args:
        conn: DuckDB connection
        model_sql: The SELECT statement for the model
        target_schema: Schema of the target table (e.g., "gold")
        target_table: Name of the target table (e.g., "earthquake_summary")
        primary_key: Optional list of primary key columns for modified row detection
        full: If True, return all changed rows instead of capped samples

    Returns:
        DiffResult with row counts and sample data
    """
    model_name = f"{target_schema}.{target_table}"
    sample_limit = 1_000_000 if full else 20

    # Step 1: Run the model SQL into a temp table
    try:
        conn.execute(f"CREATE OR REPLACE TEMP TABLE _dp_diff_new AS\n{model_sql}")
    except Exception as e:
        return DiffResult(model=model_name, error=f"Model SQL failed: {e}")

    # Step 2: Get new table info
    new_columns = _get_temp_column_info(conn, "_dp_diff_new")
    total_after = conn.execute("SELECT COUNT(*) FROM _dp_diff_new").fetchone()[0]

    # Step 3: Check if target exists
    target_exists = _table_exists(conn, target_schema, target_table)
    target_ref = f'"{target_schema}"."{target_table}"'

    if not target_exists:
        # Everything is new
        sample_added = _rows_to_dicts(conn, "SELECT * FROM _dp_diff_new", sample_limit)
        conn.execute("DROP TABLE IF EXISTS _dp_diff_new")
        return DiffResult(
            model=model_name,
            added=total_after,
            removed=0,
            modified=0,
            total_before=0,
            total_after=total_after,
            sample_added=sample_added,
            is_new=True,
        )

    # Step 4: Get existing table info
    old_columns = _get_column_info(conn, target_schema, target_table)
    total_before = conn.execute(f"SELECT COUNT(*) FROM {target_ref}").fetchone()[0]

    # Step 5: Compute schema changes
    schema_changes = _compute_schema_changes(old_columns, new_columns)

    # Step 6: Compute row-level delta
    # Find common columns for comparison (only compare overlapping columns)
    common_cols = sorted(set(old_columns.keys()) & set(new_columns.keys()))

    if not common_cols:
        # No common columns — can't compare rows
        conn.execute("DROP TABLE IF EXISTS _dp_diff_new")
        return DiffResult(
            model=model_name,
            total_before=total_before,
            total_after=total_after,
            schema_changes=schema_changes,
        )

    quoted_common = _quote_columns(common_cols)

    # Added rows: in new but not in old
    added_sql = (
        f"SELECT {quoted_common} FROM _dp_diff_new "
        f"EXCEPT "
        f"SELECT {quoted_common} FROM {target_ref}"
    )
    added_count = conn.execute(f"SELECT COUNT(*) FROM ({added_sql}) AS _a").fetchone()[0]
    sample_added = _rows_to_dicts(conn, added_sql, sample_limit)

    # Removed rows: in old but not in new
    removed_sql = (
        f"SELECT {quoted_common} FROM {target_ref} "
        f"EXCEPT "
        f"SELECT {quoted_common} FROM _dp_diff_new"
    )
    removed_count = conn.execute(f"SELECT COUNT(*) FROM ({removed_sql}) AS _r").fetchone()[0]
    sample_removed = _rows_to_dicts(conn, removed_sql, sample_limit)

    # Modified rows: if PK is defined, join on PK and compare non-key columns
    modified_count = 0
    sample_modified: list[dict] = []

    if primary_key:
        # Validate PK columns exist in both
        pk_valid = all(c in old_columns and c in new_columns for c in primary_key)
        if pk_valid:
            non_key_cols = [c for c in common_cols if c not in primary_key]
            if non_key_cols:
                join_cond = " AND ".join(
                    f'_old."{c}" = _new."{c}"' for c in primary_key
                )
                # Rows where PK matches but at least one non-key column differs
                diff_conditions = " OR ".join(
                    f'_old."{c}" IS DISTINCT FROM _new."{c}"' for c in non_key_cols
                )
                modified_sql = (
                    f"SELECT _new.* FROM _dp_diff_new AS _new "
                    f"INNER JOIN {target_ref} AS _old ON {join_cond} "
                    f"WHERE {diff_conditions}"
                )
                try:
                    modified_count = conn.execute(
                        f"SELECT COUNT(*) FROM ({modified_sql}) AS _m"
                    ).fetchone()[0]
                    sample_modified = _rows_to_dicts(conn, modified_sql, sample_limit)

                    # Adjust added/removed counts to exclude modified rows
                    # Modified rows appear in both EXCEPT results, so subtract them
                    added_count = max(0, added_count - modified_count)
                    removed_count = max(0, removed_count - modified_count)
                    # Re-sample added/removed excluding modified PKs if we have modified rows
                    if modified_count > 0:
                        pk_quoted = _quote_columns(primary_key)
                        modified_pks_sql = (
                            f"SELECT {pk_quoted} FROM ({modified_sql}) AS _mod"
                        )
                        added_excl_sql = (
                            f"SELECT * FROM ({added_sql}) AS _a "
                            f"WHERE ({pk_quoted}) NOT IN ({modified_pks_sql})"
                        )
                        removed_excl_sql = (
                            f"SELECT * FROM ({removed_sql}) AS _r "
                            f"WHERE ({pk_quoted}) NOT IN ({modified_pks_sql})"
                        )
                        sample_added = _rows_to_dicts(conn, added_excl_sql, sample_limit)
                        sample_removed = _rows_to_dicts(conn, removed_excl_sql, sample_limit)
                except Exception:
                    # If modified detection fails, fall back to added/removed only
                    pass

    # Cleanup
    conn.execute("DROP TABLE IF EXISTS _dp_diff_new")

    return DiffResult(
        model=model_name,
        added=added_count,
        removed=removed_count,
        modified=modified_count,
        total_before=total_before,
        total_after=total_after,
        sample_added=sample_added,
        sample_removed=sample_removed,
        sample_modified=sample_modified,
        schema_changes=schema_changes,
    )


def diff_models(
    conn: duckdb.DuckDBPyConnection,
    transform_dir,
    targets: list[str] | None = None,
    target_schema: str | None = None,
    project_config=None,
    full: bool = False,
) -> list[DiffResult]:
    """Diff multiple models.

    Args:
        conn: DuckDB connection
        transform_dir: Path to the transform/ directory
        targets: Specific models to diff (None = all)
        target_schema: Only diff models in this schema
        project_config: Project config for PK lookup
        full: If True, return all changed rows

    Returns:
        List of DiffResult objects
    """
    from dp.engine.transform import build_dag, discover_models

    models = discover_models(transform_dir)
    if not models:
        return []

    # Filter to targets if specified
    if targets:
        target_set = set(targets)
        models = [m for m in models if m.full_name in target_set or m.name in target_set]

    # Filter by schema
    if target_schema:
        models = [m for m in models if m.schema == target_schema]

    # Sort by DAG order
    ordered = build_dag(models)

    results: list[DiffResult] = []
    for model in ordered:
        pk = get_primary_key(model.sql, project_config, model.full_name)
        result = diff_model(
            conn,
            model.query,
            model.schema,
            model.name,
            primary_key=pk,
            full=full,
        )
        results.append(result)

    return results
