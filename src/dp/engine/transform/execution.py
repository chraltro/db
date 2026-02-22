"""Model execution: incremental strategies, single-model runner."""

from __future__ import annotations

import logging
import time

import duckdb

from dp.engine.database import ensure_meta_table, log_run
from dp.engine.utils import validate_identifier

from .discovery import _compute_upstream_hash, _has_changed, _update_state
from .models import AssertionResult, ModelResult, ProfileResult, SQLModel
from .quality import (
    _save_assertions,
    _save_profile,
    profile_model,
    run_assertions,
)

logger = logging.getLogger("dp.transform")


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
            # Validate partition column is a safe identifier
            validate_identifier(part_col, "partition_by column")
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
