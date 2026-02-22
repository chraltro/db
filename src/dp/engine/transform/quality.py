"""Data quality assertions and auto-profiling."""

from __future__ import annotations

import logging
import re

import duckdb

from .models import AssertionResult, ProfileResult, SQLModel

logger = logging.getLogger("dp.transform")


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
