"""Debug notebook generation for failed models."""

from __future__ import annotations

import re
from pathlib import Path

import duckdb

from .io import _make_cell_id


def generate_debug_notebook(
    conn: duckdb.DuckDBPyConnection,
    model_full_name: str,
    transform_dir: Path,
    error_message: str | None = None,
    assertion_failures: list[dict] | None = None,
) -> dict:
    """Generate a debug notebook for a failed model.

    Pre-populates the notebook with:
    - Error explanation
    - Upstream dependency queries
    - The failing model SQL
    - Assertion failure details (if applicable)

    Args:
        conn: DuckDB connection
        model_full_name: e.g. "silver.customers"
        transform_dir: Path to transform/ directory
        error_message: The error that caused the failure
        assertion_failures: List of failed assertions with details

    Returns:
        The notebook dict
    """
    from dp.engine.transform import discover_models

    models = discover_models(transform_dir)
    model_map = {m.full_name: m for m in models}

    target = model_map.get(model_full_name)
    if not target:
        raise ValueError(f"Model '{model_full_name}' not found")

    cells: list[dict] = []

    # Error summary
    error_desc = ""
    if error_message:
        error_desc = f"\n\n**Error:**\n```\n{error_message}\n```"
    elif assertion_failures:
        failed_names = [a.get("expression", "?") for a in assertion_failures]
        error_desc = f"\n\n**Failed assertions:** {', '.join(failed_names)}"

    cells.append({
        "id": _make_cell_id(),
        "type": "markdown",
        "source": (
            f"# Debug: {model_full_name}\n\n"
            f"This notebook was auto-generated to help debug a failure in "
            f"`{model_full_name}`.{error_desc}\n\n"
            f"**Materialized as:** {target.materialized}\n"
            f"**Path:** `{target.path}`\n"
            f"**Dependencies:** {', '.join(target.depends_on) or 'none'}"
        ),
    })

    # Upstream dependency cells
    if target.depends_on:
        cells.append({
            "id": _make_cell_id(),
            "type": "markdown",
            "source": "## Upstream Data\n\nCheck the data feeding into this model:",
        })

        for dep in target.depends_on:
            # Schema check
            cells.append({
                "id": _make_cell_id(),
                "type": "sql",
                "source": (
                    f"-- Row count and schema for {dep}\n"
                    f"SELECT COUNT(*) AS row_count FROM {dep}"
                ),
                "outputs": [],
            })
            cells.append({
                "id": _make_cell_id(),
                "type": "sql",
                "source": f"SELECT * FROM {dep} LIMIT 20",
                "outputs": [],
            })

    # The failing model SQL
    cells.append({
        "id": _make_cell_id(),
        "type": "markdown",
        "source": (
            f"## Model SQL\n\n"
            f"The SQL below is from `{target.path}`. "
            f"Edit and run to test fixes:"
        ),
    })
    cells.append({
        "id": _make_cell_id(),
        "type": "sql",
        "source": target.sql,
        "outputs": [],
    })

    # Assertion failure details
    if assertion_failures:
        cells.append({
            "id": _make_cell_id(),
            "type": "markdown",
            "source": "## Assertion Failures\n\nThe following assertions failed:",
        })

        for af in assertion_failures:
            expr = af.get("expression", "")
            detail = af.get("detail", "")

            cells.append({
                "id": _make_cell_id(),
                "type": "markdown",
                "source": f"### `{expr}`\n\nDetail: {detail}",
            })

            # Generate diagnostic query based on assertion type
            diag_sql = _assertion_diagnostic_sql(model_full_name, expr)
            if diag_sql:
                cells.append({
                    "id": _make_cell_id(),
                    "type": "sql",
                    "source": diag_sql,
                    "outputs": [],
                })

    return {
        "title": f"Debug: {model_full_name}",
        "cells": cells,
    }


def _assertion_diagnostic_sql(table: str, expr: str) -> str:
    """Generate diagnostic SQL for a failed assertion."""
    # unique(column) — show duplicate rows
    m = re.match(r"unique\((\w+)\)", expr)
    if m:
        col = m.group(1)
        return (
            f"-- Show duplicate values for {col}\n"
            f'SELECT "{col}", COUNT(*) AS cnt\n'
            f"FROM {table}\n"
            f'GROUP BY "{col}"\n'
            f"HAVING COUNT(*) > 1\n"
            f"ORDER BY cnt DESC\n"
            f"LIMIT 20"
        )

    # no_nulls(column) — show null rows
    m = re.match(r"no_nulls\((\w+)\)", expr)
    if m:
        col = m.group(1)
        return (
            f"-- Show rows where {col} is NULL\n"
            f"SELECT *\n"
            f"FROM {table}\n"
            f'WHERE "{col}" IS NULL\n'
            f"LIMIT 20"
        )

    # row_count check
    m = re.match(r"row_count\s*(>|>=|<|<=|=|==|!=)\s*(\d+)", expr)
    if m:
        return (
            f"-- Current row count\n"
            f"SELECT COUNT(*) AS row_count FROM {table}"
        )

    # accepted_values(column, [...])
    m = re.match(r"accepted_values\((\w+),\s*\[(.+)\]\)", expr)
    if m:
        col = m.group(1)
        raw_values = m.group(2)
        values = [v.strip().strip("'\"") for v in raw_values.split(",")]
        placeholders = ", ".join(f"'{v}'" for v in values)
        return (
            f"-- Show rows with invalid values for {col}\n"
            f'SELECT "{col}", COUNT(*) AS cnt\n'
            f"FROM {table}\n"
            f'WHERE "{col}" IS NOT NULL AND "{col}"::VARCHAR NOT IN ({placeholders})\n'
            f'GROUP BY "{col}"\n'
            f"ORDER BY cnt DESC\n"
            f"LIMIT 20"
        )

    return ""
