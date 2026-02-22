"""Notebook-to-model promotion and model-to-notebook conversion."""

from __future__ import annotations

from pathlib import Path

import duckdb

from dp.engine.sql_analysis import (
    extract_table_refs,
    parse_config as parse_sql_config,
    strip_config_comments,
)
from dp.engine.utils import validate_identifier as _validate_identifier

from .io import _make_cell_id


def promote_sql_to_model(
    sql_source: str,
    model_name: str,
    schema: str,
    transform_dir: Path,
    description: str = "",
    overwrite: bool = False,
) -> Path:
    """Promote a SQL cell from a notebook to a transform model file.

    Generates the SQL file with proper config and depends_on comments
    based on the SQL content, and writes it to the correct transform directory.

    Args:
        sql_source: The SQL source from the notebook cell
        model_name: Name for the model (becomes the filename)
        schema: Target schema (bronze, silver, gold, etc.)
        transform_dir: Path to the transform/ directory
        description: Optional model description
        overwrite: If False (default), raises FileExistsError when model file already exists

    Returns:
        Path to the created .sql file
    """
    # Validate identifiers to prevent path traversal and SQL injection
    _validate_identifier(model_name, "model_name")
    _validate_identifier(schema, "schema")

    # Parse existing config from SQL if present
    existing_config = parse_sql_config(sql_source)
    query = strip_config_comments(sql_source).strip()

    # Use existing config values or defaults
    materialized = existing_config.get("materialized", "table")
    target_schema = existing_config.get("schema", schema)
    _validate_identifier(target_schema, "target schema from config")

    # Infer dependencies from the SQL
    refs = extract_table_refs(query)

    # Build the model file content
    lines = []
    config_parts = [f"materialized={materialized}", f"schema={target_schema}"]
    lines.append(f"-- config: {', '.join(config_parts)}")

    if refs:
        lines.append(f"-- depends_on: {', '.join(refs)}")

    if description:
        lines.append(f"-- description: {description}")

    lines.append("")
    lines.append(query)
    lines.append("")

    content = "\n".join(lines)

    # Write to the correct directory
    schema_dir = transform_dir / target_schema
    schema_dir.mkdir(parents=True, exist_ok=True)
    model_path = schema_dir / f"{model_name}.sql"

    if model_path.exists() and not overwrite:
        raise FileExistsError(
            f"Model file already exists: {model_path}. "
            f"Use overwrite=True to replace it."
        )

    model_path.write_text(content)

    return model_path


def model_to_notebook(
    conn: duckdb.DuckDBPyConnection,
    model_full_name: str,
    transform_dir: Path,
    notebook_dir: Path,
) -> dict:
    """Create a notebook from a transform model for interactive debugging.

    Generates a notebook with:
    - A markdown cell explaining the model
    - SQL cells querying each upstream dependency (sample data)
    - The model's SQL as a SQL cell
    - A SQL cell showing the model's current output

    Args:
        conn: DuckDB connection
        model_full_name: e.g. "silver.customers"
        transform_dir: Path to the transform/ directory
        notebook_dir: Path to write the notebook

    Returns:
        The notebook dict
    """
    from dp.engine.transform import discover_models

    models = discover_models(transform_dir)
    model_map = {m.full_name: m for m in models}

    target = model_map.get(model_full_name)
    if not target:
        raise ValueError(f"Model '{model_full_name}' not found in transform directory")

    cells: list[dict] = []

    # Title cell
    cells.append({
        "id": _make_cell_id(),
        "type": "markdown",
        "source": (
            f"# Debug: {model_full_name}\n\n"
            f"**Materialized as:** {target.materialized}\n"
            f"**Path:** `{target.path}`\n"
            f"**Dependencies:** {', '.join(target.depends_on) or 'none'}"
        ),
    })

    # Upstream data cells
    for dep in target.depends_on:
        cells.append({
            "id": _make_cell_id(),
            "type": "markdown",
            "source": f"### Upstream: `{dep}`",
        })
        cells.append({
            "id": _make_cell_id(),
            "type": "sql",
            "source": f"SELECT * FROM {dep} LIMIT 100",
            "outputs": [],
        })

    # The model SQL itself
    cells.append({
        "id": _make_cell_id(),
        "type": "markdown",
        "source": f"### Model SQL: `{model_full_name}`\n\nEdit and re-run to test changes:",
    })
    cells.append({
        "id": _make_cell_id(),
        "type": "sql",
        "source": target.sql,
        "outputs": [],
    })

    # Current output (if table exists)
    cells.append({
        "id": _make_cell_id(),
        "type": "markdown",
        "source": f"### Current output of `{model_full_name}`",
    })
    cells.append({
        "id": _make_cell_id(),
        "type": "sql",
        "source": f"SELECT * FROM {model_full_name} LIMIT 100",
        "outputs": [],
    })

    nb = {
        "title": f"Debug: {model_full_name}",
        "cells": cells,
    }

    return nb
