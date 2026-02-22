"""Notebook runner: execute all cells sequentially."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from .code_cell import execute_cell
from .ingest_cell import execute_ingest_cell
from .sql_cell import execute_sql_cell


def run_notebook(
    conn: duckdb.DuckDBPyConnection,
    notebook: dict,
    project_dir: Path | None = None,
) -> dict:
    """Execute all executable cells in a notebook sequentially.

    Handles code, sql, and ingest cell types. Markdown cells are skipped.
    Returns the notebook with updated outputs and per-cell timing.
    """
    namespace: dict[str, Any] = {}
    total_ms = 0
    cell_results: list[dict] = []

    for cell in notebook.get("cells", []):
        cell_type = cell.get("type", "")
        source = cell.get("source", "")

        if cell_type == "code":
            result = execute_cell(conn, source, namespace)
            namespace = result["namespace"]
        elif cell_type == "sql":
            result = execute_sql_cell(conn, source)
        elif cell_type == "ingest":
            result = execute_ingest_cell(conn, source, project_dir)
        else:
            continue

        cell["outputs"] = result["outputs"]
        cell["duration_ms"] = result["duration_ms"]
        total_ms += result["duration_ms"]
        cell_results.append({
            "cell_id": cell.get("id"),
            "type": cell_type,
            "duration_ms": result["duration_ms"],
            "has_error": any(o.get("type") == "error" for o in result["outputs"]),
            "outputs": result["outputs"],
        })

    notebook["last_run_ms"] = total_ms
    notebook["cell_results"] = cell_results
    return notebook
