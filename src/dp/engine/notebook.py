"""Notebook-style Python execution with cell-by-cell output.

Notebooks are stored as .dpnb files (JSON format).
Each cell has a type (code/markdown), source, and outputs.
Code cells share a namespace with `db` (DuckDB connection) and `pd` (pandas if available).
"""

from __future__ import annotations

import ast
import io
import json
import sys
import time
import traceback
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any

import duckdb


def create_notebook(title: str = "Untitled") -> dict:
    """Create a blank notebook structure."""
    return {
        "title": title,
        "cells": [
            {
                "id": "cell_1",
                "type": "markdown",
                "source": f"# {title}\n\nUse this notebook to explore your data.",
            },
            {
                "id": "cell_2",
                "type": "code",
                "source": "# Query the warehouse\nresult = db.execute('SELECT 1 AS hello').fetchdf()\nresult",
                "outputs": [],
            },
        ],
    }


def load_notebook(path: Path) -> dict:
    """Load a notebook from a .dpnb file."""
    if not path.exists():
        raise FileNotFoundError(f"Notebook not found: {path}")
    return json.loads(path.read_text())


def save_notebook(path: Path, notebook: dict) -> None:
    """Save a notebook to a .dpnb file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(notebook, indent=2) + "\n")


def _make_cell_id() -> str:
    """Generate a unique cell ID."""
    import secrets
    return f"cell_{secrets.token_hex(6)}"


def execute_cell(
    conn: duckdb.DuckDBPyConnection,
    source: str,
    namespace: dict[str, Any] | None = None,
) -> dict:
    """Execute a single code cell.

    Returns dict with:
        - outputs: list of output items (text, table, error)
        - namespace: updated namespace for subsequent cells
        - duration_ms: execution time
    """
    if namespace is None:
        namespace = {}

    # Inject helpers into namespace
    namespace["db"] = conn
    namespace["__builtins__"] = __builtins__

    # Try importing pandas for convenience
    try:
        import pandas as pd
        namespace["pd"] = pd
    except ImportError:
        pass

    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()
    outputs: list[dict] = []
    start = time.perf_counter()

    try:
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            # Parse source into an AST so we can separate the last expression
            # from preceding statements. exec() discards expression values,
            # so we eval() the last expression separately to capture its result.
            tree = ast.parse(source)
            last_expr_value = None

            if tree.body and isinstance(tree.body[-1], ast.Expr):
                # Last statement is an expression — pop it off for eval
                last_expr_node = tree.body.pop()
                # Execute all preceding statements
                if tree.body:
                    exec(compile(tree, "<cell>", "exec"), namespace)
                # Eval the last expression to capture its display value
                expr_code = compile(
                    ast.Expression(last_expr_node.value), "<cell>", "eval"
                )
                last_expr_value = eval(expr_code, namespace)
            else:
                # No trailing expression (e.g. assignment, import, etc.)
                exec(compile(tree, "<cell>", "exec"), namespace)

            if last_expr_value is not None:
                outputs.append(_format_result(last_expr_value))

        # Capture stdout
        stdout_text = stdout_capture.getvalue()
        if stdout_text:
            outputs.insert(0, {"type": "text", "text": stdout_text})

        stderr_text = stderr_capture.getvalue()
        if stderr_text:
            outputs.append({"type": "text", "text": stderr_text})

    except Exception:
        error_text = traceback.format_exc()
        outputs.append({"type": "error", "text": error_text})

    duration_ms = int((time.perf_counter() - start) * 1000)
    return {"outputs": outputs, "namespace": namespace, "duration_ms": duration_ms}


def _format_result(result: Any) -> dict:
    """Format a cell result for display."""
    # DuckDB result
    if hasattr(result, "fetchdf"):
        try:
            df = result.fetchdf()
            return _format_dataframe(df)
        except Exception:
            return {"type": "text", "text": str(result)}

    # Pandas DataFrame
    try:
        import pandas as pd
        if isinstance(result, pd.DataFrame):
            return _format_dataframe(result)
        if isinstance(result, pd.Series):
            return _format_dataframe(result.to_frame())
    except ImportError:
        pass

    # DuckDB relation
    if hasattr(result, "columns") and hasattr(result, "fetchall"):
        try:
            columns = result.columns
            rows = result.fetchall()
            return {
                "type": "table",
                "columns": columns,
                "rows": [[_serialize(v) for v in row] for row in rows[:500]],
                "total_rows": len(rows),
            }
        except Exception:
            pass

    # Plain text
    return {"type": "text", "text": repr(result)}


def _format_dataframe(df) -> dict:
    """Format a pandas DataFrame for display."""
    columns = list(df.columns)
    rows = []
    for _, row in df.head(500).iterrows():
        rows.append([_serialize(v) for v in row])
    return {
        "type": "table",
        "columns": columns,
        "rows": rows,
        "total_rows": len(df),
    }


def _serialize(value: Any) -> Any:
    """Make values JSON-serializable."""
    if value is None:
        return None
    if isinstance(value, (int, float, str, bool)):
        return value
    return str(value)


def run_notebook(conn: duckdb.DuckDBPyConnection, notebook: dict) -> dict:
    """Execute all code cells in a notebook sequentially.

    Returns the notebook with updated outputs.
    """
    namespace: dict[str, Any] = {}
    total_ms = 0

    for cell in notebook.get("cells", []):
        if cell.get("type") != "code":
            continue

        result = execute_cell(conn, cell.get("source", ""), namespace)
        cell["outputs"] = result["outputs"]
        cell["duration_ms"] = result["duration_ms"]
        namespace = result["namespace"]
        total_ms += result["duration_ms"]

    notebook["last_run_ms"] = total_ms
    return notebook
