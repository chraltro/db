"""Python code cell execution for notebooks."""

from __future__ import annotations

import ast
import io
import logging
import time
import traceback
from contextlib import redirect_stderr, redirect_stdout
from typing import Any

import duckdb

from .formatting import _format_result

logger = logging.getLogger("dp.notebook")


def execute_cell(
    conn: duckdb.DuckDBPyConnection,
    source: str,
    namespace: dict[str, Any] | None = None,
) -> dict:
    """Execute a single Python code cell.

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
