"""Python script runner for ingest and export scripts.

Scripts can be:
- .py files with top-level code (db connection is pre-injected)
- .py files with a legacy run(db) function (backward compatible)
- .dpnb notebooks (executed cell-by-cell)
"""

from __future__ import annotations

import ast
import importlib.util
import io
import logging
import signal
import sys
import threading
import time
import traceback
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import duckdb
from rich.console import Console

from dp.engine.database import ensure_meta_table, log_run

console = Console()
logger = logging.getLogger("dp.runner")

# Default script execution timeout in seconds (5 minutes)
SCRIPT_TIMEOUT_SECONDS = 300


class ScriptTimeoutError(Exception):
    """Raised when a script exceeds the execution timeout."""


def _has_run_function(source: str) -> bool:
    """Check if Python source defines a top-level run() function."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return False
    return any(
        isinstance(node, ast.FunctionDef) and node.name == "run"
        for node in tree.body
    )


def _load_module(script_path: Path):
    """Dynamically load a Python module from a file path."""
    spec = importlib.util.spec_from_file_location(script_path.stem, script_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_notebook_as_script(
    conn: duckdb.DuckDBPyConnection,
    notebook_path: Path,
) -> dict:
    """Run a .dpnb notebook as a pipeline script."""
    from dp.engine.notebook import load_notebook, run_notebook

    notebook = load_notebook(notebook_path)
    result_nb = run_notebook(conn, notebook)

    # Check cells for errors
    errors = []
    for cell in result_nb.get("cells", []):
        for output in cell.get("outputs", []):
            if output.get("type") == "error":
                errors.append(output.get("text", ""))

    duration_ms = result_nb.get("last_run_ms", 0)

    if errors:
        error_msg = "\n".join(errors)
        return {
            "script": notebook_path.name,
            "status": "error",
            "duration_ms": duration_ms,
            "log_output": error_msg,
            "error": error_msg,
        }

    return {
        "script": notebook_path.name,
        "status": "success",
        "duration_ms": duration_ms,
        "log_output": "",
        "error": None,
    }


def run_script(
    conn: duckdb.DuckDBPyConnection,
    script_path: Path,
    script_type: str = "ingest",
    timeout: int = SCRIPT_TIMEOUT_SECONDS,
) -> dict:
    """Run a single script (.py or .dpnb).

    Args:
        conn: DuckDB connection
        script_path: Path to the .py or .dpnb file
        script_type: "ingest" or "export" (for logging)
        timeout: Maximum execution time in seconds

    Returns:
        Dict with keys: script, status, duration_ms, log_output, error
    """
    ensure_meta_table(conn)

    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    label = f"[bold]{script_path.name}[/bold]"
    console.print(f"  [blue]run [/blue] {label}")

    # Dispatch .dpnb notebooks
    if script_path.suffix == ".dpnb":
        start = time.perf_counter()
        try:
            result = _run_notebook_as_script(conn, script_path)
            duration_ms = result["duration_ms"]
            log_run(
                conn, script_type, script_path.name,
                result["status"], duration_ms,
                error=result["error"],
                log_output=result["log_output"] or None,
            )
            if result["status"] == "success":
                console.print(f"  [green]done[/green] {label} ({duration_ms}ms)")
            else:
                console.print(f"  [red]fail[/red] {label}: {result['error']}")
            return result
        except Exception as e:
            duration_ms = int((time.perf_counter() - start) * 1000)
            error_msg = traceback.format_exc()
            log_run(conn, script_type, script_path.name, "error", duration_ms, error=str(e), log_output=error_msg)
            console.print(f"  [red]fail[/red] {label}: {e}")
            return {"script": script_path.name, "status": "error", "duration_ms": duration_ms, "log_output": error_msg, "error": str(e)}

    # .py scripts
    source = script_path.read_text()
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()
    start = time.perf_counter()

    # Run in a thread with timeout to prevent scripts from hanging indefinitely
    exec_error: list[Exception] = []

    def _execute():
        try:
            if _has_run_function(source):
                # Legacy mode: import module and call run(db)
                module = _load_module(script_path)
                with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                    module.run(conn)
            else:
                # New mode: exec top-level code with db pre-injected
                namespace = {
                    "db": conn,
                    "__file__": str(script_path),
                    "__name__": script_path.stem,
                    "__builtins__": __builtins__,
                }
                try:
                    import pandas as pd
                    namespace["pd"] = pd
                except ImportError:
                    pass
                with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                    exec(compile(source, str(script_path), "exec"), namespace)
        except Exception as e:
            exec_error.append(e)

    thread = threading.Thread(target=_execute, daemon=True)
    thread.start()
    thread.join(timeout=timeout)

    if thread.is_alive():
        duration_ms = int((time.perf_counter() - start) * 1000)
        error_msg = f"Script timed out after {timeout}s"
        log_output = stdout_capture.getvalue() + stderr_capture.getvalue()
        logger.warning("Script %s timed out after %ds", script_path.name, timeout)
        log_run(conn, script_type, script_path.name, "error", duration_ms, error=error_msg, log_output=log_output or None)
        console.print(f"  [red]timeout[/red] {label}: exceeded {timeout}s limit")
        return {"script": script_path.name, "status": "error", "duration_ms": duration_ms, "log_output": log_output, "error": error_msg}

    if exec_error:
        e = exec_error[0]
        duration_ms = int((time.perf_counter() - start) * 1000)
        error_msg = traceback.format_exception(type(e), e, e.__traceback__)
        error_str = "".join(error_msg)
        log_output = stdout_capture.getvalue() + stderr_capture.getvalue() + "\n" + error_str

        log_run(conn, script_type, script_path.name, "error", duration_ms, error=str(e), log_output=log_output)
        console.print(f"  [red]fail[/red] {label}: {e}")

        return {"script": script_path.name, "status": "error", "duration_ms": duration_ms, "log_output": log_output, "error": str(e)}

    duration_ms = int((time.perf_counter() - start) * 1000)
    log_output = stdout_capture.getvalue() + stderr_capture.getvalue()

    # Try to extract row count from log output (e.g., "Loaded 42 rows" or "42 rows")
    rows_affected = _extract_row_count(log_output)

    log_run(conn, script_type, script_path.name, "success", duration_ms, rows_affected=rows_affected, log_output=log_output or None)
    rows_msg = f", {rows_affected} rows" if rows_affected else ""
    console.print(f"  [green]done[/green] {label} ({duration_ms}ms{rows_msg})")

    return {"script": script_path.name, "status": "success", "duration_ms": duration_ms, "log_output": log_output, "error": None, "rows_affected": rows_affected}


def _extract_row_count(output: str) -> int:
    """Extract row count from script output by matching common patterns.

    Matches patterns like:
    - "Loaded 42 rows"
    - "Exported 100 rows"
    - "42 rows"
    - "Got 15 earthquakes"
    """
    import re
    # Look for patterns: "<number> rows/records/entries" or "Loaded/Exported/Inserted <number>"
    patterns = [
        r"(?:loaded|exported|inserted|imported|fetched|got|wrote)\s+(\d+)",
        r"(\d+)\s+(?:rows?|records?|entries|earthquakes|items?)",
    ]
    total = 0
    for pattern in patterns:
        for match in re.finditer(pattern, output, re.IGNORECASE):
            n = int(match.group(1))
            if n > total:
                total = n
    return total


def run_scripts_in_dir(
    conn: duckdb.DuckDBPyConnection,
    scripts_dir: Path,
    script_type: str = "ingest",
    targets: list[str] | None = None,
) -> list[dict]:
    """Run all scripts in a directory (or specific targets).

    Args:
        conn: DuckDB connection
        scripts_dir: Directory containing .py/.dpnb scripts
        script_type: "ingest" or "export"
        targets: Specific script names (without extension), or None for all

    Returns:
        List of result dicts from run_script
    """
    if not scripts_dir.exists():
        console.print(f"[yellow]No {script_type}/ directory found[/yellow]")
        return []

    py_scripts = list(scripts_dir.glob("*.py"))
    nb_scripts = list(scripts_dir.glob("*.dpnb"))
    scripts = sorted(py_scripts + nb_scripts, key=lambda p: p.name)

    if targets and targets != ["all"]:
        target_set = {t.removesuffix(".py").removesuffix(".dpnb") for t in targets}
        scripts = [s for s in scripts if s.stem in target_set]

    if not scripts:
        console.print(f"[yellow]No {script_type} scripts found[/yellow]")
        return []

    results = []
    for script in scripts:
        if script.name.startswith("_"):
            continue
        result = run_script(conn, script, script_type)
        results.append(result)
        # Stop on error for ingest (data integrity)
        if script_type == "ingest" and result["status"] == "error":
            console.print("[red]Stopping: ingest script failed[/red]")
            break

    return results
