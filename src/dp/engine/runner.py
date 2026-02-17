"""Python script runner for ingest and export scripts.

Contract: each script is a Python file with a `run(db)` function.
The function receives a DuckDB connection and does its work.
"""

from __future__ import annotations

import importlib.util
import io
import sys
import time
import traceback
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import duckdb
from rich.console import Console

from dp.engine.database import ensure_meta_table, log_run

console = Console()


def _load_module(script_path: Path):
    """Dynamically load a Python module from a file path."""
    spec = importlib.util.spec_from_file_location(script_path.stem, script_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_script(
    conn: duckdb.DuckDBPyConnection,
    script_path: Path,
    script_type: str = "ingest",
) -> dict:
    """Run a single Python script.

    Args:
        conn: DuckDB connection
        script_path: Path to the .py file
        script_type: "ingest" or "export" (for logging)

    Returns:
        Dict with keys: status, duration_ms, log_output, error
    """
    ensure_meta_table(conn)

    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    label = f"[bold]{script_path.name}[/bold]"
    console.print(f"  [blue]run [/blue] {label}")

    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()
    start = time.perf_counter()

    try:
        module = _load_module(script_path)
        if not hasattr(module, "run"):
            raise AttributeError(f"Script {script_path.name} has no run() function")

        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            module.run(conn)

        duration_ms = int((time.perf_counter() - start) * 1000)
        log_output = stdout_capture.getvalue() + stderr_capture.getvalue()

        log_run(conn, script_type, script_path.name, "success", duration_ms, log_output=log_output or None)
        console.print(f"  [green]done[/green] {label} ({duration_ms}ms)")

        return {"status": "success", "duration_ms": duration_ms, "log_output": log_output, "error": None}

    except Exception as e:
        duration_ms = int((time.perf_counter() - start) * 1000)
        error_msg = traceback.format_exc()
        log_output = stdout_capture.getvalue() + stderr_capture.getvalue() + "\n" + error_msg

        log_run(conn, script_type, script_path.name, "error", duration_ms, error=str(e), log_output=log_output)
        console.print(f"  [red]fail[/red] {label}: {e}")

        return {"status": "error", "duration_ms": duration_ms, "log_output": log_output, "error": str(e)}


def run_scripts_in_dir(
    conn: duckdb.DuckDBPyConnection,
    scripts_dir: Path,
    script_type: str = "ingest",
    targets: list[str] | None = None,
) -> list[dict]:
    """Run all scripts in a directory (or specific targets).

    Args:
        conn: DuckDB connection
        scripts_dir: Directory containing .py scripts
        script_type: "ingest" or "export"
        targets: Specific script names (without .py), or None for all

    Returns:
        List of result dicts from run_script
    """
    if not scripts_dir.exists():
        console.print(f"[yellow]No {script_type}/ directory found[/yellow]")
        return []

    scripts = sorted(scripts_dir.glob("*.py"))

    if targets and targets != ["all"]:
        target_set = {t.removesuffix(".py") for t in targets}
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
