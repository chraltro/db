"""CLI interface for the data platform.

Split into modules by command group for maintainability.
The Typer app and shared helpers live here; each module registers its commands.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

app = typer.Typer(
    name="dp",
    help="Self-hosted data platform. DuckDB + SQL transforms + Python ingest/export.",
    no_args_is_help=True,
)
console = Console()

# Global environment override, set by --env on commands that support it.
_active_env: str | None = None


def _resolve_project(project_dir: Path | None = None) -> Path:
    project_dir = project_dir or Path.cwd()
    if not (project_dir / "project.yml").exists():
        console.print(f"[red]No project.yml found in {project_dir}[/red]")
        console.print("Run [bold]dp init[/bold] to create a new project.")
        raise typer.Exit(1)
    return project_dir


def _load_config(project_dir: Path, env: str | None = None):
    """Load project config with optional environment override."""
    from dp.config import load_project
    return load_project(project_dir, env=env or _active_env)


# Import submodules so they register their commands on `app`.
# Order doesn't matter for registration, but keep alphabetical for clarity.
from dp.cli import admin  # noqa: E402, F401
from dp.cli import connectors  # noqa: E402, F401
from dp.cli import models  # noqa: E402, F401
from dp.cli import pipeline  # noqa: E402, F401
from dp.cli import project  # noqa: E402, F401
from dp.cli import query  # noqa: E402, F401
