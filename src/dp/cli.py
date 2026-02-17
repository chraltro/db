"""CLI interface for the data platform."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(
    name="dp",
    help="Self-hosted data platform. DuckDB + SQL transforms + Python ingest/export.",
    no_args_is_help=True,
)
console = Console()


def _resolve_project(project_dir: Path | None = None) -> Path:
    project_dir = project_dir or Path.cwd()
    if not (project_dir / "project.yml").exists():
        console.print(f"[red]No project.yml found in {project_dir}[/red]")
        console.print("Run [bold]dp init[/bold] to create a new project.")
        raise typer.Exit(1)
    return project_dir


# --- init ---


@app.command()
def init(
    name: Annotated[str, typer.Argument(help="Project name")] = "my-project",
    directory: Annotated[Optional[Path], typer.Option("--dir", "-d", help="Target directory")] = None,
) -> None:
    """Scaffold a new data platform project."""
    from dp.config import (
        PROJECT_YML_TEMPLATE,
        SAMPLE_BRONZE_SQL,
        SAMPLE_EXPORT_SCRIPT,
        SAMPLE_INGEST_SCRIPT,
    )

    target = directory or Path.cwd() / name
    target.mkdir(parents=True, exist_ok=True)

    dirs = ["ingest", "transform/bronze", "transform/silver", "transform/gold", "export"]
    for d in dirs:
        (target / d).mkdir(parents=True, exist_ok=True)

    # project.yml
    (target / "project.yml").write_text(PROJECT_YML_TEMPLATE.format(name=name))
    # Sample files
    (target / "ingest" / "example.py").write_text(SAMPLE_INGEST_SCRIPT)
    (target / "transform" / "bronze" / "example.sql").write_text(SAMPLE_BRONZE_SQL)
    (target / "export" / "example.py").write_text(SAMPLE_EXPORT_SCRIPT)
    # .gitignore
    (target / ".gitignore").write_text("warehouse.duckdb\nwarehouse.duckdb.wal\n__pycache__/\n*.pyc\n.venv/\n")

    console.print(f"[green]Project '{name}' created at {target}[/green]")
    console.print()
    console.print("Structure:")
    for d in dirs:
        console.print(f"  {d}/")
    console.print()
    console.print("Next steps:")
    console.print("  1. Add ingest scripts to ingest/")
    console.print("  2. Write SQL transforms in transform/bronze|silver|gold/")
    console.print("  3. Run [bold]dp transform[/bold] to execute the pipeline")


# --- run ---


@app.command()
def run(
    script: Annotated[str, typer.Argument(help="Script path (e.g. ingest/customers.py)")],
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Run a single ingest or export script."""
    from dp.config import load_project
    from dp.engine.database import connect
    from dp.engine.runner import run_script

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)
    script_path = project_dir / script

    if not script_path.exists():
        console.print(f"[red]Script not found: {script_path}[/red]")
        raise typer.Exit(1)

    # Determine script type from path
    script_type = "ingest" if "ingest" in str(script_path) else "export"
    console.print(f"[bold]Running {script_type}:[/bold]")

    db_path = project_dir / config.database.path
    conn = connect(db_path)
    try:
        result = run_script(conn, script_path, script_type)
        if result["status"] == "error":
            raise typer.Exit(1)
    finally:
        conn.close()


# --- transform ---


@app.command()
def transform(
    targets: Annotated[Optional[list[str]], typer.Argument(help="Specific models to run")] = None,
    force: Annotated[bool, typer.Option("--force", "-f", help="Force rebuild all models")] = False,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Parse SQL models, resolve DAG, execute in dependency order."""
    from dp.config import load_project
    from dp.engine.database import connect
    from dp.engine.transform import run_transform

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)
    transform_dir = project_dir / "transform"

    console.print("[bold]Transform:[/bold]")

    db_path = project_dir / config.database.path
    conn = connect(db_path)
    try:
        results = run_transform(conn, transform_dir, targets=targets, force=force)
        if not results:
            return
        built = sum(1 for s in results.values() if s == "built")
        skipped = sum(1 for s in results.values() if s == "skipped")
        errors = sum(1 for s in results.values() if s == "error")
        console.print()
        console.print(f"  {built} built, {skipped} skipped, {errors} errors")
        if errors:
            raise typer.Exit(1)
    finally:
        conn.close()


# --- stream ---


@app.command()
def stream(
    name: Annotated[str, typer.Argument(help="Stream name from project.yml")],
    force: Annotated[bool, typer.Option("--force", "-f", help="Force rebuild all models")] = False,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Run a full stream: ingest -> transform -> export as defined in project.yml."""
    from dp.config import load_project
    from dp.engine.database import connect
    from dp.engine.runner import run_scripts_in_dir
    from dp.engine.transform import run_transform

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)

    if name not in config.streams:
        console.print(f"[red]Stream '{name}' not found in project.yml[/red]")
        available = ", ".join(config.streams.keys()) or "(none)"
        console.print(f"Available streams: {available}")
        raise typer.Exit(1)

    stream_config = config.streams[name]
    console.print(f"[bold]Stream: {name}[/bold]")
    if stream_config.description:
        console.print(f"  {stream_config.description}")
    console.print()

    db_path = project_dir / config.database.path
    conn = connect(db_path)
    has_error = False

    try:
        for step in stream_config.steps:
            if step.action == "ingest":
                console.print("[bold]Ingest:[/bold]")
                results = run_scripts_in_dir(conn, project_dir / "ingest", "ingest", step.targets)
                if any(r["status"] == "error" for r in results):
                    has_error = True
                    break
                console.print()

            elif step.action == "transform":
                console.print("[bold]Transform:[/bold]")
                results = run_transform(
                    conn,
                    project_dir / "transform",
                    targets=step.targets if step.targets != ["all"] else None,
                    force=force,
                )
                if any(s == "error" for s in results.values()):
                    has_error = True
                    break
                console.print()

            elif step.action == "export":
                console.print("[bold]Export:[/bold]")
                results = run_scripts_in_dir(conn, project_dir / "export", "export", step.targets)
                if any(r["status"] == "error" for r in results):
                    has_error = True
                    break
                console.print()

        if has_error:
            console.print("[red]Stream failed.[/red]")
            raise typer.Exit(1)
        else:
            console.print("[green]Stream completed successfully.[/green]")
    finally:
        conn.close()


# --- lint ---


@app.command()
def lint(
    fix: Annotated[bool, typer.Option("--fix", help="Auto-fix violations")] = False,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Lint SQL files in the transform directory with SQLFluff."""
    from dp.config import load_project
    from dp.lint.linter import lint as run_lint
    from dp.lint.linter import print_violations

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)
    transform_dir = project_dir / "transform"

    action = "Fixing" if fix else "Linting"
    console.print(f"[bold]{action} SQL files...[/bold]")

    count, violations = run_lint(
        transform_dir,
        fix=fix,
        dialect=config.lint.dialect,
        rules=config.lint.rules or None,
    )

    print_violations(violations)

    if fix:
        console.print(f"[green]Fixed {count} violations.[/green]")
    elif count > 0:
        raise typer.Exit(1)


# --- query ---


@app.command()
def query(
    sql: Annotated[str, typer.Argument(help="SQL query to execute")],
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Run an ad-hoc SQL query against the warehouse."""
    from dp.config import load_project
    from dp.engine.database import connect

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)

    db_path = project_dir / config.database.path
    conn = connect(db_path, read_only=True)
    try:
        result = conn.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()

        table = Table()
        for col in columns:
            table.add_column(col)
        for row in rows:
            table.add_row(*[str(v) for v in row])
        console.print(table)
        console.print(f"[dim]{len(rows)} rows[/dim]")
    finally:
        conn.close()


# --- tables ---


@app.command()
def tables(
    schema: Annotated[Optional[str], typer.Argument(help="Schema to list (all if omitted)")] = None,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """List tables and views in the warehouse."""
    from dp.config import load_project
    from dp.engine.database import connect

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)

    db_path = project_dir / config.database.path
    if not db_path.exists():
        console.print("[yellow]No warehouse database found. Run a pipeline first.[/yellow]")
        return

    conn = connect(db_path, read_only=True)
    try:
        sql = """
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', '_dp_internal')
        """
        if schema:
            sql += f" AND table_schema = '{schema}'"
        sql += " ORDER BY table_schema, table_name"

        result = conn.execute(sql).fetchall()
        if not result:
            console.print("[yellow]No tables found.[/yellow]")
            return

        table = Table(title="Warehouse Objects")
        table.add_column("Schema", style="cyan")
        table.add_column("Name", style="bold")
        table.add_column("Type")
        for row in result:
            type_style = "dim" if row[2] == "VIEW" else ""
            table.add_row(row[0], row[1], row[2], style=type_style)
        console.print(table)
    finally:
        conn.close()


# --- history ---


@app.command()
def history(
    limit: Annotated[int, typer.Option("--limit", "-n", help="Number of entries")] = 20,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Show recent run history."""
    from dp.config import load_project
    from dp.engine.database import connect, ensure_meta_table

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)

    db_path = project_dir / config.database.path
    if not db_path.exists():
        console.print("[yellow]No warehouse database found.[/yellow]")
        return

    conn = connect(db_path, read_only=False)
    ensure_meta_table(conn)
    try:
        result = conn.execute(
            """
            SELECT run_type, target, status, started_at, duration_ms, rows_affected, error
            FROM _dp_internal.run_log
            ORDER BY started_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()

        if not result:
            console.print("[yellow]No run history yet.[/yellow]")
            return

        table = Table(title="Run History")
        table.add_column("Type", style="cyan")
        table.add_column("Target", style="bold")
        table.add_column("Status")
        table.add_column("Time")
        table.add_column("Duration", justify="right")
        table.add_column("Rows", justify="right")
        table.add_column("Error")

        for row in result:
            status_style = "[green]" if row[2] == "success" else "[red]"
            dur = f"{row[4]}ms" if row[4] else ""
            rows = str(row[5]) if row[5] else ""
            error = (row[6][:60] + "...") if row[6] and len(row[6]) > 60 else (row[6] or "")
            table.add_row(
                row[0],
                row[1],
                f"{status_style}{row[2]}[/]",
                str(row[3])[:19] if row[3] else "",
                dur,
                rows,
                error,
            )

        console.print(table)
    finally:
        conn.close()


# --- serve ---


@app.command()
def serve(
    host: Annotated[str, typer.Option(help="Host to bind to")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Port to bind to")] = 3000,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Start the web UI server."""
    import uvicorn

    project_dir = _resolve_project(project_dir)

    import dp.server.app as server_app

    server_app.PROJECT_DIR = project_dir
    console.print(f"[bold]Starting dp server at http://{host}:{port}[/bold]")
    uvicorn.run(server_app.app, host=host, port=port)


if __name__ == "__main__":
    app()
