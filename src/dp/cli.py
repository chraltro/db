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
        CLAUDE_MD_TEMPLATE,
        PROJECT_YML_TEMPLATE,
        SAMPLE_BRONZE_SQL,
        SAMPLE_EXPORT_SCRIPT,
        SAMPLE_INGEST_SCRIPT,
    )
    from dp.engine.secrets import ENV_TEMPLATE

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
    # .env secrets file
    (target / ".env").write_text(ENV_TEMPLATE)
    # Notebooks directory
    (target / "notebooks").mkdir(parents=True, exist_ok=True)
    # .gitignore
    (target / ".gitignore").write_text(
        "warehouse.duckdb\nwarehouse.duckdb.wal\n__pycache__/\n*.pyc\n.venv/\n.env\n"
    )
    # Agent instructions for LLM tools (Claude Code, Cursor, etc.)
    (target / "CLAUDE.md").write_text(CLAUDE_MD_TEMPLATE.format(name=name))

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
    console.print()
    console.print("[dim]AI assistant ready:[/dim] CLAUDE.md included for Claude Code, Cursor, and others.")
    console.print("[dim]Run [bold]dp context[/bold] to generate a project summary for any AI chat.[/dim]")


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


# --- docs ---


@app.command()
def docs(
    output: Annotated[Optional[Path], typer.Option("--output", "-o", help="Write to file instead of stdout")] = None,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Generate markdown documentation from the warehouse schema."""
    from dp.config import load_project
    from dp.engine.database import connect
    from dp.engine.docs import generate_docs

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)
    db_path = project_dir / config.database.path

    if not db_path.exists():
        console.print("[yellow]No warehouse database found. Run a pipeline first.[/yellow]")
        raise typer.Exit(1)

    conn = connect(db_path, read_only=True)
    try:
        md = generate_docs(conn, project_dir / "transform")
        if output:
            output.write_text(md)
            console.print(f"[green]Documentation written to {output}[/green]")
        else:
            console.print(md)
    finally:
        conn.close()


# --- watch ---


@app.command()
def watch(
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Watch for file changes and auto-rebuild transforms."""
    from dp.engine.scheduler import FileWatcher

    project_dir = _resolve_project(project_dir)
    console.print("[bold]Watching for changes...[/bold] (Ctrl+C to stop)")
    console.print(f"  transform/  -> auto-rebuild SQL models")

    watcher = FileWatcher(project_dir)
    watcher.start()

    try:
        watcher.join()
    except KeyboardInterrupt:
        watcher.stop()
        console.print("\n[dim]Watcher stopped.[/dim]")


# --- schedule ---


@app.command()
def schedule(
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Show scheduled streams and start the scheduler."""
    from dp.engine.scheduler import SchedulerThread, get_scheduled_streams

    project_dir = _resolve_project(project_dir)
    scheduled = get_scheduled_streams(project_dir)

    if not scheduled:
        console.print("[yellow]No scheduled streams found in project.yml[/yellow]")
        console.print("Add a schedule to a stream:")
        console.print('  schedule: "0 6 * * *"  # 6am daily')
        return

    table = Table(title="Scheduled Streams")
    table.add_column("Stream", style="bold")
    table.add_column("Schedule")
    table.add_column("Description")
    for s in scheduled:
        table.add_row(s["name"], s["schedule"], s["description"])
    console.print(table)
    console.print()

    console.print("[bold]Starting scheduler...[/bold] (Ctrl+C to stop)")
    scheduler = SchedulerThread(project_dir)
    scheduler.start()

    try:
        scheduler.join()
    except KeyboardInterrupt:
        scheduler.stop()
        console.print("\n[dim]Scheduler stopped.[/dim]")


# --- serve ---


@app.command()
def serve(
    host: Annotated[str, typer.Option(help="Host to bind to")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Port to bind to")] = 3000,
    watch_files: Annotated[bool, typer.Option("--watch", "-w", help="Watch files for changes")] = False,
    scheduler_on: Annotated[bool, typer.Option("--schedule", "-s", help="Enable cron scheduler")] = False,
    auth: Annotated[bool, typer.Option("--auth", help="Enable authentication")] = False,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Start the web UI server."""
    import uvicorn

    from dp.engine.scheduler import FileWatcher, SchedulerThread

    project_dir = _resolve_project(project_dir)

    import dp.server.app as server_app

    server_app.PROJECT_DIR = project_dir
    server_app.AUTH_ENABLED = auth

    # Start optional background services
    threads = []
    if watch_files:
        watcher = FileWatcher(project_dir)
        watcher.start()
        threads.append(watcher)
        console.print("[bold]File watcher enabled[/bold]")

    if scheduler_on:
        scheduler = SchedulerThread(project_dir)
        scheduler.start()
        threads.append(scheduler)
        console.print("[bold]Scheduler enabled[/bold]")

    if auth:
        console.print("[bold]Authentication enabled[/bold]")
    else:
        console.print("[dim]Auth disabled (use --auth to enable)[/dim]")

    console.print(f"[bold]Starting dp server at http://{host}:{port}[/bold]")
    uvicorn.run(server_app.app, host=host, port=port)


# --- secrets ---

secrets_app = typer.Typer(name="secrets", help="Manage .env secrets.")
app.add_typer(secrets_app)


@secrets_app.command("list")
def secrets_list(
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """List all secrets (keys only, values masked)."""
    from dp.engine.secrets import list_secrets

    project_dir = _resolve_project(project_dir)
    secrets = list_secrets(project_dir)

    if not secrets:
        console.print("[yellow]No secrets found. Add them to .env[/yellow]")
        return

    table = Table(title="Secrets")
    table.add_column("Key", style="bold")
    table.add_column("Value (masked)")
    table.add_column("Set?")
    for s in secrets:
        table.add_row(s["key"], s["masked_value"], "[green]Yes[/green]" if s["is_set"] else "[red]No[/red]")
    console.print(table)


@secrets_app.command("set")
def secrets_set(
    key: Annotated[str, typer.Argument(help="Secret key")],
    value: Annotated[str, typer.Argument(help="Secret value")],
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Set or update a secret in .env."""
    from dp.engine.secrets import set_secret

    project_dir = _resolve_project(project_dir)
    set_secret(project_dir, key, value)
    console.print(f"[green]Secret '{key}' set.[/green]")


@secrets_app.command("delete")
def secrets_delete(
    key: Annotated[str, typer.Argument(help="Secret key")],
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Delete a secret from .env."""
    from dp.engine.secrets import delete_secret

    project_dir = _resolve_project(project_dir)
    if delete_secret(project_dir, key):
        console.print(f"[green]Secret '{key}' deleted.[/green]")
    else:
        console.print(f"[red]Secret '{key}' not found.[/red]")
        raise typer.Exit(1)


# --- users ---

users_app = typer.Typer(name="users", help="Manage platform users.")
app.add_typer(users_app)


@users_app.command("list")
def users_list(
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """List all users."""
    from dp.config import load_project
    from dp.engine.auth import list_users
    from dp.engine.database import connect

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)
    db_path = project_dir / config.database.path
    conn = connect(db_path)
    try:
        users = list_users(conn)
        if not users:
            console.print("[yellow]No users. Create one with: dp users create <username> <password>[/yellow]")
            return

        table = Table(title="Users")
        table.add_column("Username", style="bold")
        table.add_column("Role")
        table.add_column("Display Name")
        table.add_column("Last Login")
        for u in users:
            role_style = {"admin": "red", "editor": "yellow", "viewer": "green"}.get(u["role"], "")
            table.add_row(
                u["username"],
                f"[{role_style}]{u['role']}[/{role_style}]",
                u["display_name"] or "",
                u["last_login"] or "never",
            )
        console.print(table)
    finally:
        conn.close()


@users_app.command("create")
def users_create(
    username: Annotated[str, typer.Argument(help="Username")],
    password: Annotated[str, typer.Argument(help="Password")],
    role: Annotated[str, typer.Option("--role", "-r", help="Role: admin, editor, viewer")] = "viewer",
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Create a new user."""
    from dp.config import load_project
    from dp.engine.auth import create_user
    from dp.engine.database import connect

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)
    db_path = project_dir / config.database.path
    conn = connect(db_path)
    try:
        user = create_user(conn, username, password, role)
        console.print(f"[green]User '{user['username']}' created with role '{user['role']}'[/green]")
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(1)
    finally:
        conn.close()


@users_app.command("delete")
def users_delete(
    username: Annotated[str, typer.Argument(help="Username to delete")],
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Delete a user."""
    from dp.config import load_project
    from dp.engine.auth import delete_user
    from dp.engine.database import connect

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)
    db_path = project_dir / config.database.path
    conn = connect(db_path)
    try:
        if delete_user(conn, username):
            console.print(f"[green]User '{username}' deleted.[/green]")
        else:
            console.print(f"[red]User '{username}' not found.[/red]")
            raise typer.Exit(1)
    finally:
        conn.close()


# --- context ---


@app.command()
def context(
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Generate a project summary to paste into any AI assistant (ChatGPT, Claude, etc.)."""
    from dp.config import load_project
    from dp.engine.database import connect, ensure_meta_table
    from dp.engine.transform import discover_models

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)

    lines: list[str] = []
    lines.append(f"# dp project: {config.name}")
    lines.append("")
    lines.append("This is a dp data platform project. dp uses DuckDB for analytics,")
    lines.append("plain SQL for transforms, and Python for ingest/export scripts.")
    lines.append("")

    # Project config summary
    lines.append("## Configuration (project.yml)")
    lines.append(f"- Database: {config.database.path}")
    if config.connections:
        lines.append(f"- Connections: {', '.join(config.connections.keys())}")
    if config.streams:
        for name, s in config.streams.items():
            desc = f" — {s.description}" if s.description else ""
            sched = f" (schedule: {s.schedule})" if s.schedule else ""
            lines.append(f"- Stream '{name}'{desc}{sched}")
    lines.append("")

    # SQL models
    transform_dir = project_dir / "transform"
    models = discover_models(transform_dir)
    if models:
        lines.append("## SQL Models")
        for m in models:
            deps = f" (depends on: {', '.join(m.depends_on)})" if m.depends_on else ""
            lines.append(f"- {m.full_name} [{m.materialized}]{deps}")
        lines.append("")

    # Ingest/export scripts
    for script_type in ("ingest", "export"):
        script_dir = project_dir / script_type
        if script_dir.exists():
            scripts = sorted(f.name for f in script_dir.glob("*.py") if not f.name.startswith("_"))
            if scripts:
                lines.append(f"## {script_type.title()} Scripts")
                for s in scripts:
                    lines.append(f"- {script_type}/{s}")
                lines.append("")

    # Warehouse tables
    db_path = project_dir / config.database.path
    if db_path.exists():
        conn = connect(db_path, read_only=True)
        try:
            rows = conn.execute(
                """
                SELECT table_schema, table_name, table_type
                FROM information_schema.tables
                WHERE table_schema NOT IN ('information_schema', '_dp_internal')
                ORDER BY table_schema, table_name
                """
            ).fetchall()
            if rows:
                lines.append("## Warehouse Tables")
                for schema, name, ttype in rows:
                    lines.append(f"- {schema}.{name} ({ttype.lower()})")
                lines.append("")

            # Recent history
            ensure_meta_table(conn)
            history_rows = conn.execute(
                """
                SELECT run_type, target, status, started_at, error
                FROM _dp_internal.run_log
                ORDER BY started_at DESC
                LIMIT 10
                """
            ).fetchall()
            if history_rows:
                lines.append("## Recent Run History")
                for rtype, target, status, started, error in history_rows:
                    ts = str(started)[:19] if started else ""
                    err = f" — {error}" if error else ""
                    lines.append(f"- [{status}] {rtype}: {target} ({ts}){err}")
                lines.append("")
        finally:
            conn.close()

    lines.append("## Available Commands")
    lines.append("- dp transform — build SQL models in dependency order")
    lines.append("- dp transform --force — force rebuild all")
    lines.append("- dp run <script> — run an ingest or export script")
    lines.append("- dp stream <name> — run a full pipeline")
    lines.append("- dp query \"<sql>\" — run ad-hoc SQL queries")
    lines.append("- dp tables — list warehouse tables")
    lines.append("- dp lint — lint SQL files")
    lines.append("- dp history — show run log")
    lines.append("- dp serve — start the web UI")
    lines.append("")
    lines.append("## How to Help Me")
    lines.append("I'm working on this dp data platform project. You can help me by:")
    lines.append("- Writing SQL transform files (put them in transform/bronze/, silver/, or gold/)")
    lines.append("- Writing Python ingest scripts (put them in ingest/, must have a run(db) function)")
    lines.append("- Debugging failed pipeline runs")
    lines.append("- Writing queries to analyze data in the warehouse")
    lines.append("- Adding new data sources or exports")

    output = "\n".join(lines)
    console.print(output)
    console.print()
    console.print("[dim]---[/dim]")
    console.print("[dim]Copy the text above and paste it into any AI assistant.[/dim]")
    console.print("[dim]Then ask your question about this project.[/dim]")


if __name__ == "__main__":
    app()
