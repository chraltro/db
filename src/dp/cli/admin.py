"""Admin commands: serve, snapshot, ci, secrets, users."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.table import Table

from dp.cli import _resolve_project, app, console


# --- serve ---


@app.command()
def serve(
    host: Annotated[str, typer.Option(help="Host to bind to")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Port to bind to")] = 3000,
    watch_files: Annotated[bool, typer.Option("--watch", "-w", help="Watch files for changes")] = False,
    scheduler_on: Annotated[bool, typer.Option("--schedule", "-s", help="Enable cron scheduler")] = False,
    auth: Annotated[bool, typer.Option("--auth", help="Enable authentication")] = False,
    env: Annotated[Optional[str], typer.Option("--env", "-e", help="Environment to use")] = None,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Start the web UI server."""
    import uvicorn

    from dp import setup_logging
    from dp.engine.scheduler import FileWatcher, SchedulerThread

    setup_logging()
    project_dir = _resolve_project(project_dir)

    import dp.server.app as server_app

    server_app.PROJECT_DIR = project_dir
    server_app.AUTH_ENABLED = auth
    server_app.ACTIVE_ENV = env

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

    if env:
        console.print(f"[bold]Environment: {env}[/bold]")

    console.print(f"[bold]Starting dp server at http://{host}:{port}[/bold]")
    uvicorn.run(server_app.app, host=host, port=port)


# --- snapshot ---


snapshot_app = typer.Typer(name="snapshot", help="Manage named snapshots of project + data state.")
app.add_typer(snapshot_app)


@snapshot_app.command("create")
def snapshot_create(
    name: Annotated[Optional[str], typer.Argument(help="Snapshot name (auto-generated if omitted)")] = None,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Create a named snapshot of the current project and data state."""
    from dp.config import load_project
    from dp.engine.database import connect, ensure_meta_table
    from dp.engine.snapshot import create_snapshot

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)
    db_path = project_dir / config.database.path

    conn = connect(db_path)
    try:
        ensure_meta_table(conn)
        result = create_snapshot(conn, project_dir, name)
        console.print(f"[green]Snapshot '{result['name']}' created.[/green]")
        console.print(f"  Tables: {result['table_count']}, Files: {result['file_count']}")
    finally:
        conn.close()


@snapshot_app.command("list")
def snapshot_list(
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """List all snapshots."""
    from dp.config import load_project
    from dp.engine.database import connect, ensure_meta_table
    from dp.engine.snapshot import list_snapshots

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)
    db_path = project_dir / config.database.path

    if not db_path.exists():
        console.print("[yellow]No warehouse database found.[/yellow]")
        return

    conn = connect(db_path)
    try:
        ensure_meta_table(conn)
        snapshots = list_snapshots(conn)
        if not snapshots:
            console.print("[yellow]No snapshots. Create one with: dp snapshot create[/yellow]")
            return

        table = Table(title="Snapshots")
        table.add_column("Name", style="bold")
        table.add_column("Created")
        table.add_column("Tables", justify="right")
        table.add_column("Files", justify="right")
        for s in snapshots:
            sigs = s.get("table_signatures", {})
            manifest = s.get("file_manifest", {})
            table.add_row(
                s["name"],
                str(s["created_at"])[:19],
                str(len(sigs) if isinstance(sigs, dict) else 0),
                str(len(manifest) if isinstance(manifest, dict) else 0),
            )
        console.print(table)
    finally:
        conn.close()


@snapshot_app.command("delete")
def snapshot_delete(
    name: Annotated[str, typer.Argument(help="Snapshot name to delete")],
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Delete a snapshot."""
    from dp.config import load_project
    from dp.engine.database import connect, ensure_meta_table
    from dp.engine.snapshot import delete_snapshot

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)
    db_path = project_dir / config.database.path

    if not db_path.exists():
        console.print("[red]No warehouse database found.[/red]")
        raise typer.Exit(1)

    conn = connect(db_path)
    try:
        ensure_meta_table(conn)
        if delete_snapshot(conn, name):
            console.print(f"[green]Snapshot '{name}' deleted.[/green]")
        else:
            console.print(f"[red]Snapshot '{name}' not found.[/red]")
            raise typer.Exit(1)
    finally:
        conn.close()


# --- ci ---


ci_app = typer.Typer(name="ci", help="GitHub Actions CI integration.")
app.add_typer(ci_app)


@ci_app.command("generate")
def ci_generate(
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Generate a GitHub Actions workflow for dp CI."""
    from dp.engine.ci import generate_workflow

    project_dir = _resolve_project(project_dir)
    result = generate_workflow(project_dir)
    console.print(f"[green]Generated {result['path']}[/green]")
    console.print()
    console.print("[bold]Next steps:[/bold]")
    console.print("  1. Review the generated workflow file")
    console.print("  2. Commit and push to your repository")
    console.print("  3. Open a pull request to see dp diff results as PR comments")


@ci_app.command("diff-comment")
def ci_diff_comment(
    json_path: Annotated[str, typer.Option("--json", help="Path to diff-results.json")] = "diff-results.json",
    repo: Annotated[Optional[str], typer.Option("--repo", help="GitHub repo (owner/repo)")] = None,
    pr: Annotated[Optional[int], typer.Option("--pr", help="Pull request number")] = None,
) -> None:
    """Post a formatted diff comment to a GitHub pull request."""
    from dp.engine.ci import post_diff_comment

    result = post_diff_comment(json_path, repo, pr)
    if result.get("error"):
        console.print(f"[red]{result['error']}[/red]")
        raise typer.Exit(1)
    console.print(f"[green]Posted diff comment to PR #{result.get('pr')}[/green]")


# --- secrets ---

secrets_app = typer.Typer(name="secrets", help="Manage .env secrets.")
app.add_typer(secrets_app)


@secrets_app.command("list")
def secrets_list(
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
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
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Set or update a secret in .env."""
    from dp.engine.secrets import set_secret

    project_dir = _resolve_project(project_dir)
    set_secret(project_dir, key, value)
    console.print(f"[green]Secret '{key}' set.[/green]")


@secrets_app.command("delete")
def secrets_delete(
    key: Annotated[str, typer.Argument(help="Secret key")],
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
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
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
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
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
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
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
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
