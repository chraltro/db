"""Connector commands: connect, connectors subapp (list, test, sync, remove, regenerate, available)."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.table import Table

from dp.cli import _resolve_project, app, console


# --- connect ---


@app.command()
def connect(
    connector_type: Annotated[str, typer.Argument(help="Connector type (e.g. postgres, stripe, google-sheets)")],
    name: Annotated[Optional[str], typer.Option("--name", "-n", help="Connection name")] = None,
    tables: Annotated[Optional[str], typer.Option("--tables", "-t", help="Comma-separated tables to sync")] = None,
    target_schema: Annotated[str, typer.Option("--schema", "-s", help="Target schema")] = "landing",
    schedule: Annotated[Optional[str], typer.Option("--schedule", help="Cron schedule")] = None,
    test_only: Annotated[bool, typer.Option("--test", help="Only test the connection")] = False,
    discover_only: Annotated[bool, typer.Option("--discover", help="Only discover available tables")] = False,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
    config_json: Annotated[Optional[str], typer.Option("--config", "-c", help="JSON string or file path with connector params")] = None,
    # Convenience shortcuts for the most common params (override --config)
    host: Annotated[Optional[str], typer.Option(help="Host (shortcut for config)")] = None,
    port: Annotated[Optional[int], typer.Option(help="Port (shortcut for config)")] = None,
    database: Annotated[Optional[str], typer.Option(help="Database name (shortcut for config)")] = None,
    user: Annotated[Optional[str], typer.Option(help="Username (shortcut for config)")] = None,
    password: Annotated[Optional[str], typer.Option(help="Password (shortcut for config)")] = None,
    url: Annotated[Optional[str], typer.Option(help="URL (shortcut for config)")] = None,
    api_key: Annotated[Optional[str], typer.Option(help="API key (shortcut for config)")] = None,
    token: Annotated[Optional[str], typer.Option(help="Access token (shortcut for config)")] = None,
    path: Annotated[Optional[str], typer.Option(help="File/bucket path (shortcut for config)")] = None,
    set: Annotated[Optional[list[str]], typer.Option("--set", help="Set param as key=value (repeatable)")] = None,
) -> None:
    """Set up a data connector: test connection, generate ingest script, schedule sync.

    Pass connector parameters via --config (JSON string or file path), --set key=value
    flags, or convenience shortcuts like --host, --database, --api-key.

    Examples:
      dp connect postgres --host localhost --database mydb --user admin --password secret
      dp connect stripe --api-key sk_live_xxx
      dp connect csv --path /data/customers.csv
      dp connect postgres --config '{"host":"db.prod","database":"app","user":"ro","password":"s3cret"}'
      dp connect postgres --config ./postgres.json
      dp connect hubspot --set api_key=xxx --set objects=contacts,deals
    """
    import json as json_mod
    import dp.connectors  # noqa: F401 — registers all connectors
    from dp.engine.connector import (
        discover_connector,
        get_connector,
        list_connectors,
        setup_connector,
        test_connector,
    )

    # Normalize connector type (allow hyphens)
    connector_type = connector_type.replace("-", "_")

    # Show available connectors
    if connector_type == "list":
        available = list_connectors()
        table_out = Table(title="Available Connectors")
        table_out.add_column("Name", style="bold")
        table_out.add_column("Display Name")
        table_out.add_column("Description")
        table_out.add_column("Schedule")
        for c in available:
            table_out.add_row(
                c["name"],
                c["display_name"],
                c["description"],
                c.get("default_schedule") or "on-demand",
            )
        console.print(table_out)
        return

    # Build config: start from --config (JSON string or file), then overlay --set and shortcuts
    config: dict = {}
    if config_json is not None:
        # Try as file path first, then as raw JSON
        config_path = Path(config_json)
        if config_path.exists():
            try:
                config = json_mod.loads(config_path.read_text())
            except json_mod.JSONDecodeError as e:
                console.print(f"[red]Invalid JSON in {config_json}: {e}[/red]")
                raise typer.Exit(1)
        else:
            try:
                config = json_mod.loads(config_json)
            except json_mod.JSONDecodeError as e:
                console.print(f"[red]Invalid JSON: {e}[/red]")
                raise typer.Exit(1)
        if not isinstance(config, dict):
            console.print("[red]--config must be a JSON object[/red]")
            raise typer.Exit(1)

    # --set key=value overrides
    if set:
        for item in set:
            if "=" not in item:
                console.print(f"[red]Invalid --set format: {item!r} (expected key=value)[/red]")
                raise typer.Exit(1)
            k, v = item.split("=", 1)
            config[k.strip()] = v.strip()

    # Convenience shortcuts override --config and --set
    _shortcuts = {
        "host": host, "port": port, "database": database, "user": user,
        "password": password, "url": url, "api_key": api_key,
        "access_token": token, "path": path,
    }
    for k, v in _shortcuts.items():
        if v is not None:
            config[k] = v

    # Validate connector exists
    try:
        connector = get_connector(connector_type)
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(1)

    # Fill defaults from param spec
    for pspec in connector.params:
        if pspec.name not in config and pspec.default is not None:
            config[pspec.name] = pspec.default

    # Check required params
    missing = [
        p.name for p in connector.params
        if p.required and p.name not in config
    ]
    if missing:
        console.print(f"[red]Missing required parameters: {', '.join(missing)}[/red]")
        console.print()
        console.print(f"[bold]{connector.display_name}[/bold] parameters:")
        for p in connector.params:
            req = "[red]*[/red]" if p.required else " "
            default = f" [dim](default: {p.default})[/dim]" if p.default else ""
            secret = " [yellow](secret)[/yellow]" if p.secret else ""
            console.print(f"  {req} --set {p.name}=...: {p.description}{default}{secret}")
        console.print()
        console.print("[dim]Tip: pass all params at once with --config '{...}' or --config file.json[/dim]")
        raise typer.Exit(1)

    connection_name = name or f"{connector_type}_{config.get('database', config.get('store', config.get('table_name', 'default')))}"

    # Test only
    if test_only:
        console.print(f"[bold]Testing {connector.display_name} connection...[/bold]")
        result = test_connector(connector_type, config)
        if result.get("success"):
            console.print("[green]Connection successful![/green]")
        else:
            console.print(f"[red]Connection failed: {result.get('error')}[/red]")
            raise typer.Exit(1)
        return

    # Discover only
    if discover_only:
        console.print(f"[bold]Discovering {connector.display_name} resources...[/bold]")
        resources_list = discover_connector(connector_type, config)
        if not resources_list:
            console.print("[yellow]No resources found.[/yellow]")
            return
        table_out = Table(title="Available Resources")
        table_out.add_column("Name", style="bold")
        table_out.add_column("Schema")
        table_out.add_column("Description")
        for r in resources_list:
            table_out.add_row(r["name"], r.get("schema", ""), r.get("description", ""))
        console.print(table_out)
        return

    # Full setup
    project_dir = _resolve_project(project_dir)
    console.print(f"[bold]Setting up {connector.display_name} connector...[/bold]")
    console.print()

    console.print("  [blue]test[/blue]  Testing connection...")
    table_list = [t.strip() for t in tables.split(",")] if tables else None

    result = setup_connector(
        project_dir=project_dir,
        connector_type=connector_type,
        connection_name=connection_name,
        config=config,
        tables=table_list,
        target_schema=target_schema,
        schedule=schedule,
    )

    if result["status"] == "error":
        console.print(f"  [red]fail[/red]  {result.get('error')}")
        raise typer.Exit(1)

    console.print("[green]  done[/green]  Connection verified")
    console.print(f"[green]  done[/green]  Generated {result['script_path']}")
    console.print(f"[green]  done[/green]  Added connection '{result['connection_name']}' to project.yml")
    if result.get("schedule"):
        console.print(f"[green]  done[/green]  Scheduled sync: {result['schedule']}")
    console.print()
    console.print(f"  Tables to sync: {', '.join(result.get('tables', []))}")
    console.print()
    console.print("[bold]Next steps:[/bold]")
    console.print(f"  dp run {result['script_path']}          # run sync now")
    console.print(f"  dp transform                             # build downstream models")
    console.print(f"  dp connect --test --name {connection_name}  # re-test later")


# --- connectors subapp ---

connectors_app = typer.Typer(name="connectors", help="Manage configured data connectors.")
app.add_typer(connectors_app)


@connectors_app.command("list")
def connectors_list(
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """List configured connectors."""
    import dp.connectors  # noqa: F401
    from dp.engine.connector import list_configured_connectors

    project_dir = _resolve_project(project_dir)
    connectors = list_configured_connectors(project_dir)

    if not connectors:
        console.print("[yellow]No connectors configured. Set one up with: dp connect <type>[/yellow]")
        return

    table = Table(title="Configured Connectors")
    table.add_column("Name", style="bold")
    table.add_column("Type", style="cyan")
    table.add_column("Script")
    table.add_column("Status")
    for c in connectors:
        status = "[green]ready[/green]" if c["has_script"] else "[yellow]no script[/yellow]"
        table.add_row(c["name"], c["type"], c["script_path"], status)
    console.print(table)


@connectors_app.command("test")
def connectors_test(
    connection_name: Annotated[str, typer.Argument(help="Connection name from project.yml")],
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Test a configured connector."""
    import dp.connectors  # noqa: F401
    from dp.config import load_project
    from dp.engine.connector import test_connector

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)

    if connection_name not in config.connections:
        console.print(f"[red]Connection '{connection_name}' not found in project.yml[/red]")
        raise typer.Exit(1)

    conn_config = config.connections[connection_name]
    console.print(f"[bold]Testing '{connection_name}' ({conn_config.type})...[/bold]")

    result = test_connector(conn_config.type, conn_config.params)
    if result.get("success"):
        console.print("[green]Connection successful![/green]")
    else:
        console.print(f"[red]Connection failed: {result.get('error')}[/red]")
        raise typer.Exit(1)


@connectors_app.command("sync")
def connectors_sync(
    connection_name: Annotated[str, typer.Argument(help="Connection name")],
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Run sync for a configured connector."""
    import dp.connectors  # noqa: F401
    from dp.engine.connector import sync_connector

    project_dir = _resolve_project(project_dir)

    console.print(f"[bold]Syncing '{connection_name}'...[/bold]")
    result = sync_connector(project_dir, connection_name)

    if result.get("status") == "error":
        console.print(f"[red]Sync failed: {result.get('error')}[/red]")
        raise typer.Exit(1)

    console.print(f"[green]Sync completed ({result.get('duration_ms', 0)}ms)[/green]")


@connectors_app.command("remove")
def connectors_remove(
    connection_name: Annotated[str, typer.Argument(help="Connection name to remove")],
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Remove a configured connector (deletes script and config)."""
    import dp.connectors  # noqa: F401
    from dp.engine.connector import remove_connector

    project_dir = _resolve_project(project_dir)
    result = remove_connector(project_dir, connection_name)

    if result["status"] == "error":
        console.print(f"[red]{result.get('error')}[/red]")
        raise typer.Exit(1)

    console.print(f"[green]Connector '{connection_name}' removed.[/green]")


@connectors_app.command("regenerate")
def connectors_regenerate(
    connection_name: Annotated[str, typer.Argument(help="Connection name to regenerate")],
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Regenerate the ingest script for a connector from current config."""
    import dp.connectors  # noqa: F401
    from dp.engine.connector import regenerate_connector

    project_dir = _resolve_project(project_dir)
    result = regenerate_connector(project_dir, connection_name)

    if result["status"] == "error":
        console.print(f"[red]{result.get('error')}[/red]")
        raise typer.Exit(1)

    console.print(f"[green]Regenerated script: {result['script_path']}[/green]")
    if result.get("tables"):
        console.print(f"Tables: {', '.join(result['tables'])}")


@connectors_app.command("available")
def connectors_available() -> None:
    """List all available connector types."""
    import dp.connectors  # noqa: F401
    from dp.engine.connector import list_connectors

    available = list_connectors()
    table = Table(title="Available Connector Types")
    table.add_column("Name", style="bold")
    table.add_column("Display Name")
    table.add_column("Description")
    table.add_column("Default Schedule")
    for c in available:
        table.add_row(
            c["name"].replace("_", "-"),
            c["display_name"],
            c["description"],
            c.get("default_schedule") or "on-demand",
        )
    console.print(table)
    console.print()
    console.print("Set up a connector with: [bold]dp connect <type>[/bold]")
