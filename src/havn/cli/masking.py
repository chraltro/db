"""Masking policy CLI commands: havn mask list|add|remove."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.table import Table

from havn.cli import _load_config, _resolve_project, app, console


@app.command("mask")
def mask(
    action: Annotated[str, typer.Argument(help="Action: list, add, or remove")],
    schema: Annotated[Optional[str], typer.Option("--schema", "-s", help="Schema name")] = None,
    table: Annotated[Optional[str], typer.Option("--table", "-t", help="Table name")] = None,
    column: Annotated[Optional[str], typer.Option("--column", "-c", help="Column name")] = None,
    method: Annotated[Optional[str], typer.Option("--method", "-m", help="Masking method: hash, redact, null, partial")] = None,
    show_first: Annotated[Optional[int], typer.Option(help="Partial mask: chars to show at start")] = None,
    show_last: Annotated[Optional[int], typer.Option(help="Partial mask: chars to show at end")] = None,
    condition_column: Annotated[Optional[str], typer.Option("--condition-column", help="Only mask when this column...")] = None,
    condition_value: Annotated[Optional[str], typer.Option("--condition-value", help="...equals this value")] = None,
    policy_id: Annotated[Optional[str], typer.Option("--id", help="Policy ID (for remove)")] = None,
    env: Annotated[Optional[str], typer.Option("--env", "-e", help="Environment to use")] = None,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory")] = None,
) -> None:
    """Manage column-level data masking policies.

    Actions: list, add, remove.

    Examples:
        havn mask list
        havn mask add --schema gold --table customers --column email --method redact
        havn mask add --schema gold --table customers --column ssn --method partial --show-first 0 --show-last 4
        havn mask remove --id <policy-id>
    """
    from havn.engine.database import connect, ensure_meta_table
    from havn.engine.masking import (
        create_policy,
        delete_policy,
        ensure_masking_table,
        list_policies,
    )

    project_dir = _resolve_project(project_dir)
    config = _load_config(project_dir, env)
    db_path = project_dir / config.database.path

    if not db_path.exists():
        console.print("[yellow]No warehouse database found.[/yellow]")
        raise typer.Exit(1)

    conn = connect(db_path)
    try:
        ensure_meta_table(conn)
        ensure_masking_table(conn)

        if action == "list":
            policies = list_policies(conn)
            if not policies:
                console.print("[yellow]No masking policies defined.[/yellow]")
                return
            tbl = Table(title="Masking Policies")
            tbl.add_column("ID", style="dim")
            tbl.add_column("Schema")
            tbl.add_column("Table")
            tbl.add_column("Column", style="bold")
            tbl.add_column("Method")
            tbl.add_column("Condition")
            tbl.add_column("Exempted")
            for p in policies:
                cond = ""
                if p["condition_column"]:
                    cond = f"{p['condition_column']}={p['condition_value']}"
                tbl.add_row(
                    p["id"][:8] + "...",
                    p["schema_name"],
                    p["table_name"],
                    p["column_name"],
                    p["method"],
                    cond or "-",
                    ", ".join(p["exempted_roles"]),
                )
            console.print(tbl)

        elif action == "add":
            if not all([schema, table, column, method]):
                console.print("[red]--schema, --table, --column, and --method are required for 'add'.[/red]")
                raise typer.Exit(1)
            method_config = None
            if method == "partial":
                method_config = {
                    "show_first": show_first or 0,
                    "show_last": show_last or 0,
                }
            try:
                policy = create_policy(
                    conn,
                    schema_name=schema,
                    table_name=table,
                    column_name=column,
                    method=method,
                    method_config=method_config,
                    condition_column=condition_column,
                    condition_value=condition_value,
                )
                console.print(f"[green]Policy created:[/green] {policy['id'][:8]}... "
                              f"({schema}.{table}.{column} -> {method})")
            except ValueError as e:
                console.print(f"[red]{e}[/red]")
                raise typer.Exit(1)

        elif action == "remove":
            if not policy_id:
                console.print("[red]--id is required for 'remove'.[/red]")
                raise typer.Exit(1)
            if delete_policy(conn, policy_id):
                console.print(f"[green]Policy {policy_id[:8]}... deleted.[/green]")
            else:
                console.print(f"[red]Policy not found: {policy_id}[/red]")
                raise typer.Exit(1)

        else:
            console.print(f"[red]Unknown action: {action}. Use list, add, or remove.[/red]")
            raise typer.Exit(1)
    finally:
        conn.close()
