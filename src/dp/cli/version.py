"""Warehouse versioning commands: version create/list/diff/restore/timeline/cleanup."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.table import Table

from dp.cli import _resolve_project, app, console


@app.command()
def version(
    action: Annotated[str, typer.Argument(help="Action: create, list, diff, restore, timeline, cleanup")] = "list",
    version_id: Annotated[Optional[str], typer.Option("--id", "-i", help="Version ID (for diff, restore, timeline)")] = None,
    table: Annotated[Optional[str], typer.Option("--table", "-t", help="Table name (for timeline)")] = None,
    description: Annotated[Optional[str], typer.Option("--desc", "-d", help="Description (for create)")] = None,
    from_version: Annotated[Optional[str], typer.Option("--from", help="From version (for diff)")] = None,
    keep: Annotated[int, typer.Option("--keep", help="Versions to keep (for cleanup)")] = 10,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory")] = None,
) -> None:
    """Manage warehouse versions with Parquet-based time travel.

    Examples:
      dp version create --desc "Before migration"
      dp version list
      dp version diff --from run-5
      dp version diff --from run-5 --id run-8
      dp version restore --id run-5
      dp version timeline --table gold.customers
      dp version cleanup --keep 5
    """
    from dp.config import load_project
    from dp.engine.database import connect
    from dp.engine.versioning import (
        cleanup_old_versions,
        create_version,
        diff_versions,
        list_versions,
        restore_version,
        table_timeline,
    )

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)
    db_path = project_dir / config.database.path

    if not db_path.exists():
        console.print("[yellow]No warehouse database found. Run a pipeline first.[/yellow]")
        raise typer.Exit(1)

    conn = connect(db_path)
    try:
        if action == "create":
            result = create_version(conn, project_dir, description=description or "")
            console.print(f"[green]Version created:[/green] {result['version_id']}")
            console.print(f"  Tables: {result['table_count']}")
            if result.get("description"):
                console.print(f"  Description: {result['description']}")

        elif action == "list":
            versions = list_versions(conn)
            if not versions:
                console.print("[yellow]No versions yet. Use 'dp version create' to create one.[/yellow]")
                return
            tbl = Table(title="Warehouse Versions")
            tbl.add_column("Version", style="bold")
            tbl.add_column("Created At")
            tbl.add_column("Trigger")
            tbl.add_column("Tables")
            tbl.add_column("Total Rows", justify="right")
            tbl.add_column("Description")
            for v in versions:
                tbl.add_row(
                    v["version_id"],
                    v["created_at"][:19],
                    v["trigger"],
                    str(v["table_count"]),
                    f"{v['total_rows']:,}",
                    v["description"][:50] if v["description"] else "",
                )
            console.print(tbl)

        elif action == "diff":
            if not from_version:
                console.print("[red]--from is required for diff[/red]")
                raise typer.Exit(1)
            result = diff_versions(conn, project_dir, from_version, version_id)
            if "error" in result:
                console.print(f"[red]{result['error']}[/red]")
                raise typer.Exit(1)
            changes = result["changes"]
            if not changes:
                console.print(f"[green]No changes between {from_version} and {result['to_version']}[/green]")
                return
            tbl = Table(title=f"Changes: {from_version} -> {result['to_version']}")
            tbl.add_column("Table", style="bold")
            tbl.add_column("Change")
            tbl.add_column("Rows Before", justify="right")
            tbl.add_column("Rows After", justify="right")
            tbl.add_column("Diff", justify="right")
            for c in changes:
                diff_str = ""
                if "row_diff" in c:
                    diff_str = f"+{c['row_diff']}" if c["row_diff"] > 0 else str(c["row_diff"])
                tbl.add_row(
                    c["table"],
                    c["change"],
                    str(c.get("rows_before", "")),
                    str(c.get("rows_after", c.get("rows", ""))),
                    diff_str,
                )
            console.print(tbl)

        elif action == "restore":
            if not version_id:
                console.print("[red]--id is required for restore[/red]")
                raise typer.Exit(1)
            result = restore_version(conn, project_dir, version_id)
            if "error" in result:
                console.print(f"[red]{result['error']}[/red]")
                raise typer.Exit(1)
            console.print(f"[green]Restored {result['tables_restored']} table(s) from {version_id}[/green]")
            for d in result["details"]:
                status = "[green]ok[/green]" if d["status"] == "success" else "[red]fail[/red]"
                rows = f" ({d.get('rows_restored', 0):,} rows)" if d["status"] == "success" else ""
                console.print(f"  {status}  {d['table']}{rows}")

        elif action == "timeline":
            if not table:
                console.print("[red]--table is required for timeline[/red]")
                raise typer.Exit(1)
            timeline = table_timeline(conn, table)
            if not timeline:
                console.print(f"[yellow]No version history for {table}[/yellow]")
                return
            tbl = Table(title=f"Timeline: {table}")
            tbl.add_column("Version", style="bold")
            tbl.add_column("Created At")
            tbl.add_column("Trigger")
            tbl.add_column("Rows", justify="right")
            tbl.add_column("Description")
            for t in timeline:
                tbl.add_row(
                    t["version_id"],
                    t["created_at"][:19],
                    t["trigger"],
                    f"{t['row_count']:,}",
                    t["description"][:40] if t["description"] else "",
                )
            console.print(tbl)

        elif action == "cleanup":
            result = cleanup_old_versions(project_dir, conn, keep=keep)
            console.print(f"[green]Cleaned up {result['removed']} version(s), kept {result['kept']}[/green]")

        else:
            console.print(f"[red]Unknown action: {action}[/red]")
            console.print("Available: create, list, diff, restore, timeline, cleanup")
            raise typer.Exit(1)
    finally:
        conn.close()
