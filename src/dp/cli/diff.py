"""Diff command: compare model SQL output against materialized tables."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.table import Table

from dp.cli import _resolve_project, app, console


@app.command()
def diff(
    targets: Annotated[Optional[list[str]], typer.Argument(help="Models to diff (e.g. gold.earthquake_summary)")] = None,
    target: Annotated[Optional[str], typer.Option("--target", "-t", help="Diff all models in a schema")] = None,
    format: Annotated[str, typer.Option("--format", "-f", help="Output format: table or json")] = "table",
    rows: Annotated[bool, typer.Option("--rows", help="Include sample rows in output")] = False,
    full: Annotated[bool, typer.Option("--full", help="Show all changed rows, not just samples")] = False,
    against: Annotated[Optional[str], typer.Option("--against", help="Only diff models changed vs a git branch/ref")] = None,
    snapshot: Annotated[Optional[str], typer.Option("--snapshot", help="Compare current state against a snapshot")] = None,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Show what would change if transforms are run now. Compares model SQL output against materialized tables."""
    import json as json_mod

    from dp.config import load_project
    from dp.engine.database import connect, ensure_meta_table
    from dp.engine.diff import DiffResult, diff_model, diff_models, get_primary_key

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)
    transform_dir = project_dir / "transform"
    db_path = project_dir / config.database.path

    if not db_path.exists():
        console.print("[yellow]No warehouse database found. Run a pipeline first.[/yellow]")
        raise typer.Exit(1)

    # Snapshot comparison mode
    if snapshot:
        from dp.engine.snapshot import diff_against_snapshot
        conn = connect(db_path)
        try:
            snap_results = diff_against_snapshot(conn, project_dir, snapshot)
            if snap_results is None:
                console.print(f"[red]Snapshot '{snapshot}' not found.[/red]")
                raise typer.Exit(1)
            if format == "json":
                console.print(json_mod.dumps(snap_results, indent=2, default=str))
            else:
                _print_snapshot_diff(snap_results)
        finally:
            conn.close()
        return

    # Git-aware diff: only diff models whose SQL files changed
    filter_targets = None
    if against:
        from dp.engine.git import diff_files_between, is_git_repo
        from dp.engine.transform import discover_models as _discover
        if not is_git_repo(project_dir):
            console.print("[red]Not a git repository. Cannot use --against.[/red]")
            raise typer.Exit(1)
        changed_files = diff_files_between(project_dir, against, "HEAD")
        # Map changed .sql files to model targets
        models = _discover(transform_dir)
        model_by_path = {str(m.path.relative_to(project_dir)): m.full_name for m in models}
        filter_targets = []
        for f in changed_files:
            if f in model_by_path:
                filter_targets.append(model_by_path[f])
        if not filter_targets:
            console.print(f"[green]No model SQL files changed vs {against}. Nothing to diff.[/green]")
            return

    conn = connect(db_path)
    try:
        ensure_meta_table(conn)
        diff_targets = targets or filter_targets
        results = diff_models(
            conn,
            transform_dir,
            targets=diff_targets,
            target_schema=target,
            project_config=config,
            full=full,
        )

        if not results:
            console.print("[yellow]No models found to diff.[/yellow]")
            return

        if format == "json":
            json_out = [_diff_result_to_dict(r) for r in results]
            console.print(json_mod.dumps(json_out, indent=2, default=str))
        else:
            _print_diff_summary(results)
            # Detailed output for single model
            if (targets and len(targets) == 1) or (rows or full):
                for r in results:
                    _print_diff_detail(r, show_rows=rows or full)
    finally:
        conn.close()


def _diff_result_to_dict(r) -> dict:
    """Convert a DiffResult to a JSON-serializable dict."""
    return {
        "model": r.model,
        "added": r.added,
        "removed": r.removed,
        "modified": r.modified,
        "total_before": r.total_before,
        "total_after": r.total_after,
        "is_new": r.is_new,
        "error": r.error,
        "schema_changes": [
            {"column": sc.column, "change_type": sc.change_type,
             "old_type": sc.old_type, "new_type": sc.new_type}
            for sc in r.schema_changes
        ],
        "sample_added": r.sample_added,
        "sample_removed": r.sample_removed,
        "sample_modified": r.sample_modified,
    }


def _print_diff_summary(results) -> None:
    """Print a summary table of diff results."""
    table = Table(title="Diff Summary")
    table.add_column("Model", style="bold")
    table.add_column("Before", justify="right")
    table.add_column("After", justify="right")
    table.add_column("Added", justify="right", style="green")
    table.add_column("Removed", justify="right", style="red")
    table.add_column("Modified", justify="right", style="yellow")
    table.add_column("Schema")

    for r in results:
        if r.error:
            error_display = r.error[:80] + ("..." if len(r.error) > 80 else "")
            table.add_row(r.model, "", "", "", "", "", f"[red]ERROR: {error_display}[/red]")
            continue

        before = "NEW" if r.is_new else f"{r.total_before:,}"
        after = f"{r.total_after:,}"
        added = f"+{r.added}" if r.added else "0"
        removed = str(r.removed) if r.removed else "0"
        modified = str(r.modified) if r.modified else "0"

        schema_label = "\u2014"
        if r.schema_changes:
            adds = sum(1 for sc in r.schema_changes if sc.change_type == "added")
            removes = sum(1 for sc in r.schema_changes if sc.change_type == "removed")
            changes = sum(1 for sc in r.schema_changes if sc.change_type == "type_changed")
            parts = []
            if adds:
                parts.append(f"+{adds} col")
            if removes:
                parts.append(f"-{removes} col")
            if changes:
                parts.append(f"~{changes} col")
            schema_label = "[blue]" + ", ".join(parts) + "[/blue]"

        table.add_row(r.model, before, after, added, removed, modified, schema_label)

    console.print(table)


def _print_diff_detail(r, show_rows: bool = False) -> None:
    """Print detailed diff output for a single model."""
    if r.error:
        console.print(f"\n[red]Error diffing {r.model}: {r.error}[/red]")
        return

    if r.schema_changes:
        console.print(f"\n[bold]Schema changes for {r.model}:[/bold]")
        for sc in r.schema_changes:
            if sc.change_type == "added":
                console.print(f"  [green]+[/green] {sc.column} ({sc.new_type})")
            elif sc.change_type == "removed":
                console.print(f"  [red]-[/red] {sc.column} ({sc.old_type})")
            elif sc.change_type == "type_changed":
                console.print(f"  [yellow]~[/yellow] {sc.column}: {sc.old_type} -> {sc.new_type}")

    if show_rows:
        if r.sample_added:
            console.print(f"\n[green]Added rows ({r.added}):[/green]")
            _print_sample_table(r.sample_added)
        if r.sample_removed:
            console.print(f"\n[red]Removed rows ({r.removed}):[/red]")
            _print_sample_table(r.sample_removed)
        if r.sample_modified:
            console.print(f"\n[yellow]Modified rows ({r.modified}):[/yellow]")
            _print_sample_table(r.sample_modified)


def _print_sample_table(rows: list[dict]) -> None:
    """Print a list of dicts as a Rich table."""
    if not rows:
        return
    table = Table()
    for col in rows[0].keys():
        table.add_column(col)
    for row in rows:
        table.add_row(*[str(v) for v in row.values()])
    console.print(table)


def _print_snapshot_diff(snap_results: dict) -> None:
    """Print snapshot comparison results."""
    console.print(f"[bold]Snapshot: {snap_results['snapshot_name']}[/bold]")
    console.print(f"Created: {snap_results['created_at']}")
    console.print()

    # File changes
    file_changes = snap_results.get("file_changes", {})
    if file_changes.get("added") or file_changes.get("removed") or file_changes.get("modified"):
        console.print("[bold]File changes:[/bold]")
        for f in file_changes.get("added", []):
            console.print(f"  [green]+[/green] {f}")
        for f in file_changes.get("removed", []):
            console.print(f"  [red]-[/red] {f}")
        for f in file_changes.get("modified", []):
            console.print(f"  [yellow]~[/yellow] {f}")
        console.print()

    # Table changes
    table_changes = snap_results.get("table_changes", [])
    if table_changes:
        table = Table(title="Table Changes")
        table.add_column("Table", style="bold")
        table.add_column("Status")
        table.add_column("Before Rows", justify="right")
        table.add_column("After Rows", justify="right")
        for tc in table_changes:
            table.add_row(
                tc["table"],
                tc["status"],
                str(tc.get("snapshot_rows", "")),
                str(tc.get("current_rows", "")),
            )
        console.print(table)
    else:
        console.print("[green]No table changes detected.[/green]")
