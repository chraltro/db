"""Pipeline Rewind commands: history, restore, gc."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.table import Table

from dp.cli import _load_config, _resolve_project, app, console


@app.command()
def rewind(
    action: Annotated[str, typer.Argument(help="Action: history, runs, snapshot, gc")] = "runs",
    run_id: Annotated[Optional[str], typer.Option("--run", "-r", help="Run ID to inspect")] = None,
    model: Annotated[Optional[str], typer.Option("--model", "-m", help="Model name")] = None,
    limit: Annotated[int, typer.Option("--limit", "-n", help="Number of results")] = 20,
    env: Annotated[Optional[str], typer.Option("--env", "-e", help="Environment")] = None,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory")] = None,
) -> None:
    """Pipeline Rewind: browse snapshot history and inspect past runs.

    Actions:
      runs       List recent pipeline runs with snapshot counts
      snapshot   View snapshot details for a run (--run required)
      sample     Preview snapshot data (--run and --model required)
      gc         Run garbage collection on expired snapshots
    """
    from dp.engine.snapshots import (
        get_all_snapshots,
        get_runs,
        get_snapshot_sample,
        get_snapshots_for_run,
        run_gc,
    )

    project_dir = _resolve_project(project_dir)
    config = _load_config(project_dir, env)

    if not config.rewind.enabled:
        console.print("[yellow]Pipeline Rewind is disabled in project.yml[/yellow]")
        console.print("Set [bold]rewind.enabled: true[/bold] to enable it.")
        return

    if action == "runs":
        runs = get_runs(project_dir, limit=limit)
        if not runs:
            console.print("[yellow]No pipeline runs recorded yet.[/yellow]")
            console.print("Run [bold]dp transform[/bold] to start capturing snapshots.")
            return

        tbl = Table(title="Pipeline Runs (Rewind)")
        tbl.add_column("Run ID", style="dim", max_width=36)
        tbl.add_column("Started", style="bold")
        tbl.add_column("Status")
        tbl.add_column("Trigger")
        tbl.add_column("Models", justify="right")

        for r in runs:
            status_style = {
                "success": "[green]success[/green]",
                "failed": "[red]failed[/red]",
                "partial": "[yellow]partial[/yellow]",
                "running": "[blue]running[/blue]",
            }.get(r.status, r.status)
            tbl.add_row(
                r.run_id[:12] + "...",
                r.started_at[:19] if r.started_at else "-",
                status_style,
                r.trigger,
                str(len(r.models_run)),
            )
        console.print(tbl)
        console.print(f"\n[dim]Use [bold]dp rewind snapshot --run <run_id>[/bold] to inspect a run[/dim]")

    elif action == "snapshot":
        if not run_id:
            console.print("[red]--run is required for snapshot action[/red]")
            raise typer.Exit(1)

        # Allow prefix matching
        runs = get_runs(project_dir, limit=500)
        matched = [r for r in runs if r.run_id.startswith(run_id)]
        if not matched:
            console.print(f"[red]No run found matching '{run_id}'[/red]")
            raise typer.Exit(1)
        full_run_id = matched[0].run_id

        snapshots = get_snapshots_for_run(project_dir, full_run_id)
        if not snapshots:
            console.print(f"[yellow]No snapshots found for run {full_run_id[:12]}...[/yellow]")
            return

        tbl = Table(title=f"Snapshots for Run {full_run_id[:12]}...")
        tbl.add_column("Model", style="bold")
        tbl.add_column("Rows", justify="right")
        tbl.add_column("Cols", justify="right")
        tbl.add_column("Size", justify="right")
        tbl.add_column("Restorable")

        for s in snapshots:
            size_str = f"{s.size_bytes / 1024:.1f} KB" if s.size_bytes < 1_048_576 else f"{s.size_bytes / 1_048_576:.1f} MB"
            restorable = "[green]yes[/green]" if s.file_path else "[dim]expired[/dim]"
            tbl.add_row(s.model_name, f"{s.row_count:,}", str(s.col_count), size_str, restorable)
        console.print(tbl)

    elif action == "sample":
        if not run_id or not model:
            console.print("[red]--run and --model are required for sample action[/red]")
            raise typer.Exit(1)

        runs = get_runs(project_dir, limit=500)
        matched = [r for r in runs if r.run_id.startswith(run_id)]
        if not matched:
            console.print(f"[red]No run found matching '{run_id}'[/red]")
            raise typer.Exit(1)
        full_run_id = matched[0].run_id

        result = get_snapshot_sample(project_dir, full_run_id, model, limit=limit)
        if "error" in result and result["error"]:
            console.print(f"[red]{result['error']}[/red]")
            raise typer.Exit(1)

        if result["columns"] and result["rows"]:
            tbl = Table(title=f"Sample: {model} @ {full_run_id[:12]}...")
            for col in result["columns"]:
                tbl.add_column(col)
            for row in result["rows"][:limit]:
                tbl.add_row(*[str(v) if v is not None else "" for v in row])
            console.print(tbl)
        else:
            console.print("[yellow]No data in snapshot.[/yellow]")

    elif action == "gc":
        from dp.engine.snapshots import RewindConfig

        rw_cfg = RewindConfig(
            enabled=config.rewind.enabled,
            retention=config.rewind.retention,
            max_storage=config.rewind.max_storage,
            dedup=config.rewind.dedup,
            exclude=config.rewind.exclude,
        )
        deleted = run_gc(project_dir, rw_cfg)
        if deleted:
            console.print(f"[green]Cleaned up {deleted} expired snapshot file(s).[/green]")
        else:
            console.print("[dim]No expired snapshots to clean up.[/dim]")

    else:
        console.print(f"[red]Unknown action: {action}[/red]")
        console.print("Available: runs, snapshot, sample, gc")
        raise typer.Exit(1)


@app.command()
def restore(
    model: Annotated[str, typer.Argument(help="Model name to restore (e.g. silver.customers)")],
    run_id: Annotated[str, typer.Option("--run", "-r", help="Run ID to restore from")],
    cascade: Annotated[bool, typer.Option("--cascade/--no-cascade", help="Re-run downstream models")] = True,
    env: Annotated[Optional[str], typer.Option("--env", "-e", help="Environment")] = None,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory")] = None,
) -> None:
    """Restore a model to a previous snapshot state.

    Optionally re-runs all downstream models (cascade) to propagate the restored data.
    """
    from dp.engine.database import connect
    from dp.engine.snapshots import (
        get_downstream_models,
        get_runs,
        restore_snapshot,
        restore_with_cascade,
    )

    project_dir = _resolve_project(project_dir)
    config = _load_config(project_dir, env)

    # Resolve run_id prefix
    runs = get_runs(project_dir, limit=500)
    matched = [r for r in runs if r.run_id.startswith(run_id)]
    if not matched:
        console.print(f"[red]No run found matching '{run_id}'[/red]")
        raise typer.Exit(1)
    full_run_id = matched[0].run_id

    db_path = project_dir / config.database.path
    conn = connect(db_path)
    transform_dir = project_dir / "transform"

    try:
        if cascade:
            # Show downstream models first
            downstream = get_downstream_models(model, transform_dir)
            if downstream:
                console.print(f"[bold]Restoring {model} from run {full_run_id[:12]}...[/bold]")
                console.print(f"  Downstream models to re-run: {', '.join(downstream)}")
            else:
                console.print(f"[bold]Restoring {model} from run {full_run_id[:12]}... (no downstream models)[/bold]")

            result = restore_with_cascade(
                project_dir, conn, full_run_id, model, transform_dir,
                db_path=str(db_path),
            )
        else:
            console.print(f"[bold]Restoring {model} from run {full_run_id[:12]}... (no cascade)[/bold]")
            result = restore_snapshot(project_dir, conn, full_run_id, model)

        if result["status"] == "error":
            console.print(f"[red]{result['message']}[/red]")
            raise typer.Exit(1)

        console.print(f"[green]{result['message']}[/green]")
        if result.get("schema_warning"):
            console.print("[yellow]Warning: schema has changed since this snapshot was taken.[/yellow]")

        if cascade and result.get("cascade_results"):
            built = sum(1 for s in result["cascade_results"].values() if s == "built")
            errors = sum(1 for s in result["cascade_results"].values() if s == "error")
            console.print(f"  Cascade: {built} rebuilt, {errors} errors")
            if errors:
                raise typer.Exit(1)
    finally:
        conn.close()
