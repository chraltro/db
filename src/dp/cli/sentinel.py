"""Schema Sentinel commands: check, history, impacts, apply-fix."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.table import Table

from dp.cli import _load_config, _resolve_project, app, console


@app.command()
def sentinel(
    action: Annotated[str, typer.Argument(help="Action: check, diffs, impacts, history, apply-fix")] = "check",
    source: Annotated[Optional[str], typer.Option("--source", "-s", help="Source name")] = None,
    diff_id: Annotated[Optional[str], typer.Option("--diff", "-d", help="Diff ID for impacts")] = None,
    limit: Annotated[int, typer.Option("--limit", "-n", help="Number of results")] = 20,
    env: Annotated[Optional[str], typer.Option("--env", "-e", help="Environment")] = None,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory")] = None,
) -> None:
    """Schema Sentinel: detect upstream schema changes and analyze impact.

    Actions:
      check      Run schema check on all sources (or --source)
      diffs      Show recent schema diffs
      impacts    Show impact analysis for a diff (--diff required)
      history    Show schema history for a source (--source required)
    """
    from dp.engine.database import connect
    from dp.engine.sentinel import (
        SentinelConfig,
        get_impacts_for_diff,
        get_recent_diffs,
        get_schema_history,
        get_source_names_from_models,
        run_sentinel_check,
    )

    project_dir = _resolve_project(project_dir)
    config = _load_config(project_dir, env)

    if not config.sentinel.enabled:
        console.print("[yellow]Schema Sentinel is disabled in project.yml[/yellow]")
        console.print("Set [bold]sentinel.enabled: true[/bold] to enable it.")
        return

    if action == "check":
        db_path = project_dir / config.database.path
        if not db_path.exists():
            console.print("[yellow]No warehouse database found. Run a pipeline first.[/yellow]")
            return

        conn = connect(db_path)
        try:
            if source:
                source_names = [source]
            else:
                source_names = get_source_names_from_models(project_dir)
                # Filter to existing tables
                existing = []
                for sn in source_names:
                    parts = sn.split(".")
                    if len(parts) == 2:
                        try:
                            exists = conn.execute(
                                "SELECT COUNT(*) FROM information_schema.tables "
                                "WHERE table_schema = ? AND table_name = ?",
                                [parts[0], parts[1]],
                            ).fetchone()[0]
                            if exists:
                                existing.append(sn)
                        except Exception:
                            pass
                source_names = existing

            if not source_names:
                console.print("[dim]No source tables found to check.[/dim]")
                return

            console.print(f"[bold]Schema Sentinel[/bold]: checking {len(source_names)} source(s)...")

            sc = SentinelConfig(
                enabled=config.sentinel.enabled,
                on_change=config.sentinel.on_change,
                track_ordering=config.sentinel.track_ordering,
                rename_inference=config.sentinel.rename_inference,
                auto_fix=config.sentinel.auto_fix,
                select_star_warning=config.sentinel.select_star_warning,
            )
            diffs = run_sentinel_check(project_dir, conn, source_names, config=sc)

            if not diffs:
                console.print("[green]No schema changes detected.[/green]")
                return

            for diff in diffs:
                has_break = "[red]BREAKING[/red]" if diff.has_breaking else "[yellow]CHANGES[/yellow]"
                console.print(f"\n  {has_break} in [bold]{diff.source_name}[/bold] (diff: {diff.diff_id[:8]}...)")

                tbl = Table()
                tbl.add_column("Change", style="bold")
                tbl.add_column("Severity")
                tbl.add_column("Column")
                tbl.add_column("Details")

                for ch in diff.changes:
                    sev_style = {"breaking": "red", "warning": "yellow", "info": "dim"}.get(ch.severity, "")
                    detail = ""
                    if ch.old_value and ch.new_value:
                        detail = f"{ch.old_value} -> {ch.new_value}"
                    elif ch.old_value:
                        detail = f"was: {ch.old_value}"
                    elif ch.new_value:
                        detail = ch.new_value
                    if ch.rename_candidate:
                        detail += f" (rename candidate: {ch.rename_candidate})"
                    tbl.add_row(ch.change_type, f"[{sev_style}]{ch.severity}[/{sev_style}]", ch.column_name, detail)

                console.print(tbl)

                # Show impacts
                impacts = get_impacts_for_diff(project_dir, diff.diff_id)
                if impacts:
                    console.print(f"\n  Impact analysis ({len(impacts)} model(s) affected):")
                    for imp in impacts:
                        icon = {"direct": "[red]!", "transitive": "[yellow]~", "safe": "[green]ok"}.get(imp["impact_type"], "?")
                        icon_close = {"direct": "[/red]", "transitive": "[/yellow]", "safe": "[/green]"}.get(imp["impact_type"], "")
                        cols = ", ".join(imp["columns_affected"]) if imp["columns_affected"] else "-"
                        console.print(f"    {icon}{icon_close} {imp['model_name']} ({imp['impact_type']}) cols: {cols}")
                        if imp["fix_suggestion"]:
                            console.print(f"      [dim]Fix: {imp['fix_suggestion'][:120]}[/dim]")
        finally:
            conn.close()

    elif action == "diffs":
        diffs = get_recent_diffs(project_dir, limit=limit)
        if not diffs:
            console.print("[dim]No schema diffs recorded. Run [bold]dp sentinel check[/bold] first.[/dim]")
            return

        tbl = Table(title="Schema Diffs")
        tbl.add_column("Diff ID", style="dim", max_width=12)
        tbl.add_column("Source", style="bold")
        tbl.add_column("Changes", justify="right")
        tbl.add_column("Breaking")
        tbl.add_column("Date")

        for d in diffs:
            changes = d.get("changes", [])
            n = len(changes)
            has_break = any(c.get("severity") == "breaking" for c in changes)
            tbl.add_row(
                d["diff_id"][:10] + "..",
                d["source_name"],
                str(n),
                "[red]yes[/red]" if has_break else "[green]no[/green]",
                d.get("created_at", "")[:19],
            )
        console.print(tbl)
        console.print("\n[dim]Use [bold]dp sentinel impacts --diff <id>[/bold] to see impact analysis[/dim]")

    elif action == "impacts":
        if not diff_id:
            console.print("[red]--diff is required for impacts action[/red]")
            raise typer.Exit(1)

        # Prefix match
        all_diffs = get_recent_diffs(project_dir, limit=500)
        matched = [d for d in all_diffs if d["diff_id"].startswith(diff_id)]
        if not matched:
            console.print(f"[red]No diff found matching '{diff_id}'[/red]")
            raise typer.Exit(1)
        full_diff_id = matched[0]["diff_id"]

        impacts = get_impacts_for_diff(project_dir, full_diff_id)
        if not impacts:
            console.print("[dim]No impact records for this diff.[/dim]")
            return

        tbl = Table(title=f"Impacts for Diff {full_diff_id[:10]}.. ({matched[0]['source_name']})")
        tbl.add_column("Model", style="bold")
        tbl.add_column("Impact")
        tbl.add_column("Columns Affected")
        tbl.add_column("Fix Suggestion", max_width=60)

        for imp in impacts:
            imp_style = {"direct": "[red]direct[/red]", "transitive": "[yellow]transitive[/yellow]", "safe": "[green]safe[/green]"}.get(imp["impact_type"], imp["impact_type"])
            cols = ", ".join(imp["columns_affected"]) if imp["columns_affected"] else "-"
            tbl.add_row(imp["model_name"], imp_style, cols, imp.get("fix_suggestion", "")[:60])
        console.print(tbl)

    elif action == "history":
        if not source:
            console.print("[red]--source is required for history action[/red]")
            raise typer.Exit(1)

        history = get_schema_history(project_dir, source, limit=limit)
        if not history:
            console.print(f"[dim]No schema history for {source}.[/dim]")
            return

        tbl = Table(title=f"Schema History: {source}")
        tbl.add_column("Snapshot ID", style="dim", max_width=12)
        tbl.add_column("Date")
        tbl.add_column("Columns", justify="right")
        tbl.add_column("Hash", style="dim")

        for s in history:
            cols = s.get("columns", [])
            tbl.add_row(
                s["snapshot_id"][:10] + "..",
                s.get("captured_at", "")[:19],
                str(len(cols)),
                s.get("schema_hash", "")[:8],
            )
        console.print(tbl)

    else:
        console.print(f"[red]Unknown action: {action}[/red]")
        console.print("Available: check, diffs, impacts, history")
        raise typer.Exit(1)
