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
        SAMPLE_GOLD_REGIONS_SQL,
        SAMPLE_GOLD_SUMMARY_SQL,
        SAMPLE_GOLD_TOP_SQL,
        SAMPLE_INGEST_NOTEBOOK,
        SAMPLE_SILVER_DAILY_SQL,
        SAMPLE_SILVER_EVENTS_SQL,
    )
    from dp.engine.secrets import ENV_TEMPLATE

    target = directory or Path.cwd() / name
    target.mkdir(parents=True, exist_ok=True)

    dirs = ["ingest", "transform/bronze", "transform/silver", "transform/gold", "export"]
    for d in dirs:
        (target / d).mkdir(parents=True, exist_ok=True)

    # project.yml
    (target / "project.yml").write_text(PROJECT_YML_TEMPLATE.format(name=name))
    # Sample pipeline: earthquake data from USGS API
    (target / "ingest" / "earthquakes.dpnb").write_text(SAMPLE_INGEST_NOTEBOOK)
    (target / "transform" / "bronze" / "earthquakes.sql").write_text(SAMPLE_BRONZE_SQL)
    (target / "transform" / "silver" / "earthquake_events.sql").write_text(SAMPLE_SILVER_EVENTS_SQL)
    (target / "transform" / "silver" / "earthquake_daily.sql").write_text(SAMPLE_SILVER_DAILY_SQL)
    (target / "transform" / "gold" / "earthquake_summary.sql").write_text(SAMPLE_GOLD_SUMMARY_SQL)
    (target / "transform" / "gold" / "top_earthquakes.sql").write_text(SAMPLE_GOLD_TOP_SQL)
    (target / "transform" / "gold" / "region_risk.sql").write_text(SAMPLE_GOLD_REGIONS_SQL)
    (target / "export" / "earthquake_report.py").write_text(SAMPLE_EXPORT_SCRIPT)
    # .env secrets file
    (target / ".env").write_text(ENV_TEMPLATE)
    # Notebooks directory
    (target / "notebooks").mkdir(parents=True, exist_ok=True)
    # .gitignore
    (target / ".gitignore").write_text(
        "warehouse.duckdb\nwarehouse.duckdb.wal\n__pycache__/\n*.pyc\n.venv/\n.env\noutput/\n"
    )
    # Agent instructions for LLM tools (Claude Code, Cursor, etc.)
    (target / "CLAUDE.md").write_text(CLAUDE_MD_TEMPLATE.format(name=name))

    console.print(f"[green]Project '{name}' created at {target}[/green]")
    console.print()
    console.print("Structure:")
    for d in dirs:
        console.print(f"  {d}/")
    console.print()
    console.print("Quick start:")
    console.print(f"  cd {name}")
    console.print("  dp stream full-refresh    # fetch earthquake data & build pipeline")
    console.print("  dp serve                  # open web UI")
    console.print()
    console.print("[dim]AI assistant ready:[/dim] CLAUDE.md included for Claude Code, Cursor, and others.")
    console.print("[dim]Run [bold]dp context[/bold] to generate a project summary for any AI chat.[/dim]")


# --- run ---


@app.command()
def run(
    script: Annotated[str, typer.Argument(help="Script path (e.g. ingest/customers.py)")],
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Run a single ingest or export script (.py or .dpnb notebook)."""
    from dp.config import load_project
    from dp.engine.database import connect
    from dp.engine.runner import run_script

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)
    script_path = project_dir / script

    if not script_path.exists():
        console.print(f"[red]Script not found: {script_path}[/red]")
        raise typer.Exit(1)

    # Determine script type from immediate parent directory
    parent_name = script_path.parent.name
    if parent_name == "ingest":
        script_type = "ingest"
    elif parent_name == "export":
        script_type = "export"
    else:
        script_type = "script"
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


# --- diff ---


@app.command()
def diff(
    targets: Annotated[Optional[list[str]], typer.Argument(help="Models to diff (e.g. gold.earthquake_summary)")] = None,
    target: Annotated[Optional[str], typer.Option("--target", "-t", help="Diff all models in a schema")] = None,
    format: Annotated[str, typer.Option("--format", "-f", help="Output format: table or json")] = "table",
    rows: Annotated[bool, typer.Option("--rows", help="Include sample rows in output")] = False,
    full: Annotated[bool, typer.Option("--full", help="Show all changed rows, not just samples")] = False,
    against: Annotated[Optional[str], typer.Option("--against", help="Only diff models changed vs a git branch/ref")] = None,
    snapshot: Annotated[Optional[str], typer.Option("--snapshot", help="Compare current state against a snapshot")] = None,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
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
        conn = connect(db_path, read_only=True)
        try:
            ensure_meta_table(conn)
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


# --- stream ---


@app.command()
def stream(
    name: Annotated[str, typer.Argument(help="Stream name from project.yml")],
    force: Annotated[bool, typer.Option("--force", "-f", help="Force rebuild all models")] = False,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Run a full stream: ingest -> transform -> export as defined in project.yml."""
    import time as _time

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
    if stream_config.retries:
        console.print(f"  [dim]retries: {stream_config.retries}, delay: {stream_config.retry_delay}s[/dim]")
    console.print()

    def _run_step(step, conn_):
        """Run a single step. Returns True on success."""
        if step.action == "ingest":
            console.print("[bold]Ingest:[/bold]")
            results = run_scripts_in_dir(conn_, project_dir / "ingest", "ingest", step.targets)
            if any(r["status"] == "error" for r in results):
                return False
        elif step.action == "transform":
            console.print("[bold]Transform:[/bold]")
            results = run_transform(
                conn_,
                project_dir / "transform",
                targets=step.targets if step.targets != ["all"] else None,
                force=force,
            )
            if any(s == "error" for s in results.values()):
                return False
        elif step.action == "export":
            console.print("[bold]Export:[/bold]")
            results = run_scripts_in_dir(conn_, project_dir / "export", "export", step.targets)
            if any(r["status"] == "error" for r in results):
                return False
        console.print()
        return True

    db_path = project_dir / config.database.path
    conn = connect(db_path)
    has_error = False
    start = _time.perf_counter()

    try:
        for step in stream_config.steps:
            success = _run_step(step, conn)
            if not success and stream_config.retries > 0:
                for attempt in range(1, stream_config.retries + 1):
                    console.print(
                        f"[yellow]Retrying {step.action} (attempt {attempt}/{stream_config.retries}) "
                        f"after {stream_config.retry_delay}s...[/yellow]"
                    )
                    _time.sleep(stream_config.retry_delay)
                    success = _run_step(step, conn)
                    if success:
                        break
            if not success:
                has_error = True
                break

        duration_s = round(_time.perf_counter() - start, 1)
        if has_error:
            console.print(f"[red]Stream failed after {duration_s}s.[/red]")
        else:
            console.print(f"[green]Stream completed successfully in {duration_s}s.[/green]")

        # Webhook notification
        if stream_config.webhook_url:
            _send_webhook(stream_config.webhook_url, name, "failed" if has_error else "success", duration_s)

        if has_error:
            raise typer.Exit(1)
    finally:
        conn.close()


def _send_webhook(url: str, stream_name: str, status: str, duration_s: float) -> None:
    """Send a POST webhook notification for stream completion."""
    import json
    from datetime import datetime
    from urllib.request import Request, urlopen

    payload = json.dumps({
        "stream": stream_name,
        "status": status,
        "duration_seconds": duration_s,
        "timestamp": datetime.now().isoformat(),
    }).encode()

    try:
        req = Request(url, data=payload, headers={"Content-Type": "application/json"})
        urlopen(req, timeout=10)
        console.print(f"[dim]Webhook sent to {url}[/dim]")
    except Exception as e:
        console.print(f"[yellow]Webhook failed: {e}[/yellow]")


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

    count, violations, fixed = run_lint(
        transform_dir,
        fix=fix,
        dialect=config.lint.dialect,
        rules=config.lint.rules or None,
    )

    print_violations(violations)

    if fix:
        if fixed > 0:
            console.print(f"[green]{fixed} fixed.[/green]")
        if count > 0:
            console.print(f"[yellow]{count} violation(s) remain (unfixable by SQLFluff).[/yellow]")
            raise typer.Exit(1)
        if fixed == 0 and count == 0:
            console.print("[green]All clean — no violations found.[/green]")
    elif count > 0:
        console.print(f"\n[red]{count} violation(s) found.[/red] Run [bold]dp lint --fix[/bold] to auto-fix.")
        raise typer.Exit(1)
    else:
        console.print("[green]All clean — no violations found.[/green]")


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

    sql = sql.strip()
    if not sql:
        console.print("[red]Empty query. Provide a SQL statement to execute.[/red]")
        raise typer.Exit(1)

    db_path = project_dir / config.database.path
    if not db_path.exists():
        console.print("[yellow]No warehouse database found. Run a pipeline first.[/yellow]")
        raise typer.Exit(1)

    conn = connect(db_path, read_only=True)
    try:
        result = conn.execute(sql)
        if result.description is None:
            console.print("[yellow]Query executed successfully (no results returned).[/yellow]")
            return
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()

        table = Table()
        for col in columns:
            table.add_column(col)
        for row in rows:
            table.add_row(*[str(v) for v in row])
        console.print(table)
        console.print(f"[dim]{len(rows)} rows[/dim]")
    except Exception as e:
        console.print(f"[red]Query error:[/red] {e}")
        raise typer.Exit(1)
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
        if schema:
            sql = """
                SELECT table_schema, table_name, table_type
                FROM information_schema.tables
                WHERE table_schema NOT IN ('information_schema', '_dp_internal')
                  AND table_schema = ?
                ORDER BY table_schema, table_name
            """
            result = conn.execute(sql, [schema]).fetchall()
        else:
            sql = """
                SELECT table_schema, table_name, table_type
                FROM information_schema.tables
                WHERE table_schema NOT IN ('information_schema', '_dp_internal')
                ORDER BY table_schema, table_name
            """
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
    import duckdb

    from dp.config import load_project
    from dp.engine.database import connect

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)

    db_path = project_dir / config.database.path
    if not db_path.exists():
        console.print("[yellow]No warehouse database found.[/yellow]")
        return

    conn = connect(db_path, read_only=True)
    try:
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
        except duckdb.CatalogException:
            console.print("[yellow]No run history yet.[/yellow]")
            return

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


# --- status ---


@app.command()
def status(
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Show project health: git info, warehouse stats, last run."""
    from dp.config import load_project
    from dp.engine.database import connect, ensure_meta_table

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)

    console.print(f"[bold]dp project:[/bold] {config.name}")

    # Git info
    try:
        from dp.engine.git import current_branch, is_dirty, is_git_repo, changed_files

        if is_git_repo(project_dir):
            branch = current_branch(project_dir) or "unknown"
            console.print(f"[bold]git branch:[/bold] {branch}")
            dirty = is_dirty(project_dir)
            if dirty:
                files = changed_files(project_dir)
                console.print(f"[bold]git status:[/bold] {len(files)} files modified (uncommitted)")
                for f in files[:10]:
                    console.print(f"  [yellow]modified:[/yellow] {f}")
                if len(files) > 10:
                    console.print(f"  [dim]... and {len(files) - 10} more[/dim]")
            else:
                console.print("[bold]git status:[/bold] [green]clean[/green]")
        else:
            console.print("[dim]git: not a git repository[/dim]")
    except Exception:
        pass

    # Warehouse stats
    db_path = project_dir / config.database.path
    if db_path.exists():
        conn = connect(db_path, read_only=True)
        try:
            rows = conn.execute(
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_schema NOT IN ('information_schema', '_dp_internal')"
            ).fetchall()
            total_tables = len(rows)
            total_rows = 0
            for schema, tname in rows:
                try:
                    count = conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{tname}"').fetchone()[0]
                    total_rows += count
                except Exception:
                    pass
            console.print(f"[bold]warehouse:[/bold] {total_tables} tables, {total_rows:,} rows")

            # Last run
            ensure_meta_table(conn)
            last = conn.execute(
                "SELECT run_type, target, status, started_at, duration_ms "
                "FROM _dp_internal.run_log ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
            if last:
                import datetime
                run_type, run_target, run_status, started, dur = last
                status_color = "green" if run_status == "success" else "red"
                ago = ""
                if started:
                    try:
                        delta = datetime.datetime.now() - started
                        if delta.days > 0:
                            ago = f"{delta.days}d ago"
                        elif delta.seconds > 3600:
                            ago = f"{delta.seconds // 3600}h ago"
                        elif delta.seconds > 60:
                            ago = f"{delta.seconds // 60}m ago"
                        else:
                            ago = "just now"
                    except Exception:
                        ago = str(started)[:19]
                console.print(
                    f"[bold]last run:[/bold]  {run_type} {run_target} "
                    f"([{status_color}]{run_status}[/{status_color}], {ago})"
                )
        finally:
            conn.close()
    else:
        console.print("[bold]warehouse:[/bold] [yellow]not created yet[/yellow]")


# --- checkpoint ---


@app.command()
def checkpoint(
    message: Annotated[Optional[str], typer.Option("--message", "-m", help="Custom commit message")] = None,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Smart git commit: stages files, auto-generates commit message from changes."""
    import subprocess

    from dp.engine.git import current_branch, is_git_repo

    project_dir = _resolve_project(project_dir)

    if not is_git_repo(project_dir):
        console.print("[red]Not a git repository. Run 'git init' first.[/red]")
        raise typer.Exit(1)

    # Check for .env in staged files and warn
    try:
        staged = subprocess.run(
            ["git", "diff", "--cached", "--name-only"],
            cwd=project_dir,
            capture_output=True,
            text=True,
        )
        if ".env" in (staged.stdout or ""):
            console.print("[yellow]Warning: .env is staged. Unstaging to prevent committing secrets.[/yellow]")
            subprocess.run(["git", "reset", "HEAD", ".env"], cwd=project_dir, capture_output=True)
    except Exception:
        pass

    # Stage everything except .env
    subprocess.run(["git", "add", "--all"], cwd=project_dir, capture_output=True)
    # Unstage .env if it got added
    subprocess.run(["git", "reset", "HEAD", ".env"], cwd=project_dir, capture_output=True, check=False)

    # Check if there's anything to commit
    result = subprocess.run(
        ["git", "diff", "--cached", "--name-only"],
        cwd=project_dir,
        capture_output=True,
        text=True,
    )
    staged_files = [f for f in (result.stdout or "").strip().split("\n") if f]
    if not staged_files:
        console.print("[yellow]No changes to commit.[/yellow]")
        return

    # Auto-generate commit message if not provided
    if not message:
        message = _generate_commit_message(staged_files)

    # Commit
    commit_result = subprocess.run(
        ["git", "commit", "-m", message],
        cwd=project_dir,
        capture_output=True,
        text=True,
    )
    if commit_result.returncode != 0:
        console.print(f"[red]Commit failed: {commit_result.stderr}[/red]")
        raise typer.Exit(1)

    branch = current_branch(project_dir) or "unknown"
    console.print(f"[green]Committed {len(staged_files)} file(s) on branch {branch}[/green]")
    console.print(f"  [dim]{message}[/dim]")


def _generate_commit_message(staged_files: list[str]) -> str:
    """Generate a commit message from staged file paths."""
    parts = []
    models_changed = []
    scripts_changed = []
    config_changed = False

    for f in staged_files:
        if f.startswith("transform/") and f.endswith(".sql"):
            # Extract model name: transform/gold/region_risk.sql -> gold.region_risk
            rel = f[len("transform/"):]
            parts_path = rel.rsplit("/", 1)
            if len(parts_path) == 2:
                schema, name = parts_path
                models_changed.append(f"{schema}.{name.replace('.sql', '')}")
            else:
                models_changed.append(rel.replace(".sql", ""))
        elif f.startswith("ingest/") or f.startswith("export/"):
            scripts_changed.append(f)
        elif f == "project.yml":
            config_changed = True

    if models_changed:
        if len(models_changed) <= 3:
            parts.append("Update " + ", ".join(models_changed))
        else:
            parts.append(f"Update {len(models_changed)} models")
    if scripts_changed:
        if len(scripts_changed) <= 3:
            parts.append("update " + ", ".join(scripts_changed))
        else:
            parts.append(f"update {len(scripts_changed)} scripts")
    if config_changed:
        parts.append("modify pipeline config")

    if parts:
        return "; ".join(parts)
    return f"Update {len(staged_files)} file(s)"


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


# --- snapshot ---


snapshot_app = typer.Typer(name="snapshot", help="Manage named snapshots of project + data state.")
app.add_typer(snapshot_app)


@snapshot_app.command("create")
def snapshot_create(
    name: Annotated[Optional[str], typer.Argument(help="Snapshot name (auto-generated if omitted)")] = None,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
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
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
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
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
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
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
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

    from dp import setup_logging
    from dp.engine.scheduler import FileWatcher, SchedulerThread

    setup_logging()
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


# --- backup ---


@app.command()
def backup(
    output: Annotated[Optional[Path], typer.Option("--output", "-o", help="Backup file path")] = None,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Create a backup of the warehouse database."""
    import shutil

    project_dir = _resolve_project(project_dir)
    from dp.config import load_project

    config = load_project(project_dir)
    db_path = project_dir / config.database.path

    if not db_path.exists():
        console.print("[red]No warehouse database found. Nothing to backup.[/red]")
        raise typer.Exit(1)

    # Default backup path: warehouse.duckdb.backup
    if output is None:
        from datetime import datetime
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output = project_dir / f"{config.database.path}.backup_{ts}"

    # Ensure WAL is flushed by checkpointing via a temporary connection
    from dp.engine.database import connect
    try:
        conn = connect(db_path)
        conn.execute("CHECKPOINT")
        conn.close()
    except Exception:
        pass  # proceed with copy even if checkpoint fails

    shutil.copy2(str(db_path), str(output))
    size_mb = output.stat().st_size / (1024 * 1024)
    console.print(f"[green]Backup created: {output} ({size_mb:.1f} MB)[/green]")


@app.command()
def restore(
    backup_path: Annotated[Path, typer.Argument(help="Path to the backup file")],
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Restore the warehouse database from a backup."""
    import shutil

    project_dir = _resolve_project(project_dir)
    from dp.config import load_project

    config = load_project(project_dir)
    db_path = project_dir / config.database.path

    if not backup_path.exists():
        console.print(f"[red]Backup file not found: {backup_path}[/red]")
        raise typer.Exit(1)

    if db_path.exists():
        console.print(f"[yellow]Overwriting existing database: {db_path}[/yellow]")

    shutil.copy2(str(backup_path), str(db_path))
    # Remove WAL file if present (stale WAL from old db)
    wal_path = Path(str(db_path) + ".wal")
    if wal_path.exists():
        wal_path.unlink()

    size_mb = db_path.stat().st_size / (1024 * 1024)
    console.print(f"[green]Database restored from {backup_path} ({size_mb:.1f} MB)[/green]")


# --- validate ---


@app.command()
def validate(
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
) -> None:
    """Validate project structure, config, and SQL model dependencies."""
    from dp.config import load_project
    from dp.engine.transform import build_dag, discover_models

    project_dir = _resolve_project(project_dir)
    errors: list[str] = []
    warnings: list[str] = []

    # 1. Validate project.yml
    try:
        config = load_project(project_dir)
        console.print("[green]project.yml[/green] parsed successfully")
    except Exception as e:
        console.print(f"[red]project.yml[/red] failed to parse: {e}")
        raise typer.Exit(1)

    # 2. Check required directories exist
    for d in ("transform",):
        if not (project_dir / d).exists():
            warnings.append(f"Directory '{d}/' not found")

    # 3. Validate streams reference valid actions
    for name, stream in config.streams.items():
        for step in stream.steps:
            if step.action not in ("ingest", "transform", "export"):
                errors.append(f"Stream '{name}': unknown action '{step.action}'")

    # 4. Discover and validate SQL models
    transform_dir = project_dir / "transform"
    models = discover_models(transform_dir)
    model_names = {m.full_name for m in models}

    # Check for duplicate model names
    seen: dict[str, str] = {}
    for m in models:
        if m.full_name in seen:
            errors.append(f"Duplicate model: {m.full_name} (in {m.path} and {seen[m.full_name]})")
        seen[m.full_name] = str(m.path)

    # Check depends_on references
    for m in models:
        for dep in m.depends_on:
            # External deps (landing.*) are fine — only flag deps that look like
            # they should be models but aren't
            if dep in model_names:
                continue
            schema = dep.split(".")[0] if "." in dep else ""
            if schema in ("bronze", "silver", "gold"):
                warnings.append(f"Model {m.full_name}: depends on '{dep}' which is not a known model")

    # 5. Check for circular dependencies
    try:
        build_dag(models)
        console.print(f"[green]DAG[/green] {len(models)} models, no circular dependencies")
    except Exception as e:
        errors.append(f"Circular dependency detected: {e}")

    # 6. Check .env variables referenced in config
    config_text = (project_dir / "project.yml").read_text() if (project_dir / "project.yml").exists() else ""
    import re
    env_refs = set(re.findall(r"\$\{(\w+)\}", config_text))
    if env_refs:
        import os
        missing = [v for v in env_refs if not os.environ.get(v)]
        if missing:
            for v in missing:
                warnings.append(f"Environment variable ${{{v}}} referenced in project.yml but not set")

    # Report
    if warnings:
        console.print()
        for w in warnings:
            console.print(f"  [yellow]warn[/yellow]  {w}")
    if errors:
        console.print()
        for e in errors:
            console.print(f"  [red]error[/red] {e}")
        console.print()
        console.print(f"[red]Validation failed: {len(errors)} error(s), {len(warnings)} warning(s)[/red]")
        raise typer.Exit(1)
    else:
        console.print()
        console.print(f"[green]Validation passed ({len(warnings)} warning(s))[/green]")


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
            py_files = list(script_dir.glob("*.py"))
            nb_files = list(script_dir.glob("*.dpnb"))
            scripts = sorted(f.name for f in py_files + nb_files if not f.name.startswith("_"))
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

            # Recent history (skip if meta tables don't exist yet)
            try:
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
            except Exception:
                pass  # no run history yet
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
    lines.append("- Writing Python ingest scripts (put them in ingest/, `db` connection is pre-injected)")
    lines.append("- Debugging failed pipeline runs")
    lines.append("- Writing queries to analyze data in the warehouse")
    lines.append("- Adding new data sources or exports")

    output = "\n".join(lines)
    console.print(output)
    console.print()
    console.print("[dim]---[/dim]")
    console.print("[dim]Copy the text above and paste it into any AI assistant.[/dim]")
    console.print("[dim]Then ask your question about this project.[/dim]")


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
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
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


# --- connectors ---

connectors_app = typer.Typer(name="connectors", help="Manage configured data connectors.")
app.add_typer(connectors_app)


@connectors_app.command("list")
def connectors_list(
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
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
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
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
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
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
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
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
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p")] = None,
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


if __name__ == "__main__":
    app()
