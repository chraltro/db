"""Pipeline commands: run, seed, transform, stream, lint, watch, schedule."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.table import Table

from dp.cli import _load_config, _resolve_project, app, console


@app.command()
def run(
    script: Annotated[str, typer.Argument(help="Script path (e.g. ingest/customers.py)")],
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
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

    if script_path.is_dir():
        console.print(f"[red]Expected a script file, got a directory: {script_path}[/red]")
        console.print("Hint: use [bold]dp stream[/bold] to run a full pipeline, or specify a file like [bold]dp run ingest/script.py[/bold]")
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


@app.command()
def seed(
    force: Annotated[bool, typer.Option("--force", "-f", help="Force reload all seeds")] = False,
    schema: Annotated[str, typer.Option("--schema", "-s", help="Target schema for seeds")] = "seeds",
    env: Annotated[Optional[str], typer.Option("--env", "-e", help="Environment to use")] = None,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Load CSV files from seeds/ directory into DuckDB tables.

    Seeds are change-detected: only modified CSVs are reloaded.
    Use --force to reload everything.
    """
    from dp.engine.database import connect
    from dp.engine.seeds import run_seeds

    project_dir = _resolve_project(project_dir)
    config = _load_config(project_dir, env)
    seeds_dir = project_dir / "seeds"

    if not seeds_dir.exists():
        console.print("[yellow]No seeds/ directory found.[/yellow]")
        console.print("Create a seeds/ directory with CSV files to load as seed data.")
        return

    env_label = f" [dim](env={config.active_environment})[/dim]" if config.active_environment else ""
    console.print(f"[bold]Loading seeds{env_label}:[/bold]")

    db_path = project_dir / config.database.path
    conn = connect(db_path)
    try:
        results = run_seeds(conn, seeds_dir, schema=schema, force=force)
        if results:
            built = sum(1 for s in results.values() if s == "built")
            skipped = sum(1 for s in results.values() if s == "skipped")
            errors = sum(1 for s in results.values() if s == "error")
            console.print(f"\n  {built} loaded, {skipped} skipped, {errors} errors")
            if errors:
                raise typer.Exit(1)
    finally:
        conn.close()


@app.command()
def transform(
    targets: Annotated[Optional[list[str]], typer.Argument(help="Specific models to run")] = None,
    force: Annotated[bool, typer.Option("--force", "-f", help="Force rebuild all models")] = False,
    parallel: Annotated[bool, typer.Option("--parallel", help="Run independent models in parallel")] = False,
    workers: Annotated[int, typer.Option("--workers", "-w", help="Max parallel workers")] = 4,
    env: Annotated[Optional[str], typer.Option("--env", "-e", help="Environment to use (e.g. dev, prod)")] = None,
    skip_check: Annotated[bool, typer.Option("--skip-check", help="Skip pre-transform validation")] = False,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Parse SQL models, resolve DAG, execute in dependency order.

    Supports incremental models, data quality assertions, auto-profiling,
    and parallel execution of independent models.
    """
    from dp.engine.database import connect
    from dp.engine.transform import run_transform

    project_dir = _resolve_project(project_dir)
    config = _load_config(project_dir, env)
    transform_dir = project_dir / "transform"

    mode = "parallel" if parallel else "sequential"
    console.print(f"[bold]Transform[/bold] [dim]({mode})[/dim]:")

    db_path = project_dir / config.database.path
    conn = connect(db_path)
    try:
        results = run_transform(
            conn, transform_dir, targets=targets, force=force,
            parallel=parallel, max_workers=workers, db_path=str(db_path),
        )
        if not results:
            return
        built = sum(1 for s in results.values() if s == "built")
        skipped = sum(1 for s in results.values() if s == "skipped")
        errors = sum(1 for s in results.values() if s == "error")
        assertions_failed = sum(1 for s in results.values() if s == "assertion_failed")
        console.print()
        parts = [f"{built} built", f"{skipped} skipped", f"{errors} errors"]
        if assertions_failed:
            parts.append(f"{assertions_failed} assertion failures")
        console.print(f"  {', '.join(parts)}")

        # Send alerts if configured
        if config.alerts.slack_webhook_url or config.alerts.webhook_url:
            from dp.engine.alerts import AlertConfig, alert_pipeline_success, alert_pipeline_failure
            alert_cfg = AlertConfig(
                slack_webhook_url=config.alerts.slack_webhook_url,
                webhook_url=config.alerts.webhook_url,
                channels=config.alerts.channels,
            )
            if errors or assertions_failed:
                if config.alerts.on_failure:
                    alert_pipeline_failure("transform", 0, f"{errors} errors, {assertions_failed} assertion failures", alert_cfg, conn)
            elif config.alerts.on_success and built > 0:
                alert_pipeline_success("transform", 0, alert_cfg, conn, models_built=built)

        if errors or assertions_failed:
            raise typer.Exit(1)
    finally:
        conn.close()


@app.command()
def stream(
    name: Annotated[str, typer.Argument(help="Stream name from project.yml")],
    force: Annotated[bool, typer.Option("--force", "-f", help="Force rebuild all models")] = False,
    env: Annotated[Optional[str], typer.Option("--env", "-e", help="Environment to use")] = None,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Run a full stream: ingest -> transform -> export as defined in project.yml."""
    import time as _time

    from dp.engine.database import connect
    from dp.engine.runner import run_scripts_in_dir
    from dp.engine.transform import run_transform

    project_dir = _resolve_project(project_dir)
    config = _load_config(project_dir, env)

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
        elif step.action == "seed":
            console.print("[bold]Seeds:[/bold]")
            from dp.engine.seeds import run_seeds
            results = run_seeds(conn_, project_dir / "seeds", force=force)
            if any(s == "error" for s in results.values()):
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


@app.command()
def lint(
    fix: Annotated[bool, typer.Option("--fix", help="Auto-fix violations")] = False,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
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


@app.command()
def watch(
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
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


@app.command()
def schedule(
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
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
