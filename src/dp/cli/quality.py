"""Data quality commands: check, freshness, profile, assertions, contracts."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.table import Table

from dp.cli import _load_config, _resolve_project, app, console


@app.command()
def check(
    targets: Annotated[Optional[list[str]], typer.Argument(help="Specific models to check")] = None,
    env: Annotated[Optional[str], typer.Option("--env", "-e", help="Environment to use")] = None,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Validate all SQL models without executing them.

    Checks that SQL parses correctly, referenced tables exist in the DAG,
    sources.yml, the DuckDB catalog, or seeds. Validates column references
    against upstream tables. Reports all errors at once.
    """
    from dp.engine.database import connect, ensure_meta_table
    from dp.engine.seeds import discover_seeds
    from dp.engine.transform import discover_models, validate_models

    project_dir = _resolve_project(project_dir)
    config = _load_config(project_dir, env)
    transform_dir = project_dir / "transform"
    seeds_dir = project_dir / "seeds"
    db_path = project_dir / config.database.path

    models = discover_models(transform_dir)
    if not models:
        console.print("[yellow]No SQL models found in transform/[/yellow]")
        return

    if targets and targets != ["all"]:
        target_set = set(targets)
        models = [m for m in models if m.full_name in target_set or m.name in target_set]

    # Gather known table names from seeds and sources
    known_tables: set[str] = set()
    seeds = discover_seeds(seeds_dir)
    for s in seeds:
        known_tables.add(s["full_name"])
    for src in config.sources:
        for t in src.tables:
            known_tables.add(f"{src.schema}.{t.name}")

    # Gather declared source columns for column validation
    source_columns: dict[str, set[str]] = {}
    for src in config.sources:
        for t in src.tables:
            full = f"{src.schema}.{t.name}"
            source_columns[full] = {c.name for c in t.columns}

    conn = None
    if db_path.exists():
        conn = connect(db_path, read_only=True)
        ensure_meta_table(conn)

    try:
        env_label = f" [dim](env={config.active_environment})[/dim]" if config.active_environment else ""
        console.print(f"[bold]Checking {len(models)} model(s)...{env_label}[/bold]")
        errors = validate_models(conn, models, known_tables=known_tables, source_columns=source_columns)

        if not errors:
            console.print(f"[green]All {len(models)} models passed validation.[/green]")
            return

        err_count = sum(1 for e in errors if e.severity == "error")
        warn_count = sum(1 for e in errors if e.severity == "warning")

        for e in errors:
            icon = "[red]error[/red]" if e.severity == "error" else "[yellow]warn[/yellow]"
            console.print(f"  {icon}  [bold]{e.model}[/bold]: {e.message}")

        console.print()
        console.print(f"  {err_count} error(s), {warn_count} warning(s)")
        if err_count:
            raise typer.Exit(1)
    finally:
        if conn:
            conn.close()


@app.command()
def freshness(
    hours: Annotated[float, typer.Option("--hours", "-h", help="Max age in hours before a model is stale")] = 24.0,
    alert: Annotated[bool, typer.Option("--alert", help="Send alerts for stale models")] = False,
    sources_only: Annotated[bool, typer.Option("--sources", help="Only check source freshness from sources.yml")] = False,
    env: Annotated[Optional[str], typer.Option("--env", "-e", help="Environment to use")] = None,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Check model and source freshness.

    Without --sources, checks model freshness as before.
    With --sources, checks source freshness against SLAs declared in sources.yml.
    """
    from dp.engine.database import connect, ensure_meta_table
    from dp.engine.transform import check_freshness

    project_dir = _resolve_project(project_dir)
    config = _load_config(project_dir, env)
    db_path = project_dir / config.database.path

    if not db_path.exists():
        console.print("[yellow]No warehouse database found.[/yellow]")
        return

    conn = connect(db_path, read_only=True)
    try:
        ensure_meta_table(conn)
        results = check_freshness(conn, max_age_hours=hours)
        if not results:
            console.print("[yellow]No model state found. Run a transform first.[/yellow]")
            return

        table = Table(title=f"Model Freshness (stale > {hours}h)")
        table.add_column("Model", style="bold")
        table.add_column("Last Run")
        table.add_column("Hours Ago", justify="right")
        table.add_column("Rows", justify="right")
        table.add_column("Status")

        stale_models = []
        for r in results:
            hours_ago = r["hours_since_run"]
            is_stale = r["is_stale"]
            if is_stale:
                stale_models.append(r)
            status = "[red]STALE[/red]" if is_stale else "[green]fresh[/green]"
            table.add_row(
                r["model"],
                r["last_run_at"][:19] if r["last_run_at"] else "never",
                f"{hours_ago}h" if hours_ago is not None else "?",
                str(r["row_count"]) if r["row_count"] else "",
                status,
            )
        console.print(table)

        if stale_models:
            console.print(f"\n[yellow]{len(stale_models)} stale model(s)[/yellow]")
            if alert and (config.alerts.slack_webhook_url or config.alerts.webhook_url):
                from dp.engine.alerts import AlertConfig, alert_stale_models
                alert_cfg = AlertConfig(
                    slack_webhook_url=config.alerts.slack_webhook_url,
                    webhook_url=config.alerts.webhook_url,
                    channels=config.alerts.channels,
                )
                alert_stale_models(stale_models, alert_cfg)
                console.print("[dim]Stale alert sent.[/dim]")
        else:
            console.print(f"\n[green]All models are fresh (within {hours}h).[/green]")
    finally:
        conn.close()


@app.command()
def profile(
    model: Annotated[Optional[str], typer.Argument(help="Model name (e.g. gold.earthquake_summary)")] = None,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Show auto-computed profile stats for models (row counts, nulls, cardinality)."""
    import json as json_mod

    from dp.config import load_project
    from dp.engine.database import connect, ensure_meta_table

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)
    db_path = project_dir / config.database.path

    if not db_path.exists():
        console.print("[yellow]No warehouse database found.[/yellow]")
        return

    conn = connect(db_path, read_only=True)
    try:
        ensure_meta_table(conn)

        if model:
            # Show detailed profile for a specific model
            row = conn.execute(
                "SELECT model_path, row_count, column_count, null_percentages, distinct_counts, profiled_at "
                "FROM _dp_internal.model_profiles WHERE model_path = ?",
                [model],
            ).fetchone()
            if not row:
                console.print(f"[yellow]No profile data for '{model}'. Run dp transform first.[/yellow]")
                return

            model_path, row_count, col_count, null_pcts_json, distinct_json, profiled_at = row
            null_pcts = json_mod.loads(null_pcts_json) if null_pcts_json else {}
            distinct = json_mod.loads(distinct_json) if distinct_json else {}

            console.print(f"[bold]{model_path}[/bold]  ({row_count:,} rows, {col_count} columns)")
            console.print(f"  Profiled at: {str(profiled_at)[:19]}\n")

            table = Table(title="Column Statistics")
            table.add_column("Column", style="bold")
            table.add_column("Null %", justify="right")
            table.add_column("Distinct", justify="right")
            table.add_column("Status")

            for col_name in null_pcts:
                null_pct = null_pcts.get(col_name, 0)
                dist = distinct.get(col_name, 0)
                if null_pct > 50:
                    status = "[red]high nulls[/red]"
                elif null_pct > 0:
                    status = "[yellow]has nulls[/yellow]"
                else:
                    status = "[green]ok[/green]"
                table.add_row(col_name, f"{null_pct}%", str(dist), status)
            console.print(table)
        else:
            # Show summary for all profiled models
            rows = conn.execute(
                "SELECT model_path, row_count, column_count, profiled_at "
                "FROM _dp_internal.model_profiles ORDER BY model_path"
            ).fetchall()
            if not rows:
                console.print("[yellow]No profile data. Run dp transform first.[/yellow]")
                return

            table = Table(title="Model Profiles")
            table.add_column("Model", style="bold")
            table.add_column("Rows", justify="right")
            table.add_column("Columns", justify="right")
            table.add_column("Profiled At")
            for r in rows:
                table.add_row(r[0], f"{r[1]:,}", str(r[2]), str(r[3])[:19] if r[3] else "")
            console.print(table)
    finally:
        conn.close()


@app.command()
def assertions(
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Show recent data quality assertion results."""
    from dp.config import load_project
    from dp.engine.database import connect, ensure_meta_table

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)
    db_path = project_dir / config.database.path

    if not db_path.exists():
        console.print("[yellow]No warehouse database found.[/yellow]")
        return

    conn = connect(db_path, read_only=True)
    try:
        ensure_meta_table(conn)
        rows = conn.execute(
            """
            SELECT model_path, expression, passed, detail, checked_at
            FROM _dp_internal.assertion_results
            ORDER BY checked_at DESC
            LIMIT 50
            """
        ).fetchall()
        if not rows:
            console.print("[yellow]No assertion results yet. Add -- assert: comments to your SQL models.[/yellow]")
            console.print()
            console.print("[dim]Example:[/dim]")
            console.print("  [dim]-- assert: row_count > 0[/dim]")
            console.print("  [dim]-- assert: no_nulls(email)[/dim]")
            console.print("  [dim]-- assert: unique(customer_id)[/dim]")
            return

        table = Table(title="Data Quality Assertions")
        table.add_column("Model", style="bold")
        table.add_column("Assertion")
        table.add_column("Status")
        table.add_column("Detail")
        table.add_column("Checked At")
        for r in rows:
            status = "[green]PASS[/green]" if r[2] else "[red]FAIL[/red]"
            table.add_row(r[0], r[1], status, r[3] or "", str(r[4])[:19] if r[4] else "")
        console.print(table)
    finally:
        conn.close()


@app.command()
def contracts(
    targets: Annotated[Optional[list[str]], typer.Argument(help="Contract names or model names to run")] = None,
    history: Annotated[bool, typer.Option("--history", help="Show contract history instead of running")] = False,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory")] = None,
) -> None:
    """Run data contracts from the contracts/ directory.

    Contracts are YAML files that define assertions against specific models.
    They complement inline -- assert: comments with standalone, reusable rules.

    Example contracts/orders.yml:

        contracts:
          - name: orders_not_empty
            model: gold.orders
            assertions:
              - row_count > 0
              - unique(order_id)
    """
    from dp.config import load_project
    from dp.engine.contracts import get_contract_history, run_contracts
    from dp.engine.database import connect

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)
    db_path = project_dir / config.database.path

    if not db_path.exists():
        console.print("[yellow]No warehouse database found. Run a pipeline first.[/yellow]")
        raise typer.Exit(1)

    conn = connect(db_path)
    try:
        if history:
            results = get_contract_history(conn, limit=50)
            if not results:
                console.print("[yellow]No contract history yet.[/yellow]")
                return
            table = Table(title="Contract History")
            table.add_column("Contract", style="bold")
            table.add_column("Model")
            table.add_column("Status")
            table.add_column("Severity")
            table.add_column("Checked At")
            for r in results:
                status = "[green]PASS[/green]" if r["passed"] else "[red]FAIL[/red]"
                sev = "[yellow]warn[/yellow]" if r["severity"] == "warn" else r["severity"]
                table.add_row(r["contract_name"], r["model"], status, sev, r["checked_at"][:19])
            console.print(table)
            return

        contracts_dir = project_dir / "contracts"
        if not contracts_dir.exists():
            console.print("[yellow]No contracts/ directory found.[/yellow]")
            console.print("Create contracts/my_contract.yml to get started.")
            return

        results = run_contracts(conn, contracts_dir, targets=targets)
        if not results:
            console.print("[yellow]No contracts found.[/yellow]")
            return

        console.print(f"[bold]Running {len(results)} contract(s)...[/bold]")
        console.print()

        all_passed = True
        for cr in results:
            status = "[green]PASS[/green]" if cr.passed else "[red]FAIL[/red]"
            console.print(f"  {status}  [bold]{cr.contract_name}[/bold] ({cr.model}) [{cr.duration_ms}ms]")
            for ar in cr.results:
                if ar["passed"]:
                    console.print(f"         [green]pass[/green]  {ar['expression']}")
                else:
                    console.print(f"         [red]FAIL[/red]  {ar['expression']} ({ar['detail']})")
            if not cr.passed:
                all_passed = False
            if cr.error:
                console.print(f"         [red]Error:[/red] {cr.error}")

        console.print()
        passed = sum(1 for cr in results if cr.passed)
        failed = len(results) - passed
        if all_passed:
            console.print(f"[green]All {passed} contract(s) passed.[/green]")
        else:
            console.print(f"[red]{failed} contract(s) failed[/red], {passed} passed.")
            raise typer.Exit(1)
    finally:
        conn.close()
