"""Model analysis commands: diff, promote, debug, check, impact, freshness, lineage, profile, assertions."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.table import Table

from dp.cli import _load_config, _resolve_project, app, console


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


# --- promote ---


@app.command()
def promote(
    sql_source: Annotated[str, typer.Argument(help="SQL source string, or path to a .sql/.dpnb file")] = "",
    name: Annotated[str, typer.Option("--name", "-n", help="Model name")] = "",
    schema: Annotated[str, typer.Option("--schema", "-s", help="Target schema")] = "bronze",
    description: Annotated[str, typer.Option("--desc", help="Model description")] = "",
    file: Annotated[Optional[Path], typer.Option("--file", "-f", help="Read SQL from a file instead of positional arg")] = None,
    overwrite: Annotated[bool, typer.Option("--overwrite", help="Overwrite existing model file")] = False,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Promote SQL to a transform model file.

    Takes a SQL query and creates a proper .sql model file in the transform
    directory with auto-generated config and depends_on comments.

    SQL can be provided as a positional argument, via --file, or piped from stdin.
    """
    from dp.engine.notebook import promote_sql_to_model
    from dp.engine.transform import build_dag, discover_models

    project_dir = _resolve_project(project_dir)
    transform_dir = project_dir / "transform"

    # Resolve SQL source: --file flag, positional arg (file path or literal), or stdin
    if file:
        if not file.exists():
            console.print(f"[red]File not found: {file}[/red]")
            raise typer.Exit(1)
        sql_source = file.read_text()
    elif sql_source:
        source_path = Path(sql_source)
        if source_path.exists() and source_path.suffix in (".sql", ".dpnb"):
            if source_path.suffix == ".dpnb":
                import json as _json
                nb_data = _json.loads(source_path.read_text())
                sql_cells = [c["source"] for c in nb_data.get("cells", []) if c.get("type") == "sql"]
                if not sql_cells:
                    console.print("[red]No SQL cells found in notebook[/red]")
                    raise typer.Exit(1)
                sql_source = sql_cells[-1]  # Use the last SQL cell
            else:
                sql_source = source_path.read_text()
    else:
        console.print("[red]SQL source is required (positional arg, --file, or pipe)[/red]")
        raise typer.Exit(1)

    if not name:
        console.print("[red]Model name is required (--name)[/red]")
        raise typer.Exit(1)

    try:
        model_path = promote_sql_to_model(
            sql_source=sql_source,
            model_name=name,
            schema=schema,
            transform_dir=transform_dir,
            description=description,
            overwrite=overwrite,
        )

        rel_path = model_path.relative_to(project_dir)
        console.print(f"[green]Model created:[/green] {rel_path}")

        # Validate the new model fits into the DAG
        try:
            models = discover_models(transform_dir)
            build_dag(models)
            console.print(f"[green]DAG validation passed[/green] ({len(models)} models)")
        except Exception as e:
            console.print(f"[yellow]DAG validation warning:[/yellow] {e}")

    except FileExistsError as e:
        console.print(f"[red]{e}[/red]")
        console.print("[dim]Use --overwrite to replace the existing model file.[/dim]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Failed to promote: {e}[/red]")
        raise typer.Exit(1)


# --- debug ---


@app.command()
def debug(
    model_name: Annotated[str, typer.Argument(help="Model to debug (e.g. silver.customers)")],
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Generate a debug notebook for a failed model.

    Creates a .dpnb notebook pre-populated with:
    - Error description from the run log
    - SQL cells for each upstream dependency
    - The failing model's SQL for interactive editing
    - Assertion failure diagnostics (if applicable)

    Use this to interactively debug transform failures.
    """
    from dp.config import load_project
    from dp.engine.database import connect, ensure_meta_table
    from dp.engine.notebook import generate_debug_notebook, save_notebook

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)
    transform_dir = project_dir / "transform"
    db_path = project_dir / config.database.path

    if not db_path.exists():
        console.print("[yellow]No warehouse database found. Run a pipeline first.[/yellow]")
        raise typer.Exit(1)

    conn = connect(db_path)
    try:
        ensure_meta_table(conn)

        # Look up most recent error from run log
        error_message = None
        try:
            row = conn.execute(
                "SELECT error FROM _dp_internal.run_log "
                "WHERE target = ? AND status IN ('error', 'assertion_failed') "
                "ORDER BY started_at DESC LIMIT 1",
                [model_name],
            ).fetchone()
            if row and row[0]:
                error_message = row[0]
        except Exception:
            pass

        # Check for assertion failures
        assertion_failures = None
        try:
            assertion_rows = conn.execute(
                "SELECT expression, detail FROM _dp_internal.assertion_results "
                "WHERE model_path = ? AND passed = false "
                "ORDER BY checked_at DESC LIMIT 10",
                [model_name],
            ).fetchall()
            if assertion_rows:
                assertion_failures = [
                    {"expression": r[0], "detail": r[1]} for r in assertion_rows
                ]
        except Exception:
            pass

        nb = generate_debug_notebook(
            conn, model_name, transform_dir,
            error_message=error_message,
            assertion_failures=assertion_failures,
        )

        safe_name = model_name.replace(".", "_")
        nb_path = project_dir / "notebooks" / f"debug_{safe_name}.dpnb"
        save_notebook(nb_path, nb)

        rel_path = nb_path.relative_to(project_dir)
        console.print(f"[green]Debug notebook created:[/green] {rel_path}")
        if error_message:
            console.print(f"  [dim]Error: {error_message[:120]}{'...' if len(error_message) > 120 else ''}[/dim]")
        if assertion_failures:
            for af in assertion_failures:
                console.print(f"  [red]FAIL[/red]  assert: {af['expression']} ({af.get('detail', '')})")
        console.print()
        console.print(f"Open with: [bold]dp serve[/bold] and navigate to notebooks, or edit {rel_path} directly.")

    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(1)
    finally:
        conn.close()


# --- check ---


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


# --- impact ---


@app.command()
def impact(
    model: Annotated[str, typer.Argument(help="Model name (e.g. silver.customers)")],
    column: Annotated[Optional[str], typer.Option("--column", "-c", help="Specific column to trace")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Output as JSON")] = False,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Analyze downstream impact of changing a model or column.

    Shows all models and columns that would be affected by a change.
    """
    import json as json_mod

    from dp.config import load_project
    from dp.engine.database import connect, ensure_meta_table
    from dp.engine.transform import discover_models, impact_analysis

    project_dir = _resolve_project(project_dir)
    config = load_project(project_dir)
    transform_dir = project_dir / "transform"
    db_path = project_dir / config.database.path

    models = discover_models(transform_dir)
    model_map = {m.full_name: m for m in models}

    # Resolve model name
    if model not in model_map:
        matches = [m for m in models if m.name == model]
        if matches:
            model = matches[0].full_name
        else:
            console.print(f"[red]Model '{model}' not found.[/red]")
            available = [m.full_name for m in models]
            if available:
                console.print(f"[dim]Available: {', '.join(available)}[/dim]")
            raise typer.Exit(1)

    conn = None
    if db_path.exists():
        conn = connect(db_path, read_only=True)
        ensure_meta_table(conn)

    try:
        result = impact_analysis(models, model, column=column, conn=conn)

        if json_output:
            console.print(json_mod.dumps(result, indent=2))
            return

        console.print(f"[bold]Impact analysis for {model}[/bold]")
        if column:
            console.print(f"  Column: [cyan]{column}[/cyan]")
        console.print()

        downstream = result["downstream_models"]
        if not downstream:
            console.print("  [green]No downstream models affected.[/green]")
            return

        console.print(f"  [yellow]{len(downstream)} downstream model(s) affected:[/yellow]")
        for ds in downstream:
            console.print(f"    {ds}")

        if result.get("affected_columns"):
            console.print()
            console.print(f"  [yellow]Affected columns:[/yellow]")
            for ac in result["affected_columns"]:
                console.print(f"    {ac['model']}.{ac['column']}")

        if result.get("impact_chain"):
            console.print()
            console.print("  [dim]Impact chain:[/dim]")
            for parent, children in result["impact_chain"].items():
                console.print(f"    {parent} -> {', '.join(children)}")
    finally:
        if conn:
            conn.close()


# --- freshness ---


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


# --- lineage ---


@app.command()
def lineage(
    model: Annotated[str, typer.Argument(help="Model name (e.g. gold.earthquake_summary)")],
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
    json_output: Annotated[bool, typer.Option("--json", help="Output as JSON")] = False,
) -> None:
    """Show column-level lineage for a model. Traces each output column back to its source."""
    import json as json_mod

    from dp.engine.transform import discover_models, extract_column_lineage

    project_dir = _resolve_project(project_dir)
    transform_dir = project_dir / "transform"
    models = discover_models(transform_dir)
    model_map = {m.full_name: m for m in models}

    target = model_map.get(model)
    if not target:
        # Try matching by short name
        matches = [m for m in models if m.name == model]
        if matches:
            target = matches[0]
        else:
            console.print(f"[red]Model '{model}' not found.[/red]")
            available = [m.full_name for m in models]
            if available:
                console.print(f"[dim]Available: {', '.join(available)}[/dim]")
            raise typer.Exit(1)

    lineage_map = extract_column_lineage(target)

    if json_output:
        console.print(json_mod.dumps(lineage_map, indent=2))
        return

    console.print(f"[bold]Column lineage for {target.full_name}:[/bold]\n")
    for out_col, sources in lineage_map.items():
        if sources:
            source_strs = [f"{s['source_table']}.{s['source_column']}" for s in sources]
            console.print(f"  [cyan]{out_col}[/cyan] <- {', '.join(source_strs)}")
        else:
            console.print(f"  [cyan]{out_col}[/cyan] <- [dim](computed)[/dim]")


# --- profile ---


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


# --- assertions ---


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
