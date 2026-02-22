"""Model analysis commands: promote, debug, impact, lineage."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.table import Table

from dp.cli import _resolve_project, app, console


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
