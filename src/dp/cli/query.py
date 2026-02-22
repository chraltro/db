"""Query and inspection commands: query, tables, history."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.table import Table

from dp.cli import _load_config, _resolve_project, app, console


@app.command()
def query(
    sql: Annotated[str, typer.Argument(help="SQL query to execute")],
    csv: Annotated[bool, typer.Option("--csv", help="Output as CSV")] = False,
    json_output: Annotated[bool, typer.Option("--json", help="Output as JSON")] = False,
    limit: Annotated[int, typer.Option("--limit", "-n", help="Max rows to return")] = 0,
    env: Annotated[Optional[str], typer.Option("--env", "-e", help="Environment to use")] = None,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Run an ad-hoc SQL query against the warehouse."""
    import json as json_mod

    from dp.engine.database import connect

    project_dir = _resolve_project(project_dir)
    config = _load_config(project_dir, env)

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
        if limit > 0:
            rows = rows[:limit]

        if csv:
            import io as _io
            import csv as _csv
            buf = _io.StringIO()
            writer = _csv.writer(buf)
            writer.writerow(columns)
            for row in rows:
                writer.writerow(row)
            console.print(buf.getvalue().rstrip())
        elif json_output:
            data = [dict(zip(columns, [_json_safe(v) for v in row])) for row in rows]
            console.print(json_mod.dumps(data, indent=2, default=str))
        else:
            table = Table(show_lines=len(columns) > 8)
            for col in columns:
                table.add_column(col, no_wrap=False, max_width=60)
            for row in rows:
                table.add_row(*[str(v) for v in row])
            console.print(table)
            console.print(f"[dim]{len(rows)} rows[/dim]")
    except Exception as e:
        err_msg = str(e)
        if "read-only mode" in err_msg:
            console.print("[red]Query error:[/red] dp query is read-only. Use [bold]dp run[/bold] for write operations.")
        else:
            console.print(f"[red]Query error:[/red] {e}")
        raise typer.Exit(1)
    finally:
        conn.close()


def _json_safe(v):
    """Convert DuckDB values to JSON-safe types."""
    import datetime
    if isinstance(v, (datetime.date, datetime.datetime)):
        return str(v)
    return v


@app.command()
def tables(
    schema: Annotated[Optional[str], typer.Argument(help="Schema to list (all if omitted)")] = None,
    env: Annotated[Optional[str], typer.Option("--env", "-e", help="Environment to use")] = None,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """List tables and views in the warehouse."""
    from dp.engine.database import connect

    project_dir = _resolve_project(project_dir)
    config = _load_config(project_dir, env)

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


@app.command()
def history(
    limit: Annotated[int, typer.Option("--limit", "-n", help="Number of entries")] = 20,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
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
