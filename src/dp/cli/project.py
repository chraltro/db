"""Project management commands: init, validate, status, context, backup, restore, checkpoint."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.table import Table

from dp.cli import _load_config, _resolve_project, app, console


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


@app.command()
def validate(
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
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


@app.command()
def status(
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Show project health: git info, warehouse stats, last run."""
    from dp.config import load_project
    from dp.engine.database import connect

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

            # Last run (skip if meta tables don't exist yet)
            try:
                last = conn.execute(
                    "SELECT run_type, target, status, started_at, duration_ms "
                    "FROM _dp_internal.run_log ORDER BY started_at DESC LIMIT 1"
                ).fetchone()
            except Exception:
                last = None
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


@app.command()
def checkpoint(
    message: Annotated[Optional[str], typer.Option("--message", "-m", help="Custom commit message")] = None,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
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


@app.command()
def context(
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
) -> None:
    """Generate a project summary to paste into any AI assistant (ChatGPT, Claude, etc.)."""
    from dp.config import load_project
    from dp.engine.database import connect
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
                    for rtype, target, rstatus, started, error in history_rows:
                        ts = str(started)[:19] if started else ""
                        err = f" — {error}" if error else ""
                        lines.append(f"- [{rstatus}] {rtype}: {target} ({ts}){err}")
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


@app.command()
def backup(
    output: Annotated[Optional[Path], typer.Option("--output", "-o", help="Backup file path")] = None,
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
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
    project_dir: Annotated[Optional[Path], typer.Option("--project", "-p", help="Project directory (default: current dir)")] = None,
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


# --- version ---


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
