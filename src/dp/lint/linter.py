"""SQLFluff integration for linting SQL transform files."""

from __future__ import annotations

from pathlib import Path

from rich.console import Console
from rich.table import Table

console = Console()


def lint(
    transform_dir: Path,
    fix: bool = False,
    dialect: str = "duckdb",
    rules: list[str] | None = None,
) -> tuple[int, list[dict]]:
    """Lint SQL files in the transform directory.

    Args:
        transform_dir: Path to transform/ directory
        fix: Whether to auto-fix violations
        dialect: SQL dialect for SQLFluff
        rules: Specific rules to check (None = all)

    Returns:
        Tuple of (violation_count, violations_list)
    """
    # Import here to avoid hard dependency at module level
    from sqlfluff.core import FluffConfig, Linter

    sql_files = sorted(transform_dir.rglob("*.sql"))
    if not sql_files:
        console.print("[yellow]No SQL files found in transform/[/yellow]")
        return 0, []

    # Use .sqlfluff config file from project root if it exists,
    # falling back to kwargs-based config
    project_dir = transform_dir.parent
    sqlfluff_file = project_dir / ".sqlfluff"
    if sqlfluff_file.exists():
        overrides: dict = {}
        if rules:
            overrides["rules"] = ",".join(rules)
        config = FluffConfig.from_path(path=str(project_dir), overrides=overrides or None)
    else:
        config_kwargs: dict = {"dialect": dialect}
        if rules:
            config_kwargs["rules"] = rules
        config = FluffConfig.from_kwargs(**config_kwargs)
    linter = Linter(config=config)

    all_violations: list[dict] = []

    for sql_file in sql_files:
        sql = sql_file.read_text()

        # Strip config comments before linting (they're not SQL)
        # Count how many header lines to skip, then take the rest
        lines = sql.split("\n")
        header_count = 0
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("-- config:") or stripped.startswith("-- depends_on:") or stripped == "":
                header_count += 1
            else:
                break
        clean_sql = "\n".join(lines[header_count:])

        result = linter.lint_string(clean_sql, fix=fix)

        if fix:
            fixed_sql, changed = result.fix_string()
            if changed:
                # Re-insert config comment header
                header_lines = lines[:header_count]
                sql_file.write_text("\n".join(header_lines) + "\n" + fixed_sql)
                # Re-lint to report only remaining (unfixable) violations
                result = linter.lint_string(fixed_sql)

        rel_path = sql_file.relative_to(transform_dir.parent)
        for violation in result.get_violations():
            all_violations.append({
                "file": str(rel_path),
                "line": violation.line_no + header_count,
                "col": violation.line_pos,
                "code": violation.rule_code(),
                "description": violation.desc(),
                "fixable": bool(violation.fixable),
            })

    return len(all_violations), all_violations


def print_violations(violations: list[dict]) -> None:
    """Pretty-print lint violations."""
    if not violations:
        console.print("[green]All SQL files pass linting.[/green]")
        return

    table = Table(title="Lint Violations")
    table.add_column("File", style="cyan")
    table.add_column("Line", justify="right")
    table.add_column("Col", justify="right")
    table.add_column("Rule", style="yellow")
    table.add_column("Description")

    for v in violations:
        table.add_row(str(v["file"]), str(v["line"]), str(v["col"]), v["code"], v["description"])

    console.print(table)
