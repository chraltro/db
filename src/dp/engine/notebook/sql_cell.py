"""SQL cell execution for notebooks."""

from __future__ import annotations

import time

import duckdb

from dp.engine.sql_analysis import (
    parse_config as parse_sql_config,
    strip_config_comments,
)

from .formatting import _serialize


def _split_sql_statements(sql: str) -> list[str]:
    """Split SQL on semicolons, respecting quoted strings.

    Handles single-quoted strings (including escaped quotes via '')
    so that semicolons inside string literals are not treated as
    statement separators.
    """
    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False

    i = 0
    while i < len(sql):
        ch = sql[i]
        if in_single_quote:
            current.append(ch)
            if ch == "'" and i + 1 < len(sql) and sql[i + 1] == "'":
                # Escaped single quote ''
                current.append(sql[i + 1])
                i += 2
                continue
            elif ch == "'":
                in_single_quote = False
        elif ch == "'":
            in_single_quote = True
            current.append(ch)
        elif ch == "-" and i + 1 < len(sql) and sql[i + 1] == "-":
            # Line comment — consume to end of line
            while i < len(sql) and sql[i] != "\n":
                current.append(sql[i])
                i += 1
            continue
        elif ch == ";":
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
        else:
            current.append(ch)
        i += 1

    # Last statement (no trailing semicolon)
    stmt = "".join(current).strip()
    if stmt:
        statements.append(stmt)

    return statements


def execute_sql_cell(
    conn: duckdb.DuckDBPyConnection,
    source: str,
) -> dict:
    """Execute a SQL cell directly against DuckDB.

    Returns dict with:
        - outputs: list of output items (table, text, error)
        - duration_ms: execution time
        - config: parsed config comments (if any)
    """
    outputs: list[dict] = []
    start = time.perf_counter()
    config = parse_sql_config(source)
    query = strip_config_comments(source).strip()

    if not query:
        duration_ms = int((time.perf_counter() - start) * 1000)
        return {"outputs": [], "duration_ms": duration_ms, "config": config}

    try:
        # Split on semicolons (respecting quoted strings) for multi-statement support
        statements = _split_sql_statements(query)

        for i, stmt in enumerate(statements):
            result = conn.execute(stmt)

            # For queries that return results, render as table
            if result.description:
                columns = [desc[0] for desc in result.description]
                # Fetch only what we need for display (plus 1 to detect truncation)
                max_display = 500
                rows = result.fetchmany(max_display + 1)
                truncated = len(rows) > max_display
                display_rows = rows[:max_display]
                outputs.append({
                    "type": "table",
                    "columns": columns,
                    "rows": [[_serialize(v) for v in row] for row in display_rows],
                    "total_rows": len(display_rows),
                    "truncated": truncated,
                })
            else:
                # DDL/DML: report what happened
                if i == len(statements) - 1 or len(statements) == 1:
                    # Only show status for the last statement or single statements
                    stmt_upper = stmt.strip().upper()
                    if stmt_upper.startswith("CREATE"):
                        outputs.append({"type": "text", "text": "Statement executed successfully."})
                    elif stmt_upper.startswith(("INSERT", "UPDATE", "DELETE")):
                        outputs.append({"type": "text", "text": "Statement executed successfully."})

    except Exception as e:
        outputs.append({"type": "error", "text": str(e)})

    duration_ms = int((time.perf_counter() - start) * 1000)
    return {"outputs": outputs, "duration_ms": duration_ms, "config": config}
