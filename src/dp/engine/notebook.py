"""Notebook-style execution with cell-by-cell output.

Notebooks are stored as .dpnb files (JSON format).
Cell types:
  - code:     Python code (shared namespace with `db` and `pd`)
  - markdown: Rendered text (not executed)
  - sql:      Pure SQL executed against DuckDB, results auto-rendered as table
  - ingest:   Structured data ingestion (source → landing table)
"""

from __future__ import annotations

import ast
import io
import json
import re
import time
import traceback
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any

import duckdb


# --- Identifier validation ---

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(value: str, label: str = "identifier") -> str:
    """Validate that a value is a safe SQL identifier.

    Raises ValueError if the identifier contains unsafe characters.
    """
    if not _IDENTIFIER_RE.match(value):
        raise ValueError(f"Invalid {label}: {value!r} (must match [A-Za-z_][A-Za-z0-9_]*)")
    return value


# Import shared SQL analysis functions
from dp.engine.sql_analysis import (
    extract_table_refs as _extract_table_refs,
    parse_config as _parse_sql_config_shared,
    strip_config_comments as _strip_sql_comments_shared,
)


def create_notebook(title: str = "Untitled") -> dict:
    """Create a blank notebook structure."""
    return {
        "title": title,
        "cells": [
            {
                "id": "cell_1",
                "type": "markdown",
                "source": f"# {title}\n\nUse this notebook to explore your data.",
            },
            {
                "id": "cell_2",
                "type": "sql",
                "source": "SELECT 1 AS hello",
                "outputs": [],
            },
        ],
    }


def load_notebook(path: Path) -> dict:
    """Load a notebook from a .dpnb file."""
    if not path.exists():
        raise FileNotFoundError(f"Notebook not found: {path}")
    return json.loads(path.read_text())


def save_notebook(path: Path, notebook: dict) -> None:
    """Save a notebook to a .dpnb file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(notebook, indent=2) + "\n")


def _make_cell_id() -> str:
    """Generate a unique cell ID."""
    import secrets
    return f"cell_{secrets.token_hex(6)}"


# --- SQL cell execution ---


def _parse_sql_config(sql: str) -> dict[str, str]:
    """Parse -- config: key=value, key=value from SQL cell source."""
    return _parse_sql_config_shared(sql)


def _strip_sql_comments(sql: str) -> str:
    """Remove config/depends comment lines, return the actual query."""
    return _strip_sql_comments_shared(sql)


def _infer_table_refs(sql: str) -> list[str]:
    """Extract schema.table references from SQL using AST parsing."""
    return _extract_table_refs(sql)


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
    config = _parse_sql_config(source)
    query = _strip_sql_comments(source).strip()

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


# --- Ingest cell execution ---


def execute_ingest_cell(
    conn: duckdb.DuckDBPyConnection,
    source: str,
    project_dir: Path | None = None,
) -> dict:
    """Execute a structured ingest cell.

    The cell source is JSON with fields:
        - source_type: "csv", "parquet", "json", "database", "url"
        - source_path: file path, URL, or query
        - target_schema: target schema (default "landing")
        - target_table: target table name
        - connection: optional connection name from project.yml
        - options: optional dict of extra options

    Returns dict with:
        - outputs: list of output items
        - duration_ms: execution time
    """
    outputs: list[dict] = []
    start = time.perf_counter()

    try:
        spec = json.loads(source)
    except json.JSONDecodeError as e:
        outputs.append({"type": "error", "text": f"Invalid ingest cell JSON: {e}"})
        duration_ms = int((time.perf_counter() - start) * 1000)
        return {"outputs": outputs, "duration_ms": duration_ms}

    source_type = spec.get("source_type", "").lower()
    source_path = spec.get("source_path", "")
    target_schema = spec.get("target_schema", "landing")
    target_table = spec.get("target_table", "")
    connection_name = spec.get("connection")
    options = spec.get("options", {})

    # Validate required fields
    if not source_type:
        return _ingest_error("Missing 'source_type' in ingest cell.", start)
    if not target_table:
        return _ingest_error("Missing 'target_table' in ingest cell.", start)

    # Validate identifiers to prevent SQL injection
    try:
        _validate_identifier(target_schema, "target_schema")
        _validate_identifier(target_table, "target_table")
    except ValueError as e:
        return _ingest_error(str(e), start)

    try:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
        full_table = f"{target_schema}.{target_table}"

        if source_type == "csv":
            resolved = _resolve_path(source_path, project_dir)
            conn.execute(
                f"CREATE OR REPLACE TABLE {full_table} AS "
                f"SELECT * FROM read_csv_auto('{resolved}')"
            )
        elif source_type == "parquet":
            resolved = _resolve_path(source_path, project_dir)
            conn.execute(
                f"CREATE OR REPLACE TABLE {full_table} AS "
                f"SELECT * FROM read_parquet('{resolved}')"
            )
        elif source_type == "json":
            resolved = _resolve_path(source_path, project_dir)
            conn.execute(
                f"CREATE OR REPLACE TABLE {full_table} AS "
                f"SELECT * FROM read_json_auto('{resolved}')"
            )
        elif source_type == "url":
            # DuckDB's httpfs extension for remote files
            conn.execute("INSTALL httpfs; LOAD httpfs;")
            if source_path.endswith(".parquet"):
                conn.execute(
                    f"CREATE OR REPLACE TABLE {full_table} AS "
                    f"SELECT * FROM read_parquet('{source_path}')"
                )
            elif source_path.endswith(".json"):
                conn.execute(
                    f"CREATE OR REPLACE TABLE {full_table} AS "
                    f"SELECT * FROM read_json_auto('{source_path}')"
                )
            else:
                conn.execute(
                    f"CREATE OR REPLACE TABLE {full_table} AS "
                    f"SELECT * FROM read_csv_auto('{source_path}')"
                )
        elif source_type == "database":
            # Use connection from project.yml
            if not connection_name:
                return _ingest_error("Database source requires 'connection' name.", start)
            if not project_dir:
                return _ingest_error("Database ingest requires project context.", start)

            from dp.config import load_project
            config = load_project(project_dir)
            conn_config = config.connections.get(connection_name)
            if not conn_config:
                return _ingest_error(f"Connection '{connection_name}' not found in project.yml.", start)
            from dp.engine.importer import import_from_connection
            result = import_from_connection(
                conn, conn_config.type, conn_config.__dict__,
                source_path, target_schema, target_table,
            )
            if result.get("error"):
                return _ingest_error(result["error"], start)
        else:
            return _ingest_error(f"Unsupported source_type: {source_type}", start)

        # Show preview of loaded data
        row_count = conn.execute(f"SELECT COUNT(*) FROM {full_table}").fetchone()[0]
        preview = conn.execute(f"SELECT * FROM {full_table} LIMIT 10")
        columns = [desc[0] for desc in preview.description]
        rows = preview.fetchall()

        outputs.append({
            "type": "text",
            "text": f"Loaded {row_count:,} rows into {full_table}",
        })
        outputs.append({
            "type": "table",
            "columns": columns,
            "rows": [[_serialize(v) for v in row] for row in rows],
            "total_rows": row_count,
        })

        # Log to run history
        from dp.engine.database import ensure_meta_table, log_run
        try:
            ensure_meta_table(conn)
            log_run(conn, "ingest", full_table, "success",
                    int((time.perf_counter() - start) * 1000), row_count)
        except Exception:
            pass  # Don't fail the cell on logging errors

    except Exception as e:
        outputs.append({"type": "error", "text": str(e)})

    duration_ms = int((time.perf_counter() - start) * 1000)
    return {"outputs": outputs, "duration_ms": duration_ms}


def _ingest_error(msg: str, start: float) -> dict:
    """Build an ingest cell error response."""
    return {
        "outputs": [{"type": "error", "text": msg}],
        "duration_ms": int((time.perf_counter() - start) * 1000),
    }


def _resolve_path(source_path: str, project_dir: Path | None) -> str:
    """Resolve a relative path against the project directory.

    Validates that the resolved path stays within the project directory
    to prevent path traversal attacks.
    """
    p = Path(source_path)
    if not p.is_absolute() and project_dir:
        p = project_dir / p
    resolved = p.resolve()
    # Prevent path traversal: resolved path must stay within project_dir
    if project_dir and not str(resolved).startswith(str(project_dir.resolve())):
        raise ValueError(f"Path traversal detected: {source_path!r} resolves outside project directory")
    return str(resolved)


# --- Python code cell execution ---


def execute_cell(
    conn: duckdb.DuckDBPyConnection,
    source: str,
    namespace: dict[str, Any] | None = None,
) -> dict:
    """Execute a single Python code cell.

    Returns dict with:
        - outputs: list of output items (text, table, error)
        - namespace: updated namespace for subsequent cells
        - duration_ms: execution time
    """
    if namespace is None:
        namespace = {}

    # Inject helpers into namespace
    namespace["db"] = conn
    namespace["__builtins__"] = __builtins__

    # Try importing pandas for convenience
    try:
        import pandas as pd
        namespace["pd"] = pd
    except ImportError:
        pass

    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()
    outputs: list[dict] = []
    start = time.perf_counter()

    try:
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            # Parse source into an AST so we can separate the last expression
            # from preceding statements. exec() discards expression values,
            # so we eval() the last expression separately to capture its result.
            tree = ast.parse(source)
            last_expr_value = None

            if tree.body and isinstance(tree.body[-1], ast.Expr):
                # Last statement is an expression — pop it off for eval
                last_expr_node = tree.body.pop()
                # Execute all preceding statements
                if tree.body:
                    exec(compile(tree, "<cell>", "exec"), namespace)
                # Eval the last expression to capture its display value
                expr_code = compile(
                    ast.Expression(last_expr_node.value), "<cell>", "eval"
                )
                last_expr_value = eval(expr_code, namespace)
            else:
                # No trailing expression (e.g. assignment, import, etc.)
                exec(compile(tree, "<cell>", "exec"), namespace)

            if last_expr_value is not None:
                outputs.append(_format_result(last_expr_value))

        # Capture stdout
        stdout_text = stdout_capture.getvalue()
        if stdout_text:
            outputs.insert(0, {"type": "text", "text": stdout_text})

        stderr_text = stderr_capture.getvalue()
        if stderr_text:
            outputs.append({"type": "text", "text": stderr_text})

    except Exception:
        error_text = traceback.format_exc()
        outputs.append({"type": "error", "text": error_text})

    duration_ms = int((time.perf_counter() - start) * 1000)
    return {"outputs": outputs, "namespace": namespace, "duration_ms": duration_ms}


def _format_result(result: Any) -> dict:
    """Format a cell result for display."""
    # DuckDB result
    if hasattr(result, "fetchdf"):
        try:
            df = result.fetchdf()
            return _format_dataframe(df)
        except Exception:
            return {"type": "text", "text": str(result)}

    # Pandas DataFrame
    try:
        import pandas as pd
        if isinstance(result, pd.DataFrame):
            return _format_dataframe(result)
        if isinstance(result, pd.Series):
            return _format_dataframe(result.to_frame())
    except ImportError:
        pass

    # DuckDB relation
    if hasattr(result, "columns") and hasattr(result, "fetchall"):
        try:
            columns = result.columns
            rows = result.fetchall()
            return {
                "type": "table",
                "columns": columns,
                "rows": [[_serialize(v) for v in row] for row in rows[:500]],
                "total_rows": len(rows),
            }
        except Exception:
            pass

    # Plain text
    return {"type": "text", "text": repr(result)}


def _format_dataframe(df) -> dict:
    """Format a pandas DataFrame for display."""
    columns = list(df.columns)
    rows = []
    for _, row in df.head(500).iterrows():
        rows.append([_serialize(v) for v in row])
    return {
        "type": "table",
        "columns": columns,
        "rows": rows,
        "total_rows": len(df),
    }


def _serialize(value: Any) -> Any:
    """Make values JSON-serializable."""
    if value is None:
        return None
    if isinstance(value, (int, float, str, bool)):
        return value
    return str(value)


# --- Run all cells ---


def run_notebook(
    conn: duckdb.DuckDBPyConnection,
    notebook: dict,
    project_dir: Path | None = None,
) -> dict:
    """Execute all executable cells in a notebook sequentially.

    Handles code, sql, and ingest cell types. Markdown cells are skipped.
    Returns the notebook with updated outputs and per-cell timing.
    """
    namespace: dict[str, Any] = {}
    total_ms = 0
    cell_results: list[dict] = []

    for cell in notebook.get("cells", []):
        cell_type = cell.get("type", "")
        source = cell.get("source", "")

        if cell_type == "code":
            result = execute_cell(conn, source, namespace)
            namespace = result["namespace"]
        elif cell_type == "sql":
            result = execute_sql_cell(conn, source)
        elif cell_type == "ingest":
            result = execute_ingest_cell(conn, source, project_dir)
        else:
            continue

        cell["outputs"] = result["outputs"]
        cell["duration_ms"] = result["duration_ms"]
        total_ms += result["duration_ms"]
        cell_results.append({
            "cell_id": cell.get("id"),
            "type": cell_type,
            "duration_ms": result["duration_ms"],
            "has_error": any(o.get("type") == "error" for o in result["outputs"]),
            "outputs": result["outputs"],
        })

    notebook["last_run_ms"] = total_ms
    notebook["cell_results"] = cell_results
    return notebook


# --- Promote SQL cell to model ---


def promote_sql_to_model(
    sql_source: str,
    model_name: str,
    schema: str,
    transform_dir: Path,
    description: str = "",
    overwrite: bool = False,
) -> Path:
    """Promote a SQL cell from a notebook to a transform model file.

    Generates the SQL file with proper config and depends_on comments
    based on the SQL content, and writes it to the correct transform directory.

    Args:
        sql_source: The SQL source from the notebook cell
        model_name: Name for the model (becomes the filename)
        schema: Target schema (bronze, silver, gold, etc.)
        transform_dir: Path to the transform/ directory
        description: Optional model description
        overwrite: If False (default), raises FileExistsError when model file already exists

    Returns:
        Path to the created .sql file
    """
    # Validate identifiers to prevent path traversal and SQL injection
    _validate_identifier(model_name, "model_name")
    _validate_identifier(schema, "schema")

    # Parse existing config from SQL if present
    existing_config = _parse_sql_config(sql_source)
    query = _strip_sql_comments(sql_source).strip()

    # Use existing config values or defaults
    materialized = existing_config.get("materialized", "table")
    target_schema = existing_config.get("schema", schema)
    _validate_identifier(target_schema, "target schema from config")

    # Infer dependencies from the SQL
    refs = _infer_table_refs(query)

    # Build the model file content
    lines = []
    config_parts = [f"materialized={materialized}", f"schema={target_schema}"]
    lines.append(f"-- config: {', '.join(config_parts)}")

    if refs:
        lines.append(f"-- depends_on: {', '.join(refs)}")

    if description:
        lines.append(f"-- description: {description}")

    lines.append("")
    lines.append(query)
    lines.append("")

    content = "\n".join(lines)

    # Write to the correct directory
    schema_dir = transform_dir / target_schema
    schema_dir.mkdir(parents=True, exist_ok=True)
    model_path = schema_dir / f"{model_name}.sql"

    if model_path.exists() and not overwrite:
        raise FileExistsError(
            f"Model file already exists: {model_path}. "
            f"Use overwrite=True to replace it."
        )

    model_path.write_text(content)

    return model_path


def model_to_notebook(
    conn: duckdb.DuckDBPyConnection,
    model_full_name: str,
    transform_dir: Path,
    notebook_dir: Path,
) -> dict:
    """Create a notebook from a transform model for interactive debugging.

    Generates a notebook with:
    - A markdown cell explaining the model
    - SQL cells querying each upstream dependency (sample data)
    - The model's SQL as a SQL cell
    - A SQL cell showing the model's current output

    Args:
        conn: DuckDB connection
        model_full_name: e.g. "silver.customers"
        transform_dir: Path to the transform/ directory
        notebook_dir: Path to write the notebook

    Returns:
        The notebook dict
    """
    from dp.engine.transform import discover_models

    models = discover_models(transform_dir)
    model_map = {m.full_name: m for m in models}

    target = model_map.get(model_full_name)
    if not target:
        raise ValueError(f"Model '{model_full_name}' not found in transform directory")

    cells: list[dict] = []

    # Title cell
    cells.append({
        "id": _make_cell_id(),
        "type": "markdown",
        "source": (
            f"# Debug: {model_full_name}\n\n"
            f"**Materialized as:** {target.materialized}\n"
            f"**Path:** `{target.path}`\n"
            f"**Dependencies:** {', '.join(target.depends_on) or 'none'}"
        ),
    })

    # Upstream data cells
    for dep in target.depends_on:
        cells.append({
            "id": _make_cell_id(),
            "type": "markdown",
            "source": f"### Upstream: `{dep}`",
        })
        cells.append({
            "id": _make_cell_id(),
            "type": "sql",
            "source": f"SELECT * FROM {dep} LIMIT 100",
            "outputs": [],
        })

    # The model SQL itself
    cells.append({
        "id": _make_cell_id(),
        "type": "markdown",
        "source": f"### Model SQL: `{model_full_name}`\n\nEdit and re-run to test changes:",
    })
    cells.append({
        "id": _make_cell_id(),
        "type": "sql",
        "source": target.sql,
        "outputs": [],
    })

    # Current output (if table exists)
    cells.append({
        "id": _make_cell_id(),
        "type": "markdown",
        "source": f"### Current output of `{model_full_name}`",
    })
    cells.append({
        "id": _make_cell_id(),
        "type": "sql",
        "source": f"SELECT * FROM {model_full_name} LIMIT 100",
        "outputs": [],
    })

    nb = {
        "title": f"Debug: {model_full_name}",
        "cells": cells,
    }

    return nb


# --- Debug notebook generation ---


def generate_debug_notebook(
    conn: duckdb.DuckDBPyConnection,
    model_full_name: str,
    transform_dir: Path,
    error_message: str | None = None,
    assertion_failures: list[dict] | None = None,
) -> dict:
    """Generate a debug notebook for a failed model.

    Pre-populates the notebook with:
    - Error explanation
    - Upstream dependency queries
    - The failing model SQL
    - Assertion failure details (if applicable)

    Args:
        conn: DuckDB connection
        model_full_name: e.g. "silver.customers"
        transform_dir: Path to transform/ directory
        error_message: The error that caused the failure
        assertion_failures: List of failed assertions with details

    Returns:
        The notebook dict
    """
    from dp.engine.transform import discover_models

    models = discover_models(transform_dir)
    model_map = {m.full_name: m for m in models}

    target = model_map.get(model_full_name)
    if not target:
        raise ValueError(f"Model '{model_full_name}' not found")

    cells: list[dict] = []

    # Error summary
    error_desc = ""
    if error_message:
        error_desc = f"\n\n**Error:**\n```\n{error_message}\n```"
    elif assertion_failures:
        failed_names = [a.get("expression", "?") for a in assertion_failures]
        error_desc = f"\n\n**Failed assertions:** {', '.join(failed_names)}"

    cells.append({
        "id": _make_cell_id(),
        "type": "markdown",
        "source": (
            f"# Debug: {model_full_name}\n\n"
            f"This notebook was auto-generated to help debug a failure in "
            f"`{model_full_name}`.{error_desc}\n\n"
            f"**Materialized as:** {target.materialized}\n"
            f"**Path:** `{target.path}`\n"
            f"**Dependencies:** {', '.join(target.depends_on) or 'none'}"
        ),
    })

    # Upstream dependency cells
    if target.depends_on:
        cells.append({
            "id": _make_cell_id(),
            "type": "markdown",
            "source": "## Upstream Data\n\nCheck the data feeding into this model:",
        })

        for dep in target.depends_on:
            # Schema check
            cells.append({
                "id": _make_cell_id(),
                "type": "sql",
                "source": (
                    f"-- Row count and schema for {dep}\n"
                    f"SELECT COUNT(*) AS row_count FROM {dep}"
                ),
                "outputs": [],
            })
            cells.append({
                "id": _make_cell_id(),
                "type": "sql",
                "source": f"SELECT * FROM {dep} LIMIT 20",
                "outputs": [],
            })

    # The failing model SQL
    cells.append({
        "id": _make_cell_id(),
        "type": "markdown",
        "source": (
            f"## Model SQL\n\n"
            f"The SQL below is from `{target.path}`. "
            f"Edit and run to test fixes:"
        ),
    })
    cells.append({
        "id": _make_cell_id(),
        "type": "sql",
        "source": target.sql,
        "outputs": [],
    })

    # Assertion failure details
    if assertion_failures:
        cells.append({
            "id": _make_cell_id(),
            "type": "markdown",
            "source": "## Assertion Failures\n\nThe following assertions failed:",
        })

        for af in assertion_failures:
            expr = af.get("expression", "")
            detail = af.get("detail", "")

            cells.append({
                "id": _make_cell_id(),
                "type": "markdown",
                "source": f"### `{expr}`\n\nDetail: {detail}",
            })

            # Generate diagnostic query based on assertion type
            diag_sql = _assertion_diagnostic_sql(model_full_name, expr)
            if diag_sql:
                cells.append({
                    "id": _make_cell_id(),
                    "type": "sql",
                    "source": diag_sql,
                    "outputs": [],
                })

    return {
        "title": f"Debug: {model_full_name}",
        "cells": cells,
    }


def _assertion_diagnostic_sql(table: str, expr: str) -> str:
    """Generate diagnostic SQL for a failed assertion."""
    # unique(column) — show duplicate rows
    m = re.match(r"unique\((\w+)\)", expr)
    if m:
        col = m.group(1)
        return (
            f"-- Show duplicate values for {col}\n"
            f'SELECT "{col}", COUNT(*) AS cnt\n'
            f"FROM {table}\n"
            f'GROUP BY "{col}"\n'
            f"HAVING COUNT(*) > 1\n"
            f"ORDER BY cnt DESC\n"
            f"LIMIT 20"
        )

    # no_nulls(column) — show null rows
    m = re.match(r"no_nulls\((\w+)\)", expr)
    if m:
        col = m.group(1)
        return (
            f"-- Show rows where {col} is NULL\n"
            f"SELECT *\n"
            f"FROM {table}\n"
            f'WHERE "{col}" IS NULL\n'
            f"LIMIT 20"
        )

    # row_count check
    m = re.match(r"row_count\s*(>|>=|<|<=|=|==|!=)\s*(\d+)", expr)
    if m:
        return (
            f"-- Current row count\n"
            f"SELECT COUNT(*) AS row_count FROM {table}"
        )

    # accepted_values(column, [...])
    m = re.match(r"accepted_values\((\w+),\s*\[(.+)\]\)", expr)
    if m:
        col = m.group(1)
        raw_values = m.group(2)
        values = [v.strip().strip("'\"") for v in raw_values.split(",")]
        placeholders = ", ".join(f"'{v}'" for v in values)
        return (
            f"-- Show rows with invalid values for {col}\n"
            f'SELECT "{col}", COUNT(*) AS cnt\n'
            f"FROM {table}\n"
            f'WHERE "{col}" IS NOT NULL AND "{col}"::VARCHAR NOT IN ({placeholders})\n'
            f'GROUP BY "{col}"\n'
            f"ORDER BY cnt DESC\n"
            f"LIMIT 20"
        )

    return ""


# --- Notebook output declarations ---


def extract_notebook_outputs(notebook: dict) -> list[str]:
    """Extract declared output tables from a notebook.

    Notebooks can declare outputs via a top-level "outputs" key or
    by scanning SQL/ingest cells for table creation patterns.

    Returns list of fully-qualified table names (e.g. ["landing.earthquakes"]).
    """
    # Explicit declaration
    declared = notebook.get("outputs", [])
    if declared:
        return declared

    # Infer from cells
    outputs: set[str] = set()

    create_pattern = re.compile(
        r"(?:CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+|INTO\s+)"
        r"(\w+\.\w+)",
        re.IGNORECASE,
    )

    for cell in notebook.get("cells", []):
        cell_type = cell.get("type", "")
        source = cell.get("source", "")

        if cell_type == "sql":
            for match in create_pattern.finditer(source):
                outputs.add(match.group(1).lower())

        elif cell_type == "ingest":
            try:
                spec = json.loads(source)
                schema = spec.get("target_schema", "landing")
                table = spec.get("target_table", "")
                if table:
                    outputs.add(f"{schema}.{table}")
            except (json.JSONDecodeError, TypeError):
                pass

        elif cell_type == "code":
            for match in create_pattern.finditer(source):
                outputs.add(match.group(1).lower())

    return sorted(outputs)
