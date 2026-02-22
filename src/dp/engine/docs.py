"""Documentation generator.

Generates markdown docs from DuckDB information_schema + SQL model files.
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

logger = logging.getLogger("dp.docs")


def generate_docs(
    conn: duckdb.DuckDBPyConnection,
    transform_dir: Path,
    sources: list | None = None,
    exposures: list | None = None,
) -> str:
    """Generate markdown documentation for the warehouse.

    Combines:
    - information_schema for table/column metadata
    - SQL model files for dependencies and config
    - sources.yml declarations
    - exposures.yml declarations
    """
    from dp.engine.transform import discover_models

    lines: list[str] = []
    lines.append("# Data Warehouse Documentation\n")

    # Discover models for dependency info
    models = discover_models(transform_dir) if transform_dir.exists() else []
    model_map = {m.full_name: m for m in models}

    # Get all schemas (excluding internal)
    schemas = conn.execute("""
        SELECT DISTINCT table_schema
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', '_dp_internal')
        ORDER BY
            CASE table_schema
                WHEN 'landing' THEN 1
                WHEN 'bronze' THEN 2
                WHEN 'silver' THEN 3
                WHEN 'gold' THEN 4
                ELSE 5
            END
    """).fetchall()

    if not schemas:
        lines.append("*No tables found. Run a pipeline first.*\n")
        return "\n".join(lines)

    # Table of contents
    lines.append("## Overview\n")
    for (schema_name,) in schemas:
        tables = conn.execute("""
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = ?
            ORDER BY table_name
        """, [schema_name]).fetchall()
        lines.append(f"### {schema_name}\n")
        for table_name, table_type in tables:
            kind = "view" if table_type == "VIEW" else "table"
            lines.append(f"- [`{schema_name}.{table_name}`](#{schema_name}{table_name}) ({kind})")
        lines.append("")

    # Detailed documentation per table
    lines.append("---\n")
    lines.append("## Models\n")

    for (schema_name,) in schemas:
        tables = conn.execute("""
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = ?
            ORDER BY table_name
        """, [schema_name]).fetchall()

        for table_name, table_type in tables:
            full_name = f"{schema_name}.{table_name}"
            kind = "VIEW" if table_type == "VIEW" else "TABLE"

            lines.append(f"### <a id=\"{schema_name}{table_name}\"></a>`{full_name}` ({kind})\n")

            # Model metadata if available
            model = model_map.get(full_name)
            if model:
                if model.description:
                    lines.append(f"{model.description}\n")
                if model.depends_on:
                    deps = ", ".join(f"`{d}`" for d in model.depends_on)
                    lines.append(f"**Depends on:** {deps}\n")
                lines.append(f"**Materialized as:** {model.materialized}\n")

            # Row count for tables
            if kind == "TABLE":
                try:
                    count = conn.execute(f"SELECT count(*) FROM {full_name}").fetchone()[0]
                    lines.append(f"**Row count:** {count:,}\n")
                except Exception as e:
                    logger.debug("Could not get table stats: %s", e)

            # Columns
            cols = conn.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = ? AND table_name = ?
                ORDER BY ordinal_position
            """, [schema_name, table_name]).fetchall()

            col_docs = model.column_docs if model else {}
            has_docs = any(c[0] in col_docs for c in cols)

            if has_docs:
                lines.append("| Column | Type | Nullable | Description |")
                lines.append("|--------|------|----------|-------------|")
            else:
                lines.append("| Column | Type | Nullable |")
                lines.append("|--------|------|----------|")
            for col_name, data_type, nullable, default in cols:
                null_str = "yes" if nullable == "YES" else "no"
                if has_docs:
                    desc = col_docs.get(col_name, "")
                    lines.append(f"| `{col_name}` | {data_type} | {null_str} | {desc} |")
                else:
                    lines.append(f"| `{col_name}` | {data_type} | {null_str} |")
            lines.append("")

            # SQL source if available
            if model:
                lines.append("<details><summary>SQL Source</summary>\n")
                lines.append("```sql")
                lines.append(model.sql.strip())
                lines.append("```")
                lines.append("</details>\n")

    # Sources section
    if sources:
        lines.append("---\n")
        lines.append("## Sources\n")
        for src in sources:
            lines.append(f"### {src.name}\n")
            if src.description:
                lines.append(f"{src.description}\n")
            if src.freshness_hours is not None:
                lines.append(f"**Freshness SLA:** {src.freshness_hours} hours\n")
            if src.connection:
                lines.append(f"**Connection:** `{src.connection}`\n")
            for tbl in src.tables:
                lines.append(f"#### `{src.schema}.{tbl.name}`\n")
                if tbl.description:
                    lines.append(f"{tbl.description}\n")
                if tbl.columns:
                    lines.append("| Column | Description |")
                    lines.append("|--------|-------------|")
                    for col in tbl.columns:
                        lines.append(f"| `{col.name}` | {col.description} |")
                    lines.append("")

    # Exposures section
    if exposures:
        lines.append("---\n")
        lines.append("## Exposures\n")
        for exp in exposures:
            lines.append(f"### {exp.name}\n")
            if exp.description:
                lines.append(f"{exp.description}\n")
            if exp.owner:
                lines.append(f"**Owner:** {exp.owner}\n")
            if exp.type:
                lines.append(f"**Type:** {exp.type}\n")
            if exp.depends_on:
                deps = ", ".join(f"`{d}`" for d in exp.depends_on)
                lines.append(f"**Depends on:** {deps}\n")

    # Lineage summary
    if models:
        lines.append("---\n")
        lines.append("## Lineage\n")
        lines.append("```")
        for model in models:
            if model.depends_on:
                for dep in model.depends_on:
                    lines.append(f"{dep} --> {model.full_name}")
            else:
                lines.append(f"(source) --> {model.full_name}")
        if exposures:
            for exp in exposures:
                for dep in exp.depends_on:
                    lines.append(f"{dep} --> [exposure:{exp.name}]")
        lines.append("```\n")

    return "\n".join(lines)


def generate_structured_docs(
    conn: duckdb.DuckDBPyConnection, transform_dir: Path
) -> dict:
    """Generate structured documentation for the warehouse.

    Returns a JSON-serializable dict with schema/table metadata
    for a two-pane UI layout.
    """
    from dp.engine.transform import discover_models

    models = discover_models(transform_dir) if transform_dir.exists() else []
    model_map = {m.full_name: m for m in models}

    # Get all schemas (excluding internal)
    schemas_raw = conn.execute("""
        SELECT DISTINCT table_schema
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', '_dp_internal')
        ORDER BY
            CASE table_schema
                WHEN 'landing' THEN 1
                WHEN 'bronze' THEN 2
                WHEN 'silver' THEN 3
                WHEN 'gold' THEN 4
                ELSE 5
            END
    """).fetchall()

    if not schemas_raw:
        return {"schemas": []}

    schema_list = []
    for (schema_name,) in schemas_raw:
        tables_raw = conn.execute("""
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = ?
            ORDER BY table_name
        """, [schema_name]).fetchall()

        table_list = []
        for table_name, table_type in tables_raw:
            full_name = f"{schema_name}.{table_name}"
            kind = "view" if table_type == "VIEW" else "table"
            model = model_map.get(full_name)

            # Row count
            row_count = None
            if kind == "table":
                try:
                    row_count = conn.execute(
                        f"SELECT count(*) FROM {full_name}"
                    ).fetchone()[0]
                except Exception as e:
                    logger.debug("Could not get table stats for JSON: %s", e)

            # Columns
            cols_raw = conn.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = ? AND table_name = ?
                ORDER BY ordinal_position
            """, [schema_name, table_name]).fetchall()

            col_docs = model.column_docs if model else {}
            columns = []
            for col_name, data_type, nullable in cols_raw:
                columns.append({
                    "name": col_name,
                    "type": data_type,
                    "nullable": nullable == "YES",
                    "description": col_docs.get(col_name, ""),
                })

            table_info: dict = {
                "name": table_name,
                "full_name": full_name,
                "type": kind,
                "columns": columns,
                "row_count": row_count,
            }

            if model:
                table_info["description"] = model.description or ""
                table_info["depends_on"] = model.depends_on
                table_info["materialized"] = model.materialized
                table_info["sql"] = model.sql.strip()

            table_list.append(table_info)

        schema_list.append({"name": schema_name, "tables": table_list})

    # Lineage
    lineage = []
    for m in models:
        if m.depends_on:
            for dep in m.depends_on:
                lineage.append({"source": dep, "target": m.full_name})
        else:
            lineage.append({"source": "(source)", "target": m.full_name})

    return {"schemas": schema_list, "lineage": lineage}
