"""SQL query execution, table browsing, and autocomplete endpoints."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from dp.server.deps import (
    DbConnReadOnly,
    _require_permission,
    _serialize,
    _validate_identifier,
)

logger = logging.getLogger("dp.server")

router = APIRouter()


# --- Pydantic models ---


class QueryRequest(BaseModel):
    sql: str = Field(..., min_length=1, max_length=100_000)
    limit: int = Field(default=1000, gt=0, le=50_000)
    offset: int = Field(default=0, ge=0)


# --- Constants ---

_QUERY_TIMEOUT_SECONDS = 30


# --- Query endpoint ---


@router.post("/api/query")
def run_query(request: Request, req: QueryRequest, conn: DbConnReadOnly) -> dict:
    """Run an ad-hoc SQL query with a timeout."""
    _require_permission(request, "read")
    try:
        import threading

        query_result: dict = {}
        query_error: list[Exception] = []

        def _exec_query():
            try:
                if req.offset > 0:
                    wrapped = f"SELECT * FROM ({req.sql}) AS _q OFFSET {req.offset} LIMIT {req.limit}"
                    result = conn.execute(wrapped)
                else:
                    result = conn.execute(req.sql)
                columns = [desc[0] for desc in result.description]
                rows = result.fetchmany(req.limit)
                query_result["data"] = {
                    "columns": columns,
                    "rows": [[_serialize(v) for v in row] for row in rows],
                    "truncated": len(rows) == req.limit,
                    "offset": req.offset,
                    "limit": req.limit,
                }
            except Exception as e:
                query_error.append(e)

        thread = threading.Thread(target=_exec_query, daemon=True)
        thread.start()
        thread.join(timeout=_QUERY_TIMEOUT_SECONDS)

        if thread.is_alive():
            conn.interrupt()
            raise HTTPException(
                408, f"Query timed out after {_QUERY_TIMEOUT_SECONDS}s"
            )
        if query_error:
            raise query_error[0]
        return query_result["data"]
    except HTTPException:
        raise
    except Exception as e:
        logger.warning("Query failed: %s", e)
        raise HTTPException(400, str(e))


# --- Tables ---


@router.get("/api/tables")
def list_tables(
    request: Request, conn: DbConnReadOnly, schema: str | None = None
) -> list[dict]:
    """List warehouse tables and views."""
    _require_permission(request, "read")
    if schema:
        _validate_identifier(schema, "schema")
        rows = conn.execute(
            """
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', '_dp_internal')
              AND table_schema = ?
            ORDER BY table_schema, table_name
            """,
            [schema],
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', '_dp_internal')
            ORDER BY table_schema, table_name
            """
        ).fetchall()
    return [{"schema": r[0], "name": r[1], "type": r[2]} for r in rows]


@router.get("/api/tables/{schema}/{table}")
def describe_table(
    request: Request, schema: str, table: str, conn: DbConnReadOnly
) -> dict:
    """Get column info for a table."""
    _require_permission(request, "read")
    _validate_identifier(schema, "schema")
    _validate_identifier(table, "table")
    cols = conn.execute(
        """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
        """,
        [schema, table],
    ).fetchall()
    return {
        "schema": schema,
        "name": table,
        "columns": [
            {"name": c[0], "type": c[1], "nullable": c[2] == "YES"} for c in cols
        ],
    }


@router.get("/api/tables/{schema}/{table}/sample")
def sample_table(
    request: Request,
    schema: str,
    table: str,
    conn: DbConnReadOnly,
    limit: int = 100,
    offset: int = 0,
) -> dict:
    """Get sample rows from a table with pagination."""
    _require_permission(request, "read")
    _validate_identifier(schema, "schema")
    _validate_identifier(table, "table")
    limit = max(1, min(limit, 10_000))
    offset = max(0, offset)
    try:
        quoted = f'"{schema}"."{table}"'
        result = conn.execute(f"SELECT * FROM {quoted} LIMIT {limit} OFFSET {offset}")
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return {
            "schema": schema,
            "table": table,
            "columns": columns,
            "rows": [[_serialize(v) for v in row] for row in rows],
            "limit": limit,
            "offset": offset,
        }
    except Exception as e:
        logger.warning("Sample query failed for %s.%s: %s", schema, table, e)
        raise HTTPException(400, str(e))


@router.get("/api/tables/{schema}/{table}/profile")
def profile_table(
    request: Request, schema: str, table: str, conn: DbConnReadOnly
) -> dict:
    """Get column-level statistics for a table."""
    _require_permission(request, "read")
    _validate_identifier(schema, "schema")
    _validate_identifier(table, "table")
    try:
        quoted = f'"{schema}"."{table}"'
        row_count = conn.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()[0]
        cols = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            [schema, table],
        ).fetchall()

        profiles = []
        for col_name, col_type in cols:
            qcol = f'"{col_name}"'
            stats: dict = {"name": col_name, "type": col_type}

            basic = conn.execute(
                f"SELECT COUNT(*) - COUNT({qcol}), COUNT(DISTINCT {qcol}) FROM {quoted}"
            ).fetchone()
            stats["null_count"] = basic[0]
            stats["distinct_count"] = basic[1]

            is_numeric = any(
                t in col_type.upper()
                for t in (
                    "INT",
                    "FLOAT",
                    "DOUBLE",
                    "DECIMAL",
                    "NUMERIC",
                    "BIGINT",
                    "SMALLINT",
                    "TINYINT",
                    "HUGEINT",
                )
            )
            if is_numeric:
                num = conn.execute(
                    f"SELECT MIN({qcol}), MAX({qcol}), AVG({qcol}::DOUBLE) FROM {quoted}"
                ).fetchone()
                stats["min"] = _serialize(num[0])
                stats["max"] = _serialize(num[1])
                stats["avg"] = round(num[2], 4) if num[2] is not None else None
            else:
                minmax = conn.execute(
                    f"SELECT MIN({qcol}::VARCHAR), MAX({qcol}::VARCHAR) FROM {quoted}"
                ).fetchone()
                stats["min"] = minmax[0]
                stats["max"] = minmax[1]

            samples = conn.execute(
                f"SELECT DISTINCT {qcol}::VARCHAR FROM {quoted} WHERE {qcol} IS NOT NULL LIMIT 5"
            ).fetchall()
            stats["sample_values"] = [s[0] for s in samples]

            profiles.append(stats)

        return {
            "schema": schema,
            "table": table,
            "row_count": row_count,
            "columns": profiles,
        }
    except Exception as e:
        logger.warning("Profile failed for %s.%s: %s", schema, table, e)
        raise HTTPException(400, str(e))


# --- Autocomplete ---


@router.get("/api/autocomplete")
def get_autocomplete(request: Request, conn: DbConnReadOnly) -> dict:
    """Get table and column names for query autocomplete."""
    _require_permission(request, "read")
    tables = conn.execute(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', '_dp_internal')
        ORDER BY table_schema, table_name
        """
    ).fetchall()

    columns = conn.execute(
        """
        SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema', '_dp_internal')
        ORDER BY table_schema, table_name, ordinal_position
        """
    ).fetchall()

    return {
        "tables": [
            {"schema": t[0], "name": t[1], "full_name": f"{t[0]}.{t[1]}"}
            for t in tables
        ],
        "columns": [
            {
                "schema": c[0],
                "table": c[1],
                "name": c[2],
                "type": c[3],
                "full_name": f"{c[0]}.{c[1]}.{c[2]}",
            }
            for c in columns
        ],
    }
