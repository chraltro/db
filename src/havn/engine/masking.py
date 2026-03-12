"""Column-level data masking engine.

Provides post-query masking of sensitive columns based on policies stored
in ``_dp_internal.masking_policies``.  Masking is applied to result rows
*after* query execution, before returning to the client.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import duckdb


# ---------------------------------------------------------------------------
# Table bootstrap
# ---------------------------------------------------------------------------


def ensure_masking_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the masking_policies table if it doesn't exist."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS _dp_internal")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _dp_internal.masking_policies (
            id               VARCHAR PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR,
            schema_name      VARCHAR NOT NULL,
            table_name       VARCHAR NOT NULL,
            column_name      VARCHAR NOT NULL,
            method           VARCHAR NOT NULL,
            method_config    JSON,
            condition_column VARCHAR,
            condition_value  VARCHAR,
            exempted_roles   JSON DEFAULT '["admin"]',
            created_at       TIMESTAMP DEFAULT current_timestamp
        )
    """)


# ---------------------------------------------------------------------------
# Masking functions
# ---------------------------------------------------------------------------


def mask_hash(value: Any) -> str:
    """SHA-256 hash, first 8 hex chars."""
    if value is None:
        return None
    return hashlib.sha256(str(value).encode()).hexdigest()[:8]


def mask_redact(value: Any) -> str:
    """Replace with ``***``."""
    if value is None:
        return None
    return "***"


def mask_null(value: Any) -> None:
    """Replace with None."""
    return None


def mask_partial(value: Any, show_first: int = 0, show_last: int = 0) -> str:
    """Show first/last N chars, mask the rest with ``*``."""
    if value is None:
        return None
    s = str(value)
    if show_first + show_last >= len(s):
        return s
    masked_len = len(s) - show_first - show_last
    return s[:show_first] + "*" * masked_len + s[len(s) - show_last:] if show_last else s[:show_first] + "*" * masked_len


_MASKING_FNS = {
    "hash": mask_hash,
    "redact": mask_redact,
    "null": mask_null,
    "partial": mask_partial,
}


def apply_mask(value: Any, method: str, method_config: dict | None = None) -> Any:
    """Apply a single masking method to a value."""
    fn = _MASKING_FNS.get(method)
    if fn is None:
        return value
    if method == "partial" and method_config:
        return fn(value, show_first=method_config.get("show_first", 0), show_last=method_config.get("show_last", 0))
    return fn(value)


# ---------------------------------------------------------------------------
# Policy matching & application
# ---------------------------------------------------------------------------


def _load_policies(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """Load all masking policies from the database."""
    ensure_masking_table(conn)
    rows = conn.execute(
        "SELECT id, schema_name, table_name, column_name, method, method_config, "
        "condition_column, condition_value, exempted_roles "
        "FROM _dp_internal.masking_policies"
    ).fetchall()
    policies = []
    for r in rows:
        config = json.loads(r[5]) if r[5] else None
        exempted = json.loads(r[8]) if r[8] else ["admin"]
        policies.append({
            "id": r[0],
            "schema_name": r[1],
            "table_name": r[2],
            "column_name": r[3],
            "method": r[4],
            "method_config": config,
            "condition_column": r[6],
            "condition_value": r[7],
            "exempted_roles": exempted,
        })
    return policies


def apply_masking(
    columns: list[str],
    rows: list[list[Any]],
    user_role: str,
    conn: duckdb.DuckDBPyConnection,
    schema: str | None = None,
    table: str | None = None,
) -> list[list[Any]]:
    """Apply masking policies to query result rows.

    Parameters
    ----------
    columns : column names from the result set
    rows : list of row lists (mutable — will be modified in place)
    user_role : the requesting user's role
    conn : DuckDB connection for loading policies
    schema / table : when known (e.g. /sample), enables exact matching.
        When None (ad-hoc /query), does best-effort column-name matching.

    Returns the (possibly modified) rows.
    """
    policies = _load_policies(conn)
    if not policies:
        return rows

    # Build a column-index map
    col_idx = {c.lower(): i for i, c in enumerate(columns)}

    # Filter to relevant policies
    matched: list[tuple[dict, int]] = []  # (policy, column_index)
    for p in policies:
        # Check role exemption
        if user_role in p["exempted_roles"]:
            continue

        col_lower = p["column_name"].lower()
        if col_lower not in col_idx:
            continue

        # If schema/table known, require exact match
        if schema is not None and table is not None:
            if p["schema_name"].lower() != schema.lower() or p["table_name"].lower() != table.lower():
                continue
        # If no schema/table (ad-hoc query), match on column name alone

        matched.append((p, col_idx[col_lower]))

    if not matched:
        return rows

    # Apply masking
    for row in rows:
        for policy, idx in matched:
            # Check condition if present
            if policy["condition_column"]:
                cond_col_lower = policy["condition_column"].lower()
                if cond_col_lower in col_idx:
                    cond_idx = col_idx[cond_col_lower]
                    if str(row[cond_idx]) != policy["condition_value"]:
                        continue
                else:
                    # Condition column not in result — skip masking
                    continue

            row[idx] = apply_mask(row[idx], policy["method"], policy["method_config"])

    return rows


# ---------------------------------------------------------------------------
# CRUD helpers
# ---------------------------------------------------------------------------


def create_policy(
    conn: duckdb.DuckDBPyConnection,
    *,
    schema_name: str,
    table_name: str,
    column_name: str,
    method: str,
    method_config: dict | None = None,
    condition_column: str | None = None,
    condition_value: str | None = None,
    exempted_roles: list[str] | None = None,
) -> dict:
    """Insert a new masking policy and return it."""
    if method not in _MASKING_FNS:
        raise ValueError(f"Unknown masking method: {method!r}. Must be one of {list(_MASKING_FNS)}")

    ensure_masking_table(conn)
    exempted = exempted_roles or ["admin"]
    config_json = json.dumps(method_config) if method_config else None
    exempted_json = json.dumps(exempted)

    row = conn.execute(
        """
        INSERT INTO _dp_internal.masking_policies
            (schema_name, table_name, column_name, method, method_config,
             condition_column, condition_value, exempted_roles)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        RETURNING id, created_at
        """,
        [schema_name, table_name, column_name, method, config_json,
         condition_column, condition_value, exempted_json],
    ).fetchone()

    return {
        "id": row[0],
        "schema_name": schema_name,
        "table_name": table_name,
        "column_name": column_name,
        "method": method,
        "method_config": method_config,
        "condition_column": condition_column,
        "condition_value": condition_value,
        "exempted_roles": exempted,
        "created_at": str(row[1]),
    }


def list_policies(conn: duckdb.DuckDBPyConnection) -> list[dict]:
    """Return all masking policies."""
    return _load_policies(conn)


def get_policy(conn: duckdb.DuckDBPyConnection, policy_id: str) -> dict | None:
    """Return a single policy by ID, or None."""
    ensure_masking_table(conn)
    row = conn.execute(
        "SELECT id, schema_name, table_name, column_name, method, method_config, "
        "condition_column, condition_value, exempted_roles, created_at "
        "FROM _dp_internal.masking_policies WHERE id = ?",
        [policy_id],
    ).fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "schema_name": row[1],
        "table_name": row[2],
        "column_name": row[3],
        "method": row[4],
        "method_config": json.loads(row[5]) if row[5] else None,
        "condition_column": row[6],
        "condition_value": row[7],
        "exempted_roles": json.loads(row[8]) if row[8] else ["admin"],
        "created_at": str(row[9]) if row[9] else None,
    }


def update_policy(conn: duckdb.DuckDBPyConnection, policy_id: str, **updates) -> dict | None:
    """Update fields of an existing policy. Returns updated policy or None."""
    existing = get_policy(conn, policy_id)
    if not existing:
        return None

    allowed = {"schema_name", "table_name", "column_name", "method", "method_config",
               "condition_column", "condition_value", "exempted_roles"}
    sets = []
    params = []
    for key, val in updates.items():
        if key not in allowed:
            continue
        if key == "method" and val not in _MASKING_FNS:
            raise ValueError(f"Unknown masking method: {val!r}")
        if key in ("method_config", "exempted_roles"):
            val = json.dumps(val) if val is not None else None
        sets.append(f"{key} = ?")
        params.append(val)

    if not sets:
        return existing

    params.append(policy_id)
    conn.execute(
        f"UPDATE _dp_internal.masking_policies SET {', '.join(sets)} WHERE id = ?",
        params,
    )
    return get_policy(conn, policy_id)


def delete_policy(conn: duckdb.DuckDBPyConnection, policy_id: str) -> bool:
    """Delete a policy by ID. Returns True if deleted."""
    ensure_masking_table(conn)
    before = conn.execute("SELECT COUNT(*) FROM _dp_internal.masking_policies WHERE id = ?", [policy_id]).fetchone()[0]
    if not before:
        return False
    conn.execute("DELETE FROM _dp_internal.masking_policies WHERE id = ?", [policy_id])
    return True
