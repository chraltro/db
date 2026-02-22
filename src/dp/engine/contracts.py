"""Data contracts & assertions framework.

Discovers YAML contract files in the contracts/ directory, evaluates them
against the warehouse, and tracks historical pass/fail results. Contracts
are standalone data quality rules that complement inline ``-- assert:`` comments.

Contract YAML format::

    contracts:
      - name: orders_not_empty
        description: "Orders table must have data"
        model: gold.orders
        assertions:
          - row_count > 0
          - no_nulls(order_id)
          - unique(order_id)
          - accepted_values(status, ['pending', 'shipped', 'delivered'])
          - "total_amount >= 0"

      - name: customers_fresh
        description: "Customers must be loaded within 24h"
        model: silver.customers
        severity: warn
        assertions:
          - row_count > 0
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import yaml

from dp.engine.database import ensure_meta_table, log_run

logger = logging.getLogger("dp.contracts")


@dataclass
class Contract:
    """A single data contract definition."""

    name: str
    model: str  # e.g. "gold.orders"
    assertions: list[str] = field(default_factory=list)
    description: str = ""
    severity: str = "error"  # "error" or "warn"
    path: Path | None = None  # source file


@dataclass
class ContractResult:
    """Result of evaluating a single contract."""

    contract_name: str
    model: str
    passed: bool
    severity: str
    results: list[dict]  # [{expression, passed, detail}]
    duration_ms: int = 0
    error: str | None = None


def discover_contracts(contracts_dir: Path) -> list[Contract]:
    """Discover all contract YAML files in the contracts/ directory.

    Each .yml file can contain a ``contracts:`` list with one or more
    contract definitions.
    """
    contracts: list[Contract] = []
    if not contracts_dir.exists():
        return contracts

    for yml_file in sorted(contracts_dir.glob("*.yml")):
        try:
            raw = yaml.safe_load(yml_file.read_text()) or {}
            for c_raw in raw.get("contracts", []):
                contracts.append(Contract(
                    name=c_raw.get("name", yml_file.stem),
                    model=c_raw.get("model", ""),
                    assertions=c_raw.get("assertions", []),
                    description=c_raw.get("description", ""),
                    severity=c_raw.get("severity", "error"),
                    path=yml_file,
                ))
        except Exception as e:
            logger.warning("Failed to parse contract file %s: %s", yml_file, e)

    return contracts


def evaluate_contract(
    conn: duckdb.DuckDBPyConnection,
    contract: Contract,
) -> ContractResult:
    """Evaluate a single contract against the warehouse.

    Uses the same assertion evaluation logic as inline ``-- assert:`` comments.
    """
    from dp.engine.transform import SQLModel, _evaluate_assertion

    start = time.perf_counter()
    results: list[dict] = []
    all_passed = True

    # Check table exists
    parts = contract.model.split(".")
    if len(parts) != 2:
        return ContractResult(
            contract_name=contract.name,
            model=contract.model,
            passed=False,
            severity=contract.severity,
            results=[],
            error=f"Invalid model name: {contract.model}",
        )

    schema, name = parts
    exists = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        [schema, name],
    ).fetchone()[0] > 0

    if not exists:
        return ContractResult(
            contract_name=contract.name,
            model=contract.model,
            passed=False,
            severity=contract.severity,
            results=[],
            error=f"Table {contract.model} does not exist",
        )

    # Build a minimal SQLModel for assertion evaluation
    dummy_model = SQLModel(
        path=contract.path or Path("."),
        name=name,
        schema=schema,
        full_name=contract.model,
        sql="",
        query="",
        materialized="table",
    )

    for expr in contract.assertions:
        try:
            ar = _evaluate_assertion(conn, dummy_model, expr)
            results.append({
                "expression": ar.expression,
                "passed": ar.passed,
                "detail": ar.detail,
            })
            if not ar.passed:
                all_passed = False
        except Exception as e:
            results.append({
                "expression": expr,
                "passed": False,
                "detail": f"Error: {e}",
            })
            all_passed = False

    duration_ms = int((time.perf_counter() - start) * 1000)

    return ContractResult(
        contract_name=contract.name,
        model=contract.model,
        passed=all_passed,
        severity=contract.severity,
        results=results,
        duration_ms=duration_ms,
    )


def run_contracts(
    conn: duckdb.DuckDBPyConnection,
    contracts_dir: Path,
    targets: list[str] | None = None,
) -> list[ContractResult]:
    """Discover and run all contracts. Returns results for each contract.

    Args:
        conn: DuckDB connection.
        contracts_dir: Path to contracts/ directory.
        targets: Optional list of contract names or model names to filter.

    Returns:
        List of ContractResult for each evaluated contract.
    """
    ensure_meta_table(conn)
    _ensure_contracts_table(conn)

    contracts = discover_contracts(contracts_dir)
    if not contracts:
        return []

    # Filter if targets specified
    if targets:
        target_set = set(targets)
        contracts = [
            c for c in contracts
            if c.name in target_set or c.model in target_set
        ]

    results: list[ContractResult] = []
    for contract in contracts:
        cr = evaluate_contract(conn, contract)
        results.append(cr)

        # Save to metadata
        _save_contract_result(conn, cr)
        log_run(
            conn,
            "contract",
            f"{cr.contract_name}:{cr.model}",
            "success" if cr.passed else "failed",
            cr.duration_ms,
        )

    return results


def get_contract_history(
    conn: duckdb.DuckDBPyConnection,
    limit: int = 100,
) -> list[dict]:
    """Get recent contract evaluation history."""
    try:
        rows = conn.execute(
            """
            SELECT contract_name, model, passed, severity, detail, checked_at
            FROM _dp_internal.contract_results
            ORDER BY checked_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        return [
            {
                "contract_name": r[0],
                "model": r[1],
                "passed": r[2],
                "severity": r[3],
                "detail": r[4],
                "checked_at": str(r[5]),
            }
            for r in rows
        ]
    except Exception:
        return []


def _ensure_contracts_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the contract results metadata table."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _dp_internal.contract_results (
            id              VARCHAR DEFAULT gen_random_uuid()::VARCHAR,
            contract_name   VARCHAR NOT NULL,
            model           VARCHAR NOT NULL,
            passed          BOOLEAN NOT NULL,
            severity        VARCHAR NOT NULL DEFAULT 'error',
            detail          JSON,
            checked_at      TIMESTAMP DEFAULT current_timestamp
        )
    """)


def _save_contract_result(
    conn: duckdb.DuckDBPyConnection,
    cr: ContractResult,
) -> None:
    """Save a contract evaluation result to the metadata table."""
    import json
    conn.execute(
        """
        INSERT INTO _dp_internal.contract_results
            (contract_name, model, passed, severity, detail, checked_at)
        VALUES (?, ?, ?, ?, ?::JSON, current_timestamp)
        """,
        [cr.contract_name, cr.model, cr.passed, cr.severity, json.dumps(cr.results)],
    )
