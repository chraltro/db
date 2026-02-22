"""Tests for Data Contracts & Assertions Framework."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest


class TestContracts:
    """Test the contracts engine."""

    def _setup(self, tmp_path: Path):
        """Create a project with a warehouse and contracts."""
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        from dp.engine.database import ensure_meta_table
        ensure_meta_table(conn)

        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        conn.execute(
            "CREATE TABLE gold.orders AS "
            "SELECT 1 AS order_id, 'pending' AS status, 100.0 AS amount "
            "UNION ALL SELECT 2, 'shipped', 200.0 "
            "UNION ALL SELECT 3, 'delivered', 150.0"
        )

        contracts_dir = tmp_path / "contracts"
        contracts_dir.mkdir()
        (contracts_dir / "orders.yml").write_text(
            "contracts:\n"
            "  - name: orders_valid\n"
            "    model: gold.orders\n"
            "    description: Orders must be valid\n"
            "    assertions:\n"
            "      - row_count > 0\n"
            "      - unique(order_id)\n"
            "      - no_nulls(order_id)\n"
            "      - \"accepted_values(status, ['pending', 'shipped', 'delivered'])\"\n"
        )

        return conn, contracts_dir

    def test_discover_contracts(self, tmp_path: Path):
        from dp.engine.contracts import discover_contracts

        _, contracts_dir = self._setup(tmp_path)
        contracts = discover_contracts(contracts_dir)
        assert len(contracts) == 1
        assert contracts[0].name == "orders_valid"
        assert contracts[0].model == "gold.orders"
        assert len(contracts[0].assertions) == 4

    def test_run_contracts_all_pass(self, tmp_path: Path):
        from dp.engine.contracts import run_contracts

        conn, contracts_dir = self._setup(tmp_path)
        results = run_contracts(conn, contracts_dir)
        assert len(results) == 1
        assert results[0].passed is True
        assert all(r["passed"] for r in results[0].results)
        conn.close()

    def test_run_contracts_with_failure(self, tmp_path: Path):
        from dp.engine.contracts import run_contracts

        conn, contracts_dir = self._setup(tmp_path)

        # Add a contract that will fail
        (contracts_dir / "impossible.yml").write_text(
            "contracts:\n"
            "  - name: impossible\n"
            "    model: gold.orders\n"
            "    severity: warn\n"
            "    assertions:\n"
            "      - row_count > 1000\n"
        )

        results = run_contracts(conn, contracts_dir)
        assert len(results) == 2

        impossible = [r for r in results if r.contract_name == "impossible"][0]
        assert impossible.passed is False
        assert impossible.severity == "warn"
        conn.close()

    def test_contract_missing_table(self, tmp_path: Path):
        from dp.engine.contracts import evaluate_contract, Contract

        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        from dp.engine.database import ensure_meta_table
        ensure_meta_table(conn)

        contract = Contract(
            name="missing",
            model="gold.nonexistent",
            assertions=["row_count > 0"],
        )

        result = evaluate_contract(conn, contract)
        assert result.passed is False
        assert "does not exist" in result.error
        conn.close()

    def test_contract_history(self, tmp_path: Path):
        from dp.engine.contracts import get_contract_history, run_contracts

        conn, contracts_dir = self._setup(tmp_path)
        run_contracts(conn, contracts_dir)

        history = get_contract_history(conn, limit=10)
        assert len(history) == 1
        assert history[0]["contract_name"] == "orders_valid"
        assert history[0]["passed"] is True
        conn.close()


# ---------------------------------------------------------------------------
# API Integration Tests
# ---------------------------------------------------------------------------


@pytest.fixture
def project_with_contracts(tmp_path):
    """Create a test project with contracts and a warehouse."""
    (tmp_path / "project.yml").write_text("name: test\ndatabase:\n  path: warehouse.duckdb\nstreams:\n  test:\n    steps:\n      - transform: [all]\n")
    (tmp_path / "transform" / "bronze").mkdir(parents=True)
    (tmp_path / "transform" / "bronze" / "test.sql").write_text(
        "-- config: materialized=table, schema=bronze\nSELECT 1 AS id, 'Alice' AS name"
    )
    (tmp_path / "ingest").mkdir()
    (tmp_path / "export").mkdir()

    # Create contracts
    contracts_dir = tmp_path / "contracts"
    contracts_dir.mkdir()
    (contracts_dir / "test.yml").write_text(
        "contracts:\n"
        "  - name: test_contract\n"
        "    model: bronze.test\n"
        "    assertions:\n"
        "      - row_count > 0\n"
    )

    # Create warehouse
    conn = duckdb.connect(str(tmp_path / "warehouse.duckdb"))
    from dp.engine.database import ensure_meta_table
    ensure_meta_table(conn)
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    conn.execute("CREATE TABLE bronze.test AS SELECT 1 AS id, 'Alice' AS name")
    conn.close()

    return tmp_path


@pytest.fixture
def api_client(project_with_contracts):
    """Create a FastAPI TestClient."""
    from starlette.testclient import TestClient
    import dp.server.app as server_app
    server_app.PROJECT_DIR = project_with_contracts
    server_app.AUTH_ENABLED = False
    return TestClient(server_app.app)


class TestContractsAPI:
    def test_list_contracts(self, api_client):
        resp = api_client.get("/api/contracts")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["name"] == "test_contract"

    def test_run_contracts(self, api_client):
        resp = api_client.post("/api/contracts/run")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["passed"] == 1
        assert data["failed"] == 0

    def test_contracts_history(self, api_client):
        # Run first to have history
        api_client.post("/api/contracts/run")
        resp = api_client.get("/api/contracts/history")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1


# ---------------------------------------------------------------------------
# Security Hardening Tests
# ---------------------------------------------------------------------------


class TestContractsSecurity:
    """Test identifier injection prevention in contracts."""

    def test_contract_rejects_invalid_model_name(self, tmp_path: Path):
        from dp.engine.contracts import Contract, evaluate_contract

        conn = duckdb.connect(str(tmp_path / "test.duckdb"))
        from dp.engine.database import ensure_meta_table
        ensure_meta_table(conn)

        contract = Contract(
            name="bad",
            model="gold.users; DROP TABLE--",
            assertions=["row_count > 0"],
        )
        result = evaluate_contract(conn, contract)
        assert result.passed is False
        assert "Invalid" in result.error
        conn.close()
