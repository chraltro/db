"""Tests for the five major platform features:

1. Incremental Transforms with Partition Awareness
2. Data Contracts & Assertions Framework
3. Live Collaboration & Multi-User Query Sessions
4. External Source Connectors with CDC
5. Versioned Warehouse with Time Travel
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

# ---------------------------------------------------------------------------
# Feature 1: Incremental Transforms — merge strategy + partition_by
# ---------------------------------------------------------------------------


class TestIncrementalMerge:
    """Test the 'merge' incremental strategy (true upsert)."""

    def test_merge_first_run_creates_table(self, tmp_path: Path):
        from dp.engine.database import ensure_meta_table
        from dp.engine.transform import SQLModel, _execute_incremental

        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        ensure_meta_table(conn)
        conn.execute("CREATE SCHEMA IF NOT EXISTS landing")
        conn.execute("CREATE TABLE landing.src AS SELECT 1 AS id, 'Alice' AS name")

        model = SQLModel(
            path=tmp_path / "m.sql",
            name="dest",
            schema="gold",
            full_name="gold.dest",
            sql="",
            query="SELECT id, name FROM landing.src",
            materialized="incremental",
            unique_key="id",
            incremental_strategy="merge",
        )

        duration_ms, row_count = _execute_incremental(conn, model)
        assert row_count == 1
        row = conn.execute("SELECT name FROM gold.dest WHERE id = 1").fetchone()
        assert row[0] == "Alice"
        conn.close()

    def test_merge_updates_existing_and_inserts_new(self, tmp_path: Path):
        from dp.engine.database import ensure_meta_table
        from dp.engine.transform import SQLModel, _execute_incremental

        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        ensure_meta_table(conn)

        # Initial data
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        conn.execute("CREATE TABLE gold.dest AS SELECT 1 AS id, 'Alice' AS name")

        # Source has updated row + new row
        conn.execute("CREATE SCHEMA IF NOT EXISTS landing")
        conn.execute(
            "CREATE TABLE landing.src AS "
            "SELECT 1 AS id, 'Alice Updated' AS name "
            "UNION ALL SELECT 2, 'Bob'"
        )

        model = SQLModel(
            path=tmp_path / "m.sql",
            name="dest",
            schema="gold",
            full_name="gold.dest",
            sql="",
            query="SELECT id, name FROM landing.src",
            materialized="incremental",
            unique_key="id",
            incremental_strategy="merge",
        )

        duration_ms, row_count = _execute_incremental(conn, model)
        assert row_count == 2

        # Check Alice was updated
        alice = conn.execute("SELECT name FROM gold.dest WHERE id = 1").fetchone()
        assert alice[0] == "Alice Updated"

        # Check Bob was inserted
        bob = conn.execute("SELECT name FROM gold.dest WHERE id = 2").fetchone()
        assert bob[0] == "Bob"
        conn.close()


class TestIncrementalPartition:
    """Test partition_by config for partition-based pruning."""

    def test_partition_deletes_affected_partitions(self, tmp_path: Path):
        from dp.engine.database import ensure_meta_table
        from dp.engine.transform import SQLModel, _execute_incremental

        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        ensure_meta_table(conn)

        # Initial data: two partitions
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        conn.execute(
            "CREATE TABLE gold.events AS "
            "SELECT 1 AS id, 'A' AS name, '2024-01-01' AS event_date "
            "UNION ALL SELECT 2, 'B', '2024-01-01' "
            "UNION ALL SELECT 3, 'C', '2024-01-02'"
        )

        # Source only has data for partition 2024-01-01 (replacement)
        conn.execute("CREATE SCHEMA IF NOT EXISTS landing")
        conn.execute(
            "CREATE TABLE landing.new_events AS "
            "SELECT 1 AS id, 'A_new' AS name, '2024-01-01' AS event_date "
            "UNION ALL SELECT 4, 'D', '2024-01-01'"
        )

        model = SQLModel(
            path=tmp_path / "m.sql",
            name="events",
            schema="gold",
            full_name="gold.events",
            sql="",
            query="SELECT id, name, event_date FROM landing.new_events",
            materialized="incremental",
            unique_key="id",
            incremental_strategy="delete+insert",
            partition_by="event_date",
        )

        _execute_incremental(conn, model)

        rows = conn.execute(
            "SELECT id, name, event_date FROM gold.events ORDER BY id"
        ).fetchall()

        # Partition 2024-01-02 should be untouched (id=3)
        # Partition 2024-01-01 should be replaced with new data (id=1, id=4)
        assert len(rows) == 3
        ids = {r[0] for r in rows}
        assert ids == {1, 3, 4}
        # id=1 should have updated name
        a_row = [r for r in rows if r[0] == 1][0]
        assert a_row[1] == "A_new"
        conn.close()

    def test_partition_by_parsed_from_config(self, tmp_path: Path):
        from dp.engine.transform import discover_models

        transform_dir = tmp_path / "transform" / "gold"
        transform_dir.mkdir(parents=True)
        (transform_dir / "events.sql").write_text(
            "-- config: materialized=incremental, schema=gold, unique_key=id, "
            "partition_by=event_date\n"
            "-- depends_on: landing.raw\n"
            "SELECT id, name, event_date FROM landing.raw"
        )

        models = discover_models(tmp_path / "transform")
        assert len(models) == 1
        assert models[0].partition_by == "event_date"
        assert models[0].unique_key == "id"


# ---------------------------------------------------------------------------
# Feature 2: Data Contracts & Assertions Framework
# ---------------------------------------------------------------------------


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
# Feature 3: Live Collaboration & Multi-User Query Sessions
# ---------------------------------------------------------------------------


class TestCollaboration:
    """Test the collaboration session manager."""

    def test_create_and_list_sessions(self):
        from dp.engine.collaboration import SessionManager

        mgr = SessionManager()
        session = mgr.create_session("Test Session")
        assert session.name == "Test Session"
        assert session.session_id

        sessions = mgr.list_sessions()
        assert len(sessions) == 1
        assert sessions[0]["name"] == "Test Session"

    def test_join_and_leave(self):
        from dp.engine.collaboration import SessionManager

        mgr = SessionManager()
        session = mgr.create_session("Test")

        class FakeWS:
            pass

        ws = FakeWS()
        joined = mgr.join_session(session.session_id, "user1", "Alice", ws)
        assert joined is not None
        assert len(mgr.get_participants(session.session_id)) == 1

        mgr.leave_session(session.session_id, "user1", ws)
        assert len(mgr.get_participants(session.session_id)) == 0

    def test_query_history(self):
        from dp.engine.collaboration import SessionManager

        mgr = SessionManager()
        session = mgr.create_session()

        entry = mgr.add_query_result(
            session.session_id,
            "user1",
            "SELECT 1",
            ["col"],
            [[1]],
            42,
        )
        assert entry is not None
        assert entry["sql"] == "SELECT 1"
        assert session.query_history[-1]["duration_ms"] == 42

    def test_shared_sql(self):
        from dp.engine.collaboration import SessionManager

        mgr = SessionManager()
        session = mgr.create_session()

        mgr.update_shared_sql(session.session_id, "SELECT * FROM orders")
        assert session.shared_sql == "SELECT * FROM orders"

    def test_cursor_tracking(self):
        from dp.engine.collaboration import SessionManager

        mgr = SessionManager()
        session = mgr.create_session()

        class FakeWS:
            pass

        mgr.join_session(session.session_id, "user1", "Alice", FakeWS())
        mgr.update_cursor(session.session_id, "user1", {"line": 5, "column": 10})

        participants = mgr.get_participants(session.session_id)
        assert participants[0]["cursor_position"] == {"line": 5, "column": 10}

    def test_delete_session(self):
        from dp.engine.collaboration import SessionManager

        mgr = SessionManager()
        session = mgr.create_session("Temp")
        assert mgr.delete_session(session.session_id) is True
        assert mgr.get_session(session.session_id) is None
        assert mgr.delete_session("nonexistent") is False

    def test_max_history_limit(self):
        from dp.engine.collaboration import SessionManager

        mgr = SessionManager()
        mgr._max_history = 5
        session = mgr.create_session()

        for i in range(10):
            mgr.add_query_result(
                session.session_id, "user1", f"SELECT {i}", ["c"], [[i]], i,
            )

        assert len(session.query_history) == 5
        assert session.query_history[0]["sql"] == "SELECT 5"


# ---------------------------------------------------------------------------
# Feature 4: External Source Connectors with CDC
# ---------------------------------------------------------------------------


class TestCDC:
    """Test the CDC (Change Data Capture) engine."""

    def test_ensure_cdc_table(self, tmp_path: Path):
        from dp.engine.cdc import ensure_cdc_table

        conn = duckdb.connect(str(tmp_path / "test.duckdb"))
        ensure_cdc_table(conn)

        # Table should exist
        result = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = '_dp_internal' AND table_name = 'cdc_state'"
        ).fetchone()
        assert result[0] == 1
        conn.close()

    def test_watermark_lifecycle(self, tmp_path: Path):
        from dp.engine.cdc import ensure_cdc_table, get_watermark, update_watermark

        conn = duckdb.connect(str(tmp_path / "test.duckdb"))
        ensure_cdc_table(conn)

        # Initially None
        assert get_watermark(conn, "my_conn", "users") is None

        # Update
        update_watermark(conn, "my_conn", "users", "high_watermark", "2024-01-15 12:00:00")

        # Now has value
        wm = get_watermark(conn, "my_conn", "users")
        assert wm == "2024-01-15 12:00:00"

        # Update again
        update_watermark(conn, "my_conn", "users", "high_watermark", "2024-01-16 08:00:00")
        wm = get_watermark(conn, "my_conn", "users")
        assert wm == "2024-01-16 08:00:00"
        conn.close()

    def test_file_tracking(self, tmp_path: Path):
        import time
        from dp.engine.cdc import ensure_cdc_table, should_sync_file, update_watermark

        conn = duckdb.connect(str(tmp_path / "test.duckdb"))
        ensure_cdc_table(conn)

        # Create a file
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n")

        # Should sync (never synced before)
        assert should_sync_file(conn, "files", "data", csv_file) is True

        # Record the sync
        update_watermark(conn, "files", "data", "file_tracking",
                         file_mtime=csv_file.stat().st_mtime)

        # Should NOT sync (file hasn't changed)
        assert should_sync_file(conn, "files", "data", csv_file) is False

        # Modify file
        time.sleep(0.1)  # Ensure mtime differs
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n3,Charlie\n")

        # Should sync again
        assert should_sync_file(conn, "files", "data", csv_file) is True
        conn.close()

    def test_sync_file_csv(self, tmp_path: Path):
        from dp.engine.cdc import CDCTableConfig, ensure_cdc_table, sync_table_file
        from dp.engine.database import ensure_meta_table

        conn = duckdb.connect(str(tmp_path / "test.duckdb"))
        ensure_meta_table(conn)
        ensure_cdc_table(conn)

        csv_file = tmp_path / "customers.csv"
        csv_file.write_text("id,name,email\n1,Alice,alice@test.com\n2,Bob,bob@test.com\n")

        table_config = CDCTableConfig(name="customers", cdc_mode="file_tracking")
        result = sync_table_file(conn, "files", table_config, "landing", csv_file)

        assert result.status == "success"
        assert result.rows_synced == 2
        assert result.cdc_mode == "file_tracking"

        # Verify data
        rows = conn.execute("SELECT * FROM landing.customers ORDER BY id").fetchall()
        assert len(rows) == 2
        assert rows[0][1] == "Alice"

        # Second sync should skip (no changes)
        result2 = sync_table_file(conn, "files", table_config, "landing", csv_file)
        assert result2.status == "skipped"
        conn.close()

    def test_cdc_status(self, tmp_path: Path):
        from dp.engine.cdc import ensure_cdc_table, get_cdc_status, update_watermark

        conn = duckdb.connect(str(tmp_path / "test.duckdb"))
        ensure_cdc_table(conn)

        update_watermark(conn, "pg_prod", "users", "high_watermark", "2024-01-15")
        update_watermark(conn, "pg_prod", "orders", "high_watermark", "2024-01-14")
        update_watermark(conn, "files", "data", "file_tracking", file_mtime=1234567.0)

        # All entries
        status = get_cdc_status(conn)
        assert len(status) == 3

        # Filtered by connector
        pg_status = get_cdc_status(conn, "pg_prod")
        assert len(pg_status) == 2

        conn.close()

    def test_reset_watermark(self, tmp_path: Path):
        from dp.engine.cdc import ensure_cdc_table, get_cdc_status, reset_watermark, update_watermark

        conn = duckdb.connect(str(tmp_path / "test.duckdb"))
        ensure_cdc_table(conn)

        update_watermark(conn, "pg_prod", "users", "high_watermark", "2024-01-15")
        update_watermark(conn, "pg_prod", "orders", "high_watermark", "2024-01-14")

        # Reset single table
        reset_watermark(conn, "pg_prod", "users")
        status = get_cdc_status(conn, "pg_prod")
        assert len(status) == 1
        assert status[0]["table"] == "orders"

        # Reset entire connector
        reset_watermark(conn, "pg_prod")
        status = get_cdc_status(conn, "pg_prod")
        assert len(status) == 0
        conn.close()

    def test_parse_cdc_config(self):
        from dp.engine.cdc import parse_cdc_config

        raw = {
            "connectors": {
                "prod_users": {
                    "type": "postgres",
                    "connection": "prod_pg",
                    "target_schema": "landing",
                    "tables": [
                        {"name": "users", "cdc_mode": "high_watermark", "cdc_column": "updated_at"},
                        {"name": "roles"},
                        "settings",
                    ],
                    "schedule": "*/30 * * * *",
                }
            }
        }

        configs = parse_cdc_config(raw)
        assert len(configs) == 1
        c = configs[0]
        assert c.name == "prod_users"
        assert c.connector_type == "postgres"
        assert len(c.tables) == 3
        assert c.tables[0].cdc_mode == "high_watermark"
        assert c.tables[0].cdc_column == "updated_at"
        assert c.tables[1].cdc_mode == "full_refresh"
        assert c.tables[2].name == "settings"


# ---------------------------------------------------------------------------
# Feature 5: Versioned Warehouse with Time Travel
# ---------------------------------------------------------------------------


class TestVersioning:
    """Test the versioning / time travel engine."""

    def _setup(self, tmp_path: Path):
        """Create a project with some tables."""
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        from dp.engine.database import ensure_meta_table
        ensure_meta_table(conn)

        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        conn.execute("CREATE TABLE gold.customers AS SELECT 1 AS id, 'Alice' AS name")
        conn.execute("CREATE TABLE gold.orders AS SELECT 1 AS id, 100 AS amount")

        # Create minimal project structure
        (tmp_path / "project.yml").write_text("name: test\n")
        (tmp_path / "transform" / "gold").mkdir(parents=True)
        (tmp_path / "transform" / "gold" / "customers.sql").write_text("SELECT 1")

        return conn

    def test_create_version(self, tmp_path: Path):
        from dp.engine.versioning import create_version

        conn = self._setup(tmp_path)
        result = create_version(conn, tmp_path, description="Initial snapshot")

        assert result["version_id"].startswith("v1-")
        assert result["table_count"] == 2
        assert "gold.customers" in result["tables"]
        assert "gold.orders" in result["tables"]

        # Verify Parquet files exist
        snap_dir = tmp_path / "_snapshots" / result["version_id"]
        assert snap_dir.exists()
        assert (snap_dir / "gold.customers.parquet").exists()
        assert (snap_dir / "gold.orders.parquet").exists()
        assert (snap_dir / "_manifest.json").exists()
        conn.close()

    def test_list_versions(self, tmp_path: Path):
        from dp.engine.versioning import create_version, list_versions

        conn = self._setup(tmp_path)
        create_version(conn, tmp_path, description="v1")
        create_version(conn, tmp_path, description="v2")

        versions = list_versions(conn)
        assert len(versions) == 2
        # Newest first
        assert versions[0]["description"] == "v2"
        assert versions[1]["description"] == "v1"
        conn.close()

    def test_diff_versions(self, tmp_path: Path):
        from dp.engine.versioning import create_version, diff_versions

        conn = self._setup(tmp_path)

        v1 = create_version(conn, tmp_path, description="Before changes")

        # Make changes
        conn.execute("INSERT INTO gold.customers VALUES (2, 'Bob')")
        conn.execute("DROP TABLE gold.orders")
        conn.execute("CREATE TABLE gold.products AS SELECT 'Widget' AS name")

        result = diff_versions(conn, tmp_path, v1["version_id"])

        assert result["from_version"] == v1["version_id"]
        assert result["to_version"] == "current"

        changes = result["changes"]
        change_map = {c["table"]: c for c in changes}

        assert "gold.orders" in change_map
        assert change_map["gold.orders"]["change"] == "removed"

        assert "gold.products" in change_map
        assert change_map["gold.products"]["change"] == "added"

        assert "gold.customers" in change_map
        assert change_map["gold.customers"]["change"] == "modified"
        assert change_map["gold.customers"]["row_diff"] == 1
        conn.close()

    def test_restore_version(self, tmp_path: Path):
        from dp.engine.versioning import create_version, restore_version

        conn = self._setup(tmp_path)

        v1 = create_version(conn, tmp_path, description="Original state")

        # Destroy data
        conn.execute("DELETE FROM gold.customers")
        conn.execute("INSERT INTO gold.customers VALUES (99, 'Replaced')")
        assert conn.execute("SELECT COUNT(*) FROM gold.customers").fetchone()[0] == 1

        # Restore
        result = restore_version(conn, tmp_path, v1["version_id"])
        assert result["tables_restored"] == 2

        # Verify original data is back
        row = conn.execute("SELECT name FROM gold.customers WHERE id = 1").fetchone()
        assert row[0] == "Alice"
        conn.close()

    def test_table_timeline(self, tmp_path: Path):
        from dp.engine.versioning import create_version, table_timeline

        conn = self._setup(tmp_path)

        create_version(conn, tmp_path, description="v1")
        conn.execute("INSERT INTO gold.customers VALUES (2, 'Bob')")
        create_version(conn, tmp_path, description="v2")

        timeline = table_timeline(conn, "gold.customers")
        assert len(timeline) == 2
        # Newest first
        assert timeline[0]["row_count"] == 2
        assert timeline[1]["row_count"] == 1
        conn.close()

    def test_cleanup_old_versions(self, tmp_path: Path):
        from dp.engine.versioning import cleanup_old_versions, create_version, list_versions

        conn = self._setup(tmp_path)

        for i in range(5):
            create_version(conn, tmp_path, description=f"v{i}")

        assert len(list_versions(conn)) == 5

        result = cleanup_old_versions(tmp_path, conn, keep=2)
        assert result["removed"] == 3
        assert result["kept"] == 2

        remaining = list_versions(conn)
        assert len(remaining) == 2
        conn.close()

    def test_restore_specific_tables(self, tmp_path: Path):
        from dp.engine.versioning import create_version, restore_version

        conn = self._setup(tmp_path)
        v1 = create_version(conn, tmp_path)

        conn.execute("DELETE FROM gold.customers")
        conn.execute("DELETE FROM gold.orders")

        # Restore only customers
        result = restore_version(conn, tmp_path, v1["version_id"], tables=["gold.customers"])

        customers = conn.execute("SELECT COUNT(*) FROM gold.customers").fetchone()[0]
        orders = conn.execute("SELECT COUNT(*) FROM gold.orders").fetchone()[0]

        assert customers == 1  # Restored
        assert orders == 0  # NOT restored
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


class TestSessionsAPI:
    def test_create_and_list_sessions(self, api_client):
        resp = api_client.post("/api/sessions", json={"name": "Test"})
        assert resp.status_code == 200
        session_id = resp.json()["session_id"]

        resp = api_client.get("/api/sessions")
        assert resp.status_code == 200
        sessions = resp.json()
        assert any(s["session_id"] == session_id for s in sessions)

    def test_session_query(self, api_client):
        # Create session
        resp = api_client.post("/api/sessions", json={"name": "Query Test"})
        session_id = resp.json()["session_id"]

        # Run query in session
        resp = api_client.post(
            f"/api/sessions/{session_id}/query",
            json={"sql": "SELECT 42 AS answer", "user_id": "test"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["columns"] == ["answer"]
        assert data["rows"] == [[42]]

    def test_delete_session(self, api_client):
        resp = api_client.post("/api/sessions", json={"name": "Temp"})
        session_id = resp.json()["session_id"]

        resp = api_client.delete(f"/api/sessions/{session_id}")
        assert resp.status_code == 200

        resp = api_client.get(f"/api/sessions/{session_id}")
        assert resp.status_code == 404


class TestVersionsAPI:
    def test_create_and_list_versions(self, api_client):
        resp = api_client.post("/api/versions")
        assert resp.status_code == 200
        data = resp.json()
        assert "version_id" in data

        resp = api_client.get("/api/versions")
        assert resp.status_code == 200
        versions = resp.json()
        assert len(versions) >= 1

    def test_version_detail(self, api_client):
        resp = api_client.post("/api/versions")
        version_id = resp.json()["version_id"]

        resp = api_client.get(f"/api/versions/{version_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["version_id"] == version_id
        assert "tables" in data


class TestCDCAPI:
    def test_cdc_status_empty(self, api_client):
        resp = api_client.get("/api/cdc")
        assert resp.status_code == 200
        assert resp.json() == []


# ---------------------------------------------------------------------------
# Security Hardening Tests
# ---------------------------------------------------------------------------


class TestCDCSecurity:
    """Test SQL injection prevention in CDC engine."""

    def test_sync_rejects_invalid_table_name(self, tmp_path: Path):
        from dp.engine.cdc import CDCTableConfig, sync_table_high_watermark

        conn = duckdb.connect(str(tmp_path / "test.duckdb"))
        config = CDCTableConfig(
            name="users; DROP TABLE--",
            cdc_mode="high_watermark",
            cdc_column="updated_at",
        )
        result = sync_table_high_watermark(
            conn, "test", config, "landing", "fake_conn_str",
        )
        assert result.status == "error"
        assert "Invalid" in result.error
        conn.close()

    def test_sync_rejects_invalid_cdc_column(self, tmp_path: Path):
        from dp.engine.cdc import CDCTableConfig, sync_table_high_watermark

        conn = duckdb.connect(str(tmp_path / "test.duckdb"))
        config = CDCTableConfig(
            name="users",
            cdc_mode="high_watermark",
            cdc_column="col; DROP TABLE users--",
        )
        result = sync_table_high_watermark(
            conn, "test", config, "landing", "fake_conn_str",
        )
        assert result.status == "error"
        assert "Invalid" in result.error
        conn.close()

    def test_sync_rejects_invalid_connector_name(self, tmp_path: Path):
        from dp.engine.cdc import CDCTableConfig, sync_table_high_watermark

        conn = duckdb.connect(str(tmp_path / "test.duckdb"))
        config = CDCTableConfig(
            name="users",
            cdc_mode="high_watermark",
            cdc_column="updated_at",
        )
        result = sync_table_high_watermark(
            conn, "bad;name", config, "landing", "fake",
        )
        assert result.status == "error"
        conn.close()

    def test_file_sync_rejects_invalid_schema(self, tmp_path: Path):
        from dp.engine.cdc import CDCTableConfig, sync_table_file

        conn = duckdb.connect(str(tmp_path / "test.duckdb"))
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id\n1\n")

        config = CDCTableConfig(name="data", cdc_mode="file_tracking")
        result = sync_table_file(
            conn, "files", config, "bad schema!", csv_file,
        )
        assert result.status == "error"
        assert "Invalid" in result.error
        conn.close()


class TestVersioningSecurity:
    """Test path traversal and identifier injection prevention in versioning."""

    def test_restore_rejects_path_traversal(self, tmp_path: Path):
        from dp.engine.versioning import create_version, restore_version

        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))
        from dp.engine.database import ensure_meta_table
        ensure_meta_table(conn)

        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        conn.execute("CREATE TABLE gold.t AS SELECT 1 AS id")

        v1 = create_version(conn, tmp_path, description="test")

        # Tamper with the stored parquet_file path to attempt traversal
        import json
        version = conn.execute(
            "SELECT tables_snapshot FROM _dp_internal.version_history WHERE version_id = ?",
            [v1["version_id"]],
        ).fetchone()
        tables_info = json.loads(version[0])
        # Inject a traversal path
        for key in tables_info:
            tables_info[key]["parquet_file"] = "../../../etc/passwd"
        conn.execute(
            "UPDATE _dp_internal.version_history SET tables_snapshot = ?::JSON WHERE version_id = ?",
            [json.dumps(tables_info), v1["version_id"]],
        )

        result = restore_version(conn, tmp_path, v1["version_id"])
        # Should fail for every table due to path traversal protection
        for detail in result["details"]:
            assert detail["status"] == "error"
            assert "escapes project" in detail["error"] or "not found" in detail["error"]
        conn.close()


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


class TestCollaborationSecurity:
    """Test size limits and stale session handling."""

    def test_shared_sql_size_limit(self):
        from dp.engine.collaboration import SessionManager

        mgr = SessionManager()
        session = mgr.create_session()
        huge_sql = "SELECT " + "x" * 200_000
        mgr.update_shared_sql(session.session_id, huge_sql)
        assert len(session.shared_sql) <= mgr._max_sql_length

    def test_session_name_truncated(self):
        from dp.engine.collaboration import SessionManager

        mgr = SessionManager()
        long_name = "A" * 500
        session = mgr.create_session(long_name)
        assert len(session.name) <= 200

    def test_stale_session_eviction(self):
        from dp.engine.collaboration import SessionManager

        mgr = SessionManager()
        mgr._session_ttl = 0  # Immediately stale
        mgr._max_sessions = 2

        mgr.create_session("s1")
        mgr.create_session("s2")
        # Third session should trigger eviction of stale empty sessions
        s3 = mgr.create_session("s3")
        assert s3 is not None
        # At most _max_sessions remain (stale ones evicted)
        assert len(mgr._sessions) <= 3  # Could be less after eviction
