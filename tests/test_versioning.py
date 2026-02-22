"""Tests for Versioned Warehouse with Time Travel."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest


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


# ---------------------------------------------------------------------------
# Security Hardening Tests
# ---------------------------------------------------------------------------


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
