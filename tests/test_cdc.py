"""Tests for External Source Connectors with CDC."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest


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
