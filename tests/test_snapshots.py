"""Tests for Pipeline Rewind snapshot engine."""

import textwrap
from pathlib import Path

import duckdb
import pytest

from havn.engine.snapshots import (
    RewindConfig,
    SnapshotInfo,
    _compute_checksum,
    _compute_schema_hash,
    _ensure_meta_db,
    capture_snapshot,
    finish_run,
    get_all_snapshots,
    get_downstream_models,
    get_runs,
    get_snapshot_sample,
    get_snapshots_for_run,
    restore_snapshot,
    restore_with_cascade,
    run_gc,
    start_run,
)


@pytest.fixture
def project(tmp_path):
    """Create a minimal project directory with a warehouse DB."""
    # Create project.yml
    (tmp_path / "project.yml").write_text("name: test\ndatabase:\n  path: warehouse.duckdb\n")

    # Create warehouse with test data
    db_path = tmp_path / "warehouse.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    conn.execute("CREATE TABLE bronze.customers AS SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob'")
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
    conn.execute("CREATE TABLE silver.orders AS SELECT 1 AS order_id, 1 AS customer_id, 100 AS amount")
    conn.close()

    return tmp_path


@pytest.fixture
def warehouse_conn(project):
    """Get a DuckDB connection to the test warehouse."""
    db_path = project / "warehouse.duckdb"
    conn = duckdb.connect(str(db_path))
    yield conn
    conn.close()


class TestRunManagement:
    def test_start_and_finish_run(self, project):
        run_id = start_run(project, trigger="manual")
        assert run_id
        assert len(run_id) == 36  # UUID format

        finish_run(project, run_id, "success", ["bronze.customers"])

        runs = get_runs(project)
        assert len(runs) == 1
        assert runs[0].run_id == run_id
        assert runs[0].status == "success"
        assert runs[0].trigger == "manual"
        assert runs[0].models_run == ["bronze.customers"]
        assert runs[0].finished_at is not None

    def test_multiple_runs(self, project):
        r1 = start_run(project, trigger="manual")
        finish_run(project, r1, "success", ["a"])

        r2 = start_run(project, trigger="scheduled")
        finish_run(project, r2, "failed", ["b"])

        runs = get_runs(project)
        assert len(runs) == 2
        # Most recent first
        assert runs[0].run_id == r2
        assert runs[1].run_id == r1


class TestSnapshotCapture:
    def test_capture_creates_parquet(self, project, warehouse_conn):
        run_id = start_run(project)
        result = capture_snapshot(project, warehouse_conn, "bronze.customers", run_id, 2)
        assert result is True

        # Check parquet file exists
        snap_dir = project / ".dp" / "snapshots" / "bronze" / "customers"
        parquet_files = list(snap_dir.glob("*.parquet"))
        assert len(parquet_files) == 1
        assert run_id in parquet_files[0].name

    def test_capture_stores_metadata(self, project, warehouse_conn):
        run_id = start_run(project)
        capture_snapshot(project, warehouse_conn, "bronze.customers", run_id, 2)

        snapshots = get_snapshots_for_run(project, run_id)
        assert len(snapshots) == 1
        s = snapshots[0]
        assert s.model_name == "bronze.customers"
        assert s.row_count == 2
        assert s.col_count == 2
        assert s.schema_hash != ""
        assert s.checksum != ""
        assert s.file_path is not None
        assert s.size_bytes > 0

    def test_capture_dedup(self, project, warehouse_conn):
        """Identical snapshots should reuse the same file."""
        r1 = start_run(project)
        capture_snapshot(project, warehouse_conn, "bronze.customers", r1, 2)

        r2 = start_run(project)
        result = capture_snapshot(project, warehouse_conn, "bronze.customers", r2, 2)
        assert result is False  # Deduped

        s1 = get_snapshots_for_run(project, r1)
        s2 = get_snapshots_for_run(project, r2)
        assert s1[0].file_path == s2[0].file_path
        assert s1[0].checksum == s2[0].checksum

    def test_capture_no_dedup(self, project, warehouse_conn):
        """Changed data should create a new file."""
        r1 = start_run(project)
        capture_snapshot(project, warehouse_conn, "bronze.customers", r1, 2)

        # Modify data
        warehouse_conn.execute("INSERT INTO bronze.customers VALUES (3, 'Charlie')")

        r2 = start_run(project)
        result = capture_snapshot(project, warehouse_conn, "bronze.customers", r2, 3)
        assert result is True  # New file

        s1 = get_snapshots_for_run(project, r1)
        s2 = get_snapshots_for_run(project, r2)
        assert s1[0].file_path != s2[0].file_path

    def test_capture_disabled(self, project, warehouse_conn):
        run_id = start_run(project)
        config = RewindConfig(enabled=False)
        result = capture_snapshot(project, warehouse_conn, "bronze.customers", run_id, 2, config)
        assert result is False

    def test_capture_excluded_model(self, project, warehouse_conn):
        run_id = start_run(project)
        config = RewindConfig(exclude=["bronze.customers"])
        result = capture_snapshot(project, warehouse_conn, "bronze.customers", run_id, 2, config)
        assert result is False


class TestSnapshotSample:
    def test_get_sample(self, project, warehouse_conn):
        run_id = start_run(project)
        capture_snapshot(project, warehouse_conn, "bronze.customers", run_id, 2)

        result = get_snapshot_sample(project, run_id, "bronze.customers")
        assert "columns" in result
        assert "rows" in result
        assert len(result["columns"]) == 2
        assert len(result["rows"]) == 2
        assert "id" in result["columns"]
        assert "name" in result["columns"]

    def test_sample_missing_snapshot(self, project):
        result = get_snapshot_sample(project, "nonexistent", "bronze.customers")
        assert "error" in result


class TestRestore:
    def test_restore_snapshot(self, project, warehouse_conn):
        run_id = start_run(project)
        capture_snapshot(project, warehouse_conn, "bronze.customers", run_id, 2)

        # Modify the live table
        warehouse_conn.execute("DELETE FROM bronze.customers WHERE id = 1")
        count = warehouse_conn.execute("SELECT COUNT(*) FROM bronze.customers").fetchone()[0]
        assert count == 1

        # Restore
        result = restore_snapshot(project, warehouse_conn, run_id, "bronze.customers")
        assert result["status"] == "success"

        # Verify restored
        count = warehouse_conn.execute("SELECT COUNT(*) FROM bronze.customers").fetchone()[0]
        assert count == 2

    def test_restore_expired_snapshot(self, project, warehouse_conn):
        run_id = start_run(project)
        capture_snapshot(project, warehouse_conn, "bronze.customers", run_id, 2)

        # Null out the file_path to simulate expiry
        meta_conn = _ensure_meta_db(project)
        meta_conn.execute(
            "UPDATE snapshots SET file_path = NULL WHERE run_id = ?", [run_id]
        )
        meta_conn.close()

        result = restore_snapshot(project, warehouse_conn, run_id, "bronze.customers")
        assert result["status"] == "error"
        assert "expired" in result["message"].lower() or "Expired" in result["message"]

    def test_restore_nonexistent(self, project, warehouse_conn):
        result = restore_snapshot(project, warehouse_conn, "fake-id", "bronze.customers")
        assert result["status"] == "error"


class TestGarbageCollection:
    def test_gc_no_expired(self, project, warehouse_conn):
        run_id = start_run(project)
        capture_snapshot(project, warehouse_conn, "bronze.customers", run_id, 2)

        config = RewindConfig(retention="7d")
        deleted = run_gc(project, config)
        assert deleted == 0

    def test_gc_with_expired(self, project, warehouse_conn):
        run_id = start_run(project)
        capture_snapshot(project, warehouse_conn, "bronze.customers", run_id, 2)

        # Manually set created_at to the past
        meta_conn = _ensure_meta_db(project)
        meta_conn.execute(
            "UPDATE snapshots SET created_at = current_timestamp - INTERVAL '30 days'"
        )
        meta_conn.execute(
            "UPDATE runs SET started_at = current_timestamp - INTERVAL '30 days'"
        )
        meta_conn.close()

        config = RewindConfig(retention="7d")
        deleted = run_gc(project, config)
        assert deleted >= 1

        # Metadata should still exist but file_path nulled
        snapshots = get_snapshots_for_run(project, run_id)
        assert len(snapshots) == 1
        assert snapshots[0].file_path is None

    def test_gc_storage_cap(self, project, warehouse_conn):
        # Create multiple snapshots
        for i in range(3):
            warehouse_conn.execute(f"INSERT INTO bronze.customers VALUES ({10 + i}, 'User{i}')")
            run_id = start_run(project)
            capture_snapshot(
                project, warehouse_conn, "bronze.customers", run_id,
                warehouse_conn.execute("SELECT COUNT(*) FROM bronze.customers").fetchone()[0],
                RewindConfig(dedup=False),  # Force unique files
            )

        # Set a tiny storage cap (1 byte = delete everything)
        config = RewindConfig(max_storage=0.000000001)
        deleted = run_gc(project, config)
        assert deleted >= 1


class TestChecksums:
    def test_compute_checksum(self, project, warehouse_conn):
        cs = _compute_checksum(warehouse_conn, "bronze.customers")
        assert cs != ""
        assert len(cs) == 16

    def test_checksum_changes_with_data(self, project, warehouse_conn):
        cs1 = _compute_checksum(warehouse_conn, "bronze.customers")
        warehouse_conn.execute("INSERT INTO bronze.customers VALUES (3, 'Charlie')")
        cs2 = _compute_checksum(warehouse_conn, "bronze.customers")
        assert cs1 != cs2

    def test_schema_hash(self, project, warehouse_conn):
        hash_val, col_count = _compute_schema_hash(warehouse_conn, "bronze.customers")
        assert hash_val != ""
        assert col_count == 2


class TestGetAllSnapshots:
    def test_all_snapshots(self, project, warehouse_conn):
        r1 = start_run(project)
        capture_snapshot(project, warehouse_conn, "bronze.customers", r1, 2)
        finish_run(project, r1, "success", ["bronze.customers"])

        r2 = start_run(project)
        capture_snapshot(project, warehouse_conn, "silver.orders", r2, 1)
        finish_run(project, r2, "success", ["silver.orders"])

        all_snaps = get_all_snapshots(project)
        assert len(all_snaps) == 2
        model_names = {s.model_name for s in all_snaps}
        assert "bronze.customers" in model_names
        assert "silver.orders" in model_names


class TestDownstreamModels:
    def test_get_downstream(self, tmp_path):
        """Test finding downstream dependencies."""
        transform_dir = tmp_path / "transform"
        bronze = transform_dir / "bronze"
        silver = transform_dir / "silver"
        bronze.mkdir(parents=True)
        silver.mkdir(parents=True)

        (bronze / "customers.sql").write_text(
            "-- config: materialized=table, schema=bronze\n"
            "-- depends_on: landing.raw\n\n"
            "SELECT * FROM landing.raw\n"
        )
        (silver / "dim_customer.sql").write_text(
            "-- config: materialized=table, schema=silver\n"
            "-- depends_on: bronze.customers\n\n"
            "SELECT * FROM bronze.customers\n"
        )

        downstream = get_downstream_models("bronze.customers", transform_dir)
        assert "silver.dim_customer" in downstream

    def test_no_downstream(self, tmp_path):
        transform_dir = tmp_path / "transform"
        gold = transform_dir / "gold"
        gold.mkdir(parents=True)

        (gold / "final.sql").write_text(
            "-- config: materialized=table, schema=gold\n"
            "-- depends_on: silver.x\n\n"
            "SELECT 1\n"
        )

        downstream = get_downstream_models("gold.final", transform_dir)
        assert downstream == []


class TestRewindConfig:
    def test_retention_days(self):
        c = RewindConfig(retention="7d")
        assert c.retention_seconds == 7 * 86400

    def test_retention_hours(self):
        c = RewindConfig(retention="24h")
        assert c.retention_seconds == 24 * 3600

    def test_retention_numeric(self):
        c = RewindConfig(retention="14")
        assert c.retention_seconds == 14 * 86400

    def test_defaults(self):
        c = RewindConfig()
        assert c.enabled is True
        assert c.dedup is True
        assert c.max_storage is None
        assert c.exclude == []
