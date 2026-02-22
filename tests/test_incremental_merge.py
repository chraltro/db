"""Tests for Incremental Transforms — merge strategy + partition_by."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest


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
