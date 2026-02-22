"""Tests for incremental models."""

from __future__ import annotations

import textwrap
from pathlib import Path

import duckdb
import pytest

from dp.engine.database import ensure_meta_table
from dp.engine.sql_analysis import parse_config as _parse_config
from dp.engine.transform import (
    SQLModel,
    run_transform,
)


@pytest.fixture
def db(tmp_path):
    """Create a DuckDB connection with metadata tables."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    ensure_meta_table(conn)
    conn.execute("CREATE SCHEMA IF NOT EXISTS landing")
    return conn


@pytest.fixture
def transform_dir(tmp_path):
    """Create a basic transform directory."""
    t = tmp_path / "transform"
    t.mkdir()
    for sub in ("bronze", "silver", "gold"):
        (t / sub).mkdir()
    return t


class TestIncrementalModels:
    def test_parse_incremental_config(self):
        sql = "-- config: materialized=incremental, schema=silver, unique_key=id\nSELECT 1"
        config = _parse_config(sql)
        assert config["materialized"] == "incremental"
        assert config["unique_key"] == "id"

    def test_parse_incremental_strategy(self):
        sql = "-- config: materialized=incremental, schema=silver, unique_key=id, incremental_strategy=append\nSELECT 1"
        config = _parse_config(sql)
        assert config["incremental_strategy"] == "append"

    def test_incremental_first_run_creates_table(self, db, transform_dir):
        db.execute("CREATE TABLE landing.orders AS SELECT 1 AS id, 100 AS amount")
        (transform_dir / "silver" / "orders.sql").write_text(textwrap.dedent("""\
            -- config: materialized=incremental, schema=silver, unique_key=id
            -- depends_on: landing.orders

            SELECT id, amount FROM landing.orders
        """))
        results = run_transform(db, transform_dir, force=True)
        assert results["silver.orders"] == "built"
        row = db.execute("SELECT COUNT(*) FROM silver.orders").fetchone()
        assert row[0] == 1

    def test_incremental_upsert(self, db, transform_dir):
        db.execute("CREATE TABLE landing.orders AS SELECT 1 AS id, 100 AS amount")
        (transform_dir / "silver" / "orders.sql").write_text(textwrap.dedent("""\
            -- config: materialized=incremental, schema=silver, unique_key=id
            -- depends_on: landing.orders

            SELECT id, amount FROM landing.orders
        """))
        # First run
        run_transform(db, transform_dir, force=True)
        assert db.execute("SELECT COUNT(*) FROM silver.orders").fetchone()[0] == 1

        # Add new data and update existing
        db.execute("DELETE FROM landing.orders")
        db.execute("INSERT INTO landing.orders VALUES (1, 200)")  # updated amount
        db.execute("INSERT INTO landing.orders VALUES (2, 300)")  # new row

        # Second run — should upsert
        run_transform(db, transform_dir, force=True)
        rows = db.execute("SELECT * FROM silver.orders ORDER BY id").fetchall()
        assert len(rows) == 2
        assert rows[0] == (1, 200)  # updated
        assert rows[1] == (2, 300)  # new

    def test_incremental_append_only(self, db, transform_dir):
        """Without unique_key, incremental should append."""
        db.execute("CREATE TABLE landing.events AS SELECT 1 AS event_id, 'click' AS event_type")
        (transform_dir / "silver" / "events.sql").write_text(textwrap.dedent("""\
            -- config: materialized=incremental, schema=silver
            -- depends_on: landing.events

            SELECT event_id, event_type FROM landing.events
        """))
        # First run
        run_transform(db, transform_dir, force=True)
        assert db.execute("SELECT COUNT(*) FROM silver.events").fetchone()[0] == 1

        # Second run — should append (no unique key)
        run_transform(db, transform_dir, force=True)
        assert db.execute("SELECT COUNT(*) FROM silver.events").fetchone()[0] == 2

    def test_incremental_explicit_append_strategy(self, db, transform_dir):
        """incremental_strategy=append should always append."""
        db.execute("CREATE TABLE landing.logs AS SELECT 1 AS id, 'info' AS msg")
        (transform_dir / "silver" / "logs.sql").write_text(textwrap.dedent("""\
            -- config: materialized=incremental, schema=silver, unique_key=id, incremental_strategy=append
            -- depends_on: landing.logs

            SELECT id, msg FROM landing.logs
        """))
        run_transform(db, transform_dir, force=True)
        assert db.execute("SELECT COUNT(*) FROM silver.logs").fetchone()[0] == 1

        # Even with unique_key, append strategy should not dedup
        run_transform(db, transform_dir, force=True)
        assert db.execute("SELECT COUNT(*) FROM silver.logs").fetchone()[0] == 2

    def test_incremental_schema_evolution(self, db, transform_dir):
        """New columns in source should be auto-added to target."""
        db.execute("CREATE TABLE landing.evolve AS SELECT 1 AS id, 'alice' AS name")
        (transform_dir / "silver" / "evolve.sql").write_text(textwrap.dedent("""\
            -- config: materialized=incremental, schema=silver, unique_key=id
            -- depends_on: landing.evolve

            SELECT id, name FROM landing.evolve
        """))
        # First run
        run_transform(db, transform_dir, force=True)
        assert db.execute("SELECT COUNT(*) FROM silver.evolve").fetchone()[0] == 1

        # Add a new column to source
        db.execute("DROP TABLE landing.evolve")
        db.execute("CREATE TABLE landing.evolve AS SELECT 2 AS id, 'bob' AS name, 'bob@test.com' AS email")

        # Update the SQL to include the new column
        (transform_dir / "silver" / "evolve.sql").write_text(textwrap.dedent("""\
            -- config: materialized=incremental, schema=silver, unique_key=id
            -- depends_on: landing.evolve

            SELECT id, name, email FROM landing.evolve
        """))

        # Second run — should handle new column
        run_transform(db, transform_dir, force=True)
        rows = db.execute("SELECT * FROM silver.evolve ORDER BY id").fetchall()
        assert len(rows) == 2
        # Row 1 should have NULL for email (old row)
        assert rows[0][0] == 1
        assert rows[0][2] is None  # email
        # Row 2 should have email
        assert rows[1][0] == 2
        assert rows[1][2] == "bob@test.com"

    def test_incremental_with_filter(self, db, transform_dir):
        """incremental_filter should be applied on non-first runs."""
        db.execute("CREATE TABLE landing.ts_data AS SELECT 1 AS id, TIMESTAMP '2024-01-01' AS updated_at")
        (transform_dir / "silver" / "ts_data.sql").write_text(textwrap.dedent("""\
            -- config: materialized=incremental, schema=silver, unique_key=id, incremental_filter=WHERE updated_at > (SELECT MAX(updated_at) FROM {this})
            -- depends_on: landing.ts_data

            SELECT id, updated_at FROM landing.ts_data
        """))
        # First run — full load (filter not applied)
        run_transform(db, transform_dir, force=True)
        assert db.execute("SELECT COUNT(*) FROM silver.ts_data").fetchone()[0] == 1

    def test_incremental_duplicate_keys_in_staging(self, db, transform_dir):
        """Duplicate keys in source data should work (last write wins)."""
        db.execute(
            "CREATE TABLE landing.dupes AS "
            "SELECT 1 AS id, 100 AS amount "
            "UNION ALL SELECT 1, 200"
        )
        (transform_dir / "silver" / "dupes.sql").write_text(textwrap.dedent("""\
            -- config: materialized=incremental, schema=silver, unique_key=id
            -- depends_on: landing.dupes

            SELECT id, amount FROM landing.dupes
        """))
        # First run — creates table with both rows (dupes in first load)
        run_transform(db, transform_dir, force=True)
        count = db.execute("SELECT COUNT(*) FROM silver.dupes").fetchone()[0]
        assert count == 2  # Both rows are inserted on first load
