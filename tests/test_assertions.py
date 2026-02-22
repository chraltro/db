"""Tests for data quality assertions."""

from __future__ import annotations

import textwrap
from pathlib import Path

import duckdb
import pytest

from dp.engine.database import ensure_meta_table
from dp.engine.sql_analysis import parse_assertions as _parse_assertions
from dp.engine.transform import (
    AssertionResult,
    SQLModel,
    run_assertions,
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


class TestAssertions:
    def test_parse_assertions(self):
        sql = textwrap.dedent("""\
            -- config: materialized=table, schema=gold
            -- depends_on: silver.customers
            -- assert: row_count > 0
            -- assert: no_nulls(email)
            -- assert: unique(customer_id)

            SELECT * FROM silver.customers
        """)
        assertions = _parse_assertions(sql)
        assert len(assertions) == 3
        assert assertions[0] == "row_count > 0"
        assert assertions[1] == "no_nulls(email)"
        assert assertions[2] == "unique(customer_id)"

    def test_assert_row_count(self, db):
        db.execute("CREATE SCHEMA IF NOT EXISTS gold")
        db.execute("CREATE TABLE gold.test AS SELECT 1 AS id, 'a' AS name")
        model = SQLModel(
            path=Path("test.sql"), name="test", schema="gold",
            full_name="gold.test", sql="", query="SELECT 1",
            materialized="table", assertions=["row_count > 0"],
        )
        results = run_assertions(db, model)
        assert len(results) == 1
        assert results[0].passed is True
        assert "row_count=1" in results[0].detail

    def test_assert_row_count_fails(self, db):
        db.execute("CREATE SCHEMA IF NOT EXISTS gold")
        db.execute("CREATE TABLE gold.empty AS SELECT 1 AS id WHERE false")
        model = SQLModel(
            path=Path("test.sql"), name="empty", schema="gold",
            full_name="gold.empty", sql="", query="SELECT 1",
            materialized="table", assertions=["row_count > 0"],
        )
        results = run_assertions(db, model)
        assert len(results) == 1
        assert results[0].passed is False

    def test_assert_no_nulls(self, db):
        db.execute("CREATE SCHEMA IF NOT EXISTS gold")
        db.execute("CREATE TABLE gold.clean AS SELECT 1 AS id, 'alice@test.com' AS email")
        model = SQLModel(
            path=Path("test.sql"), name="clean", schema="gold",
            full_name="gold.clean", sql="", query="SELECT 1",
            materialized="table", assertions=["no_nulls(email)"],
        )
        results = run_assertions(db, model)
        assert results[0].passed is True

    def test_assert_no_nulls_fails(self, db):
        db.execute("CREATE SCHEMA IF NOT EXISTS gold")
        db.execute("CREATE TABLE gold.dirty AS SELECT 1 AS id, NULL AS email")
        model = SQLModel(
            path=Path("test.sql"), name="dirty", schema="gold",
            full_name="gold.dirty", sql="", query="SELECT 1",
            materialized="table", assertions=["no_nulls(email)"],
        )
        results = run_assertions(db, model)
        assert results[0].passed is False
        assert "null_count=1" in results[0].detail

    def test_assert_unique(self, db):
        db.execute("CREATE SCHEMA IF NOT EXISTS gold")
        db.execute("CREATE TABLE gold.uniq AS SELECT 1 AS id UNION ALL SELECT 2")
        model = SQLModel(
            path=Path("test.sql"), name="uniq", schema="gold",
            full_name="gold.uniq", sql="", query="SELECT 1",
            materialized="table", assertions=["unique(id)"],
        )
        results = run_assertions(db, model)
        assert results[0].passed is True

    def test_assert_unique_fails(self, db):
        db.execute("CREATE SCHEMA IF NOT EXISTS gold")
        db.execute("CREATE TABLE gold.dupes AS SELECT 1 AS id UNION ALL SELECT 1")
        model = SQLModel(
            path=Path("test.sql"), name="dupes", schema="gold",
            full_name="gold.dupes", sql="", query="SELECT 1",
            materialized="table", assertions=["unique(id)"],
        )
        results = run_assertions(db, model)
        assert results[0].passed is False
        assert "duplicate_count=1" in results[0].detail

    def test_assert_accepted_values(self, db):
        db.execute("CREATE SCHEMA IF NOT EXISTS gold")
        db.execute("CREATE TABLE gold.statuses AS SELECT 'active' AS status UNION ALL SELECT 'inactive'")
        model = SQLModel(
            path=Path("test.sql"), name="statuses", schema="gold",
            full_name="gold.statuses", sql="", query="SELECT 1",
            materialized="table",
            assertions=["accepted_values(status, ['active', 'inactive', 'pending'])"],
        )
        results = run_assertions(db, model)
        assert results[0].passed is True

    def test_assert_accepted_values_fails(self, db):
        db.execute("CREATE SCHEMA IF NOT EXISTS gold")
        db.execute("CREATE TABLE gold.bad_status AS SELECT 'UNKNOWN' AS status")
        model = SQLModel(
            path=Path("test.sql"), name="bad_status", schema="gold",
            full_name="gold.bad_status", sql="", query="SELECT 1",
            materialized="table",
            assertions=["accepted_values(status, ['active', 'inactive'])"],
        )
        results = run_assertions(db, model)
        assert results[0].passed is False

    def test_assertions_in_transform(self, db, transform_dir):
        db.execute("CREATE TABLE landing.data AS SELECT 1 AS id, 'test' AS name")
        (transform_dir / "bronze" / "data.sql").write_text(textwrap.dedent("""\
            -- config: materialized=table, schema=bronze
            -- depends_on: landing.data
            -- assert: row_count > 0
            -- assert: no_nulls(id)

            SELECT id, name FROM landing.data
        """))
        results = run_transform(db, transform_dir, force=True)
        assert results["bronze.data"] == "built"

    def test_assertion_failure_stops_pipeline(self, db, transform_dir):
        db.execute("CREATE TABLE landing.empty AS SELECT 1 AS id WHERE false")
        (transform_dir / "bronze" / "empty.sql").write_text(textwrap.dedent("""\
            -- config: materialized=table, schema=bronze
            -- depends_on: landing.empty
            -- assert: row_count > 0

            SELECT id FROM landing.empty
        """))
        results = run_transform(db, transform_dir, force=True)
        assert results["bronze.empty"] == "assertion_failed"

    def test_assertion_on_all_null_table(self, db):
        """Assertions on a table where all values are NULL."""
        db.execute("CREATE SCHEMA IF NOT EXISTS gold")
        db.execute(
            "CREATE TABLE gold.all_nulls AS "
            "SELECT NULL::INTEGER AS id, NULL::VARCHAR AS name"
        )
        model = SQLModel(
            path=Path("test.sql"), name="all_nulls", schema="gold",
            full_name="gold.all_nulls", sql="", query="SELECT 1",
            materialized="table",
            assertions=["no_nulls(id)", "row_count > 0"],
        )
        results = run_assertions(db, model)
        # no_nulls should fail
        assert results[0].passed is False
        # row_count > 0 should pass (there is one row)
        assert results[1].passed is True

    def test_assertion_error_in_expression(self, db):
        """Invalid assertion expression should fail gracefully."""
        db.execute("CREATE SCHEMA IF NOT EXISTS gold")
        db.execute("CREATE TABLE gold.test_err AS SELECT 1 AS id")
        model = SQLModel(
            path=Path("test.sql"), name="test_err", schema="gold",
            full_name="gold.test_err", sql="", query="SELECT 1",
            materialized="table",
            assertions=["INVALID SQL GARBAGE %%% !!!"],
        )
        results = run_assertions(db, model)
        assert len(results) == 1
        assert results[0].passed is False
        assert "Assertion error" in results[0].detail
