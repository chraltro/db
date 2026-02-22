"""Tests for auto data profiling and freshness monitoring."""

from __future__ import annotations

import textwrap
from pathlib import Path

import duckdb
import pytest

from dp.engine.database import ensure_meta_table
from dp.engine.transform import (
    ProfileResult,
    SQLModel,
    check_freshness,
    profile_model,
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


class TestAutoProfiling:
    def test_profile_model(self, db):
        db.execute("CREATE SCHEMA IF NOT EXISTS gold")
        db.execute(
            "CREATE TABLE gold.stats AS "
            "SELECT 1 AS id, 'alice' AS name, 25 AS age "
            "UNION ALL SELECT 2, NULL, 30"
        )
        model = SQLModel(
            path=Path("test.sql"), name="stats", schema="gold",
            full_name="gold.stats", sql="", query="SELECT 1",
            materialized="table",
        )
        profile = profile_model(db, model)
        assert profile.row_count == 2
        assert profile.column_count == 3
        assert profile.null_percentages["name"] == 50.0
        assert profile.null_percentages["id"] == 0.0
        assert profile.distinct_counts["id"] == 2
        assert profile.distinct_counts["age"] == 2

    def test_profile_saved_during_transform(self, db, transform_dir):
        db.execute("CREATE TABLE landing.data AS SELECT 1 AS id, 'test' AS name")
        (transform_dir / "bronze" / "data.sql").write_text(textwrap.dedent("""\
            -- config: materialized=table, schema=bronze
            -- depends_on: landing.data

            SELECT id, name FROM landing.data
        """))
        run_transform(db, transform_dir, force=True)

        # Check profile was saved
        row = db.execute(
            "SELECT row_count, column_count FROM _dp_internal.model_profiles WHERE model_path = 'bronze.data'"
        ).fetchone()
        assert row is not None
        assert row[0] == 1  # row_count
        assert row[1] == 2  # column_count

    def test_profile_empty_table(self, db):
        db.execute("CREATE SCHEMA IF NOT EXISTS gold")
        db.execute("CREATE TABLE gold.empty (id INTEGER, name VARCHAR)")
        model = SQLModel(
            path=Path("test.sql"), name="empty", schema="gold",
            full_name="gold.empty", sql="", query="SELECT 1",
            materialized="table",
        )
        profile = profile_model(db, model)
        assert profile.row_count == 0
        assert profile.column_count == 2


class TestFreshness:
    def test_freshness_check(self, db):
        # Insert a model state entry
        db.execute(
            "INSERT INTO _dp_internal.model_state "
            "(model_path, content_hash, upstream_hash, materialized_as, last_run_at, row_count) "
            "VALUES ('gold.test', 'abc', '', 'table', current_timestamp - INTERVAL 2 HOUR, 100)"
        )
        results = check_freshness(db, max_age_hours=24.0)
        assert len(results) == 1
        assert results[0]["model"] == "gold.test"
        assert results[0]["is_stale"] is False

    def test_freshness_stale(self, db):
        db.execute(
            "INSERT INTO _dp_internal.model_state "
            "(model_path, content_hash, upstream_hash, materialized_as, last_run_at, row_count) "
            "VALUES ('gold.old', 'abc', '', 'table', current_timestamp - INTERVAL 48 HOUR, 50)"
        )
        results = check_freshness(db, max_age_hours=24.0)
        stale = [r for r in results if r["is_stale"]]
        assert len(stale) == 1
        assert stale[0]["model"] == "gold.old"

    def test_freshness_mixed(self, db):
        db.execute(
            "INSERT INTO _dp_internal.model_state "
            "(model_path, content_hash, upstream_hash, materialized_as, last_run_at, row_count) "
            "VALUES ('gold.fresh', 'abc', '', 'table', current_timestamp - INTERVAL 1 HOUR, 100)"
        )
        db.execute(
            "INSERT INTO _dp_internal.model_state "
            "(model_path, content_hash, upstream_hash, materialized_as, last_run_at, row_count) "
            "VALUES ('gold.stale', 'def', '', 'table', current_timestamp - INTERVAL 72 HOUR, 50)"
        )
        results = check_freshness(db, max_age_hours=24.0)
        fresh = [r for r in results if not r["is_stale"]]
        stale = [r for r in results if r["is_stale"]]
        assert len(fresh) >= 1
        assert len(stale) >= 1

    def test_freshness_empty(self, db):
        results = check_freshness(db, max_age_hours=24.0)
        assert results == []
