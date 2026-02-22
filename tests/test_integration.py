"""Tests for integration: all features working together."""

from __future__ import annotations

import textwrap
from pathlib import Path

import duckdb
import pytest

from dp.engine.database import ensure_meta_table
from dp.engine.transform import (
    SQLModel,
    check_freshness,
    discover_models,
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


class TestIntegration:
    def test_full_pipeline_with_all_features(self, db, transform_dir):
        """End-to-end: incremental + assertions + profiling."""
        # Create landing data
        db.execute("CREATE TABLE landing.customers AS "
                    "SELECT 1 AS id, 'Alice' AS name, 'alice@test.com' AS email "
                    "UNION ALL SELECT 2, 'Bob', 'bob@test.com'")

        # Bronze: regular table with assertions
        (transform_dir / "bronze" / "customers.sql").write_text(textwrap.dedent("""\
            -- config: materialized=table, schema=bronze
            -- depends_on: landing.customers
            -- assert: row_count > 0
            -- assert: no_nulls(email)
            -- assert: unique(id)

            SELECT id, UPPER(name) AS name, email FROM landing.customers
        """))

        # Silver: incremental table
        (transform_dir / "silver" / "customers.sql").write_text(textwrap.dedent("""\
            -- config: materialized=incremental, schema=silver, unique_key=id
            -- depends_on: bronze.customers

            SELECT id, name, email FROM bronze.customers
        """))

        # Gold: view with assertions
        (transform_dir / "gold" / "customer_count.sql").write_text(textwrap.dedent("""\
            -- config: materialized=table, schema=gold
            -- depends_on: silver.customers
            -- assert: row_count > 0

            SELECT COUNT(*) AS total_customers FROM silver.customers
        """))

        # Run transform
        results = run_transform(db, transform_dir, force=True)
        assert results["bronze.customers"] == "built"
        assert results["silver.customers"] == "built"
        assert results["gold.customer_count"] == "built"

        # Verify data
        assert db.execute("SELECT COUNT(*) FROM bronze.customers").fetchone()[0] == 2
        assert db.execute("SELECT COUNT(*) FROM silver.customers").fetchone()[0] == 2
        assert db.execute("SELECT total_customers FROM gold.customer_count").fetchone()[0] == 2

        # Verify profiles were saved
        profile = db.execute(
            "SELECT row_count FROM _dp_internal.model_profiles WHERE model_path = 'bronze.customers'"
        ).fetchone()
        assert profile is not None
        assert profile[0] == 2

        # Verify assertions were logged
        assertion_count = db.execute(
            "SELECT COUNT(*) FROM _dp_internal.assertion_results WHERE model_path = 'bronze.customers'"
        ).fetchone()[0]
        assert assertion_count == 3  # row_count > 0, no_nulls(email), unique(id)

        # Check freshness
        freshness = check_freshness(db, max_age_hours=24.0)
        assert len(freshness) >= 3
        assert all(not r["is_stale"] for r in freshness)

    def test_discover_models_with_assertions(self, transform_dir):
        (transform_dir / "bronze" / "test.sql").write_text(textwrap.dedent("""\
            -- config: materialized=table, schema=bronze
            -- depends_on: landing.data
            -- assert: row_count > 0
            -- assert: unique(id)

            SELECT * FROM landing.data
        """))
        models = discover_models(transform_dir)
        assert len(models) == 1
        assert models[0].assertions == ["row_count > 0", "unique(id)"]

    def test_discover_incremental_model(self, transform_dir):
        (transform_dir / "silver" / "inc.sql").write_text(textwrap.dedent("""\
            -- config: materialized=incremental, schema=silver, unique_key=user_id
            -- depends_on: bronze.users

            SELECT * FROM bronze.users
        """))
        models = discover_models(transform_dir)
        assert len(models) == 1
        assert models[0].materialized == "incremental"
        assert models[0].unique_key == "user_id"

    def test_discover_incremental_model_with_strategy(self, transform_dir):
        (transform_dir / "silver" / "inc.sql").write_text(textwrap.dedent("""\
            -- config: materialized=incremental, schema=silver, unique_key=id, incremental_strategy=append
            -- depends_on: bronze.data

            SELECT * FROM bronze.data
        """))
        models = discover_models(transform_dir)
        assert models[0].incremental_strategy == "append"
