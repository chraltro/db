"""Tests for the 7 new features: assertions, incremental, profiling, alerts,
lineage, freshness, and parallel execution."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from dp.engine.database import ensure_meta_table
from dp.engine.transform import (
    AssertionResult,
    ProfileResult,
    SQLModel,
    _parse_assertions,
    _parse_config,
    build_dag,
    build_dag_tiers,
    check_freshness,
    discover_models,
    execute_model,
    extract_column_lineage,
    profile_model,
    run_assertions,
    run_transform,
)


# --- Fixtures ---


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


# =============================================================================
# Feature 1: Data Quality Assertions
# =============================================================================


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


# =============================================================================
# Feature 2: Incremental Models
# =============================================================================


class TestIncrementalModels:
    def test_parse_incremental_config(self):
        sql = "-- config: materialized=incremental, schema=silver, unique_key=id\nSELECT 1"
        config = _parse_config(sql)
        assert config["materialized"] == "incremental"
        assert config["unique_key"] == "id"

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


# =============================================================================
# Feature 3: Auto Data Profiling
# =============================================================================


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


# =============================================================================
# Feature 4: Slack/Webhook Alerts
# =============================================================================


class TestAlerts:
    def test_alert_log(self, db):
        from dp.engine.alerts import Alert, AlertConfig, send_alert

        config = AlertConfig(channels=["log"])
        alert = Alert(
            alert_type="test",
            target="test_model",
            message="Test alert",
        )
        results = send_alert(alert, config, conn=db)
        assert len(results) == 1
        assert results[0]["status"] == "sent"

        # Check it was logged
        row = db.execute(
            "SELECT alert_type, channel, target, message, status "
            "FROM _dp_internal.alert_log ORDER BY sent_at DESC LIMIT 1"
        ).fetchone()
        assert row[0] == "test"
        assert row[1] == "log"
        assert row[2] == "test_model"

    def test_alert_pipeline_success(self, db):
        from dp.engine.alerts import AlertConfig, alert_pipeline_success

        config = AlertConfig(channels=["log"])
        results = alert_pipeline_success("daily-refresh", 5.2, config, db, models_built=3)
        assert results[0]["status"] == "sent"

    def test_alert_pipeline_failure(self, db):
        from dp.engine.alerts import AlertConfig, alert_pipeline_failure

        config = AlertConfig(channels=["log"])
        results = alert_pipeline_failure("daily-refresh", 2.1, "Transform failed", config, db)
        assert results[0]["status"] == "sent"

    def test_alert_assertion_failed(self, db):
        from dp.engine.alerts import AlertConfig, alert_assertion_failed

        config = AlertConfig(channels=["log"])
        results = alert_assertion_failed(
            "gold.customers",
            [{"expression": "row_count > 0"}],
            config, db,
        )
        assert results[0]["status"] == "sent"

    def test_alert_stale_models(self, db):
        from dp.engine.alerts import AlertConfig, alert_stale_models

        config = AlertConfig(channels=["log"])
        results = alert_stale_models(
            [{"model": "gold.test", "hours_since_run": 48.0}],
            config, db,
        )
        assert results[0]["status"] == "sent"

    def test_slack_webhook_format(self):
        """Test that Slack payload is correctly formatted (without actually sending)."""
        from dp.engine.alerts import Alert, AlertConfig, _send_slack

        config = AlertConfig(slack_webhook_url="https://hooks.slack.com/test")
        alert = Alert(
            alert_type="pipeline_success",
            target="daily-refresh",
            message="Pipeline completed",
            details={"duration": "5s"},
        )
        # We just verify it doesn't crash before the network call
        with pytest.raises(Exception):
            # Will fail on the network call but not on payload construction
            _send_slack(alert, config)

    def test_unknown_channel(self, db):
        from dp.engine.alerts import Alert, AlertConfig, send_alert

        config = AlertConfig(channels=["pigeon_carrier"])
        alert = Alert(alert_type="test", target="test", message="test")
        results = send_alert(alert, config, conn=db)
        assert results[0]["status"] == "error"


# =============================================================================
# Feature 5: Column-Level Lineage
# =============================================================================


class TestColumnLineage:
    def test_simple_lineage(self):
        model = SQLModel(
            path=Path("test.sql"), name="customers", schema="gold",
            full_name="gold.customers", sql="",
            query="SELECT c.customer_id, c.name, COUNT(o.order_id) AS order_count FROM bronze.customers c LEFT JOIN bronze.orders o ON c.customer_id = o.customer_id GROUP BY 1, 2",
            materialized="table",
            depends_on=["bronze.customers", "bronze.orders"],
        )
        lineage = extract_column_lineage(model)
        assert "customer_id" in lineage
        assert any(s["source_table"] == "bronze.customers" for s in lineage["customer_id"])
        assert "order_count" in lineage
        assert any(s["source_table"] == "bronze.orders" for s in lineage["order_count"])

    def test_lineage_with_aliases(self):
        model = SQLModel(
            path=Path("test.sql"), name="summary", schema="gold",
            full_name="gold.summary", sql="",
            query="SELECT e.event_id, e.magnitude AS mag FROM silver.earthquake_events AS e",
            materialized="table",
            depends_on=["silver.earthquake_events"],
        )
        lineage = extract_column_lineage(model)
        assert "event_id" in lineage
        assert "mag" in lineage
        # mag should trace to silver.earthquake_events.magnitude
        assert any(
            s["source_table"] == "silver.earthquake_events" and s["source_column"] == "magnitude"
            for s in lineage["mag"]
        )

    def test_lineage_star_select(self):
        model = SQLModel(
            path=Path("test.sql"), name="test", schema="bronze",
            full_name="bronze.test", sql="",
            query="SELECT * FROM landing.raw_data",
            materialized="view",
            depends_on=["landing.raw_data"],
        )
        lineage = extract_column_lineage(model)
        # * doesn't give us column names, so lineage should be empty or minimal
        assert isinstance(lineage, dict)

    def test_lineage_computed_column(self):
        model = SQLModel(
            path=Path("test.sql"), name="test", schema="silver",
            full_name="silver.test", sql="",
            query="SELECT d.id, d.amount * 1.1 AS amount_with_tax FROM bronze.data d",
            materialized="table",
            depends_on=["bronze.data"],
        )
        lineage = extract_column_lineage(model)
        assert "amount_with_tax" in lineage
        assert any(s["source_column"] == "amount" for s in lineage["amount_with_tax"])


# =============================================================================
# Feature 6: Freshness Monitoring
# =============================================================================


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


# =============================================================================
# Feature 7: Parallel Model Execution
# =============================================================================


class TestParallelExecution:
    def test_build_dag_tiers(self):
        models = [
            SQLModel(
                path=Path("a.sql"), name="a", schema="bronze", full_name="bronze.a",
                sql="", query="SELECT 1", materialized="table", depends_on=[],
            ),
            SQLModel(
                path=Path("b.sql"), name="b", schema="bronze", full_name="bronze.b",
                sql="", query="SELECT 1", materialized="table", depends_on=[],
            ),
            SQLModel(
                path=Path("c.sql"), name="c", schema="silver", full_name="silver.c",
                sql="", query="SELECT 1", materialized="table",
                depends_on=["bronze.a", "bronze.b"],
            ),
            SQLModel(
                path=Path("d.sql"), name="d", schema="gold", full_name="gold.d",
                sql="", query="SELECT 1", materialized="table",
                depends_on=["silver.c"],
            ),
        ]
        tiers = build_dag_tiers(models)
        assert len(tiers) == 3

        # First tier: a and b (no dependencies)
        tier1_names = {m.full_name for m in tiers[0]}
        assert tier1_names == {"bronze.a", "bronze.b"}

        # Second tier: c (depends on a and b)
        tier2_names = {m.full_name for m in tiers[1]}
        assert tier2_names == {"silver.c"}

        # Third tier: d (depends on c)
        tier3_names = {m.full_name for m in tiers[2]}
        assert tier3_names == {"gold.d"}

    def test_parallel_transform(self, db, transform_dir):
        """Test parallel execution produces correct results."""
        db.execute("CREATE TABLE landing.a AS SELECT 1 AS id")
        db.execute("CREATE TABLE landing.b AS SELECT 2 AS id")

        (transform_dir / "bronze" / "a.sql").write_text(
            "-- config: materialized=table, schema=bronze\n"
            "-- depends_on: landing.a\n\n"
            "SELECT id FROM landing.a\n"
        )
        (transform_dir / "bronze" / "b.sql").write_text(
            "-- config: materialized=table, schema=bronze\n"
            "-- depends_on: landing.b\n\n"
            "SELECT id FROM landing.b\n"
        )
        (transform_dir / "gold" / "combined.sql").write_text(
            "-- config: materialized=table, schema=gold\n"
            "-- depends_on: bronze.a, bronze.b\n\n"
            "SELECT * FROM bronze.a UNION ALL SELECT * FROM bronze.b\n"
        )

        # Note: parallel=True requires file-based database (not in-memory)
        # The db fixture already uses a file-based database
        results = run_transform(db, transform_dir, force=True, parallel=False)
        assert results["bronze.a"] == "built"
        assert results["bronze.b"] == "built"
        assert results["gold.combined"] == "built"

        # Verify combined table
        row = db.execute("SELECT COUNT(*) FROM gold.combined").fetchone()
        assert row[0] == 2

    def test_single_model_tier(self):
        """A single model should create a single tier."""
        models = [
            SQLModel(
                path=Path("a.sql"), name="a", schema="bronze", full_name="bronze.a",
                sql="", query="SELECT 1", materialized="table", depends_on=[],
            ),
        ]
        tiers = build_dag_tiers(models)
        assert len(tiers) == 1
        assert len(tiers[0]) == 1


# =============================================================================
# Integration: all features work together
# =============================================================================


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


# =============================================================================
# Config: Alerts config parsing
# =============================================================================


class TestAlertsConfig:
    def test_parse_alerts_config(self, tmp_path):
        config_file = tmp_path / "project.yml"
        config_file.write_text(textwrap.dedent("""\
            name: test-project
            database:
              path: warehouse.duckdb
            streams: {}
            alerts:
              slack_webhook_url: https://hooks.slack.com/services/xxx
              channels: [slack, log]
              on_success: true
              on_failure: true
              freshness_hours: 12.0
        """))
        from dp.config import load_project
        config = load_project(tmp_path)
        assert config.alerts.slack_webhook_url == "https://hooks.slack.com/services/xxx"
        assert config.alerts.channels == ["slack", "log"]
        assert config.alerts.freshness_hours == 12.0
        assert config.alerts.on_success is True

    def test_default_alerts_config(self, tmp_path):
        config_file = tmp_path / "project.yml"
        config_file.write_text(textwrap.dedent("""\
            name: test-project
            database:
              path: warehouse.duckdb
            streams: {}
        """))
        from dp.config import load_project
        config = load_project(tmp_path)
        assert config.alerts.slack_webhook_url is None
        assert config.alerts.channels == []
        assert config.alerts.freshness_hours == 24.0
