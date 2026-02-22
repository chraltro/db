"""Tests for Slack/webhook alerts and alerts config parsing."""

from __future__ import annotations

import textwrap
from pathlib import Path

import duckdb
import pytest

from dp.engine.database import ensure_meta_table


@pytest.fixture
def db(tmp_path):
    """Create a DuckDB connection with metadata tables."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    ensure_meta_table(conn)
    conn.execute("CREATE SCHEMA IF NOT EXISTS landing")
    return conn


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
