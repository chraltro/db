"""Alert and notification engine.

Supports Slack webhooks, generic webhooks, and console logging.
Sends notifications for pipeline events: success, failure, assertion failures,
freshness warnings, and anomalies.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from urllib.request import Request, urlopen

import duckdb

from dp.engine.database import ensure_meta_table

logger = logging.getLogger("dp.alerts")


@dataclass
class AlertConfig:
    """Configuration for alert channels."""

    slack_webhook_url: str | None = None
    webhook_url: str | None = None
    channels: list[str] = field(default_factory=list)  # ["slack", "webhook", "log"]


@dataclass
class Alert:
    """A single alert to be sent."""

    alert_type: str  # "pipeline_success", "pipeline_failure", "assertion_failed", "stale_model", "anomaly"
    target: str  # model or stream name
    message: str
    details: dict[str, Any] = field(default_factory=dict)


def send_alert(
    alert: Alert,
    config: AlertConfig,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> list[dict]:
    """Send an alert to all configured channels.

    Returns a list of {channel, status, error} dicts.
    """
    results = []
    channels = config.channels or []

    if not channels:
        # Auto-detect from config
        if config.slack_webhook_url:
            channels.append("slack")
        if config.webhook_url:
            channels.append("webhook")

    for channel in channels:
        try:
            if channel == "slack":
                _send_slack(alert, config)
                results.append({"channel": "slack", "status": "sent"})
            elif channel == "webhook":
                _send_webhook(alert, config)
                results.append({"channel": "webhook", "status": "sent"})
            elif channel == "log":
                _send_log(alert)
                results.append({"channel": "log", "status": "sent"})
            else:
                results.append({"channel": channel, "status": "error", "error": f"Unknown channel: {channel}"})
        except Exception as e:
            logger.warning("Alert failed for channel %s: %s", channel, e)
            results.append({"channel": channel, "status": "error", "error": str(e)})

    # Log to database if connection provided
    if conn:
        try:
            ensure_meta_table(conn)
            for r in results:
                conn.execute(
                    """
                    INSERT INTO _dp_internal.alert_log
                        (alert_type, channel, target, message, status, error)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [alert.alert_type, r["channel"], alert.target, alert.message,
                     r["status"], r.get("error")],
                )
        except Exception as e:
            logger.warning("Failed to log alert: %s", e)

    return results


def _send_slack(alert: Alert, config: AlertConfig) -> None:
    """Send a Slack message via incoming webhook."""
    if not config.slack_webhook_url:
        raise ValueError("No Slack webhook URL configured")

    emoji = {
        "pipeline_success": ":white_check_mark:",
        "pipeline_failure": ":x:",
        "assertion_failed": ":warning:",
        "stale_model": ":hourglass:",
        "anomaly": ":mag:",
    }.get(alert.alert_type, ":bell:")

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{emoji} *{alert.alert_type.replace('_', ' ').title()}*\n{alert.message}",
            },
        },
    ]

    if alert.details:
        detail_lines = []
        for k, v in alert.details.items():
            detail_lines.append(f"*{k}:* {v}")
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "\n".join(detail_lines),
            },
        })

    blocks.append({
        "type": "context",
        "elements": [
            {"type": "mrkdwn", "text": f"dp | {alert.target} | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}
        ],
    })

    payload = json.dumps({"blocks": blocks}).encode()
    req = Request(
        config.slack_webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    urlopen(req, timeout=10)


def _send_webhook(alert: Alert, config: AlertConfig) -> None:
    """Send a generic webhook notification."""
    if not config.webhook_url:
        raise ValueError("No webhook URL configured")

    payload = json.dumps({
        "alert_type": alert.alert_type,
        "target": alert.target,
        "message": alert.message,
        "details": alert.details,
        "timestamp": datetime.now().isoformat(),
    }).encode()

    req = Request(
        config.webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    urlopen(req, timeout=10)


def _send_log(alert: Alert) -> None:
    """Log an alert to the Python logger."""
    level = logging.WARNING if "fail" in alert.alert_type else logging.INFO
    logger.log(level, "[%s] %s: %s", alert.alert_type, alert.target, alert.message)


# --- Convenience functions for common alert types ---


def alert_pipeline_success(
    stream_name: str,
    duration_s: float,
    config: AlertConfig,
    conn: duckdb.DuckDBPyConnection | None = None,
    models_built: int = 0,
) -> list[dict]:
    """Send alert for successful pipeline completion."""
    alert = Alert(
        alert_type="pipeline_success",
        target=stream_name,
        message=f"Pipeline `{stream_name}` completed successfully in {duration_s}s",
        details={"duration": f"{duration_s}s", "models_built": models_built},
    )
    return send_alert(alert, config, conn)


def alert_pipeline_failure(
    stream_name: str,
    duration_s: float,
    error: str,
    config: AlertConfig,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> list[dict]:
    """Send alert for pipeline failure."""
    alert = Alert(
        alert_type="pipeline_failure",
        target=stream_name,
        message=f"Pipeline `{stream_name}` failed after {duration_s}s: {error}",
        details={"duration": f"{duration_s}s", "error": error},
    )
    return send_alert(alert, config, conn)


def alert_assertion_failed(
    model_name: str,
    failed_assertions: list[dict],
    config: AlertConfig,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> list[dict]:
    """Send alert for failed data quality assertions."""
    assertion_list = ", ".join(a["expression"] for a in failed_assertions)
    alert = Alert(
        alert_type="assertion_failed",
        target=model_name,
        message=f"Data quality check failed for `{model_name}`: {assertion_list}",
        details={
            "model": model_name,
            "failed_assertions": len(failed_assertions),
            "assertions": assertion_list,
        },
    )
    return send_alert(alert, config, conn)


def alert_stale_models(
    stale_models: list[dict],
    config: AlertConfig,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> list[dict]:
    """Send alert for stale (outdated) models."""
    model_list = ", ".join(f"`{m['model']}` ({m['hours_since_run']}h)" for m in stale_models[:5])
    alert = Alert(
        alert_type="stale_model",
        target="freshness_check",
        message=f"{len(stale_models)} model(s) are stale: {model_list}",
        details={
            "stale_count": len(stale_models),
            "models": model_list,
        },
    )
    return send_alert(alert, config, conn)
