"""Data quality endpoints: profiles, assertions, freshness, alerts, and contracts."""

from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from dp.server.deps import (
    DbConn,
    DbConnReadOnly,
    DbConnReadOnlyOptional,
    _get_config,
    _get_project_dir,
    _require_permission,
    ensure_meta_table,
)

router = APIRouter()


# --- Pydantic models ---


class TestAlertRequest(BaseModel):
    channel: str = Field(..., pattern=r"^(slack|webhook|log)$")
    slack_webhook_url: str | None = None
    webhook_url: str | None = None


# --- Freshness ---


@router.get("/api/freshness")
def get_freshness(
    request: Request, conn: DbConnReadOnly, max_hours: float = 24.0
) -> list[dict]:
    """Check model freshness: which models are stale?"""
    _require_permission(request, "read")
    from dp.engine.transform import check_freshness

    ensure_meta_table(conn)
    return check_freshness(conn, max_age_hours=max_hours)


# --- Model profiles ---


@router.get("/api/profiles")
def get_profiles(request: Request, conn: DbConnReadOnly) -> list[dict]:
    """Get auto-computed profile stats for all models."""
    _require_permission(request, "read")
    ensure_meta_table(conn)
    rows = conn.execute(
        "SELECT model_path, row_count, column_count, null_percentages, distinct_counts, profiled_at "
        "FROM _dp_internal.model_profiles ORDER BY model_path"
    ).fetchall()
    return [
        {
            "model": r[0],
            "row_count": r[1],
            "column_count": r[2],
            "null_percentages": json.loads(r[3]) if r[3] else {},
            "distinct_counts": json.loads(r[4]) if r[4] else {},
            "profiled_at": str(r[5]) if r[5] else None,
        }
        for r in rows
    ]


@router.get("/api/profiles/{model_name}")
def get_profile(
    request: Request, model_name: str, conn: DbConnReadOnly
) -> dict:
    """Get profile stats for a specific model."""
    _require_permission(request, "read")
    ensure_meta_table(conn)
    row = conn.execute(
        "SELECT model_path, row_count, column_count, null_percentages, distinct_counts, profiled_at "
        "FROM _dp_internal.model_profiles WHERE model_path = ?",
        [model_name],
    ).fetchone()
    if not row:
        raise HTTPException(
            404, f"No profile for '{model_name}'. Run dp transform first."
        )
    return {
        "model": row[0],
        "row_count": row[1],
        "column_count": row[2],
        "null_percentages": json.loads(row[3]) if row[3] else {},
        "distinct_counts": json.loads(row[4]) if row[4] else {},
        "profiled_at": str(row[5]) if row[5] else None,
    }


# --- Assertions ---


@router.get("/api/assertions")
def get_assertions(
    request: Request, conn: DbConnReadOnly, limit: int = 100
) -> list[dict]:
    """Get recent data quality assertion results."""
    _require_permission(request, "read")
    ensure_meta_table(conn)
    rows = conn.execute(
        """
        SELECT model_path, expression, passed, detail, checked_at
        FROM _dp_internal.assertion_results
        ORDER BY checked_at DESC
        LIMIT ?
        """,
        [limit],
    ).fetchall()
    return [
        {
            "model": r[0],
            "expression": r[1],
            "passed": r[2],
            "detail": r[3],
            "checked_at": str(r[4]) if r[4] else None,
        }
        for r in rows
    ]


@router.get("/api/assertions/{model_name}")
def get_model_assertions(
    request: Request, model_name: str, conn: DbConnReadOnly
) -> list[dict]:
    """Get assertion results for a specific model."""
    _require_permission(request, "read")
    ensure_meta_table(conn)
    rows = conn.execute(
        """
        SELECT model_path, expression, passed, detail, checked_at
        FROM _dp_internal.assertion_results
        WHERE model_path = ?
        ORDER BY checked_at DESC
        LIMIT 50
        """,
        [model_name],
    ).fetchall()
    return [
        {
            "model": r[0],
            "expression": r[1],
            "passed": r[2],
            "detail": r[3],
            "checked_at": str(r[4]) if r[4] else None,
        }
        for r in rows
    ]


# --- Alerts ---


@router.get("/api/alerts")
def get_alert_history(
    request: Request, conn: DbConnReadOnly, limit: int = 50
) -> list[dict]:
    """Get alert history."""
    _require_permission(request, "read")
    ensure_meta_table(conn)
    rows = conn.execute(
        """
        SELECT alert_type, channel, target, message, status, sent_at, error
        FROM _dp_internal.alert_log
        ORDER BY sent_at DESC
        LIMIT ?
        """,
        [limit],
    ).fetchall()
    return [
        {
            "alert_type": r[0],
            "channel": r[1],
            "target": r[2],
            "message": r[3],
            "status": r[4],
            "sent_at": str(r[5]) if r[5] else None,
            "error": r[6],
        }
        for r in rows
    ]


@router.post("/api/alerts/test")
def test_alert(request: Request, req: TestAlertRequest) -> dict:
    """Send a test alert to verify configuration."""
    _require_permission(request, "execute")
    from dp.engine.alerts import Alert, AlertConfig, send_alert

    config = AlertConfig(
        slack_webhook_url=req.slack_webhook_url,
        webhook_url=req.webhook_url,
        channels=[req.channel],
    )
    alert = Alert(
        alert_type="test",
        target="dp_test",
        message="This is a test alert from dp. If you see this, alerts are working!",
        details={"source": "dp alerts test"},
    )
    results = send_alert(alert, config)
    if results and results[0].get("status") == "sent":
        return {"status": "sent", "channel": req.channel}
    error = (
        results[0].get("error", "Unknown error")
        if results
        else "No channels configured"
    )
    raise HTTPException(400, f"Alert test failed: {error}")


# --- Data Contracts ---


@router.get("/api/contracts")
def list_contracts(request: Request) -> list[dict]:
    """List all discovered contracts."""
    _require_permission(request, "read")
    from dp.engine.contracts import discover_contracts

    contracts_dir = _get_project_dir() / "contracts"
    contracts = discover_contracts(contracts_dir)
    return [
        {
            "name": c.name,
            "model": c.model,
            "description": c.description,
            "severity": c.severity,
            "assertions": c.assertions,
            "path": str(c.path) if c.path else None,
        }
        for c in contracts
    ]


@router.post("/api/contracts/run")
def run_contracts_endpoint(request: Request, conn: DbConn) -> dict:
    """Run all data contracts and return results."""
    _require_permission(request, "read")
    from dp.engine.contracts import run_contracts

    contracts_dir = _get_project_dir() / "contracts"
    results = run_contracts(conn, contracts_dir)
    return {
        "total": len(results),
        "passed": sum(1 for r in results if r.passed),
        "failed": sum(1 for r in results if not r.passed),
        "results": [
            {
                "contract_name": r.contract_name,
                "model": r.model,
                "passed": r.passed,
                "severity": r.severity,
                "duration_ms": r.duration_ms,
                "error": r.error,
                "assertions": r.results,
            }
            for r in results
        ],
    }


@router.get("/api/contracts/history")
def get_contracts_history(
    request: Request, conn: DbConnReadOnly
) -> list[dict]:
    """Get recent contract evaluation history."""
    _require_permission(request, "read")
    from dp.engine.contracts import get_contract_history

    return get_contract_history(conn, limit=100)
