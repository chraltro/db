"""Schema Sentinel API endpoints: check, diffs, impacts, history, apply-fix."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from havn.server.deps import (
    DbConn,
    DbConnReadOnly,
    _get_config,
    _get_project_dir,
    _require_permission,
)

logger = logging.getLogger("havn.server.sentinel")

router = APIRouter()


# --- Pydantic request models ---


class ApplyFixRequest(BaseModel):
    model_path: str = Field(..., min_length=1)
    old_name: str = Field(..., min_length=1)
    new_name: str = Field(..., min_length=1)


class ResolveRequest(BaseModel):
    diff_id: str = Field(..., min_length=1)
    model_name: str = Field(..., min_length=1)


# --- Endpoints ---


@router.post("/api/sentinel/check")
def run_check(request: Request, conn: DbConn) -> dict:
    """Run schema sentinel check on all source tables.

    Returns any schema diffs detected with their impact analysis.
    """
    _require_permission(request, "read")
    from havn.engine.sentinel import (
        SentinelConfig,
        get_impacts_for_diff,
        get_source_names_from_models,
        run_sentinel_check,
    )

    project_dir = _get_project_dir()
    config = _get_config()

    source_names = get_source_names_from_models(project_dir)

    # Filter to existing sources
    existing = []
    for sn in source_names:
        parts = sn.split(".")
        if len(parts) == 2:
            try:
                exists = conn.execute(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema = ? AND table_name = ?",
                    [parts[0], parts[1]],
                ).fetchone()[0]
                if exists:
                    existing.append(sn)
            except Exception:
                pass

    if not existing:
        return {"diffs": [], "sources_checked": 0}

    sc = SentinelConfig(
        enabled=config.sentinel.enabled,
        on_change=config.sentinel.on_change,
        track_ordering=config.sentinel.track_ordering,
        rename_inference=config.sentinel.rename_inference,
        auto_fix=config.sentinel.auto_fix,
        select_star_warning=config.sentinel.select_star_warning,
    )

    diffs = run_sentinel_check(project_dir, conn, existing, config=sc)

    result_diffs = []
    for diff in diffs:
        impacts = get_impacts_for_diff(project_dir, diff.diff_id)
        result_diffs.append({
            "diff_id": diff.diff_id,
            "source_name": diff.source_name,
            "has_breaking": diff.has_breaking,
            "changes": [c.to_dict() for c in diff.changes],
            "impacts": impacts,
        })

    return {
        "diffs": result_diffs,
        "sources_checked": len(existing),
    }


@router.get("/api/sentinel/diffs")
def get_diffs(request: Request, limit: int = 50) -> list[dict]:
    """Get recent schema diffs."""
    _require_permission(request, "read")
    from havn.engine.sentinel import get_recent_diffs

    project_dir = _get_project_dir()
    return get_recent_diffs(project_dir, limit=limit)


@router.get("/api/sentinel/impacts/{diff_id}")
def get_impacts(request: Request, diff_id: str) -> list[dict]:
    """Get impact analysis for a specific diff."""
    _require_permission(request, "read")
    from havn.engine.sentinel import get_impacts_for_diff

    project_dir = _get_project_dir()
    return get_impacts_for_diff(project_dir, diff_id)


@router.get("/api/sentinel/history/{source_name:path}")
def get_history(request: Request, source_name: str, limit: int = 20) -> list[dict]:
    """Get schema snapshot history for a source."""
    _require_permission(request, "read")
    from havn.engine.sentinel import get_schema_history

    project_dir = _get_project_dir()
    return get_schema_history(project_dir, source_name, limit=limit)


@router.get("/api/sentinel/sources")
def get_sources(request: Request, conn: DbConnReadOnly) -> list[dict]:
    """Get all source tables tracked by sentinel."""
    _require_permission(request, "read")
    from havn.engine.sentinel import get_source_names_from_models

    project_dir = _get_project_dir()
    source_names = get_source_names_from_models(project_dir)

    sources = []
    for sn in source_names:
        parts = sn.split(".")
        exists = False
        if len(parts) == 2:
            try:
                exists = bool(conn.execute(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema = ? AND table_name = ?",
                    [parts[0], parts[1]],
                ).fetchone()[0])
            except Exception:
                pass
        sources.append({"name": sn, "exists": exists})

    return sources


@router.post("/api/sentinel/apply-fix")
def apply_fix(request: Request, req: ApplyFixRequest) -> dict:
    """Apply a rename fix to a model SQL file."""
    _require_permission(request, "write")
    from havn.engine.sentinel import apply_rename_fix

    project_dir = _get_project_dir()
    result = apply_rename_fix(project_dir, req.model_path, req.old_name, req.new_name)
    if result["status"] == "error":
        raise HTTPException(400, result["message"])
    return result


@router.post("/api/sentinel/resolve")
def resolve(request: Request, req: ResolveRequest) -> dict:
    """Mark an impact as resolved/dismissed."""
    _require_permission(request, "write")
    from havn.engine.sentinel import resolve_impact

    project_dir = _get_project_dir()
    ok = resolve_impact(project_dir, req.diff_id, req.model_name)
    if not ok:
        raise HTTPException(400, "Failed to resolve impact")
    return {"status": "resolved"}
