"""Pipeline Rewind API endpoints: runs, snapshots, restore, GC."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from havn.server.deps import (
    DbConn,
    _get_config,
    _get_project_dir,
    _require_permission,
)

logger = logging.getLogger("havn.server.rewind")

router = APIRouter()


# --- Pydantic models ---


class RestoreRequest(BaseModel):
    run_id: str = Field(..., min_length=1)
    model_name: str = Field(..., min_length=1)
    cascade: bool = True


# --- Endpoints ---


@router.get("/api/rewind/runs")
def get_rewind_runs(request: Request, limit: int = 100) -> list[dict]:
    """Get pipeline runs for the rewind slider."""
    _require_permission(request, "read")
    from havn.engine.snapshots import get_runs

    project_dir = _get_project_dir()
    runs = get_runs(project_dir, limit=limit)
    return [
        {
            "run_id": r.run_id,
            "started_at": r.started_at,
            "finished_at": r.finished_at,
            "status": r.status,
            "trigger": r.trigger,
            "models_run": r.models_run,
        }
        for r in runs
    ]


@router.get("/api/rewind/snapshots")
def get_rewind_snapshots(request: Request, limit: int = 5000) -> list[dict]:
    """Get all snapshot metadata for the DAG time slider.

    Returns all snapshots in one batch for fast client-side rendering.
    """
    _require_permission(request, "read")
    from havn.engine.snapshots import get_all_snapshots

    project_dir = _get_project_dir()
    snapshots = get_all_snapshots(project_dir, limit=limit)
    return [
        {
            "run_id": s.run_id,
            "model_name": s.model_name,
            "row_count": s.row_count,
            "col_count": s.col_count,
            "schema_hash": s.schema_hash,
            "size_bytes": s.size_bytes,
            "checksum": s.checksum,
            "file_path": s.file_path,
            "created_at": s.created_at,
        }
        for s in snapshots
    ]


@router.get("/api/rewind/snapshots/{run_id}")
def get_run_snapshots(request: Request, run_id: str) -> list[dict]:
    """Get snapshots for a specific run."""
    _require_permission(request, "read")
    from havn.engine.snapshots import get_snapshots_for_run

    project_dir = _get_project_dir()
    snapshots = get_snapshots_for_run(project_dir, run_id)
    return [
        {
            "run_id": s.run_id,
            "model_name": s.model_name,
            "row_count": s.row_count,
            "col_count": s.col_count,
            "schema_hash": s.schema_hash,
            "size_bytes": s.size_bytes,
            "checksum": s.checksum,
            "file_path": s.file_path,
            "created_at": s.created_at,
        }
        for s in snapshots
    ]


@router.get("/api/rewind/sample/{run_id}/{model_name:path}")
def get_snapshot_sample_endpoint(
    request: Request, run_id: str, model_name: str, limit: int = 100
) -> dict:
    """Preview data from a snapshot."""
    _require_permission(request, "read")
    from havn.engine.snapshots import get_snapshot_sample

    project_dir = _get_project_dir()
    result = get_snapshot_sample(project_dir, run_id, model_name, limit=limit)
    if result.get("error"):
        raise HTTPException(404, result["error"])
    return result


@router.post("/api/rewind/restore")
def restore_endpoint(request: Request, req: RestoreRequest, conn: DbConn) -> dict:
    """Restore a model from a snapshot, optionally with downstream cascade."""
    _require_permission(request, "execute")
    from havn.engine.snapshots import restore_snapshot, restore_with_cascade

    project_dir = _get_project_dir()
    config = _get_config()
    transform_dir = project_dir / "transform"
    db_path = project_dir / config.database.path

    if req.cascade:
        result = restore_with_cascade(
            project_dir, conn, req.run_id, req.model_name, transform_dir,
            db_path=str(db_path),
        )
    else:
        result = restore_snapshot(project_dir, conn, req.run_id, req.model_name)

    if result["status"] == "error":
        raise HTTPException(400, result["message"])

    return result


@router.get("/api/rewind/downstream/{model_name:path}")
def get_downstream_endpoint(request: Request, model_name: str) -> dict:
    """Get downstream models for a given model."""
    _require_permission(request, "read")
    from havn.engine.snapshots import get_downstream_models

    project_dir = _get_project_dir()
    transform_dir = project_dir / "transform"
    downstream = get_downstream_models(model_name, transform_dir)
    return {"model": model_name, "downstream": downstream}


@router.post("/api/rewind/gc")
def run_gc_endpoint(request: Request) -> dict:
    """Run garbage collection on expired snapshots."""
    _require_permission(request, "execute")
    from havn.engine.snapshots import RewindConfig, run_gc

    project_dir = _get_project_dir()
    config = _get_config()
    rw_cfg = RewindConfig(
        enabled=config.rewind.enabled,
        retention=config.rewind.retention,
        max_storage=config.rewind.max_storage,
        dedup=config.rewind.dedup,
        exclude=config.rewind.exclude,
    )
    deleted = run_gc(project_dir, rw_cfg)
    return {"deleted": deleted}
