"""Notebook management and execution endpoints."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from dp.server.deps import (
    DbConn,
    DbConnReadOnlyOptional,
    _discover_models_cached,
    _get_project_dir,
    _require_permission,
    _serialize,
    build_dag,
    discover_models,
    ensure_meta_table,
)

router = APIRouter()


# --- Notebook namespace management ---

_NOTEBOOK_NS_MAX = 50
_notebook_namespaces: dict[str, dict] = {}


def _notebook_ns_set(name: str, ns: dict) -> None:
    """Store a notebook namespace, evicting oldest if at capacity."""
    _notebook_namespaces[name] = ns
    while len(_notebook_namespaces) > _NOTEBOOK_NS_MAX:
        oldest = next(iter(_notebook_namespaces))
        del _notebook_namespaces[oldest]


def _resolve_notebook(project_dir: Path, name: str) -> Path:
    """Resolve a notebook name or path to a file path.

    Validates the resolved path stays within the project directory
    to prevent path traversal attacks.
    """
    if "/" in name or name.endswith(".dpnb"):
        candidate = project_dir / name
        if not candidate.suffix:
            candidate = candidate.with_suffix(".dpnb")
        if not candidate.resolve().is_relative_to(project_dir.resolve()):
            raise HTTPException(400, "Invalid notebook path")
        if candidate.exists():
            return candidate
    nb_path = project_dir / "notebooks" / f"{name}.dpnb"
    if not nb_path.resolve().is_relative_to(project_dir.resolve()):
        raise HTTPException(400, "Invalid notebook path")
    return nb_path


# --- Pydantic models ---


class SaveNotebookRequest(BaseModel):
    notebook: dict


class NotebookRunCellRequest(BaseModel):
    source: str = Field(..., max_length=1_000_000)
    cell_type: str = Field(default="code", pattern=r"^(code|sql|ingest)$")
    cell_id: str | None = Field(default=None, max_length=200)
    reset: bool = False


class PromoteToModelRequest(BaseModel):
    model_config = {"protected_namespaces": ()}

    sql_source: str = Field(..., min_length=1, max_length=1_000_000)
    model_name: str = Field(
        ..., min_length=1, max_length=200, pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$"
    )
    target_schema: str = Field(
        default="bronze",
        min_length=1,
        max_length=100,
        pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$",
    )
    description: str = Field(default="", max_length=1000)
    overwrite: bool = Field(default=False)


# --- Notebook endpoints ---


@router.get("/api/notebooks")
def list_notebooks(request: Request) -> list[dict]:
    """List all .dpnb notebooks in the project."""
    _require_permission(request, "read")
    project_dir = _get_project_dir()
    notebooks = []
    for f in sorted(project_dir.rglob("*.dpnb")):
        rel = str(f.relative_to(project_dir)).replace("\\", "/")
        try:
            data = json.loads(f.read_text())
            notebooks.append(
                {
                    "name": f.stem,
                    "path": rel,
                    "title": data.get("title", f.stem),
                    "cells": len(data.get("cells", [])),
                }
            )
        except Exception:
            notebooks.append(
                {"name": f.stem, "path": rel, "title": f.stem, "cells": 0}
            )
    return notebooks


@router.get("/api/notebooks/open/{name:path}")
def get_notebook(request: Request, name: str) -> dict:
    """Get a notebook's contents."""
    _require_permission(request, "read")
    from dp.engine.notebook import load_notebook

    nb_path = _resolve_notebook(_get_project_dir(), name)
    if not nb_path.exists():
        raise HTTPException(404, f"Notebook '{name}' not found")
    return load_notebook(nb_path)


@router.post("/api/notebooks/save/{name:path}")
def save_notebook_endpoint(
    request: Request, name: str, req: SaveNotebookRequest
) -> dict:
    """Save a notebook."""
    _require_permission(request, "write")
    from dp.engine.notebook import save_notebook

    nb_path = _resolve_notebook(_get_project_dir(), name)
    save_notebook(nb_path, req.notebook)
    return {"status": "saved", "name": name}


@router.post("/api/notebooks/create/{name}")
def create_notebook_endpoint(request: Request, name: str, title: str = "") -> dict:
    """Create a new notebook."""
    _require_permission(request, "write")
    from dp.engine.notebook import create_notebook, save_notebook

    nb_path = _get_project_dir() / "notebooks" / f"{name}.dpnb"
    if nb_path.exists():
        raise HTTPException(400, f"Notebook '{name}' already exists")
    nb = create_notebook(title or name)
    save_notebook(nb_path, nb)
    return nb


@router.post("/api/notebooks/run/{name:path}")
def run_notebook_endpoint(request: Request, name: str, conn: DbConn) -> dict:
    """Execute all cells in a notebook (code, sql, and ingest)."""
    _require_permission(request, "execute")
    from dp.engine.notebook import load_notebook, run_notebook, save_notebook

    nb_path = _resolve_notebook(_get_project_dir(), name)
    if not nb_path.exists():
        raise HTTPException(404, f"Notebook '{name}' not found")
    nb = load_notebook(nb_path)
    result = run_notebook(conn, nb, project_dir=_get_project_dir())
    save_notebook(nb_path, result)
    return result


@router.post("/api/notebooks/run-cell/{name:path}")
def run_cell_endpoint(
    request: Request, name: str, req: NotebookRunCellRequest, conn: DbConn
) -> dict:
    """Execute a single notebook cell (code, sql, or ingest).

    Namespaces are persisted per notebook so variables defined in one cell
    are available in subsequent cells. Send reset=true to clear the namespace
    (e.g. at the start of Run All).
    """
    _require_permission(request, "execute")
    if req.reset:
        _notebook_namespaces.pop(name, None)
    if req.cell_type == "sql":
        from dp.engine.notebook import execute_sql_cell

        result = execute_sql_cell(conn, req.source)
        return {
            "outputs": result["outputs"],
            "duration_ms": result["duration_ms"],
            "config": result.get("config", {}),
        }
    elif req.cell_type == "ingest":
        from dp.engine.notebook import execute_ingest_cell

        result = execute_ingest_cell(conn, req.source, _get_project_dir())
        return {"outputs": result["outputs"], "duration_ms": result["duration_ms"]}
    else:
        from dp.engine.notebook import execute_cell

        namespace = _notebook_namespaces.get(name)
        result = execute_cell(conn, req.source, namespace)
        _notebook_ns_set(name, result["namespace"])
        return {"outputs": result["outputs"], "duration_ms": result["duration_ms"]}


# --- Promote to model ---


@router.post("/api/notebooks/promote-to-model")
def promote_to_model_endpoint(request: Request, req: PromoteToModelRequest) -> dict:
    """Promote a SQL cell from a notebook to a transform model file."""
    _require_permission(request, "write")
    from dp.engine.notebook import promote_sql_to_model

    project_dir = _get_project_dir()
    transform_dir = project_dir / "transform"

    try:
        model_path = promote_sql_to_model(
            sql_source=req.sql_source,
            model_name=req.model_name,
            schema=req.target_schema,
            transform_dir=transform_dir,
            description=req.description,
            overwrite=req.overwrite,
        )
        rel_path = str(model_path.relative_to(project_dir))

        validation_warnings = []
        try:
            models = discover_models(transform_dir)
            build_dag(models)
        except Exception as e:
            validation_warnings.append(f"DAG validation warning: {e}")

        return {
            "status": "created",
            "path": rel_path,
            "full_name": f"{req.target_schema}.{req.model_name}",
            "validation_warnings": validation_warnings,
        }
    except FileExistsError as e:
        raise HTTPException(409, str(e))
    except Exception as e:
        raise HTTPException(400, f"Failed to promote: {e}")


# --- Model to notebook ---


@router.post("/api/notebooks/model-to-notebook/{model_name:path}")
def model_to_notebook_endpoint(
    request: Request, model_name: str, conn: DbConn
) -> dict:
    """Create a notebook from a transform model for interactive debugging."""
    _require_permission(request, "write")
    from dp.engine.notebook import model_to_notebook, save_notebook

    project_dir = _get_project_dir()
    transform_dir = project_dir / "transform"
    try:
        nb = model_to_notebook(
            conn, model_name, transform_dir, project_dir / "notebooks"
        )
        safe_name = model_name.replace(".", "_")
        nb_path = project_dir / "notebooks" / f"debug_{safe_name}.dpnb"
        save_notebook(nb_path, nb)
        return {
            "status": "created",
            "path": str(nb_path.relative_to(project_dir)),
            "notebook": nb,
        }
    except ValueError as e:
        raise HTTPException(404, str(e))


# --- Debug notebook ---


@router.post("/api/notebooks/debug/{model_name:path}")
def debug_notebook_endpoint(
    request: Request, model_name: str, conn: DbConn
) -> dict:
    """Generate a debug notebook for a failed model."""
    _require_permission(request, "write")
    from dp.engine.notebook import generate_debug_notebook, save_notebook

    project_dir = _get_project_dir()
    transform_dir = project_dir / "transform"
    error_message = None
    assertion_failures = None
    try:
        ensure_meta_table(conn)
        row = conn.execute(
            "SELECT error FROM _dp_internal.run_log "
            "WHERE target = ? AND status IN ('error', 'assertion_failed') "
            "ORDER BY started_at DESC LIMIT 1",
            [model_name],
        ).fetchone()
        if row and row[0]:
            error_message = row[0]

        assertion_rows = conn.execute(
            "SELECT expression, detail FROM _dp_internal.assertion_results "
            "WHERE model_path = ? AND passed = false "
            "ORDER BY checked_at DESC LIMIT 10",
            [model_name],
        ).fetchall()
        if assertion_rows:
            assertion_failures = [
                {"expression": r[0], "detail": r[1]} for r in assertion_rows
            ]
    except Exception:
        pass

    try:
        nb = generate_debug_notebook(
            conn,
            model_name,
            transform_dir,
            error_message=error_message,
            assertion_failures=assertion_failures,
        )
        safe_name = model_name.replace(".", "_")
        nb_path = project_dir / "notebooks" / f"debug_{safe_name}.dpnb"
        save_notebook(nb_path, nb)
        return {
            "status": "created",
            "path": str(nb_path.relative_to(project_dir)),
            "notebook": nb,
        }
    except ValueError as e:
        raise HTTPException(404, str(e))
