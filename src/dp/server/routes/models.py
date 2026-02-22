"""SQL model management, transform pipeline, lineage, and documentation endpoints."""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from dp.server.deps import (
    DbConn,
    DbConnReadOnly,
    DbConnReadOnlyOptional,
    _discover_models_cached,
    _get_config,
    _get_project_dir,
    _require_permission,
    _serialize,
    build_dag,
    discover_models,
    ensure_meta_table,
    run_transform,
)

logger = logging.getLogger("dp.server")

router = APIRouter()


# --- Pydantic models ---


class TransformRequest(BaseModel):
    targets: list[str] | None = Field(default=None, max_length=500)
    force: bool = False


class DiffRequest(BaseModel):
    targets: list[str] | None = Field(default=None)
    target_schema: str | None = Field(default=None, max_length=100)
    full: bool = False


class CreateModelRequest(BaseModel):
    name: str = Field(
        ..., min_length=1, max_length=200, pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$"
    )
    schema_name: str = Field(
        default="bronze", pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$"
    )
    materialized: str = Field(default="table", pattern=r"^(table|view)$")
    sql: str = Field(default="", max_length=1_000_000)


# --- Model list ---


@router.get("/api/models")
def list_models(request: Request) -> list[dict]:
    """List all SQL transformation models."""
    _require_permission(request, "read")
    transform_dir = _get_project_dir() / "transform"
    models = _discover_models_cached(transform_dir)
    return [
        {
            "name": m.name,
            "schema": m.schema,
            "full_name": m.full_name,
            "materialized": m.materialized,
            "depends_on": m.depends_on,
            "path": str(m.path.relative_to(_get_project_dir())),
            "content_hash": m.content_hash,
        }
        for m in models
    ]


# --- Transform ---


@router.post("/api/transform")
def run_transform_endpoint(
    request: Request, req: TransformRequest, conn: DbConn
) -> dict:
    """Run the SQL transformation pipeline."""
    _require_permission(request, "execute")
    logger.info("Transform requested: targets=%s force=%s", req.targets, req.force)
    try:
        results = run_transform(
            conn,
            _get_project_dir() / "transform",
            targets=req.targets,
            force=req.force,
        )
        return {"results": results}
    except Exception as e:
        logger.exception("Transform failed")
        raise HTTPException(400, f"Transform failed: {e}")


# --- Diff ---


@router.post("/api/diff")
def run_diff_endpoint(request: Request, req: DiffRequest, conn: DbConn) -> list[dict]:
    """Diff models: compare SQL output against materialized tables."""
    _require_permission(request, "read")
    from dp.engine.diff import diff_models

    config = _get_config()
    try:
        ensure_meta_table(conn)
        results = diff_models(
            conn,
            _get_project_dir() / "transform",
            targets=req.targets,
            target_schema=req.target_schema,
            project_config=config,
            full=req.full,
        )
        return [
            {
                "model": r.model,
                "added": r.added,
                "removed": r.removed,
                "modified": r.modified,
                "total_before": r.total_before,
                "total_after": r.total_after,
                "is_new": r.is_new,
                "error": r.error,
                "schema_changes": [
                    {
                        "column": sc.column,
                        "change_type": sc.change_type,
                        "old_type": sc.old_type,
                        "new_type": sc.new_type,
                    }
                    for sc in r.schema_changes
                ],
                "sample_added": r.sample_added,
                "sample_removed": r.sample_removed,
                "sample_modified": r.sample_modified,
            }
            for r in results
        ]
    except Exception as e:
        logger.exception("Diff failed")
        raise HTTPException(400, f"Diff failed: {e}")


# --- Lineage ---


@router.get("/api/lineage/{model_name}")
def get_lineage(
    request: Request, model_name: str, conn: DbConnReadOnlyOptional = None
) -> dict:
    """Get column-level lineage for a model (AST-based via sqlglot)."""
    _require_permission(request, "read")
    from dp.engine.transform import extract_column_lineage

    transform_dir = _get_project_dir() / "transform"
    models = _discover_models_cached(transform_dir)
    model_map = {m.full_name: m for m in models}

    target = model_map.get(model_name)
    if not target:
        matches = [m for m in models if m.name == model_name]
        if matches:
            target = matches[0]
        else:
            raise HTTPException(404, f"Model '{model_name}' not found")

    lineage = extract_column_lineage(target, conn)
    return {
        "model": target.full_name,
        "columns": lineage,
        "depends_on": target.depends_on,
    }


@router.get("/api/lineage")
def get_all_lineage(
    request: Request, conn: DbConnReadOnlyOptional = None
) -> list[dict]:
    """Get column-level lineage for all models."""
    _require_permission(request, "read")
    from dp.engine.transform import extract_column_lineage

    transform_dir = _get_project_dir() / "transform"
    models = _discover_models_cached(transform_dir)

    results = []
    for model in models:
        lineage = extract_column_lineage(model, conn)
        results.append(
            {
                "model": model.full_name,
                "columns": lineage,
                "depends_on": model.depends_on,
            }
        )
    return results


# --- Impact analysis ---


@router.get("/api/impact/{model_name}")
def get_impact(
    request: Request,
    model_name: str,
    conn: DbConnReadOnlyOptional = None,
    column: str | None = None,
) -> dict:
    """Analyze downstream impact of changing a model or column."""
    _require_permission(request, "read")
    from dp.engine.transform import discover_models, impact_analysis

    transform_dir = _get_project_dir() / "transform"
    models = discover_models(transform_dir)
    model_map = {m.full_name: m for m in models}

    if model_name not in model_map:
        matches = [m for m in models if m.name == model_name]
        if matches:
            model_name = matches[0].full_name
        else:
            raise HTTPException(404, f"Model '{model_name}' not found")

    return impact_analysis(models, model_name, column=column, conn=conn)


# --- Docs ---


@router.get("/api/docs/markdown")
def get_docs_markdown(request: Request, conn: DbConnReadOnly) -> dict:
    """Generate markdown documentation."""
    _require_permission(request, "read")
    from dp.engine.docs import generate_docs

    config = _get_config()
    md = generate_docs(
        conn,
        _get_project_dir() / "transform",
        sources=config.sources,
        exposures=config.exposures,
    )
    return {"markdown": md}


@router.get("/api/docs/structured")
def get_docs_structured(request: Request, conn: DbConnReadOnly) -> dict:
    """Generate structured documentation for two-pane UI."""
    _require_permission(request, "read")
    from dp.engine.docs import generate_structured_docs

    return generate_structured_docs(conn, _get_project_dir() / "transform")


# --- Model notebook view ---


@router.get("/api/models/{model_name:path}/notebook-view")
def get_model_notebook_view(
    request: Request, model_name: str, conn: DbConnReadOnlyOptional = None
) -> dict:
    """Get a notebook-style view for a SQL model."""
    _require_permission(request, "read")
    from dp.engine.transform import extract_column_lineage

    transform_dir = _get_project_dir() / "transform"
    models = _discover_models_cached(transform_dir)
    model_map = {m.full_name: m for m in models}

    target = model_map.get(model_name)
    if not target:
        matches = [m for m in models if m.name == model_name]
        if matches:
            target = matches[0]
        else:
            raise HTTPException(404, f"Model '{model_name}' not found")

    sql_source = target.path.read_text()
    rel_path = str(target.path.relative_to(_get_project_dir()))

    sample_data = None
    if conn:
        try:
            quoted = f'"{target.schema}"."{target.name}"'
            result = conn.execute(f"SELECT * FROM {quoted} LIMIT 50")
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            sample_data = {
                "columns": columns,
                "rows": [[_serialize(v) for v in row] for row in rows],
            }
        except Exception:
            sample_data = None

    lineage = None
    try:
        lineage = extract_column_lineage(target, conn)
    except Exception:
        pass

    upstream = target.depends_on
    downstream = [
        m.full_name for m in models if target.full_name in m.depends_on
    ]

    return {
        "model": target.full_name,
        "path": rel_path,
        "sql_source": sql_source,
        "materialized": target.materialized,
        "schema": target.schema,
        "sample_data": sample_data,
        "lineage": lineage,
        "upstream": upstream,
        "downstream": downstream,
    }


# --- Create new model ---


@router.post("/api/models/create")
def create_model_endpoint(request: Request, req: CreateModelRequest) -> dict:
    """Create a new SQL model file."""
    _require_permission(request, "write")
    project_dir = _get_project_dir()
    transform_dir = project_dir / "transform"
    schema_dir = transform_dir / req.schema_name

    if not schema_dir.resolve().is_relative_to(transform_dir.resolve()):
        raise HTTPException(400, "Invalid schema name")

    schema_dir.mkdir(parents=True, exist_ok=True)

    model_path = schema_dir / f"{req.name}.sql"
    if model_path.exists():
        raise HTTPException(
            409, f"Model '{req.schema_name}.{req.name}' already exists"
        )

    sql_content = (
        req.sql
        or f"-- config: materialized={req.materialized}, schema={req.schema_name}\n\nSELECT 1 AS placeholder\n"
    )
    if not sql_content.startswith("-- config:"):
        sql_content = f"-- config: materialized={req.materialized}, schema={req.schema_name}\n\n{sql_content}"

    model_path.write_text(sql_content)

    return {
        "status": "created",
        "path": str(model_path.relative_to(project_dir)),
        "full_name": f"{req.schema_name}.{req.name}",
    }


# --- Compile-time validation ---


@router.post("/api/check")
def run_check(request: Request, conn_opt: DbConnReadOnlyOptional = None) -> dict:
    """Validate all SQL models without executing them."""
    _require_permission(request, "read")
    from dp.engine.seeds import discover_seeds
    from dp.engine.transform import discover_models, validate_models

    project_dir = _get_project_dir()
    transform_dir = project_dir / "transform"
    models = discover_models(transform_dir)
    config = _get_config()

    known_tables: set[str] = set()
    seeds = discover_seeds(project_dir / "seeds")
    for s in seeds:
        known_tables.add(s["full_name"])
    for src in config.sources:
        for t in src.tables:
            known_tables.add(f"{src.schema}.{t.name}")

    source_columns: dict[str, set[str]] = {}
    for src in config.sources:
        for t in src.tables:
            full = f"{src.schema}.{t.name}"
            source_columns[full] = {c.name for c in t.columns}

    conn = conn_opt
    ensure_meta_table(conn) if conn else None
    errors = validate_models(
        conn, models, known_tables=known_tables, source_columns=source_columns
    )
    return {
        "models_checked": len(models),
        "errors": [
            {"model": e.model, "severity": e.severity, "message": e.message}
            for e in errors
        ],
        "passed": not any(e.severity == "error" for e in errors),
    }
