"""DAG visualization and full DAG endpoints."""

from __future__ import annotations

import re
from pathlib import Path

from fastapi import APIRouter, Request

from dp.server.deps import (
    _discover_models_cached,
    _get_config,
    _get_project_dir,
    _require_permission,
    build_dag,
    connect,
)

router = APIRouter()


def _scan_ingest_targets(project_dir: Path) -> dict[str, list[str]]:
    """Scan ingest scripts for tables they create (schema.table patterns)."""
    ingest_dir = project_dir / "ingest"
    if not ingest_dir.is_dir():
        return {}

    pattern = re.compile(
        r"(?:CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+|INTO\s+)"
        r"(\w+\.\w+)",
        re.IGNORECASE,
    )

    targets: dict[str, list[str]] = {}
    files = sorted(
        list(ingest_dir.glob("*.py")) + list(ingest_dir.glob("*.dpnb")),
        key=lambda p: p.name,
    )
    for script_file in files:
        if script_file.name.startswith("_"):
            continue
        try:
            text = script_file.read_text()
        except Exception:
            continue
        for match in pattern.finditer(text):
            table_ref = match.group(1).lower()
            rel_path = str(script_file.relative_to(project_dir))
            if table_ref not in targets:
                targets[table_ref] = []
            if rel_path not in targets[table_ref]:
                targets[table_ref].append(rel_path)

    return targets


def _scan_import_sources(project_dir: Path) -> dict[str, str]:
    """Query run_log for the most recent successful import per table."""
    from dp.server.deps import _get_db_path
    db_path = _get_db_path()
    conn = connect(db_path)
    try:
        result = conn.execute(
            """
            SELECT DISTINCT ON (target) target, log_output
            FROM _dp_internal.run_log
            WHERE run_type = 'import' AND status = 'success'
            ORDER BY target, started_at DESC
        """
        ).fetchall()
        return {
            row[0]: row[1] if row[1] else row[0].split(".")[-1] for row in result
        }
    except Exception:
        return {}
    finally:
        conn.close()


@router.get("/api/dag")
def get_dag(request: Request) -> dict:
    """Get the model DAG for visualization."""
    _require_permission(request, "read")
    project_dir = _get_project_dir()
    transform_dir = project_dir / "transform"
    models = _discover_models_cached(transform_dir)
    ordered = build_dag(models)

    nodes = []
    edges = []
    model_set = {m.full_name for m in models}

    ingest_targets = _scan_ingest_targets(project_dir)
    import_sources = _scan_import_sources(project_dir)

    external_deps: set[str] = set()
    for m in models:
        for dep in m.depends_on:
            if dep not in model_set:
                external_deps.add(dep)

    added_scripts: set[str] = set()
    for dep in sorted(external_deps):
        for script_path in ingest_targets.get(dep, []):
            script_id = f"script:{script_path}"
            if script_id not in added_scripts:
                added_scripts.add(script_id)
                nodes.append(
                    {
                        "id": script_id,
                        "label": Path(script_path).name,
                        "schema": "ingest",
                        "type": "ingest",
                        "path": script_path,
                    }
                )
            edges.append({"source": script_id, "target": dep})

    added_imports: set[str] = set()
    for dep in sorted(external_deps):
        if dep in import_sources and dep not in ingest_targets:
            source_file = import_sources[dep]
            import_id = f"import:{dep}"
            if import_id not in added_imports:
                added_imports.add(import_id)
                nodes.append(
                    {
                        "id": import_id,
                        "label": source_file,
                        "schema": "import",
                        "type": "import",
                        "source_file": source_file,
                    }
                )
            edges.append({"source": import_id, "target": dep})

    for dep in sorted(external_deps):
        schema = dep.split(".")[0] if "." in dep else "source"
        nodes.append(
            {
                "id": dep,
                "label": dep,
                "schema": schema,
                "type": "source",
            }
        )

    for m in ordered:
        nodes.append(
            {
                "id": m.full_name,
                "label": m.path.name,
                "schema": m.schema,
                "type": m.materialized,
                "path": str(m.path.relative_to(project_dir)),
            }
        )

    for m in models:
        for dep in m.depends_on:
            edges.append({"source": dep, "target": m.full_name})

    return {"nodes": nodes, "edges": edges}


@router.get("/api/dag/full")
def get_full_dag(request: Request) -> dict:
    """Get the full DAG including seeds, sources, and exposures."""
    _require_permission(request, "read")
    project_dir = _get_project_dir()
    transform_dir = project_dir / "transform"
    models = _discover_models_cached(transform_dir)
    ordered = build_dag(models)
    config = _get_config()

    nodes = []
    edges = []
    model_set = {m.full_name for m in models}

    source_tables: set[str] = set()
    for src in config.sources:
        for tbl in src.tables:
            full_name = f"{src.schema}.{tbl.name}"
            source_tables.add(full_name)
            nodes.append(
                {
                    "id": full_name,
                    "label": tbl.name,
                    "schema": src.schema,
                    "type": "source",
                    "description": tbl.description or src.description,
                }
            )

    from dp.engine.seeds import discover_seeds

    seeds_dir = project_dir / "seeds"
    seeds = discover_seeds(seeds_dir)
    seed_set: set[str] = set()
    for s in seeds:
        seed_set.add(s["full_name"])
        nodes.append(
            {
                "id": s["full_name"],
                "label": s["name"],
                "schema": s["schema"],
                "type": "seed",
            }
        )

    ingest_targets = _scan_ingest_targets(project_dir)
    external_deps: set[str] = set()
    for m in models:
        for dep in m.depends_on:
            if dep not in model_set and dep not in source_tables and dep not in seed_set:
                external_deps.add(dep)

    for dep in sorted(external_deps):
        for script_path in ingest_targets.get(dep, []):
            script_id = f"script:{script_path}"
            nodes.append(
                {
                    "id": script_id,
                    "label": Path(script_path).name,
                    "schema": "ingest",
                    "type": "ingest",
                    "path": script_path,
                }
            )
            edges.append({"source": script_id, "target": dep})

        if dep not in source_tables and dep not in seed_set:
            schema = dep.split(".")[0] if "." in dep else "source"
            nodes.append(
                {
                    "id": dep,
                    "label": dep,
                    "schema": schema,
                    "type": "source",
                }
            )

    for m in ordered:
        nodes.append(
            {
                "id": m.full_name,
                "label": m.path.name,
                "schema": m.schema,
                "type": m.materialized,
                "path": str(m.path.relative_to(project_dir)),
            }
        )

    for m in models:
        for dep in m.depends_on:
            edges.append({"source": dep, "target": m.full_name})

    for exp in config.exposures:
        exp_id = f"exposure:{exp.name}"
        nodes.append(
            {
                "id": exp_id,
                "label": exp.name,
                "schema": "exposure",
                "type": "exposure",
                "description": exp.description,
                "owner": exp.owner,
            }
        )
        for dep in exp.depends_on:
            edges.append({"source": dep, "target": exp_id})

    return {"nodes": nodes, "edges": edges}
