"""FastAPI backend for the web UI."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from dp.config import load_project
from dp.engine.database import connect, ensure_meta_table
from dp.engine.runner import run_script
from dp.engine.transform import discover_models, run_transform

# Set by CLI before starting uvicorn
PROJECT_DIR: Path = Path.cwd()

app = FastAPI(title="dp", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _get_project_dir() -> Path:
    return PROJECT_DIR


def _get_config():
    return load_project(_get_project_dir())


def _get_db_path() -> Path:
    config = _get_config()
    return _get_project_dir() / config.database.path


# --- File browsing ---


class FileInfo(BaseModel):
    name: str
    path: str
    type: str  # "file" or "dir"
    children: list[FileInfo] | None = None


def _scan_dir(base: Path, rel: Path | None = None) -> list[FileInfo]:
    """Scan a directory and return file tree."""
    target = base / rel if rel else base
    if not target.exists():
        return []
    items = []
    for entry in sorted(target.iterdir()):
        if entry.name.startswith(".") or entry.name == "__pycache__":
            continue
        rel_path = str(entry.relative_to(base))
        if entry.is_dir():
            items.append(FileInfo(
                name=entry.name,
                path=rel_path,
                type="dir",
                children=_scan_dir(base, entry.relative_to(base)),
            ))
        elif entry.suffix in (".sql", ".py", ".yml", ".yaml"):
            items.append(FileInfo(name=entry.name, path=rel_path, type="file"))
    return items


@app.get("/api/files")
def list_files() -> list[FileInfo]:
    """List project files."""
    project_dir = _get_project_dir()
    return _scan_dir(project_dir)


@app.get("/api/files/{file_path:path}")
def read_file(file_path: str) -> dict:
    """Read a file's content."""
    full_path = _get_project_dir() / file_path
    if not full_path.exists():
        raise HTTPException(404, f"File not found: {file_path}")
    if not full_path.is_file():
        raise HTTPException(400, "Not a file")
    return {"path": file_path, "content": full_path.read_text(), "language": _detect_language(full_path)}


class SaveFileRequest(BaseModel):
    content: str


@app.put("/api/files/{file_path:path}")
def save_file(file_path: str, req: SaveFileRequest) -> dict:
    """Save a file."""
    full_path = _get_project_dir() / file_path
    if not full_path.exists():
        raise HTTPException(404, f"File not found: {file_path}")
    full_path.write_text(req.content)
    return {"path": file_path, "status": "saved"}


def _detect_language(path: Path) -> str:
    return {"sql": "sql", "py": "python", "yml": "yaml", "yaml": "yaml"}.get(path.suffix.lstrip("."), "text")


# --- Transform ---


@app.get("/api/models")
def list_models() -> list[dict]:
    """List all SQL transformation models."""
    transform_dir = _get_project_dir() / "transform"
    models = discover_models(transform_dir)
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


class TransformRequest(BaseModel):
    targets: list[str] | None = None
    force: bool = False


@app.post("/api/transform")
def run_transform_endpoint(req: TransformRequest) -> dict:
    """Run the SQL transformation pipeline."""
    db_path = _get_db_path()
    conn = connect(db_path)
    try:
        results = run_transform(
            conn,
            _get_project_dir() / "transform",
            targets=req.targets,
            force=req.force,
        )
        return {"results": results}
    finally:
        conn.close()


# --- Script execution ---


class RunScriptRequest(BaseModel):
    script_path: str


@app.post("/api/run")
def run_script_endpoint(req: RunScriptRequest) -> dict:
    """Run an ingest or export script."""
    script_path = _get_project_dir() / req.script_path
    if not script_path.exists():
        raise HTTPException(404, f"Script not found: {req.script_path}")
    script_type = "ingest" if "ingest" in req.script_path else "export"
    db_path = _get_db_path()
    conn = connect(db_path)
    try:
        result = run_script(conn, script_path, script_type)
        return result
    finally:
        conn.close()


# --- Stream execution ---


@app.post("/api/stream/{stream_name}")
def run_stream_endpoint(stream_name: str, force: bool = False) -> dict:
    """Run a full stream."""
    config = _get_config()
    if stream_name not in config.streams:
        raise HTTPException(404, f"Stream '{stream_name}' not found")
    stream_config = config.streams[stream_name]

    db_path = _get_db_path()
    conn = connect(db_path)
    step_results = []
    try:
        from dp.engine.runner import run_scripts_in_dir

        for step in stream_config.steps:
            if step.action == "ingest":
                results = run_scripts_in_dir(conn, _get_project_dir() / "ingest", "ingest", step.targets)
                step_results.append({"action": "ingest", "results": results})
            elif step.action == "transform":
                results = run_transform(
                    conn,
                    _get_project_dir() / "transform",
                    targets=step.targets if step.targets != ["all"] else None,
                    force=force,
                )
                step_results.append({"action": "transform", "results": results})
            elif step.action == "export":
                results = run_scripts_in_dir(conn, _get_project_dir() / "export", "export", step.targets)
                step_results.append({"action": "export", "results": results})
        return {"stream": stream_name, "steps": step_results}
    finally:
        conn.close()


# --- Query ---


class QueryRequest(BaseModel):
    sql: str
    limit: int = 1000


@app.post("/api/query")
def run_query(req: QueryRequest) -> dict:
    """Run an ad-hoc SQL query."""
    db_path = _get_db_path()
    conn = connect(db_path, read_only=True)
    try:
        result = conn.execute(req.sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchmany(req.limit)
        return {
            "columns": columns,
            "rows": [[_serialize(v) for v in row] for row in rows],
            "truncated": len(rows) == req.limit,
        }
    except Exception as e:
        raise HTTPException(400, str(e))
    finally:
        conn.close()


def _serialize(value: Any) -> Any:
    """Make values JSON-serializable."""
    if value is None:
        return None
    if isinstance(value, (int, float, str, bool)):
        return value
    return str(value)


# --- Tables ---


@app.get("/api/tables")
def list_tables(schema: str | None = None) -> list[dict]:
    """List warehouse tables and views."""
    db_path = _get_db_path()
    if not db_path.exists():
        return []
    conn = connect(db_path, read_only=True)
    try:
        sql = """
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', '_dp_internal')
        """
        if schema:
            sql += f" AND table_schema = '{schema}'"
        sql += " ORDER BY table_schema, table_name"
        rows = conn.execute(sql).fetchall()
        return [{"schema": r[0], "name": r[1], "type": r[2]} for r in rows]
    finally:
        conn.close()


@app.get("/api/tables/{schema}/{table}")
def describe_table(schema: str, table: str) -> dict:
    """Get column info for a table."""
    db_path = _get_db_path()
    conn = connect(db_path, read_only=True)
    try:
        cols = conn.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            [schema, table],
        ).fetchall()
        return {
            "schema": schema,
            "name": table,
            "columns": [{"name": c[0], "type": c[1], "nullable": c[2] == "YES"} for c in cols],
        }
    finally:
        conn.close()


# --- Streams config ---


@app.get("/api/streams")
def list_streams() -> dict:
    """List configured streams."""
    config = _get_config()
    return {
        name: {
            "description": s.description,
            "schedule": s.schedule,
            "steps": [{"action": step.action, "targets": step.targets} for step in s.steps],
        }
        for name, s in config.streams.items()
    }


# --- Run history ---


@app.get("/api/history")
def get_history(limit: int = 50) -> list[dict]:
    """Get run history."""
    db_path = _get_db_path()
    if not db_path.exists():
        return []
    conn = connect(db_path)
    ensure_meta_table(conn)
    try:
        rows = conn.execute(
            """
            SELECT run_id, run_type, target, status, started_at, duration_ms, rows_affected, error
            FROM _dp_internal.run_log
            ORDER BY started_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        return [
            {
                "run_id": r[0],
                "run_type": r[1],
                "target": r[2],
                "status": r[3],
                "started_at": str(r[4]) if r[4] else None,
                "duration_ms": r[5],
                "rows_affected": r[6],
                "error": r[7],
            }
            for r in rows
        ]
    finally:
        conn.close()


# --- Lint ---


@app.post("/api/lint")
def lint_endpoint(fix: bool = False) -> dict:
    """Run SQLFluff on transform files."""
    from dp.lint.linter import lint

    config = _get_config()
    count, violations = lint(
        _get_project_dir() / "transform",
        fix=fix,
        dialect=config.lint.dialect,
        rules=config.lint.rules or None,
    )
    return {"count": count, "violations": violations}


# --- Serve frontend ---
# The React build output goes to frontend/dist/ — serve it as static files
# with a fallback to index.html for client-side routing.

_FRONTEND_DIR = Path(__file__).parent.parent.parent.parent / "frontend" / "dist"


@app.get("/", response_class=HTMLResponse)
@app.get("/{path:path}", response_class=HTMLResponse)
def serve_frontend(path: str = "") -> HTMLResponse:
    """Serve the frontend SPA. Falls back to index.html for client-side routing."""
    # Try to serve static file first
    file_path = _FRONTEND_DIR / path
    if file_path.is_file():
        content_type = {
            ".html": "text/html",
            ".js": "application/javascript",
            ".css": "text/css",
            ".svg": "image/svg+xml",
            ".png": "image/png",
            ".ico": "image/x-icon",
        }.get(file_path.suffix, "application/octet-stream")
        return HTMLResponse(content=file_path.read_bytes(), media_type=content_type)

    # Fallback to index.html
    index = _FRONTEND_DIR / "index.html"
    if index.exists():
        return HTMLResponse(content=index.read_text())
    return HTMLResponse(
        content="<h1>dp</h1><p>Frontend not built. Run <code>cd frontend && npm run build</code></p>",
        status_code=200,
    )
