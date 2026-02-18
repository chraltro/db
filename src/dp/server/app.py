"""FastAPI backend for the web UI."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from dp.config import load_project
from dp.engine.database import connect, ensure_meta_table
from dp.engine.runner import run_script
from dp.engine.transform import build_dag, discover_models, run_transform

# Set by CLI before starting uvicorn
PROJECT_DIR: Path = Path.cwd()
AUTH_ENABLED: bool = False  # Set by CLI --auth flag

app = FastAPI(title="dp", version="0.2.0")

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


def _get_user(request: Request) -> dict | None:
    """Extract and validate user from auth header. Returns None if auth disabled."""
    if not AUTH_ENABLED:
        return {"username": "local", "role": "admin", "display_name": "Local User"}

    auth_header = request.headers.get("authorization", "")
    if not auth_header.startswith("Bearer "):
        return None

    token = auth_header[7:]
    db_path = _get_db_path()
    conn = connect(db_path)
    try:
        from dp.engine.auth import validate_token
        return validate_token(conn, token)
    finally:
        conn.close()


def _require_user(request: Request) -> dict:
    """Require authentication. Raises 401 if not authenticated."""
    user = _get_user(request)
    if user is None:
        raise HTTPException(401, "Authentication required")
    return user


def _require_permission(request: Request, permission: str) -> dict:
    """Require a specific permission."""
    user = _require_user(request)
    from dp.engine.auth import has_permission
    if not has_permission(user["role"], permission):
        raise HTTPException(403, f"Permission denied: {permission}")
    return user


# --- Auth endpoints ---


class LoginRequest(BaseModel):
    username: str
    password: str


class CreateUserRequest(BaseModel):
    username: str
    password: str
    role: str = "viewer"
    display_name: str | None = None


class UpdateUserRequest(BaseModel):
    role: str | None = None
    password: str | None = None
    display_name: str | None = None


@app.post("/api/auth/login")
def login(req: LoginRequest) -> dict:
    """Authenticate and get a token."""
    from dp.engine.auth import authenticate
    db_path = _get_db_path()
    conn = connect(db_path)
    try:
        token = authenticate(conn, req.username, req.password)
        if not token:
            raise HTTPException(401, "Invalid credentials")
        return {"token": token, "username": req.username}
    finally:
        conn.close()


@app.get("/api/auth/me")
def get_current_user(request: Request) -> dict:
    """Get current authenticated user."""
    return _require_user(request)


@app.get("/api/auth/status")
def get_auth_status() -> dict:
    """Check if auth is enabled and if initial setup is needed."""
    if not AUTH_ENABLED:
        return {"auth_enabled": False, "needs_setup": False}
    db_path = _get_db_path()
    conn = connect(db_path)
    try:
        from dp.engine.auth import has_any_users
        return {"auth_enabled": True, "needs_setup": not has_any_users(conn)}
    finally:
        conn.close()


@app.post("/api/auth/setup")
def initial_setup(req: CreateUserRequest) -> dict:
    """Create the first admin user (only works when no users exist)."""
    from dp.engine.auth import create_user, has_any_users, authenticate
    db_path = _get_db_path()
    conn = connect(db_path)
    try:
        if has_any_users(conn):
            raise HTTPException(400, "Setup already completed")
        create_user(conn, req.username, req.password, "admin", req.display_name)
        token = authenticate(conn, req.username, req.password)
        return {"token": token, "username": req.username, "role": "admin"}
    finally:
        conn.close()


# --- User management ---


@app.get("/api/users")
def list_users(request: Request) -> list[dict]:
    """List all users (admin only)."""
    _require_permission(request, "manage_users")
    from dp.engine.auth import list_users as _list_users
    db_path = _get_db_path()
    conn = connect(db_path)
    try:
        return _list_users(conn)
    finally:
        conn.close()


@app.post("/api/users")
def create_user_endpoint(request: Request, req: CreateUserRequest) -> dict:
    """Create a new user (admin only)."""
    _require_permission(request, "manage_users")
    from dp.engine.auth import create_user
    db_path = _get_db_path()
    conn = connect(db_path)
    try:
        return create_user(conn, req.username, req.password, req.role, req.display_name)
    except ValueError as e:
        raise HTTPException(400, str(e))
    finally:
        conn.close()


@app.put("/api/users/{username}")
def update_user_endpoint(request: Request, username: str, req: UpdateUserRequest) -> dict:
    """Update a user (admin only)."""
    _require_permission(request, "manage_users")
    from dp.engine.auth import update_user
    db_path = _get_db_path()
    conn = connect(db_path)
    try:
        found = update_user(conn, username, req.role, req.password, req.display_name)
        if not found:
            raise HTTPException(404, f"User '{username}' not found")
        return {"status": "updated"}
    except ValueError as e:
        raise HTTPException(400, str(e))
    finally:
        conn.close()


@app.delete("/api/users/{username}")
def delete_user_endpoint(request: Request, username: str) -> dict:
    """Delete a user (admin only)."""
    _require_permission(request, "manage_users")
    from dp.engine.auth import delete_user
    db_path = _get_db_path()
    conn = connect(db_path)
    try:
        found = delete_user(conn, username)
        if not found:
            raise HTTPException(404, f"User '{username}' not found")
        return {"status": "deleted"}
    finally:
        conn.close()


# --- Secrets management ---


@app.get("/api/secrets")
def list_secrets(request: Request) -> list[dict]:
    """List secrets (keys and masked values only)."""
    _require_permission(request, "manage_secrets")
    from dp.engine.secrets import list_secrets as _list_secrets
    return _list_secrets(_get_project_dir())


class SetSecretRequest(BaseModel):
    key: str
    value: str


@app.post("/api/secrets")
def set_secret(request: Request, req: SetSecretRequest) -> dict:
    """Set or update a secret."""
    _require_permission(request, "manage_secrets")
    from dp.engine.secrets import set_secret as _set_secret
    _set_secret(_get_project_dir(), req.key, req.value)
    return {"status": "set", "key": req.key}


@app.delete("/api/secrets/{key}")
def delete_secret(request: Request, key: str) -> dict:
    """Delete a secret."""
    _require_permission(request, "manage_secrets")
    from dp.engine.secrets import delete_secret as _delete_secret
    found = _delete_secret(_get_project_dir(), key)
    if not found:
        raise HTTPException(404, f"Secret '{key}' not found")
    return {"status": "deleted", "key": key}


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
        elif entry.suffix in (".sql", ".py", ".yml", ".yaml", ".dpnb"):
            items.append(FileInfo(name=entry.name, path=rel_path, type="file"))
    return items


@app.get("/api/files")
def list_files(request: Request) -> list[FileInfo]:
    """List project files."""
    _require_permission(request, "read")
    project_dir = _get_project_dir()
    return _scan_dir(project_dir)


@app.get("/api/files/{file_path:path}")
def read_file(request: Request, file_path: str) -> dict:
    """Read a file's content."""
    _require_permission(request, "read")
    full_path = _get_project_dir() / file_path
    if not full_path.exists():
        raise HTTPException(404, f"File not found: {file_path}")
    if not full_path.is_file():
        raise HTTPException(400, "Not a file")
    return {"path": file_path, "content": full_path.read_text(), "language": _detect_language(full_path)}


class SaveFileRequest(BaseModel):
    content: str


@app.put("/api/files/{file_path:path}")
def save_file(request: Request, file_path: str, req: SaveFileRequest) -> dict:
    """Save a file (creates it if it doesn't exist)."""
    _require_permission(request, "write")
    project_dir = _get_project_dir()
    full_path = (project_dir / file_path).resolve()
    # Path traversal protection
    if not str(full_path).startswith(str(project_dir.resolve())):
        raise HTTPException(400, "Invalid file path")
    # Only allow known file extensions
    if full_path.suffix not in (".sql", ".py", ".yml", ".yaml", ".dpnb", ".sqlfluff"):
        raise HTTPException(400, f"Unsupported file type: {full_path.suffix}")
    full_path.parent.mkdir(parents=True, exist_ok=True)
    full_path.write_text(req.content)
    return {"path": file_path, "status": "saved"}


@app.delete("/api/files/{file_path:path}")
def delete_file(request: Request, file_path: str) -> dict:
    """Delete a file."""
    _require_permission(request, "write")
    project_dir = _get_project_dir()
    full_path = (project_dir / file_path).resolve()
    # Path traversal protection
    if not str(full_path).startswith(str(project_dir.resolve())):
        raise HTTPException(400, "Invalid file path")
    if not full_path.exists():
        raise HTTPException(404, f"File not found: {file_path}")
    if not full_path.is_file():
        raise HTTPException(400, "Not a file")
    # Prevent deleting critical files
    if full_path.name in ("project.yml", ".env", ".gitignore"):
        raise HTTPException(400, f"Cannot delete {full_path.name}")
    full_path.unlink()
    # Remove empty parent directories up to project root
    parent = full_path.parent
    while parent != project_dir.resolve() and parent.is_dir() and not any(parent.iterdir()):
        parent.rmdir()
        parent = parent.parent
    return {"path": file_path, "status": "deleted"}


def _detect_language(path: Path) -> str:
    return {
        "sql": "sql", "py": "python", "yml": "yaml", "yaml": "yaml", "dpnb": "json",
    }.get(path.suffix.lstrip("."), "text")


# --- Transform ---


@app.get("/api/models")
def list_models(request: Request) -> list[dict]:
    """List all SQL transformation models."""
    _require_permission(request, "read")
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
def run_transform_endpoint(request: Request, req: TransformRequest) -> dict:
    """Run the SQL transformation pipeline."""
    _require_permission(request, "execute")
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
def run_script_endpoint(request: Request, req: RunScriptRequest) -> dict:
    """Run an ingest or export script."""
    _require_permission(request, "execute")
    script_path = _get_project_dir() / req.script_path
    if not script_path.exists():
        raise HTTPException(404, f"Script not found: {req.script_path}")
    script_type = "ingest" if "ingest" in req.script_path else "export"
    db_path = _get_db_path()
    conn = connect(db_path)
    try:
        result = run_script(conn, script_path, script_type)
        # Mask secrets in output
        from dp.engine.secrets import mask_output
        if result.get("log_output"):
            result["log_output"] = mask_output(result["log_output"], _get_project_dir())
        return result
    finally:
        conn.close()


# --- Stream execution ---


@app.post("/api/stream/{stream_name}")
def run_stream_endpoint(request: Request, stream_name: str, force: bool = False) -> dict:
    """Run a full stream."""
    _require_permission(request, "execute")
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
def run_query(request: Request, req: QueryRequest) -> dict:
    """Run an ad-hoc SQL query."""
    _require_permission(request, "read")
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
def list_tables(request: Request, schema: str | None = None) -> list[dict]:
    """List warehouse tables and views."""
    _require_permission(request, "read")
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
def describe_table(request: Request, schema: str, table: str) -> dict:
    """Get column info for a table."""
    _require_permission(request, "read")
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


@app.get("/api/tables/{schema}/{table}/sample")
def sample_table(request: Request, schema: str, table: str, limit: int = 100) -> dict:
    """Get sample rows from a table."""
    _require_permission(request, "read")
    db_path = _get_db_path()
    conn = connect(db_path, read_only=True)
    try:
        result = conn.execute(f"SELECT * FROM {schema}.{table} LIMIT {limit}")
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return {
            "schema": schema,
            "table": table,
            "columns": columns,
            "rows": [[_serialize(v) for v in row] for row in rows],
        }
    except Exception as e:
        raise HTTPException(400, str(e))
    finally:
        conn.close()


# --- Streams config ---


@app.get("/api/streams")
def list_streams(request: Request) -> dict:
    """List configured streams."""
    _require_permission(request, "read")
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
def get_history(request: Request, limit: int = 50) -> list[dict]:
    """Get run history."""
    _require_permission(request, "read")
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
def lint_endpoint(request: Request, fix: bool = False) -> dict:
    """Run SQLFluff on transform files."""
    _require_permission(request, "execute")
    from dp.lint.linter import lint

    config = _get_config()
    count, violations = lint(
        _get_project_dir() / "transform",
        fix=fix,
        dialect=config.lint.dialect,
        rules=config.lint.rules or None,
    )
    return {"count": count, "violations": violations}


@app.get("/api/lint/config")
def get_lint_config(request: Request) -> dict:
    """Get the .sqlfluff config file contents."""
    _require_permission(request, "read")
    sqlfluff_path = _get_project_dir() / ".sqlfluff"
    if not sqlfluff_path.exists():
        return {"exists": False, "content": ""}
    return {"exists": True, "content": sqlfluff_path.read_text()}


class LintConfigRequest(BaseModel):
    content: str


@app.put("/api/lint/config")
def save_lint_config(request: Request, req: LintConfigRequest) -> dict:
    """Save the .sqlfluff config file."""
    _require_permission(request, "write")
    sqlfluff_path = _get_project_dir() / ".sqlfluff"
    sqlfluff_path.write_text(req.content)
    return {"status": "saved"}


@app.delete("/api/lint/config")
def delete_lint_config(request: Request) -> dict:
    """Delete the .sqlfluff config file (revert to defaults)."""
    _require_permission(request, "write")
    sqlfluff_path = _get_project_dir() / ".sqlfluff"
    if sqlfluff_path.exists():
        sqlfluff_path.unlink()
    return {"status": "deleted"}


# --- DAG ---


def _scan_ingest_targets(project_dir: Path) -> dict[str, list[str]]:
    """Scan ingest scripts for tables they create (schema.table patterns).

    Returns a mapping of fully-qualified table name -> list of script paths.
    """
    import re

    ingest_dir = project_dir / "ingest"
    if not ingest_dir.is_dir():
        return {}

    pattern = re.compile(
        r"(?:CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+|INTO\s+)"
        r"(\w+\.\w+)",
        re.IGNORECASE,
    )

    targets: dict[str, list[str]] = {}
    files = sorted(list(ingest_dir.glob("*.py")) + list(ingest_dir.glob("*.dpnb")), key=lambda p: p.name)
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


@app.get("/api/dag")
def get_dag(request: Request) -> dict:
    """Get the model DAG for visualization."""
    _require_permission(request, "read")
    project_dir = _get_project_dir()
    transform_dir = project_dir / "transform"
    models = discover_models(transform_dir)
    ordered = build_dag(models)

    nodes = []
    edges = []
    model_set = {m.full_name for m in models}

    ingest_targets = _scan_ingest_targets(project_dir)

    external_deps: set[str] = set()
    for m in models:
        for dep in m.depends_on:
            if dep not in model_set:
                external_deps.add(dep)

    # Add ingest script nodes that feed into source tables
    added_scripts: set[str] = set()
    for dep in sorted(external_deps):
        for script_path in ingest_targets.get(dep, []):
            script_id = f"script:{script_path}"
            if script_id not in added_scripts:
                added_scripts.add(script_id)
                nodes.append({
                    "id": script_id,
                    "label": Path(script_path).name,
                    "schema": "ingest",
                    "type": "ingest",
                    "path": script_path,
                })
            edges.append({"source": script_id, "target": dep})

    for dep in sorted(external_deps):
        schema = dep.split(".")[0] if "." in dep else "source"
        nodes.append({
            "id": dep,
            "label": dep,
            "schema": schema,
            "type": "source",
        })

    for m in ordered:
        nodes.append({
            "id": m.full_name,
            "label": m.path.name,
            "schema": m.schema,
            "type": m.materialized,
            "path": str(m.path.relative_to(project_dir)),
        })

    for m in models:
        for dep in m.depends_on:
            edges.append({"source": dep, "target": m.full_name})

    return {"nodes": nodes, "edges": edges}


# --- Docs ---


@app.get("/api/docs/markdown")
def get_docs_markdown(request: Request) -> dict:
    """Generate markdown documentation."""
    _require_permission(request, "read")
    from dp.engine.docs import generate_docs

    db_path = _get_db_path()
    if not db_path.exists():
        return {"markdown": "*No warehouse database found. Run a pipeline first.*"}
    conn = connect(db_path, read_only=True)
    try:
        md = generate_docs(conn, _get_project_dir() / "transform")
        return {"markdown": md}
    finally:
        conn.close()


@app.get("/api/docs/structured")
def get_docs_structured(request: Request) -> dict:
    """Generate structured documentation for two-pane UI."""
    _require_permission(request, "read")
    from dp.engine.docs import generate_structured_docs

    db_path = _get_db_path()
    if not db_path.exists():
        return {"schemas": []}
    conn = connect(db_path, read_only=True)
    try:
        return generate_structured_docs(conn, _get_project_dir() / "transform")
    finally:
        conn.close()


# --- Scheduler status ---


@app.get("/api/scheduler")
def get_scheduler_status(request: Request) -> dict:
    """Get scheduler status and scheduled streams."""
    _require_permission(request, "read")
    from dp.engine.scheduler import get_scheduled_streams

    streams = get_scheduled_streams(_get_project_dir())
    return {"scheduled_streams": streams}


# --- Notebooks ---


class NotebookCellRequest(BaseModel):
    source: str
    cell_id: str | None = None
    reset: bool = False


# Per-notebook namespace store for persistent cell execution
_notebook_namespaces: dict[str, dict] = {}


class SaveNotebookRequest(BaseModel):
    notebook: dict


@app.get("/api/notebooks")
def list_notebooks(request: Request) -> list[dict]:
    """List all .dpnb notebooks in the project."""
    _require_permission(request, "read")
    project_dir = _get_project_dir()
    notebooks = []
    for f in sorted(project_dir.rglob("*.dpnb")):
        rel = str(f.relative_to(project_dir)).replace("\\", "/")
        try:
            data = json.loads(f.read_text())
            notebooks.append({
                "name": f.stem,
                "path": rel,
                "title": data.get("title", f.stem),
                "cells": len(data.get("cells", [])),
            })
        except Exception:
            notebooks.append({"name": f.stem, "path": rel, "title": f.stem, "cells": 0})
    return notebooks


def _resolve_notebook(project_dir: Path, name: str) -> Path:
    """Resolve a notebook name or path to a file path."""
    # Try as a relative path first (e.g. "ingest/earthquakes.dpnb")
    if "/" in name or name.endswith(".dpnb"):
        candidate = project_dir / name
        if not candidate.suffix:
            candidate = candidate.with_suffix(".dpnb")
        if candidate.exists():
            return candidate
    # Fall back to notebooks/ directory
    nb_path = project_dir / "notebooks" / f"{name}.dpnb"
    return nb_path


@app.get("/api/notebooks/open/{name:path}")
def get_notebook(request: Request, name: str) -> dict:
    """Get a notebook's contents."""
    _require_permission(request, "read")
    from dp.engine.notebook import load_notebook
    nb_path = _resolve_notebook(_get_project_dir(), name)
    if not nb_path.exists():
        raise HTTPException(404, f"Notebook '{name}' not found")
    return load_notebook(nb_path)


@app.post("/api/notebooks/save/{name:path}")
def save_notebook_endpoint(request: Request, name: str, req: SaveNotebookRequest) -> dict:
    """Save a notebook."""
    _require_permission(request, "write")
    from dp.engine.notebook import save_notebook
    nb_path = _resolve_notebook(_get_project_dir(), name)
    save_notebook(nb_path, req.notebook)
    return {"status": "saved", "name": name}


@app.post("/api/notebooks/create/{name}")
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


@app.post("/api/notebooks/run/{name:path}")
def run_notebook_endpoint(request: Request, name: str) -> dict:
    """Execute all cells in a notebook."""
    _require_permission(request, "execute")
    from dp.engine.notebook import load_notebook, run_notebook, save_notebook
    nb_path = _resolve_notebook(_get_project_dir(), name)
    if not nb_path.exists():
        raise HTTPException(404, f"Notebook '{name}' not found")
    nb = load_notebook(nb_path)
    db_path = _get_db_path()
    conn = connect(db_path)
    try:
        result = run_notebook(conn, nb)
        save_notebook(nb_path, result)
        return result
    finally:
        conn.close()


@app.post("/api/notebooks/run-cell/{name:path}")
def run_cell_endpoint(request: Request, name: str, req: NotebookCellRequest) -> dict:
    """Execute a single notebook cell.

    Namespaces are persisted per notebook so variables defined in one cell
    are available in subsequent cells. Send reset=true to clear the namespace
    (e.g. at the start of Run All).
    """
    _require_permission(request, "execute")
    from dp.engine.notebook import execute_cell
    if req.reset:
        _notebook_namespaces.pop(name, None)
    namespace = _notebook_namespaces.get(name)
    db_path = _get_db_path()
    conn = connect(db_path)
    try:
        result = execute_cell(conn, req.source, namespace)
        _notebook_namespaces[name] = result["namespace"]
        return {"outputs": result["outputs"], "duration_ms": result["duration_ms"]}
    finally:
        conn.close()


# --- Data import ---


class ImportFileRequest(BaseModel):
    file_path: str
    target_schema: str = "landing"
    target_table: str | None = None


class TestConnectionRequest(BaseModel):
    connection_type: str
    params: dict


class ImportFromConnectionRequest(BaseModel):
    connection_type: str
    params: dict
    source_table: str
    target_schema: str = "landing"
    target_table: str | None = None


@app.post("/api/import/preview-file")
def preview_file_endpoint(request: Request, req: ImportFileRequest) -> dict:
    """Preview data from a file before importing."""
    _require_permission(request, "execute")
    from dp.engine.importer import preview_file
    try:
        return preview_file(req.file_path)
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/import/file")
def import_file_endpoint(request: Request, req: ImportFileRequest) -> dict:
    """Import a file into the warehouse."""
    _require_permission(request, "execute")
    from dp.engine.importer import import_file
    db_path = _get_db_path()
    conn = connect(db_path)
    try:
        return import_file(conn, req.file_path, req.target_schema, req.target_table)
    finally:
        conn.close()


@app.post("/api/import/test-connection")
def test_connection_endpoint(request: Request, req: TestConnectionRequest) -> dict:
    """Test a database connection."""
    _require_permission(request, "execute")
    from dp.engine.importer import test_connection
    return test_connection(req.connection_type, req.params)


@app.post("/api/import/from-connection")
def import_from_connection_endpoint(request: Request, req: ImportFromConnectionRequest) -> dict:
    """Import from an external database."""
    _require_permission(request, "execute")
    from dp.engine.importer import import_from_connection
    db_path = _get_db_path()
    conn = connect(db_path)
    try:
        return import_from_connection(
            conn, req.connection_type, req.params,
            req.source_table, req.target_schema, req.target_table,
        )
    finally:
        conn.close()


# --- Upload file for import ---


@app.post("/api/upload")
async def upload_file(request: Request) -> dict:
    """Upload a file for data import."""
    _require_permission(request, "execute")
    from fastapi import UploadFile, File

    # Parse multipart form data
    form = await request.form()
    file = form.get("file")
    if not file:
        raise HTTPException(400, "No file uploaded")

    # Save to data/ directory
    data_dir = _get_project_dir() / "data"
    data_dir.mkdir(exist_ok=True)
    file_path = data_dir / file.filename

    content = await file.read()
    file_path.write_bytes(content)

    return {"path": str(file_path), "name": file.filename, "size": len(content)}


# --- Serve frontend ---

_FRONTEND_DIR = Path(__file__).parent.parent.parent.parent / "frontend" / "dist"


@app.get("/", response_class=HTMLResponse)
@app.get("/{path:path}", response_class=HTMLResponse)
def serve_frontend(path: str = "") -> HTMLResponse:
    """Serve the frontend SPA."""
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

    index = _FRONTEND_DIR / "index.html"
    if index.exists():
        return HTMLResponse(content=index.read_text())
    return HTMLResponse(
        content="<h1>dp</h1><p>Frontend not built. Run <code>cd frontend && npm run build</code></p>",
        status_code=200,
    )
