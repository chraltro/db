"""FastAPI backend for the web UI."""

from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from typing import Any

from typing import Annotated, Generator

import duckdb
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from dp.config import load_project
from dp.engine.database import connect, ensure_meta_table
from dp.engine.runner import run_script
from dp.engine.transform import build_dag, discover_models, run_transform

logger = logging.getLogger("dp.server")

# Set by CLI before starting uvicorn
PROJECT_DIR: Path = Path.cwd()
AUTH_ENABLED: bool = False  # Set by CLI --auth flag
ACTIVE_ENV: str | None = None  # Set by CLI --env flag

app = FastAPI(title="dp", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Identifier validation ---


def _validate_identifier(value: str, label: str = "identifier") -> str:
    """Validate that a value is a safe SQL identifier (no injection)."""
    from dp.engine.utils import validate_identifier
    try:
        return validate_identifier(value, label)
    except ValueError:
        raise HTTPException(400, f"Invalid {label}: {value!r}")


# --- Config cache ---

_config_cache: dict[str, Any] = {"config": None, "mtime": 0.0, "path": None}


def _get_config_cached():
    """Load project config with file-mtime-based caching."""
    config_path = _get_project_dir() / "project.yml"
    try:
        mtime = config_path.stat().st_mtime
    except FileNotFoundError:
        return load_project(_get_project_dir(), env=ACTIVE_ENV)

    cache_key = f"{config_path}:{ACTIVE_ENV}"
    if (
        _config_cache["config"] is not None
        and _config_cache["path"] == cache_key
        and _config_cache["mtime"] == mtime
    ):
        return _config_cache["config"]

    config = load_project(_get_project_dir(), env=ACTIVE_ENV)
    _config_cache["config"] = config
    _config_cache["mtime"] = mtime
    _config_cache["path"] = cache_key
    return config


# --- Model discovery cache ---

_MODEL_CACHE_VERSION = 2  # Bump to invalidate cached models after code changes
_model_cache: dict[str, Any] = {"models": None, "mtime_map": None, "transform_dir": None, "version": None}


def _discover_models_cached(transform_dir: Path):
    """Discover models with file-mtime-based caching."""
    if not transform_dir.exists():
        return []

    # Build a map of file -> mtime for all SQL files
    current_mtimes = {}
    for sql_file in sorted(transform_dir.rglob("*.sql")):
        current_mtimes[str(sql_file)] = sql_file.stat().st_mtime

    if (
        _model_cache["models"] is not None
        and _model_cache["transform_dir"] == str(transform_dir)
        and _model_cache["mtime_map"] == current_mtimes
        and _model_cache["version"] == _MODEL_CACHE_VERSION
    ):
        return _model_cache["models"]

    models = discover_models(transform_dir)
    _model_cache["models"] = models
    _model_cache["mtime_map"] = current_mtimes
    _model_cache["transform_dir"] = str(transform_dir)
    _model_cache["version"] = _MODEL_CACHE_VERSION
    return models


def _get_project_dir() -> Path:
    return PROJECT_DIR


def _get_config():
    return _get_config_cached()


def _get_db_path() -> Path:
    config = _get_config()
    return _get_project_dir() / config.database.path


def _require_db(db_path: Path) -> None:
    """Raise 404 if the warehouse database doesn't exist yet."""
    if not db_path.exists():
        raise HTTPException(404, "Warehouse database not found. Run a pipeline first.")


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


# --- Connection dependency injection ---


def get_db() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """FastAPI dependency: yields a read-write DuckDB connection."""
    db_path = _get_db_path()
    _require_db(db_path)
    conn = connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


def get_db_readonly() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """FastAPI dependency: yields a read-only DuckDB connection."""
    db_path = _get_db_path()
    _require_db(db_path)
    conn = connect(db_path, read_only=True)
    try:
        yield conn
    finally:
        conn.close()


DbConn = Annotated[duckdb.DuckDBPyConnection, Depends(get_db)]
DbConnReadOnly = Annotated[duckdb.DuckDBPyConnection, Depends(get_db_readonly)]


# --- Rate limiting ---

_login_attempts: dict[str, list[float]] = {}
_RATE_LIMIT_WINDOW = 60.0  # seconds
_RATE_LIMIT_MAX = 5  # max attempts per window
_RATE_LIMIT_MAX_KEYS = 10_000  # max tracked IPs to prevent memory leak


def _check_rate_limit(key: str) -> None:
    """Enforce rate limiting. Raises 429 if too many attempts."""
    now = time.time()
    attempts = _login_attempts.get(key, [])
    # Prune old attempts outside the window
    attempts = [t for t in attempts if now - t < _RATE_LIMIT_WINDOW]
    if len(attempts) >= _RATE_LIMIT_MAX:
        logger.warning("Rate limit exceeded for %s", key)
        raise HTTPException(429, "Too many login attempts. Try again later.")
    attempts.append(now)
    _login_attempts[key] = attempts
    # Evict stale keys to prevent unbounded memory growth
    if len(_login_attempts) > _RATE_LIMIT_MAX_KEYS:
        stale = [k for k, v in _login_attempts.items() if not v or now - v[-1] > _RATE_LIMIT_WINDOW]
        for k in stale:
            del _login_attempts[k]


# --- Auth endpoints ---


class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=100)
    password: str = Field(..., min_length=1, max_length=500)


class CreateUserRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9_.-]+$")
    password: str = Field(..., min_length=4, max_length=500)
    role: str = Field(default="viewer", pattern=r"^(admin|editor|viewer)$")
    display_name: str | None = Field(default=None, max_length=200)


class UpdateUserRequest(BaseModel):
    role: str | None = Field(default=None, pattern=r"^(admin|editor|viewer)$")
    password: str | None = Field(default=None, min_length=4, max_length=500)
    display_name: str | None = Field(default=None, max_length=200)


@app.post("/api/auth/login")
def login(request: Request, req: LoginRequest, conn: DbConn) -> dict:
    """Authenticate and get a token (rate-limited)."""
    client_ip = request.client.host if request.client else "unknown"
    _check_rate_limit(f"login:{client_ip}")
    from dp.engine.auth import authenticate
    token = authenticate(conn, req.username, req.password)
    if not token:
        raise HTTPException(401, "Invalid credentials")
    return {"token": token, "username": req.username}


@app.get("/api/auth/me")
def get_current_user(request: Request) -> dict:
    """Get current authenticated user."""
    return _require_user(request)


@app.get("/api/auth/status")
def get_auth_status(conn: DbConn) -> dict:
    """Check if auth is enabled and if initial setup is needed."""
    if not AUTH_ENABLED:
        return {"auth_enabled": False, "needs_setup": False}
    from dp.engine.auth import has_any_users
    return {"auth_enabled": True, "needs_setup": not has_any_users(conn)}


@app.post("/api/auth/setup")
def initial_setup(req: CreateUserRequest, conn: DbConn) -> dict:
    """Create the first admin user (only works when no users exist)."""
    from dp.engine.auth import create_user, has_any_users, authenticate
    if has_any_users(conn):
        raise HTTPException(400, "Setup already completed")
    create_user(conn, req.username, req.password, "admin", req.display_name)
    token = authenticate(conn, req.username, req.password)
    return {"token": token, "username": req.username, "role": "admin"}


# --- User management ---


@app.get("/api/users")
def list_users(request: Request, conn: DbConn) -> list[dict]:
    """List all users (admin only)."""
    _require_permission(request, "manage_users")
    from dp.engine.auth import list_users as _list_users
    return _list_users(conn)


@app.post("/api/users")
def create_user_endpoint(request: Request, req: CreateUserRequest, conn: DbConn) -> dict:
    """Create a new user (admin only)."""
    _require_permission(request, "manage_users")
    from dp.engine.auth import create_user
    try:
        return create_user(conn, req.username, req.password, req.role, req.display_name)
    except ValueError as e:
        raise HTTPException(400, str(e))


@app.put("/api/users/{username}")
def update_user_endpoint(request: Request, username: str, req: UpdateUserRequest, conn: DbConn) -> dict:
    """Update a user (admin only)."""
    _require_permission(request, "manage_users")
    from dp.engine.auth import update_user
    try:
        found = update_user(conn, username, req.role, req.password, req.display_name)
        if not found:
            raise HTTPException(404, f"User '{username}' not found")
        return {"status": "updated"}
    except ValueError as e:
        raise HTTPException(400, str(e))


@app.delete("/api/users/{username}")
def delete_user_endpoint(request: Request, username: str, conn: DbConn) -> dict:
    """Delete a user (admin only)."""
    _require_permission(request, "manage_users")
    from dp.engine.auth import delete_user
    found = delete_user(conn, username)
    if not found:
        raise HTTPException(404, f"User '{username}' not found")
    return {"status": "deleted"}


# --- Secrets management ---


@app.get("/api/secrets")
def list_secrets(request: Request) -> list[dict]:
    """List secrets (keys and masked values only)."""
    _require_permission(request, "manage_secrets")
    from dp.engine.secrets import list_secrets as _list_secrets
    return _list_secrets(_get_project_dir())


class SetSecretRequest(BaseModel):
    key: str = Field(..., min_length=1, max_length=200, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    value: str = Field(..., max_length=10_000)


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
        elif entry.suffix in (".sql", ".py", ".yml", ".yaml", ".dpnb", ".csv"):
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
    content: str = Field(..., max_length=5_000_000)


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


class TransformRequest(BaseModel):
    targets: list[str] | None = Field(default=None, max_length=500)
    force: bool = False


@app.post("/api/transform")
def run_transform_endpoint(request: Request, req: TransformRequest, conn: DbConn) -> dict:
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


class DiffRequest(BaseModel):
    targets: list[str] | None = Field(default=None)
    target_schema: str | None = Field(default=None, max_length=100)
    full: bool = False


@app.post("/api/diff")
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
                    {"column": sc.column, "change_type": sc.change_type,
                     "old_type": sc.old_type, "new_type": sc.new_type}
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


# --- Git status ---


@app.get("/api/git/status")
def get_git_status(request: Request) -> dict:
    """Get git status for the project (branch, dirty, changed files)."""
    _require_permission(request, "read")
    try:
        from dp.engine.git import (
            changed_files,
            current_branch,
            is_dirty,
            is_git_repo,
            last_commit_hash,
            last_commit_message,
        )

        project_dir = _get_project_dir()
        if not is_git_repo(project_dir):
            return {"is_git_repo": False}

        return {
            "is_git_repo": True,
            "branch": current_branch(project_dir),
            "dirty": is_dirty(project_dir),
            "changed_files": changed_files(project_dir),
            "last_commit": last_commit_hash(project_dir),
            "last_message": last_commit_message(project_dir),
        }
    except Exception:
        return {"is_git_repo": False}


# --- Script execution ---


class RunScriptRequest(BaseModel):
    script_path: str = Field(..., min_length=1, max_length=500)


@app.post("/api/run")
def run_script_endpoint(request: Request, req: RunScriptRequest, conn: DbConn) -> dict:
    """Run an ingest or export script."""
    _require_permission(request, "execute")
    logger.info("Script run requested: %s", req.script_path)
    script_path = _get_project_dir() / req.script_path
    if not script_path.exists():
        raise HTTPException(404, f"Script not found: {req.script_path}")
    script_type = "ingest" if "ingest" in req.script_path else "export"
    result = run_script(conn, script_path, script_type)
    # Mask secrets in output
    from dp.engine.secrets import mask_output
    if result.get("log_output"):
        result["log_output"] = mask_output(result["log_output"], _get_project_dir())
    return result


# --- Stream execution ---


@app.post("/api/stream/{stream_name}")
def run_stream_endpoint(request: Request, stream_name: str, conn: DbConn, force: bool = False) -> dict:
    """Run a full stream with retry support."""
    _require_permission(request, "execute")
    logger.info("Stream run requested: %s (force=%s)", stream_name, force)
    config = _get_config()
    if stream_name not in config.streams:
        raise HTTPException(404, f"Stream '{stream_name}' not found")
    stream_config = config.streams[stream_name]

    step_results = []
    has_error = False
    start = time.perf_counter()

    def _run_step(step):
        from dp.engine.runner import run_scripts_in_dir
        if step.action == "ingest":
            results = run_scripts_in_dir(conn, _get_project_dir() / "ingest", "ingest", step.targets)
            return {"action": "ingest", "results": results, "error": any(r["status"] == "error" for r in results)}
        elif step.action == "transform":
            results = run_transform(
                conn, _get_project_dir() / "transform",
                targets=step.targets if step.targets != ["all"] else None, force=force,
            )
            return {"action": "transform", "results": results, "error": any(s == "error" for s in results.values())}
        elif step.action == "export":
            results = run_scripts_in_dir(conn, _get_project_dir() / "export", "export", step.targets)
            return {"action": "export", "results": results, "error": any(r["status"] == "error" for r in results)}
        elif step.action == "seed":
            from dp.engine.seeds import run_seeds
            results = run_seeds(conn, _get_project_dir() / "seeds", force=force)
            return {"action": "seed", "results": results, "error": any(s == "error" for s in results.values())}
        return {"action": step.action, "results": {}, "error": False}

    import time as _time
    for step in stream_config.steps:
        result = _run_step(step)
        if result["error"] and stream_config.retries > 0:
            for attempt in range(1, stream_config.retries + 1):
                logger.info("Retrying %s step (attempt %d/%d)", step.action, attempt, stream_config.retries)
                _time.sleep(stream_config.retry_delay)
                result = _run_step(step)
                if not result["error"]:
                    break
        step_results.append({"action": result["action"], "results": result["results"]})
        if result["error"]:
            has_error = True
            break

    duration_s = round(time.perf_counter() - start, 1)
    status = "failed" if has_error else "success"

    # Webhook notification
    if stream_config.webhook_url:
        _send_webhook_notification(stream_config.webhook_url, stream_name, status, duration_s)

    return {"stream": stream_name, "steps": step_results, "status": status, "duration_seconds": duration_s}


def _send_webhook_notification(url: str, stream_name: str, status: str, duration_s: float) -> None:
    """Send a POST webhook notification for stream completion."""
    from datetime import datetime
    from urllib.request import Request, urlopen

    payload = json.dumps({
        "stream": stream_name,
        "status": status,
        "duration_seconds": duration_s,
        "timestamp": datetime.now().isoformat(),
    }).encode()

    try:
        req = Request(url, data=payload, headers={"Content-Type": "application/json"})
        urlopen(req, timeout=10)
        logger.info("Webhook sent to %s for stream %s", url, stream_name)
    except Exception as e:
        logger.warning("Webhook failed for stream %s: %s", stream_name, e)


# --- Query ---


class QueryRequest(BaseModel):
    sql: str = Field(..., min_length=1, max_length=100_000)
    limit: int = Field(default=1000, gt=0, le=50_000)
    offset: int = Field(default=0, ge=0)


_QUERY_TIMEOUT_SECONDS = 30


@app.post("/api/query")
def run_query(request: Request, req: QueryRequest, conn: DbConnReadOnly) -> dict:
    """Run an ad-hoc SQL query with a timeout."""
    _require_permission(request, "read")
    try:
        import threading

        query_result: dict = {}
        query_error: list[Exception] = []

        def _exec_query():
            try:
                if req.offset > 0:
                    wrapped = f"SELECT * FROM ({req.sql}) AS _q OFFSET {req.offset} LIMIT {req.limit}"
                    result = conn.execute(wrapped)
                else:
                    result = conn.execute(req.sql)
                columns = [desc[0] for desc in result.description]
                rows = result.fetchmany(req.limit)
                query_result["data"] = {
                    "columns": columns,
                    "rows": [[_serialize(v) for v in row] for row in rows],
                    "truncated": len(rows) == req.limit,
                    "offset": req.offset,
                    "limit": req.limit,
                }
            except Exception as e:
                query_error.append(e)

        thread = threading.Thread(target=_exec_query, daemon=True)
        thread.start()
        thread.join(timeout=_QUERY_TIMEOUT_SECONDS)

        if thread.is_alive():
            conn.interrupt()
            raise HTTPException(408, f"Query timed out after {_QUERY_TIMEOUT_SECONDS}s")
        if query_error:
            raise query_error[0]
        return query_result["data"]
    except HTTPException:
        raise
    except Exception as e:
        logger.warning("Query failed: %s", e)
        raise HTTPException(400, str(e))


def _serialize(value: Any) -> Any:
    """Make values JSON-serializable."""
    if value is None:
        return None
    if isinstance(value, (int, float, str, bool)):
        return value
    return str(value)


# --- Tables ---


@app.get("/api/tables")
def list_tables(request: Request, conn: DbConnReadOnly, schema: str | None = None) -> list[dict]:
    """List warehouse tables and views."""
    _require_permission(request, "read")
    if schema:
        _validate_identifier(schema, "schema")
        rows = conn.execute(
            """
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', '_dp_internal')
              AND table_schema = ?
            ORDER BY table_schema, table_name
            """,
            [schema],
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', '_dp_internal')
            ORDER BY table_schema, table_name
            """
        ).fetchall()
    return [{"schema": r[0], "name": r[1], "type": r[2]} for r in rows]


@app.get("/api/tables/{schema}/{table}")
def describe_table(request: Request, schema: str, table: str, conn: DbConnReadOnly) -> dict:
    """Get column info for a table."""
    _require_permission(request, "read")
    _validate_identifier(schema, "schema")
    _validate_identifier(table, "table")
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


@app.get("/api/tables/{schema}/{table}/sample")
def sample_table(
    request: Request,
    schema: str,
    table: str,
    conn: DbConnReadOnly,
    limit: int = 100,
    offset: int = 0,
) -> dict:
    """Get sample rows from a table with pagination."""
    _require_permission(request, "read")
    _validate_identifier(schema, "schema")
    _validate_identifier(table, "table")
    limit = max(1, min(limit, 10_000))
    offset = max(0, offset)
    try:
        quoted = f'"{schema}"."{table}"'
        result = conn.execute(f"SELECT * FROM {quoted} LIMIT {limit} OFFSET {offset}")
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return {
            "schema": schema,
            "table": table,
            "columns": columns,
            "rows": [[_serialize(v) for v in row] for row in rows],
            "limit": limit,
            "offset": offset,
        }
    except Exception as e:
        logger.warning("Sample query failed for %s.%s: %s", schema, table, e)
        raise HTTPException(400, str(e))


@app.get("/api/tables/{schema}/{table}/profile")
def profile_table(request: Request, schema: str, table: str, conn: DbConnReadOnly) -> dict:
    """Get column-level statistics for a table."""
    _require_permission(request, "read")
    _validate_identifier(schema, "schema")
    _validate_identifier(table, "table")
    try:
        quoted = f'"{schema}"."{table}"'
        # Get row count
        row_count = conn.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()[0]
        # Get column info
        cols = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            [schema, table],
        ).fetchall()

        profiles = []
        for col_name, col_type in cols:
            qcol = f'"{col_name}"'
            stats: dict = {"name": col_name, "type": col_type}

            # Null count and distinct count for all types
            basic = conn.execute(
                f"SELECT COUNT(*) - COUNT({qcol}), COUNT(DISTINCT {qcol}) FROM {quoted}"
            ).fetchone()
            stats["null_count"] = basic[0]
            stats["distinct_count"] = basic[1]

            # Numeric stats
            is_numeric = any(t in col_type.upper() for t in (
                "INT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "BIGINT", "SMALLINT", "TINYINT", "HUGEINT",
            ))
            if is_numeric:
                num = conn.execute(
                    f"SELECT MIN({qcol}), MAX({qcol}), AVG({qcol}::DOUBLE) FROM {quoted}"
                ).fetchone()
                stats["min"] = _serialize(num[0])
                stats["max"] = _serialize(num[1])
                stats["avg"] = round(num[2], 4) if num[2] is not None else None
            else:
                # For non-numeric: min/max of string representation
                minmax = conn.execute(
                    f"SELECT MIN({qcol}::VARCHAR), MAX({qcol}::VARCHAR) FROM {quoted}"
                ).fetchone()
                stats["min"] = minmax[0]
                stats["max"] = minmax[1]

            # Sample values (up to 5 distinct)
            samples = conn.execute(
                f"SELECT DISTINCT {qcol}::VARCHAR FROM {quoted} WHERE {qcol} IS NOT NULL LIMIT 5"
            ).fetchall()
            stats["sample_values"] = [s[0] for s in samples]

            profiles.append(stats)

        return {"schema": schema, "table": table, "row_count": row_count, "columns": profiles}
    except Exception as e:
        logger.warning("Profile failed for %s.%s: %s", schema, table, e)
        raise HTTPException(400, str(e))


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
def get_history(request: Request, conn: DbConn, limit: int = 50) -> list[dict]:
    """Get run history."""
    _require_permission(request, "read")
    ensure_meta_table(conn)
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


# --- Lint ---


@app.post("/api/lint")
def lint_endpoint(request: Request, fix: bool = False) -> dict:
    """Run SQLFluff on transform files."""
    _require_permission(request, "execute")
    from dp.lint.linter import lint

    config = _get_config()
    count, violations, fixed = lint(
        _get_project_dir() / "transform",
        fix=fix,
        dialect=config.lint.dialect,
        rules=config.lint.rules or None,
    )
    return {"count": count, "violations": violations, "fixed": fixed}


class LintFileRequest(BaseModel):
    path: str = Field(..., max_length=1000)
    fix: bool = False
    content: str | None = Field(None, max_length=1_000_000)


@app.post("/api/lint/file")
def lint_file_endpoint(request: Request, req: LintFileRequest) -> dict:
    """Run SQLFluff on a single SQL file."""
    _require_permission(request, "execute")
    from dp.lint.linter import lint_file

    project_dir = _get_project_dir()
    config = _get_config()
    file_path = (project_dir / req.path).resolve()
    # Security: must be inside project dir
    if not str(file_path).startswith(str(project_dir.resolve())):
        raise HTTPException(status_code=400, detail="Path outside project directory")
    if file_path.suffix != ".sql":
        raise HTTPException(status_code=400, detail="Not a SQL file")

    count, violations, fixed, new_content = lint_file(
        file_path,
        project_dir=project_dir,
        fix=req.fix,
        dialect=config.lint.dialect,
        rules=config.lint.rules or None,
        content=req.content,
    )
    return {"count": count, "violations": violations, "fixed": fixed, "content": new_content}


@app.get("/api/lint/config")
def get_lint_config(request: Request) -> dict:
    """Get the .sqlfluff config file contents."""
    _require_permission(request, "read")
    sqlfluff_path = _get_project_dir() / ".sqlfluff"
    if not sqlfluff_path.exists():
        return {"exists": False, "content": ""}
    return {"exists": True, "content": sqlfluff_path.read_text()}


class LintConfigRequest(BaseModel):
    content: str = Field(..., max_length=100_000)


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


def _scan_import_sources(project_dir: Path) -> dict[str, str]:
    """Query run_log for the most recent successful import per table.

    Returns a mapping of fully-qualified table name -> source filename (e.g. "customers.csv").
    """
    db_path = _get_db_path()
    conn = connect(db_path)
    try:
        result = conn.execute("""
            SELECT DISTINCT ON (target) target, log_output
            FROM _dp_internal.run_log
            WHERE run_type = 'import' AND status = 'success'
            ORDER BY target, started_at DESC
        """).fetchall()
        # Fall back to the table name itself if source filename wasn't recorded
        return {row[0]: row[1] if row[1] else row[0].split(".")[-1] for row in result}
    except Exception:
        return {}
    finally:
        conn.close()


@app.get("/api/dag")
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

    # Add import nodes for tables loaded via the importer wizard
    added_imports: set[str] = set()
    for dep in sorted(external_deps):
        if dep in import_sources and dep not in ingest_targets:
            source_file = import_sources[dep]
            import_id = f"import:{dep}"
            if import_id not in added_imports:
                added_imports.add(import_id)
                nodes.append({
                    "id": import_id,
                    "label": source_file,
                    "schema": "import",
                    "type": "import",
                    "source_file": source_file,
                })
            edges.append({"source": import_id, "target": dep})

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
def get_docs_markdown(request: Request, conn: DbConnReadOnly) -> dict:
    """Generate markdown documentation."""
    _require_permission(request, "read")
    from dp.engine.docs import generate_docs

    config = _get_config()
    md = generate_docs(
        conn, _get_project_dir() / "transform",
        sources=config.sources,
        exposures=config.exposures,
    )
    return {"markdown": md}


@app.get("/api/docs/structured")
def get_docs_structured(request: Request, conn: DbConnReadOnly) -> dict:
    """Generate structured documentation for two-pane UI."""
    _require_permission(request, "read")
    from dp.engine.docs import generate_structured_docs

    return generate_structured_docs(conn, _get_project_dir() / "transform")


# --- Scheduler status ---


@app.get("/api/scheduler")
def get_scheduler_status(request: Request) -> dict:
    """Get scheduler status and scheduled streams."""
    _require_permission(request, "read")
    from dp.engine.scheduler import get_scheduled_streams

    streams = get_scheduled_streams(_get_project_dir())
    return {"scheduled_streams": streams}


# --- Notebooks ---


# Per-notebook namespace store for persistent cell execution.
# Bounded to prevent unbounded memory growth — evicts oldest entries.
_NOTEBOOK_NS_MAX = 50
_notebook_namespaces: dict[str, dict] = {}


def _notebook_ns_set(name: str, ns: dict) -> None:
    """Store a notebook namespace, evicting oldest if at capacity."""
    _notebook_namespaces[name] = ns
    while len(_notebook_namespaces) > _NOTEBOOK_NS_MAX:
        oldest = next(iter(_notebook_namespaces))
        del _notebook_namespaces[oldest]


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
    """Resolve a notebook name or path to a file path.

    Validates the resolved path stays within the project directory
    to prevent path traversal attacks.
    """
    # Try as a relative path first (e.g. "ingest/earthquakes.dpnb")
    if "/" in name or name.endswith(".dpnb"):
        candidate = project_dir / name
        if not candidate.suffix:
            candidate = candidate.with_suffix(".dpnb")
        # Path traversal protection
        if not candidate.resolve().is_relative_to(project_dir.resolve()):
            raise HTTPException(400, "Invalid notebook path")
        if candidate.exists():
            return candidate
    # Fall back to notebooks/ directory
    nb_path = project_dir / "notebooks" / f"{name}.dpnb"
    # Path traversal protection
    if not nb_path.resolve().is_relative_to(project_dir.resolve()):
        raise HTTPException(400, "Invalid notebook path")
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


class NotebookRunCellRequest(BaseModel):
    source: str = Field(..., max_length=1_000_000)
    cell_type: str = Field(default="code", pattern=r"^(code|sql|ingest)$")
    cell_id: str | None = Field(default=None, max_length=200)
    reset: bool = False


@app.post("/api/notebooks/run-cell/{name:path}")
def run_cell_endpoint(request: Request, name: str, req: NotebookRunCellRequest, conn: DbConn) -> dict:
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
        return {"outputs": result["outputs"], "duration_ms": result["duration_ms"], "config": result.get("config", {})}
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


class PromoteToModelRequest(BaseModel):
    model_config = {"protected_namespaces": ()}

    sql_source: str = Field(..., min_length=1, max_length=1_000_000)
    model_name: str = Field(..., min_length=1, max_length=200, pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$")
    target_schema: str = Field(default="bronze", min_length=1, max_length=100, pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$")
    description: str = Field(default="", max_length=1000)
    overwrite: bool = Field(default=False)


@app.post("/api/notebooks/promote-to-model")
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

        # Validate the new model fits into the DAG
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


@app.post("/api/notebooks/model-to-notebook/{model_name:path}")
def model_to_notebook_endpoint(request: Request, model_name: str, conn: DbConn) -> dict:
    """Create a notebook from a transform model for interactive debugging."""
    _require_permission(request, "write")
    from dp.engine.notebook import model_to_notebook, save_notebook

    project_dir = _get_project_dir()
    transform_dir = project_dir / "transform"
    try:
        nb = model_to_notebook(conn, model_name, transform_dir, project_dir / "notebooks")

        # Save the notebook
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


@app.post("/api/notebooks/debug/{model_name:path}")
def debug_notebook_endpoint(request: Request, model_name: str, conn: DbConn) -> dict:
    """Generate a debug notebook for a failed model.

    Optionally looks up the most recent failure from run_log.
    """
    _require_permission(request, "write")
    from dp.engine.notebook import generate_debug_notebook, save_notebook

    project_dir = _get_project_dir()
    transform_dir = project_dir / "transform"
    # Look up most recent error from run log
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

        # Check for assertion failures
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
            conn, model_name, transform_dir,
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


# --- Data import ---


class ImportFileRequest(BaseModel):
    file_path: str = Field(..., min_length=1, max_length=1000)
    target_schema: str = Field(default="landing", min_length=1, max_length=100)
    target_table: str | None = Field(default=None, max_length=100)


class TestConnectionRequest(BaseModel):
    connection_type: str = Field(..., min_length=1, max_length=50)
    params: dict


class ImportFromConnectionRequest(BaseModel):
    connection_type: str = Field(..., min_length=1, max_length=50)
    params: dict
    source_table: str = Field(..., min_length=1, max_length=500)
    target_schema: str = Field(default="landing", min_length=1, max_length=100)
    target_table: str | None = Field(default=None, max_length=100)


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
def import_file_endpoint(request: Request, req: ImportFileRequest, conn: DbConn) -> dict:
    """Import a file into the warehouse."""
    _require_permission(request, "execute")
    from dp.engine.importer import import_file
    return import_file(conn, req.file_path, req.target_schema, req.target_table)


@app.post("/api/import/test-connection")
def test_connection_endpoint(request: Request, req: TestConnectionRequest) -> dict:
    """Test a database connection."""
    _require_permission(request, "execute")
    from dp.engine.importer import test_connection
    return test_connection(req.connection_type, req.params)


@app.post("/api/import/from-connection")
def import_from_connection_endpoint(request: Request, req: ImportFromConnectionRequest, conn: DbConn) -> dict:
    """Import from an external database."""
    _require_permission(request, "execute")
    from dp.engine.importer import import_from_connection
    return import_from_connection(
        conn, req.connection_type, req.params,
        req.source_table, req.target_schema, req.target_table,
    )


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

    # Save to data/ directory — sanitize filename to prevent path traversal
    data_dir = _get_project_dir() / "data"
    data_dir.mkdir(exist_ok=True)
    safe_name = Path(file.filename).name  # strip any directory components
    if not safe_name or safe_name.startswith("."):
        raise HTTPException(400, "Invalid filename")
    file_path = data_dir / safe_name
    # Verify resolved path is still inside data_dir
    if not file_path.resolve().is_relative_to(data_dir.resolve()):
        raise HTTPException(400, "Invalid filename")

    content = await file.read()
    file_path.write_bytes(content)

    return {"path": str(file_path), "name": safe_name, "size": len(content)}


# --- Connectors ---


class ConnectorSetupRequest(BaseModel):
    connector_type: str = Field(..., min_length=1, max_length=50)
    connection_name: str = Field(..., min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9_-]+$")
    config: dict
    tables: list[str] | None = None
    target_schema: str = Field(default="landing", min_length=1, max_length=100)
    schedule: str | None = None


class ConnectorTestRequest(BaseModel):
    connector_type: str = Field(..., min_length=1, max_length=50)
    config: dict


class ConnectorDiscoverRequest(BaseModel):
    connector_type: str = Field(..., min_length=1, max_length=50)
    config: dict


@app.get("/api/connectors/available")
def list_available_connectors(request: Request) -> list[dict]:
    """List all available connector types."""
    _require_permission(request, "read")
    import dp.connectors  # noqa: F401
    from dp.engine.connector import list_connectors
    return list_connectors()


@app.get("/api/connectors")
def list_configured_connectors_endpoint(request: Request) -> list[dict]:
    """List connectors configured in this project."""
    _require_permission(request, "read")
    import dp.connectors  # noqa: F401
    from dp.engine.connector import list_configured_connectors
    return list_configured_connectors(_get_project_dir())


@app.post("/api/connectors/test")
def test_connector_endpoint(request: Request, req: ConnectorTestRequest) -> dict:
    """Test a connector without setting it up."""
    _require_permission(request, "execute")
    import dp.connectors  # noqa: F401
    from dp.engine.connector import test_connector
    try:
        return test_connector(req.connector_type, req.config)
    except ValueError as e:
        raise HTTPException(400, str(e))


@app.post("/api/connectors/discover")
def discover_connector_endpoint(request: Request, req: ConnectorDiscoverRequest) -> list[dict]:
    """Discover available resources for a connector."""
    _require_permission(request, "execute")
    import dp.connectors  # noqa: F401
    from dp.engine.connector import discover_connector
    try:
        return discover_connector(req.connector_type, req.config)
    except ValueError as e:
        raise HTTPException(400, str(e))


@app.post("/api/connectors/setup")
def setup_connector_endpoint(request: Request, req: ConnectorSetupRequest) -> dict:
    """Set up a new connector: test, generate script, update config."""
    _require_permission(request, "execute")
    import dp.connectors  # noqa: F401
    from dp.engine.connector import setup_connector
    try:
        result = setup_connector(
            project_dir=_get_project_dir(),
            connector_type=req.connector_type,
            connection_name=req.connection_name,
            config=req.config,
            tables=req.tables,
            target_schema=req.target_schema,
            schedule=req.schedule,
        )
        if result["status"] == "error":
            raise HTTPException(400, result.get("error", "Setup failed"))
        return result
    except ValueError as e:
        raise HTTPException(400, str(e))


@app.post("/api/connectors/regenerate/{connection_name}")
def regenerate_connector_endpoint(request: Request, connection_name: str, body: dict = {}) -> dict:
    """Regenerate the ingest script for an existing connector."""
    _require_permission(request, "execute")
    _validate_identifier(connection_name, "connection name")
    import dp.connectors  # noqa: F401
    from dp.engine.connector import regenerate_connector
    result = regenerate_connector(_get_project_dir(), connection_name, body or None)
    if result["status"] == "error":
        raise HTTPException(400, result.get("error", "Regeneration failed"))
    return result


@app.post("/api/connectors/sync/{connection_name}")
def sync_connector_endpoint(request: Request, connection_name: str) -> dict:
    """Run sync for a configured connector."""
    _require_permission(request, "execute")
    _validate_identifier(connection_name, "connection name")
    import dp.connectors  # noqa: F401
    from dp.engine.connector import sync_connector
    result = sync_connector(_get_project_dir(), connection_name)
    if result.get("status") == "error":
        raise HTTPException(400, result.get("error", "Sync failed"))
    return result


@app.delete("/api/connectors/{connection_name}")
def remove_connector_endpoint(request: Request, connection_name: str) -> dict:
    """Remove a configured connector."""
    _require_permission(request, "write")
    _validate_identifier(connection_name, "connection name")
    import dp.connectors  # noqa: F401
    from dp.engine.connector import remove_connector
    result = remove_connector(_get_project_dir(), connection_name)
    if result["status"] == "error":
        raise HTTPException(404, result.get("error", "Not found"))
    return result


@app.get("/api/connectors/health")
def connector_health_endpoint(request: Request, conn: DbConnReadOnly) -> list:
    """Get last sync status for each connector from run_log."""
    _require_permission(request, "read")
    try:
        rows = conn.execute(
            """
            SELECT target, status, started_at, duration_ms, error
            FROM _dp_internal.run_log
            WHERE run_type = 'script' AND target LIKE 'ingest/%'
            ORDER BY started_at DESC
            """
        ).fetchall()
    except Exception:
        return []

    # Deduplicate: keep only the latest run per target
    seen: dict[str, dict] = {}
    for target, status, started_at, duration_ms, error in rows:
        if target not in seen:
            seen[target] = {
                "target": target,
                "status": status,
                "started_at": str(started_at) if started_at else None,
                "duration_ms": duration_ms,
                "error": error,
            }
    return list(seen.values())


@app.post("/api/webhook/{webhook_name}")
async def receive_webhook(request: Request, webhook_name: str, conn: DbConn) -> dict:
    """Receive webhook data and store it in the inbox table."""
    _validate_identifier(webhook_name, "webhook name")

    body = await request.body()
    try:
        payload = json.loads(body)
    except Exception:
        raise HTTPException(400, "Invalid JSON payload")

    table = f"landing.{webhook_name}_inbox"
    conn.execute("CREATE SCHEMA IF NOT EXISTS landing")
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id VARCHAR DEFAULT gen_random_uuid()::VARCHAR,
            received_at TIMESTAMP DEFAULT current_timestamp,
            payload JSON
        )
    """)
    conn.execute(
        f"INSERT INTO {table} (payload) VALUES (?::JSON)",
        [json.dumps(payload)],
    )
    return {"status": "received", "table": table}


# --- Overview ---


@app.get("/api/overview")
def get_overview(request: Request, conn: DbConnReadOnly) -> dict:
    """Get an overview of the platform: pipeline health, warehouse stats, recent activity."""
    _require_permission(request, "read")

    result: dict[str, Any] = {
        "recent_runs": [],
        "schemas": [],
        "total_tables": 0,
        "total_rows": 0,
        "connectors": 0,
        "has_data": False,
        "streams": {},
    }

    # Streams config
    config = _get_config()
    result["streams"] = {
        name: {"description": s.description, "schedule": s.schedule}
        for name, s in config.streams.items()
    }

    # Connectors count
    try:
        import dp.connectors  # noqa: F401
        from dp.engine.connector import list_configured_connectors
        result["connectors"] = len(list_configured_connectors(_get_project_dir()))
    except Exception:
        pass

    # Recent runs (last 20) — may fail if meta table doesn't exist yet
    try:
        rows = conn.execute(
            """
            SELECT run_id, run_type, target, status, started_at, duration_ms, rows_affected, error
            FROM _dp_internal.run_log
            ORDER BY started_at DESC
            LIMIT 20
            """
        ).fetchall()
        result["recent_runs"] = [
            {
                "run_id": r[0], "run_type": r[1], "target": r[2], "status": r[3],
                "started_at": str(r[4]) if r[4] else None,
                "duration_ms": r[5], "rows_affected": r[6], "error": r[7],
            }
            for r in rows
        ]
    except Exception:
        pass

    # Schema summary with table counts and row counts
    try:
        tables = conn.execute(
            """
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', '_dp_internal')
            ORDER BY table_schema, table_name
            """
        ).fetchall()

        schema_map: dict[str, dict] = {}
        for schema, table_name, table_type in tables:
            if schema not in schema_map:
                schema_map[schema] = {"name": schema, "tables": 0, "views": 0, "total_rows": 0}
            if table_type == "VIEW":
                schema_map[schema]["views"] += 1
            else:
                schema_map[schema]["tables"] += 1
                try:
                    row_count = conn.execute(
                        f'SELECT COUNT(*) FROM "{schema}"."{table_name}"'
                    ).fetchone()[0]
                    schema_map[schema]["total_rows"] += row_count
                except Exception:
                    pass

        SCHEMA_ORDER = ["landing", "bronze", "silver", "gold"]
        sorted_schemas = sorted(
            schema_map.values(),
            key=lambda s: (SCHEMA_ORDER.index(s["name"]) if s["name"] in SCHEMA_ORDER else 100, s["name"]),
        )
        result["schemas"] = sorted_schemas
        result["total_tables"] = sum(s["tables"] + s["views"] for s in sorted_schemas)
        result["total_rows"] = sum(s["total_rows"] for s in sorted_schemas)
        result["has_data"] = result["total_tables"] > 0
    except Exception:
        pass

    return result


# --- Freshness ---


@app.get("/api/freshness")
def get_freshness(request: Request, conn: DbConnReadOnly, max_hours: float = 24.0) -> list[dict]:
    """Check model freshness: which models are stale?"""
    _require_permission(request, "read")
    from dp.engine.transform import check_freshness

    ensure_meta_table(conn)
    return check_freshness(conn, max_age_hours=max_hours)


# --- Column-level lineage ---


@app.get("/api/lineage/{model_name}")
def get_lineage(request: Request, model_name: str) -> dict:
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

    db_path = _get_db_path()
    conn = connect(db_path, read_only=True) if db_path.exists() else None
    try:
        lineage = extract_column_lineage(target, conn)
        return {
            "model": target.full_name,
            "columns": lineage,
            "depends_on": target.depends_on,
        }
    finally:
        if conn:
            conn.close()


@app.get("/api/lineage")
def get_all_lineage(request: Request) -> list[dict]:
    """Get column-level lineage for all models."""
    _require_permission(request, "read")
    from dp.engine.transform import extract_column_lineage

    transform_dir = _get_project_dir() / "transform"
    models = _discover_models_cached(transform_dir)

    db_path = _get_db_path()
    conn = connect(db_path, read_only=True) if db_path.exists() else None
    try:
        results = []
        for model in models:
            lineage = extract_column_lineage(model, conn)
            results.append({
                "model": model.full_name,
                "columns": lineage,
                "depends_on": model.depends_on,
            })
        return results
    finally:
        if conn:
            conn.close()


# --- Compile-time validation ---


@app.post("/api/check")
def run_check(request: Request) -> dict:
    """Validate all SQL models without executing them."""
    _require_permission(request, "read")
    from dp.engine.seeds import discover_seeds
    from dp.engine.transform import discover_models, validate_models

    project_dir = _get_project_dir()
    transform_dir = project_dir / "transform"
    models = discover_models(transform_dir)
    config = _get_config()

    # Gather known tables from seeds and sources
    known_tables: set[str] = set()
    seeds = discover_seeds(project_dir / "seeds")
    for s in seeds:
        known_tables.add(s["full_name"])
    for src in config.sources:
        for t in src.tables:
            known_tables.add(f"{src.schema}.{t.name}")

    # Gather source columns for column validation
    source_columns: dict[str, set[str]] = {}
    for src in config.sources:
        for t in src.tables:
            full = f"{src.schema}.{t.name}"
            source_columns[full] = {c.name for c in t.columns}

    db_path = _get_db_path()
    conn = connect(db_path, read_only=True) if db_path.exists() else None
    try:
        ensure_meta_table(conn) if conn else None
        errors = validate_models(conn, models, known_tables=known_tables, source_columns=source_columns)
        return {
            "models_checked": len(models),
            "errors": [
                {"model": e.model, "severity": e.severity, "message": e.message}
                for e in errors
            ],
            "passed": not any(e.severity == "error" for e in errors),
        }
    finally:
        if conn:
            conn.close()


# --- Impact analysis ---


@app.get("/api/impact/{model_name}")
def get_impact(request: Request, model_name: str, column: str | None = None) -> dict:
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

    db_path = _get_db_path()
    conn = connect(db_path, read_only=True) if db_path.exists() else None
    try:
        return impact_analysis(models, model_name, column=column, conn=conn)
    finally:
        if conn:
            conn.close()


# --- Model profiles ---


@app.get("/api/profiles")
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


@app.get("/api/profiles/{model_name}")
def get_profile(request: Request, model_name: str, conn: DbConnReadOnly) -> dict:
    """Get profile stats for a specific model."""
    _require_permission(request, "read")
    ensure_meta_table(conn)
    row = conn.execute(
        "SELECT model_path, row_count, column_count, null_percentages, distinct_counts, profiled_at "
        "FROM _dp_internal.model_profiles WHERE model_path = ?",
        [model_name],
    ).fetchone()
    if not row:
        raise HTTPException(404, f"No profile for '{model_name}'. Run dp transform first.")
    return {
        "model": row[0],
        "row_count": row[1],
        "column_count": row[2],
        "null_percentages": json.loads(row[3]) if row[3] else {},
        "distinct_counts": json.loads(row[4]) if row[4] else {},
        "profiled_at": str(row[5]) if row[5] else None,
    }


# --- Assertions ---


@app.get("/api/assertions")
def get_assertions(request: Request, conn: DbConnReadOnly, limit: int = 100) -> list[dict]:
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


@app.get("/api/assertions/{model_name}")
def get_model_assertions(request: Request, model_name: str, conn: DbConnReadOnly) -> list[dict]:
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


@app.get("/api/alerts")
def get_alert_history(request: Request, conn: DbConnReadOnly, limit: int = 50) -> list[dict]:
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


class TestAlertRequest(BaseModel):
    channel: str = Field(..., pattern=r"^(slack|webhook|log)$")
    slack_webhook_url: str | None = None
    webhook_url: str | None = None


@app.post("/api/alerts/test")
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
    error = results[0].get("error", "Unknown error") if results else "No channels configured"
    raise HTTPException(400, f"Alert test failed: {error}")


# --- Environment management ---


@app.get("/api/environment")
def get_environment(request: Request) -> dict:
    """Get current environment and available environments."""
    _require_permission(request, "read")
    config = _get_config()
    return {
        "active": config.active_environment,
        "available": list(config.environments.keys()),
        "database_path": config.database.path,
    }


@app.put("/api/environment/{env_name}")
def switch_environment(request: Request, env_name: str) -> dict:
    """Switch the active environment."""
    _require_permission(request, "write")
    global ACTIVE_ENV
    config = _get_config()
    if env_name not in config.environments:
        raise HTTPException(404, f"Environment '{env_name}' not found")
    ACTIVE_ENV = env_name
    _config_cache["config"] = None  # Invalidate cache
    new_config = _get_config()
    return {
        "active": new_config.active_environment,
        "database_path": new_config.database.path,
    }


# --- Seeds ---


@app.get("/api/seeds")
def list_seeds_endpoint(request: Request) -> list[dict]:
    """List all seed CSV files."""
    _require_permission(request, "read")
    from dp.engine.seeds import discover_seeds
    seeds_dir = _get_project_dir() / "seeds"
    seeds = discover_seeds(seeds_dir)
    return [
        {"name": s["name"], "full_name": s["full_name"], "schema": s["schema"],
         "path": str(s["path"].relative_to(_get_project_dir()))}
        for s in seeds
    ]


class SeedRequest(BaseModel):
    force: bool = False
    schema_name: str = Field(default="seeds", max_length=100, pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$")


@app.post("/api/seeds")
def run_seeds_endpoint(request: Request, req: SeedRequest, conn: DbConn) -> dict:
    """Load all seeds."""
    _require_permission(request, "execute")
    from dp.engine.seeds import run_seeds
    results = run_seeds(conn, _get_project_dir() / "seeds", schema=req.schema_name, force=req.force)
    return {"results": results}


# --- Sources ---


@app.get("/api/sources")
def list_sources_endpoint(request: Request) -> list[dict]:
    """List declared sources from sources.yml."""
    _require_permission(request, "read")
    config = _get_config()
    return [
        {
            "name": s.name,
            "schema": s.schema,
            "description": s.description,
            "freshness_hours": s.freshness_hours,
            "connection": s.connection,
            "tables": [
                {
                    "name": t.name,
                    "description": t.description,
                    "columns": [{"name": c.name, "description": c.description} for c in t.columns],
                    "loaded_at_column": t.loaded_at_column,
                }
                for t in s.tables
            ],
        }
        for s in config.sources
    ]


@app.get("/api/sources/freshness")
def check_sources_freshness(request: Request, conn: DbConnReadOnly) -> list[dict]:
    """Check source freshness against declared SLAs."""
    _require_permission(request, "read")
    config = _get_config()

    results = []
    ensure_meta_table(conn)
    for src in config.sources:
        sla_hours = src.freshness_hours
        if sla_hours is None:
            continue
        for tbl in src.tables:
            full_name = f"{src.schema}.{tbl.name}"
            # Try loaded_at_column first
            last_loaded = None
            if tbl.loaded_at_column:
                try:
                    row = conn.execute(
                        f'SELECT MAX("{tbl.loaded_at_column}") FROM "{src.schema}"."{tbl.name}"'
                    ).fetchone()
                    if row and row[0]:
                        last_loaded = str(row[0])
                except Exception:
                    pass
            # Fall back to run_log
            if not last_loaded:
                try:
                    row = conn.execute(
                        "SELECT MAX(started_at) FROM _dp_internal.run_log "
                        "WHERE target = ? AND status = 'success'",
                        [full_name],
                    ).fetchone()
                    if row and row[0]:
                        last_loaded = str(row[0])
                except Exception:
                    pass

            hours_ago = None
            is_stale = last_loaded is None
            if last_loaded:
                try:
                    row = conn.execute(
                        "SELECT EXTRACT(EPOCH FROM (current_timestamp - ?::TIMESTAMP)) / 3600",
                        [last_loaded],
                    ).fetchone()
                    if row:
                        hours_ago = round(row[0], 1)
                        is_stale = hours_ago > sla_hours
                except Exception:
                    is_stale = True

            results.append({
                "source": src.name,
                "table": full_name,
                "sla_hours": sla_hours,
                "last_loaded": last_loaded,
                "hours_ago": hours_ago,
                "is_stale": is_stale,
            })
    return results


# --- Exposures ---


@app.get("/api/exposures")
def list_exposures_endpoint(request: Request) -> list[dict]:
    """List declared exposures from exposures.yml."""
    _require_permission(request, "read")
    config = _get_config()
    return [
        {
            "name": e.name,
            "description": e.description,
            "owner": e.owner,
            "depends_on": e.depends_on,
            "type": e.type,
            "url": e.url,
        }
        for e in config.exposures
    ]


# --- Autocomplete for query panel ---


@app.get("/api/autocomplete")
def get_autocomplete(request: Request, conn: DbConnReadOnly) -> dict:
    """Get table and column names for query autocomplete."""
    _require_permission(request, "read")
    tables = conn.execute(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', '_dp_internal')
        ORDER BY table_schema, table_name
        """
    ).fetchall()

    columns = conn.execute(
        """
        SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema', '_dp_internal')
        ORDER BY table_schema, table_name, ordinal_position
        """
    ).fetchall()

    return {
        "tables": [
            {"schema": t[0], "name": t[1], "full_name": f"{t[0]}.{t[1]}"}
            for t in tables
        ],
        "columns": [
            {"schema": c[0], "table": c[1], "name": c[2], "type": c[3],
             "full_name": f"{c[0]}.{c[1]}.{c[2]}"}
            for c in columns
        ],
    }


# --- Enhanced DAG with seeds, sources, and exposures ---


@app.get("/api/dag/full")
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

    # Add source nodes
    source_tables: set[str] = set()
    for src in config.sources:
        for tbl in src.tables:
            full_name = f"{src.schema}.{tbl.name}"
            source_tables.add(full_name)
            nodes.append({
                "id": full_name,
                "label": tbl.name,
                "schema": src.schema,
                "type": "source",
                "description": tbl.description or src.description,
            })

    # Add seed nodes
    from dp.engine.seeds import discover_seeds
    seeds_dir = project_dir / "seeds"
    seeds = discover_seeds(seeds_dir)
    seed_set: set[str] = set()
    for s in seeds:
        seed_set.add(s["full_name"])
        nodes.append({
            "id": s["full_name"],
            "label": s["name"],
            "schema": s["schema"],
            "type": "seed",
        })

    # Add ingest nodes for remaining external deps
    ingest_targets = _scan_ingest_targets(project_dir)
    external_deps: set[str] = set()
    for m in models:
        for dep in m.depends_on:
            if dep not in model_set and dep not in source_tables and dep not in seed_set:
                external_deps.add(dep)

    for dep in sorted(external_deps):
        for script_path in ingest_targets.get(dep, []):
            script_id = f"script:{script_path}"
            nodes.append({
                "id": script_id,
                "label": Path(script_path).name,
                "schema": "ingest",
                "type": "ingest",
                "path": script_path,
            })
            edges.append({"source": script_id, "target": dep})

        if dep not in source_tables and dep not in seed_set:
            schema = dep.split(".")[0] if "." in dep else "source"
            nodes.append({
                "id": dep,
                "label": dep,
                "schema": schema,
                "type": "source",
            })

    # Add model nodes
    for m in ordered:
        nodes.append({
            "id": m.full_name,
            "label": m.path.name,
            "schema": m.schema,
            "type": m.materialized,
            "path": str(m.path.relative_to(project_dir)),
        })

    # Add model edges
    for m in models:
        for dep in m.depends_on:
            edges.append({"source": dep, "target": m.full_name})

    # Add exposure nodes
    for exp in config.exposures:
        exp_id = f"exposure:{exp.name}"
        nodes.append({
            "id": exp_id,
            "label": exp.name,
            "schema": "exposure",
            "type": "exposure",
            "description": exp.description,
            "owner": exp.owner,
        })
        for dep in exp.depends_on:
            edges.append({"source": dep, "target": exp_id})

    return {"nodes": nodes, "edges": edges}


# --- Model notebook view ---


@app.get("/api/models/{model_name:path}/notebook-view")
def get_model_notebook_view(request: Request, model_name: str) -> dict:
    """Get a notebook-style view for a SQL model.

    Returns the SQL source (cell 1) and sample output (cell 2),
    plus lineage info for the sidebar.
    """
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

    # Read the SQL source
    sql_source = target.path.read_text()
    rel_path = str(target.path.relative_to(_get_project_dir()))

    # Try to get sample data
    sample_data = None
    db_path = _get_db_path()
    conn = None
    if db_path.exists():
        conn = connect(db_path, read_only=True)
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

    # Get lineage
    lineage = None
    try:
        lineage = extract_column_lineage(target, conn)
    except Exception:
        pass

    if conn:
        conn.close()

    # Get upstream/downstream
    upstream = target.depends_on
    downstream = [m.full_name for m in models if target.full_name in m.depends_on]

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


class CreateModelRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=200, pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$")
    schema_name: str = Field(default="bronze", pattern=r"^[a-zA-Z_][a-zA-Z0-9_]*$")
    materialized: str = Field(default="table", pattern=r"^(table|view)$")
    sql: str = Field(default="", max_length=1_000_000)


@app.post("/api/models/create")
def create_model_endpoint(request: Request, req: CreateModelRequest) -> dict:
    """Create a new SQL model file."""
    _require_permission(request, "write")
    project_dir = _get_project_dir()
    transform_dir = project_dir / "transform"
    schema_dir = transform_dir / req.schema_name

    # Ensure path is within the transform directory
    if not schema_dir.resolve().is_relative_to(transform_dir.resolve()):
        raise HTTPException(400, "Invalid schema name")

    schema_dir.mkdir(parents=True, exist_ok=True)

    model_path = schema_dir / f"{req.name}.sql"
    if model_path.exists():
        raise HTTPException(409, f"Model '{req.schema_name}.{req.name}' already exists")

    sql_content = req.sql or f"-- config: materialized={req.materialized}, schema={req.schema_name}\n\nSELECT 1 AS placeholder\n"
    if not sql_content.startswith("-- config:"):
        sql_content = f"-- config: materialized={req.materialized}, schema={req.schema_name}\n\n{sql_content}"

    model_path.write_text(sql_content)

    return {
        "status": "created",
        "path": str(model_path.relative_to(project_dir)),
        "full_name": f"{req.schema_name}.{req.name}",
    }


# --- Serve frontend ---

_FRONTEND_DIR = Path(__file__).parent.parent.parent.parent / "frontend" / "dist"


# Reserved paths that should NOT be caught by the SPA catch-all.
# This allows FastAPI's auto-generated /docs and /redoc to work.
_RESERVED_PATHS = {"docs", "redoc", "openapi.json"}


@app.get("/", response_class=HTMLResponse)
@app.get("/{path:path}", response_class=HTMLResponse)
def serve_frontend(path: str = "") -> HTMLResponse:
    """Serve the frontend SPA (skips /docs, /redoc, /openapi.json)."""
    # Let FastAPI handle its own OpenAPI routes
    if path in _RESERVED_PATHS:
        raise HTTPException(404, "Not found")

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
