"""Shared dependencies, helpers, and utilities for the server routes."""

from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from typing import Annotated, Any, Generator

import duckdb
from fastapi import Depends, HTTPException, Request
from pydantic import BaseModel, Field

from dp.config import load_project
from dp.engine.database import connect, ensure_meta_table
from dp.engine.transform import build_dag, discover_models, run_transform

logger = logging.getLogger("dp.server")


# ---------------------------------------------------------------------------
# State accessors (globals live in app.py for backward compat)
# ---------------------------------------------------------------------------


def _get_project_dir() -> Path:
    from dp.server.app import PROJECT_DIR

    return PROJECT_DIR


def _get_active_env() -> str | None:
    from dp.server.app import ACTIVE_ENV

    return ACTIVE_ENV


def _get_auth_enabled() -> bool:
    from dp.server.app import AUTH_ENABLED

    return AUTH_ENABLED


def _set_active_env(env: str) -> None:
    import dp.server.app as _app

    _app.ACTIVE_ENV = env


# ---------------------------------------------------------------------------
# Config cache
# ---------------------------------------------------------------------------

_config_cache: dict[str, Any] = {"config": None, "mtime": 0.0, "path": None}


def _get_config_cached():
    """Load project config with file-mtime-based caching."""
    active_env = _get_active_env()
    config_path = _get_project_dir() / "project.yml"
    try:
        mtime = config_path.stat().st_mtime
    except FileNotFoundError:
        return load_project(_get_project_dir(), env=active_env)

    cache_key = f"{config_path}:{active_env}"
    if (
        _config_cache["config"] is not None
        and _config_cache["path"] == cache_key
        and _config_cache["mtime"] == mtime
    ):
        return _config_cache["config"]

    config = load_project(_get_project_dir(), env=active_env)
    _config_cache["config"] = config
    _config_cache["mtime"] = mtime
    _config_cache["path"] = cache_key
    return config


def _get_config():
    return _get_config_cached()


def _get_db_path() -> Path:
    config = _get_config()
    return _get_project_dir() / config.database.path


def invalidate_config_cache() -> None:
    """Invalidate the config cache (e.g. after environment switch)."""
    _config_cache["config"] = None


# ---------------------------------------------------------------------------
# Model discovery cache
# ---------------------------------------------------------------------------

_MODEL_CACHE_VERSION = 2
_model_cache: dict[str, Any] = {
    "models": None,
    "mtime_map": None,
    "transform_dir": None,
    "version": None,
}


def _discover_models_cached(transform_dir: Path):
    """Discover models with file-mtime-based caching."""
    if not transform_dir.exists():
        return []

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


# ---------------------------------------------------------------------------
# Identifier validation
# ---------------------------------------------------------------------------


def _validate_identifier(value: str, label: str = "identifier") -> str:
    """Validate that a value is a safe SQL identifier (no injection)."""
    from dp.engine.utils import validate_identifier

    try:
        return validate_identifier(value, label)
    except ValueError:
        raise HTTPException(400, f"Invalid {label}: {value!r}")


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


def _require_db(db_path: Path) -> None:
    """Raise 404 if the warehouse database doesn't exist yet."""
    if not db_path.exists():
        raise HTTPException(404, "Warehouse database not found. Run a pipeline first.")


# ---------------------------------------------------------------------------
# Database dependency injection
# ---------------------------------------------------------------------------


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


def get_db_readonly_optional() -> Generator[duckdb.DuckDBPyConnection | None, None, None]:
    """FastAPI dependency: yields a read-only DuckDB connection, or None if DB doesn't exist."""
    db_path = _get_db_path()
    if not db_path.exists():
        yield None
        return
    conn = connect(db_path, read_only=True)
    try:
        yield conn
    finally:
        conn.close()


DbConn = Annotated[duckdb.DuckDBPyConnection, Depends(get_db)]
DbConnReadOnly = Annotated[duckdb.DuckDBPyConnection, Depends(get_db_readonly)]
DbConnReadOnlyOptional = Annotated[
    duckdb.DuckDBPyConnection | None, Depends(get_db_readonly_optional)
]


# ---------------------------------------------------------------------------
# Authentication & authorization
# ---------------------------------------------------------------------------


def _get_user(request: Request) -> dict | None:
    """Extract and validate user from auth header. Returns None if auth disabled."""
    if not _get_auth_enabled():
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


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------

_login_attempts: dict[str, list[float]] = {}
_RATE_LIMIT_WINDOW = 60.0
_RATE_LIMIT_MAX = 5
_RATE_LIMIT_MAX_KEYS = 10_000


def _check_rate_limit(key: str) -> None:
    """Enforce rate limiting. Raises 429 if too many attempts."""
    now = time.time()
    attempts = _login_attempts.get(key, [])
    attempts = [t for t in attempts if now - t < _RATE_LIMIT_WINDOW]
    if len(attempts) >= _RATE_LIMIT_MAX:
        logger.warning("Rate limit exceeded for %s", key)
        raise HTTPException(429, "Too many login attempts. Try again later.")
    attempts.append(now)
    _login_attempts[key] = attempts
    if len(_login_attempts) > _RATE_LIMIT_MAX_KEYS:
        stale = [
            k
            for k, v in _login_attempts.items()
            if not v or now - v[-1] > _RATE_LIMIT_WINDOW
        ]
        for k in stale:
            del _login_attempts[k]


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------


def _serialize(value: Any) -> Any:
    """Make values JSON-serializable."""
    if value is None:
        return None
    if isinstance(value, (int, float, str, bool)):
        return value
    return str(value)


def _detect_language(path: Path) -> str:
    return {
        "sql": "sql",
        "py": "python",
        "yml": "yaml",
        "yaml": "yaml",
        "dpnb": "json",
    }.get(path.suffix.lstrip("."), "text")
