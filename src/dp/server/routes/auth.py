"""Authentication, user management, and secrets endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from dp.server.deps import (
    DbConn,
    _check_rate_limit,
    _get_config,
    _get_project_dir,
    _require_permission,
    _require_user,
    _get_auth_enabled,
)

router = APIRouter()


# --- Pydantic models ---


class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=100)
    password: str = Field(..., min_length=1, max_length=500)


class CreateUserRequest(BaseModel):
    username: str = Field(
        ..., min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9_.-]+$"
    )
    password: str = Field(..., min_length=4, max_length=500)
    role: str = Field(default="viewer", pattern=r"^(admin|editor|viewer)$")
    display_name: str | None = Field(default=None, max_length=200)


class UpdateUserRequest(BaseModel):
    role: str | None = Field(default=None, pattern=r"^(admin|editor|viewer)$")
    password: str | None = Field(default=None, min_length=4, max_length=500)
    display_name: str | None = Field(default=None, max_length=200)


class SetSecretRequest(BaseModel):
    key: str = Field(
        ..., min_length=1, max_length=200, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$"
    )
    value: str = Field(..., max_length=10_000)


# --- Auth endpoints ---


@router.post("/api/auth/login")
def login(request: Request, req: LoginRequest, conn: DbConn) -> dict:
    """Authenticate and get a token (rate-limited)."""
    client_ip = request.client.host if request.client else "unknown"
    _check_rate_limit(f"login:{client_ip}")
    from dp.engine.auth import authenticate

    token = authenticate(conn, req.username, req.password)
    if not token:
        raise HTTPException(401, "Invalid credentials")
    return {"token": token, "username": req.username}


@router.get("/api/auth/me")
def get_current_user(request: Request) -> dict:
    """Get current authenticated user."""
    return _require_user(request)


@router.get("/api/auth/status")
def get_auth_status(conn: DbConn) -> dict:
    """Check if auth is enabled and if initial setup is needed."""
    if not _get_auth_enabled():
        return {"auth_enabled": False, "needs_setup": False}
    from dp.engine.auth import has_any_users

    return {"auth_enabled": True, "needs_setup": not has_any_users(conn)}


@router.post("/api/auth/setup")
def initial_setup(req: CreateUserRequest, conn: DbConn) -> dict:
    """Create the first admin user (only works when no users exist)."""
    from dp.engine.auth import authenticate, create_user, has_any_users

    if has_any_users(conn):
        raise HTTPException(400, "Setup already completed")
    create_user(conn, req.username, req.password, "admin", req.display_name)
    token = authenticate(conn, req.username, req.password)
    return {"token": token, "username": req.username, "role": "admin"}


# --- User management ---


@router.get("/api/users")
def list_users(request: Request, conn: DbConn) -> list[dict]:
    """List all users (admin only)."""
    _require_permission(request, "manage_users")
    from dp.engine.auth import list_users as _list_users

    return _list_users(conn)


@router.post("/api/users")
def create_user_endpoint(
    request: Request, req: CreateUserRequest, conn: DbConn
) -> dict:
    """Create a new user (admin only)."""
    _require_permission(request, "manage_users")
    from dp.engine.auth import create_user

    try:
        return create_user(conn, req.username, req.password, req.role, req.display_name)
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.put("/api/users/{username}")
def update_user_endpoint(
    request: Request, username: str, req: UpdateUserRequest, conn: DbConn
) -> dict:
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


@router.delete("/api/users/{username}")
def delete_user_endpoint(request: Request, username: str, conn: DbConn) -> dict:
    """Delete a user (admin only)."""
    _require_permission(request, "manage_users")
    from dp.engine.auth import delete_user

    found = delete_user(conn, username)
    if not found:
        raise HTTPException(404, f"User '{username}' not found")
    return {"status": "deleted"}


# --- Secrets management ---


@router.get("/api/secrets")
def list_secrets(request: Request) -> list[dict]:
    """List secrets (keys and masked values only)."""
    _require_permission(request, "manage_secrets")
    from dp.engine.secrets import list_secrets as _list_secrets

    return _list_secrets(_get_project_dir())


@router.post("/api/secrets")
def set_secret(request: Request, req: SetSecretRequest) -> dict:
    """Set or update a secret."""
    _require_permission(request, "manage_secrets")
    from dp.engine.secrets import set_secret as _set_secret

    _set_secret(_get_project_dir(), req.key, req.value)
    return {"status": "set", "key": req.key}


@router.delete("/api/secrets/{key}")
def delete_secret(request: Request, key: str) -> dict:
    """Delete a secret."""
    _require_permission(request, "manage_secrets")
    from dp.engine.secrets import delete_secret as _delete_secret

    found = _delete_secret(_get_project_dir(), key)
    if not found:
        raise HTTPException(404, f"Secret '{key}' not found")
    return {"status": "deleted", "key": key}
