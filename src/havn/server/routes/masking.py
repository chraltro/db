"""Masking policy CRUD endpoints (admin-only)."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from havn.server.deps import DbConn, _require_permission

router = APIRouter()


# --- Pydantic models ---


class PolicyCreate(BaseModel):
    schema_name: str = Field(..., min_length=1)
    table_name: str = Field(..., min_length=1)
    column_name: str = Field(..., min_length=1)
    method: str = Field(..., pattern=r"^(hash|redact|null|partial)$")
    method_config: dict | None = None
    condition_column: str | None = None
    condition_value: str | None = None
    exempted_roles: list[str] | None = None


class PolicyUpdate(BaseModel):
    schema_name: str | None = None
    table_name: str | None = None
    column_name: str | None = None
    method: str | None = Field(default=None, pattern=r"^(hash|redact|null|partial)$")
    method_config: dict | None = None
    condition_column: str | None = None
    condition_value: str | None = None
    exempted_roles: list[str] | None = None


# --- Endpoints ---


@router.get("/api/masking/policies")
def list_policies(request: Request, conn: DbConn) -> list[dict]:
    """List all masking policies."""
    _require_permission(request, "write")
    from havn.engine.masking import list_policies as _list

    return _list(conn)


@router.post("/api/masking/policies")
def create_policy(request: Request, req: PolicyCreate, conn: DbConn) -> dict:
    """Create a new masking policy (admin-only)."""
    _require_permission(request, "write")
    from havn.engine.masking import create_policy as _create

    try:
        return _create(
            conn,
            schema_name=req.schema_name,
            table_name=req.table_name,
            column_name=req.column_name,
            method=req.method,
            method_config=req.method_config,
            condition_column=req.condition_column,
            condition_value=req.condition_value,
            exempted_roles=req.exempted_roles,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.get("/api/masking/policies/{policy_id}")
def get_policy(request: Request, policy_id: str, conn: DbConn) -> dict:
    """Get a single masking policy by ID."""
    _require_permission(request, "write")
    from havn.engine.masking import get_policy as _get

    policy = _get(conn, policy_id)
    if not policy:
        raise HTTPException(404, "Policy not found")
    return policy


@router.put("/api/masking/policies/{policy_id}")
def update_policy(request: Request, policy_id: str, req: PolicyUpdate, conn: DbConn) -> dict:
    """Update a masking policy."""
    _require_permission(request, "write")
    from havn.engine.masking import update_policy as _update

    updates = req.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(400, "No fields to update")
    try:
        result = _update(conn, policy_id, **updates)
    except ValueError as e:
        raise HTTPException(400, str(e))
    if not result:
        raise HTTPException(404, "Policy not found")
    return result


@router.delete("/api/masking/policies/{policy_id}")
def delete_policy(request: Request, policy_id: str, conn: DbConn) -> dict:
    """Delete a masking policy."""
    _require_permission(request, "write")
    from havn.engine.masking import delete_policy as _delete

    if not _delete(conn, policy_id):
        raise HTTPException(404, "Policy not found")
    return {"status": "deleted", "id": policy_id}
