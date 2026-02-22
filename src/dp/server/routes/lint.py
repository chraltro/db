"""SQL linting endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from dp.server.deps import _get_config, _get_project_dir, _require_permission

router = APIRouter()


# --- Pydantic models ---


class LintFileRequest(BaseModel):
    path: str = Field(..., max_length=1000)
    fix: bool = False
    content: str | None = Field(None, max_length=1_000_000)


class LintConfigRequest(BaseModel):
    content: str = Field(..., max_length=100_000)


# --- Lint endpoints ---


@router.post("/api/lint")
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


@router.post("/api/lint/file")
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
    return {
        "count": count,
        "violations": violations,
        "fixed": fixed,
        "content": new_content,
    }


@router.get("/api/lint/config")
def get_lint_config(request: Request) -> dict:
    """Get the .sqlfluff config file contents."""
    _require_permission(request, "read")
    sqlfluff_path = _get_project_dir() / ".sqlfluff"
    if not sqlfluff_path.exists():
        return {"exists": False, "content": ""}
    return {"exists": True, "content": sqlfluff_path.read_text()}


@router.put("/api/lint/config")
def save_lint_config(request: Request, req: LintConfigRequest) -> dict:
    """Save the .sqlfluff config file."""
    _require_permission(request, "write")
    sqlfluff_path = _get_project_dir() / ".sqlfluff"
    sqlfluff_path.write_text(req.content)
    return {"status": "saved"}


@router.delete("/api/lint/config")
def delete_lint_config(request: Request) -> dict:
    """Delete the .sqlfluff config file (revert to defaults)."""
    _require_permission(request, "write")
    sqlfluff_path = _get_project_dir() / ".sqlfluff"
    if sqlfluff_path.exists():
        sqlfluff_path.unlink()
    return {"status": "deleted"}
