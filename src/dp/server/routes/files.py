"""File browsing, editing, and git status endpoints."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from dp.server.deps import _detect_language, _get_project_dir, _require_permission

router = APIRouter()


# --- Pydantic models ---


class FileInfo(BaseModel):
    name: str
    path: str
    type: str  # "file" or "dir"
    children: list[FileInfo] | None = None


class SaveFileRequest(BaseModel):
    content: str = Field(..., max_length=5_000_000)


# --- Helpers ---


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
            items.append(
                FileInfo(
                    name=entry.name,
                    path=rel_path,
                    type="dir",
                    children=_scan_dir(base, entry.relative_to(base)),
                )
            )
        elif entry.suffix in (".sql", ".py", ".yml", ".yaml", ".dpnb", ".csv"):
            items.append(FileInfo(name=entry.name, path=rel_path, type="file"))
    return items


# --- File endpoints ---


@router.get("/api/files")
def list_files(request: Request) -> list[FileInfo]:
    """List project files."""
    _require_permission(request, "read")
    project_dir = _get_project_dir()
    return _scan_dir(project_dir)


@router.get("/api/files/{file_path:path}")
def read_file(request: Request, file_path: str) -> dict:
    """Read a file's content."""
    _require_permission(request, "read")
    full_path = _get_project_dir() / file_path
    if not full_path.exists():
        raise HTTPException(404, f"File not found: {file_path}")
    if not full_path.is_file():
        raise HTTPException(400, "Not a file")
    return {
        "path": file_path,
        "content": full_path.read_text(),
        "language": _detect_language(full_path),
    }


@router.put("/api/files/{file_path:path}")
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


@router.delete("/api/files/{file_path:path}")
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
    while (
        parent != project_dir.resolve()
        and parent.is_dir()
        and not any(parent.iterdir())
    ):
        parent.rmdir()
        parent = parent.parent
    return {"path": file_path, "status": "deleted"}


# --- Git status ---


@router.get("/api/git/status")
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
