"""File browsing, editing, and git status endpoints."""

from __future__ import annotations

import re
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request
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
    try:
        content = full_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        try:
            content = full_path.read_text(encoding="latin-1")
        except Exception:
            raise HTTPException(422, "Cannot read file: unsupported encoding")
    return {
        "path": file_path,
        "content": content,
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
def delete_file(
    request: Request,
    file_path: str,
    drop_object: bool = Query(False),
) -> dict:
    """Delete a file, optionally dropping the corresponding database object."""
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

    dropped = None
    if drop_object:
        dropped = _drop_db_object(full_path, file_path)

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
    result: dict = {"path": file_path, "status": "deleted"}
    if dropped:
        result["dropped"] = dropped
    return result


def _drop_db_object(full_path: Path, file_path: str) -> str | None:
    """Drop the DuckDB object corresponding to a transform SQL or seed CSV file."""
    from dp.engine.database import connect
    from dp.engine.utils import validate_identifier
    from dp.server.deps import _get_db_path

    normalized = file_path.replace("\\", "/")

    if full_path.suffix == ".sql" and normalized.startswith("transform/"):
        # Derive schema and name from file path / content
        name = full_path.stem
        # Default schema from folder: transform/<schema>/<name>.sql
        parts = normalized.split("/")
        schema = parts[1] if len(parts) >= 3 else "bronze"
        # Check for -- config: schema=<override> in file content
        try:
            content = full_path.read_text(encoding="utf-8")
            m = re.search(r"--\s*config:.*schema\s*=\s*(\w+)", content)
            if m:
                schema = m.group(1)
        except Exception:
            pass
    elif full_path.suffix == ".csv" and normalized.startswith("seeds/"):
        schema = "seeds"
        name = full_path.stem
    else:
        return None

    try:
        validate_identifier(schema, "schema")
        validate_identifier(name, "table name")
    except ValueError:
        return None

    db_path = _get_db_path()
    if not db_path.exists():
        return None

    conn = connect(db_path)
    try:
        # Look up the object type in information_schema
        rows = conn.execute(
            "SELECT table_type FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, name],
        ).fetchall()

        if not rows:
            return None

        table_type = rows[0][0]
        obj_kind = "VIEW" if "VIEW" in table_type.upper() else "TABLE"
        conn.execute(f'DROP {obj_kind} IF EXISTS "{schema}"."{name}"')

        # Clean up model_state metadata
        try:
            conn.execute(
                "DELETE FROM _dp_internal.model_state WHERE model_name = ?",
                [f"{schema}.{name}"],
            )
        except Exception:
            pass  # table may not exist yet

        return f"{schema}.{name}"
    finally:
        conn.close()


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
