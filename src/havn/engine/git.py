"""Git detection utilities.

All functions shell out to the git CLI via subprocess. No Python git libraries.
Every function returns None/empty gracefully if not a git repo or git is not installed.
"""

from __future__ import annotations

import subprocess
from pathlib import Path


def _run_git(project_dir: Path, *args: str) -> subprocess.CompletedProcess:
    """Run a git command and return the result."""
    try:
        return subprocess.run(
            ["git", *args],
            cwd=project_dir,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return subprocess.CompletedProcess(args=["git", *args], returncode=1, stdout="", stderr="")


def is_git_repo(project_dir: Path) -> bool:
    """Check if the directory is inside a git repository."""
    result = _run_git(project_dir, "rev-parse", "--is-inside-work-tree")
    return result.returncode == 0 and result.stdout.strip() == "true"


def current_branch(project_dir: Path) -> str | None:
    """Get the current branch name."""
    if not is_git_repo(project_dir):
        return None
    result = _run_git(project_dir, "rev-parse", "--abbrev-ref", "HEAD")
    if result.returncode != 0:
        return None
    branch = result.stdout.strip()
    return branch if branch else None


def is_dirty(project_dir: Path) -> bool:
    """Check if there are uncommitted changes."""
    if not is_git_repo(project_dir):
        return False
    result = _run_git(project_dir, "status", "--porcelain")
    return bool(result.stdout.strip())


def changed_files(project_dir: Path, ref: str = "HEAD") -> list[str]:
    """Get files changed since ref (modified, added, deleted)."""
    if not is_git_repo(project_dir):
        return []
    # Combine staged + unstaged + untracked
    files: set[str] = set()

    # Modified/added/deleted vs ref
    result = _run_git(project_dir, "diff", "--name-only", ref)
    if result.returncode == 0:
        files.update(f for f in result.stdout.strip().split("\n") if f)

    # Staged changes
    result = _run_git(project_dir, "diff", "--cached", "--name-only")
    if result.returncode == 0:
        files.update(f for f in result.stdout.strip().split("\n") if f)

    # Untracked files
    result = _run_git(project_dir, "ls-files", "--others", "--exclude-standard")
    if result.returncode == 0:
        files.update(f for f in result.stdout.strip().split("\n") if f)

    return sorted(files)


def last_commit_hash(project_dir: Path) -> str | None:
    """Get the last commit hash."""
    if not is_git_repo(project_dir):
        return None
    result = _run_git(project_dir, "rev-parse", "HEAD")
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def last_commit_message(project_dir: Path) -> str | None:
    """Get the last commit message."""
    if not is_git_repo(project_dir):
        return None
    result = _run_git(project_dir, "log", "-1", "--format=%s")
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def diff_files_between(project_dir: Path, base_ref: str, head_ref: str) -> list[str]:
    """Get files changed between two refs."""
    if not is_git_repo(project_dir):
        return []
    result = _run_git(project_dir, "diff", "--name-only", f"{base_ref}...{head_ref}")
    if result.returncode != 0:
        # Fall back to two-dot diff
        result = _run_git(project_dir, "diff", "--name-only", base_ref, head_ref)
        if result.returncode != 0:
            return []
    return [f for f in result.stdout.strip().split("\n") if f]
