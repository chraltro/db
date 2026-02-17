"""Secrets management.

Loads secrets from .env file (never committed), makes them available to scripts
via environment variables. Masks secret values in logs and API responses.
"""

from __future__ import annotations

import os
import re
from pathlib import Path


def load_env(project_dir: Path) -> dict[str, str]:
    """Load secrets from .env file into os.environ. Returns loaded keys."""
    env_path = project_dir / ".env"
    loaded: dict[str, str] = {}
    if not env_path.exists():
        return loaded

    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        # Strip surrounding quotes
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        os.environ[key] = value
        loaded[key] = value

    return loaded


def list_secrets(project_dir: Path) -> list[dict]:
    """List secret keys from .env (values are never returned)."""
    env_path = project_dir / ".env"
    secrets = []
    if not env_path.exists():
        return secrets

    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        secrets.append({
            "key": key,
            "is_set": bool(value),
            "masked_value": _mask(value),
        })

    return secrets


def set_secret(project_dir: Path, key: str, value: str) -> None:
    """Set or update a secret in .env file."""
    env_path = project_dir / ".env"
    lines: list[str] = []
    found = False

    if env_path.exists():
        for line in env_path.read_text().splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#") and "=" in stripped:
                existing_key = stripped.split("=", 1)[0].strip()
                if existing_key == key:
                    lines.append(f'{key}="{value}"')
                    found = True
                    continue
            lines.append(line)

    if not found:
        lines.append(f'{key}="{value}"')

    env_path.write_text("\n".join(lines) + "\n")
    os.environ[key] = value


def delete_secret(project_dir: Path, key: str) -> bool:
    """Delete a secret from .env file. Returns True if found."""
    env_path = project_dir / ".env"
    if not env_path.exists():
        return False

    lines: list[str] = []
    found = False
    for line in env_path.read_text().splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            existing_key = stripped.split("=", 1)[0].strip()
            if existing_key == key:
                found = True
                continue
        lines.append(line)

    if found:
        env_path.write_text("\n".join(lines) + "\n")
        os.environ.pop(key, None)

    return found


def _mask(value: str) -> str:
    """Mask a secret value for display."""
    if not value:
        return "(empty)"
    if len(value) <= 4:
        return "*" * len(value)
    return value[:2] + "*" * (len(value) - 4) + value[-2:]


def mask_output(text: str, project_dir: Path) -> str:
    """Mask any secret values that appear in text output."""
    env_path = project_dir / ".env"
    if not env_path.exists():
        return text

    for line in env_path.read_text().splitlines():
        line_stripped = line.strip()
        if not line_stripped or line_stripped.startswith("#") or "=" not in line_stripped:
            continue
        _, _, value = line_stripped.partition("=")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        if len(value) >= 4:  # Only mask non-trivial values
            text = text.replace(value, "***")

    return text


ENV_TEMPLATE = """\
# Secrets for dp project
# This file should NEVER be committed to version control.
# Add your API keys, database passwords, etc. here.
# Reference them in project.yml as ${VARIABLE_NAME}
#
# Example:
# POSTGRES_USER="myuser"
# POSTGRES_PASSWORD="mypassword"
# API_KEY="sk-..."
"""
