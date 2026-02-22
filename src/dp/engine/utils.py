"""Shared utility functions for the dp engine layer."""

from __future__ import annotations

import re

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(value: str, label: str = "identifier") -> str:
    """Validate that a value is a safe SQL identifier.

    Only allows alphanumeric characters and underscores, starting with a letter
    or underscore. Raises ValueError if the identifier is unsafe.

    This is the single validation point for SQL identifiers used across the
    engine (transform model discovery, notebook cells, API endpoints).
    """
    if not _IDENTIFIER_RE.match(value):
        raise ValueError(f"Invalid {label}: {value!r} (must match [A-Za-z_][A-Za-z0-9_]*)")
    return value
