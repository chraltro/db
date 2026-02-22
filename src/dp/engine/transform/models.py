"""Data classes for the SQL transformation engine."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path


def _hash_content(content: str) -> str:
    """Hash SQL content for change detection. Normalizes whitespace."""
    normalized = re.sub(r"\s+", " ", content.strip())
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


@dataclass
class AssertionResult:
    """Result of a data quality assertion."""

    expression: str
    passed: bool
    detail: str = ""


@dataclass
class ProfileResult:
    """Auto-computed profile stats for a model after execution."""

    row_count: int = 0
    column_count: int = 0
    null_percentages: dict[str, float] = field(default_factory=dict)
    distinct_counts: dict[str, int] = field(default_factory=dict)


@dataclass
class SQLModel:
    """A single SQL transformation model."""

    path: Path
    name: str  # e.g. "customers"
    schema: str  # e.g. "bronze"
    full_name: str  # e.g. "bronze.customers"
    sql: str  # raw SQL content
    query: str  # SQL without config comments
    materialized: str  # "view", "table", or "incremental"
    depends_on: list[str] = field(default_factory=list)
    description: str = ""
    column_docs: dict[str, str] = field(default_factory=dict)
    content_hash: str = ""
    upstream_hash: str = ""
    assertions: list[str] = field(default_factory=list)
    unique_key: str | None = None  # For incremental models
    incremental_strategy: str = "delete+insert"  # "delete+insert", "append", or "merge"
    incremental_filter: str | None = None  # e.g. "WHERE updated_at > (SELECT MAX(updated_at) FROM {this})"
    partition_by: str | None = None  # e.g. "event_date" — enables partition-based pruning

    def __post_init__(self) -> None:
        self.content_hash = _hash_content(self.query)


@dataclass
class ModelResult:
    """Full result from executing a single model."""

    status: str  # "built", "skipped", "error"
    duration_ms: int = 0
    row_count: int = 0
    error: str | None = None
    assertions: list[AssertionResult] = field(default_factory=list)
    profile: ProfileResult | None = None


@dataclass
class ValidationError:
    """A single validation error found during compile-time check."""

    model: str
    severity: str  # "error" or "warning"
    message: str
    line: int | None = None
