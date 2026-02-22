"""SQL transformation engine.

Parses SQL files with config comments, builds a DAG, executes in dependency order.
Handles change detection via content hashing, incremental models, data quality
assertions, auto-profiling, freshness monitoring, and parallel execution.

This package re-exports all public symbols so existing imports continue to work:
    from dp.engine.transform import run_transform, discover_models, SQLModel, ...
"""

from __future__ import annotations

# Data models
from .models import (
    AssertionResult,
    ModelResult,
    ProfileResult,
    SQLModel,
    ValidationError,
    _hash_content,
)

# Discovery and DAG
from .discovery import (
    _compute_upstream_hash,
    _has_changed,
    _update_state,
    build_dag,
    build_dag_tiers,
    discover_models,
)

# Data quality and profiling
from .quality import (
    _evaluate_assertion,
    _save_assertions,
    _save_profile,
    profile_model,
    run_assertions,
)

# Execution
from .execution import (
    _execute_incremental,
    _execute_single_model,
    execute_model,
)

# Analysis, validation, lineage, freshness
from .analysis import (
    check_freshness,
    extract_column_lineage,
    impact_analysis,
    validate_models,
)

# Orchestration
from .orchestration import (
    run_transform,
)

__all__ = [
    # Models
    "AssertionResult",
    "ModelResult",
    "ProfileResult",
    "SQLModel",
    "ValidationError",
    # Discovery
    "build_dag",
    "build_dag_tiers",
    "discover_models",
    # Quality
    "profile_model",
    "run_assertions",
    # Execution
    "execute_model",
    "run_transform",
    # Analysis
    "check_freshness",
    "extract_column_lineage",
    "impact_analysis",
    "validate_models",
]
