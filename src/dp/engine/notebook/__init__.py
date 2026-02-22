"""Notebook-style execution with cell-by-cell output.

Notebooks are stored as .dpnb files (JSON format).
Cell types:
  - code:     Python code (shared namespace with `db` and `pd`)
  - markdown: Rendered text (not executed)
  - sql:      Pure SQL executed against DuckDB, results auto-rendered as table
  - ingest:   Structured data ingestion (source → landing table)

This package re-exports all public symbols so existing imports continue to work:
    from dp.engine.notebook import run_notebook, execute_cell, ...
"""

from __future__ import annotations

# I/O
from .io import create_notebook, load_notebook, save_notebook

# Cell execution
from .code_cell import execute_cell
from .ingest_cell import _resolve_path, execute_ingest_cell
from .sql_cell import _split_sql_statements, execute_sql_cell

# Re-export validate_identifier for backward compatibility
from dp.engine.utils import validate_identifier as _validate_identifier

# Formatting
from .formatting import _format_dataframe, _format_result, _serialize

# Runner
from .runner import run_notebook

# Conversion
from .conversion import model_to_notebook, promote_sql_to_model

# Debug
from .debug import generate_debug_notebook

# Outputs
from .outputs import extract_notebook_outputs

__all__ = [
    # I/O
    "create_notebook",
    "load_notebook",
    "save_notebook",
    # Cell execution
    "execute_cell",
    "execute_ingest_cell",
    "execute_sql_cell",
    # Runner
    "run_notebook",
    # Conversion
    "model_to_notebook",
    "promote_sql_to_model",
    # Debug
    "generate_debug_notebook",
    # Outputs
    "extract_notebook_outputs",
]
