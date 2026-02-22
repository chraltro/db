"""Output formatting and serialization for notebook cells."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("dp.notebook")


def _serialize(value: Any) -> Any:
    """Make values JSON-serializable."""
    if value is None:
        return None
    if isinstance(value, (int, float, str, bool)):
        return value
    return str(value)


def _format_result(result: Any) -> dict:
    """Format a cell result for display."""
    # DuckDB result
    if hasattr(result, "fetchdf"):
        try:
            df = result.fetchdf()
            return _format_dataframe(df)
        except Exception:
            return {"type": "text", "text": str(result)}

    # Pandas DataFrame
    try:
        import pandas as pd
        if isinstance(result, pd.DataFrame):
            return _format_dataframe(result)
        if isinstance(result, pd.Series):
            return _format_dataframe(result.to_frame())
    except ImportError:
        pass

    # DuckDB relation
    if hasattr(result, "columns") and hasattr(result, "fetchall"):
        try:
            columns = result.columns
            rows = result.fetchall()
            return {
                "type": "table",
                "columns": columns,
                "rows": [[_serialize(v) for v in row] for row in rows[:500]],
                "total_rows": len(rows),
            }
        except Exception as e:
            logger.debug("Result serialization fallback: %s", e)

    # Plain text
    return {"type": "text", "text": repr(result)}


def _format_dataframe(df) -> dict:
    """Format a pandas DataFrame for display."""
    columns = list(df.columns)
    rows = []
    for _, row in df.head(500).iterrows():
        rows.append([_serialize(v) for v in row])
    return {
        "type": "table",
        "columns": columns,
        "rows": rows,
        "total_rows": len(df),
    }
