"""Notebook output extraction: infer table names produced by a notebook."""

from __future__ import annotations

import json
import re


def extract_notebook_outputs(notebook: dict) -> list[str]:
    """Extract declared output tables from a notebook.

    Notebooks can declare outputs via a top-level "outputs" key or
    by scanning SQL/ingest cells for table creation patterns.

    Returns list of fully-qualified table names (e.g. ["landing.earthquakes"]).
    """
    # Explicit declaration
    declared = notebook.get("outputs", [])
    if declared:
        return declared

    # Infer from cells
    outputs: set[str] = set()

    create_pattern = re.compile(
        r"(?:CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW)\s+|INTO\s+)"
        r"(\w+\.\w+)",
        re.IGNORECASE,
    )

    for cell in notebook.get("cells", []):
        cell_type = cell.get("type", "")
        source = cell.get("source", "")

        if cell_type == "sql":
            for match in create_pattern.finditer(source):
                outputs.add(match.group(1).lower())

        elif cell_type == "ingest":
            try:
                spec = json.loads(source)
                schema = spec.get("target_schema", "landing")
                table = spec.get("target_table", "")
                if table:
                    outputs.add(f"{schema}.{table}")
            except (json.JSONDecodeError, TypeError):
                pass

        elif cell_type == "code":
            for match in create_pattern.finditer(source):
                outputs.add(match.group(1).lower())

    return sorted(outputs)
