"""Tests for notebook execution."""

import json
from pathlib import Path

import duckdb

from dp.engine.notebook import (
    create_notebook,
    execute_cell,
    load_notebook,
    run_notebook,
    save_notebook,
)


def test_create_and_save_notebook(tmp_path):
    """Create a notebook and save/load it."""
    nb = create_notebook("Test Notebook")
    assert nb["title"] == "Test Notebook"
    assert len(nb["cells"]) == 2

    path = tmp_path / "test.dpnb"
    save_notebook(path, nb)
    loaded = load_notebook(path)
    assert loaded["title"] == "Test Notebook"


def test_execute_cell_expression():
    """Execute a simple expression cell."""
    conn = duckdb.connect(":memory:")
    result = execute_cell(conn, "1 + 2")
    assert len(result["outputs"]) == 1
    assert result["outputs"][0]["type"] == "text"
    assert "3" in result["outputs"][0]["text"]
    conn.close()


def test_execute_cell_statement():
    """Execute a statement cell (no return value)."""
    conn = duckdb.connect(":memory:")
    result = execute_cell(conn, "x = 42\nprint(x)")
    # Should have stdout output
    has_text = any(o["type"] == "text" and "42" in o["text"] for o in result["outputs"])
    assert has_text
    conn.close()


def test_execute_cell_query():
    """Execute a DuckDB query in a cell."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE test AS SELECT 1 AS id, 'hello' AS name")
    result = execute_cell(conn, "db.execute('SELECT * FROM test').fetchall()")
    # Should capture the result as text
    assert len(result["outputs"]) > 0
    has_output = any("hello" in str(o.get("text", "")) for o in result["outputs"])
    assert has_output
    conn.close()


def test_execute_cell_error():
    """Errors are captured, not raised."""
    conn = duckdb.connect(":memory:")
    result = execute_cell(conn, "1 / 0")
    has_error = any(o["type"] == "error" for o in result["outputs"])
    assert has_error
    conn.close()


def test_run_notebook():
    """Run all cells in a notebook."""
    conn = duckdb.connect(":memory:")
    nb = {
        "title": "Test",
        "cells": [
            {"id": "c1", "type": "markdown", "source": "# Title"},
            {"id": "c2", "type": "code", "source": "x = 10", "outputs": []},
            {"id": "c3", "type": "code", "source": "x * 2", "outputs": []},
        ],
    }
    result = run_notebook(conn, nb)
    # The second code cell should have output of 20
    code_cells = [c for c in result["cells"] if c["type"] == "code"]
    assert len(code_cells) == 2
    # The shared namespace means x is available in cell 3
    last_outputs = code_cells[1]["outputs"]
    assert len(last_outputs) > 0
    assert "20" in str(last_outputs[0].get("text", ""))
    conn.close()
