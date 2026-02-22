from __future__ import annotations

import json

import duckdb

from dp.engine.notebook import (
    create_notebook,
    load_notebook,
    run_notebook,
    save_notebook,
)


def test_create_and_save_notebook(tmp_path):
    """Create a notebook and save/load it."""
    nb = create_notebook("Test Notebook")
    assert nb["title"] == "Test Notebook"
    assert len(nb["cells"]) == 2
    # Default notebook now includes a SQL cell
    assert nb["cells"][1]["type"] == "sql"

    path = tmp_path / "test.dpnb"
    save_notebook(path, nb)
    loaded = load_notebook(path)
    assert loaded["title"] == "Test Notebook"


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


def test_run_notebook_with_sql_cells():
    """Run notebook with mixed code and SQL cells."""
    conn = duckdb.connect(":memory:")
    nb = {
        "title": "Mixed Test",
        "cells": [
            {"id": "c1", "type": "sql", "source": "CREATE TABLE t AS SELECT 42 AS val", "outputs": []},
            {"id": "c2", "type": "sql", "source": "SELECT * FROM t", "outputs": []},
            {"id": "c3", "type": "code", "source": "result = db.execute('SELECT val FROM t').fetchone()\nresult[0]", "outputs": []},
        ],
    }
    result = run_notebook(conn, nb)
    # SQL cell 2 should have table output
    sql_cell = result["cells"][1]
    assert sql_cell["type"] == "sql"
    assert len(sql_cell["outputs"]) == 1
    assert sql_cell["outputs"][0]["type"] == "table"
    assert sql_cell["outputs"][0]["rows"] == [[42]]

    # Code cell should see the table created by SQL
    code_cell = result["cells"][2]
    assert len(code_cell["outputs"]) > 0
    assert "42" in str(code_cell["outputs"][0].get("text", ""))

    # cell_results should be populated
    assert "cell_results" in result
    assert len(result["cell_results"]) == 3
    conn.close()


def test_run_notebook_with_ingest_cells(tmp_path):
    """Run notebook containing an ingest cell."""
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("x,y\n1,2\n3,4\n")

    conn = duckdb.connect(":memory:")
    nb = {
        "title": "Ingest Test",
        "cells": [
            {
                "id": "c1",
                "type": "ingest",
                "source": json.dumps({
                    "source_type": "csv",
                    "source_path": str(csv_path),
                    "target_schema": "landing",
                    "target_table": "xy_data",
                }),
                "outputs": [],
            },
            {
                "id": "c2",
                "type": "sql",
                "source": "SELECT * FROM landing.xy_data",
                "outputs": [],
            },
        ],
    }
    result = run_notebook(conn, nb, project_dir=tmp_path)
    # Ingest cell should succeed
    assert not any(o["type"] == "error" for o in result["cells"][0]["outputs"])
    # SQL cell should see the data
    sql_out = result["cells"][1]["outputs"]
    assert len(sql_out) == 1
    assert sql_out[0]["type"] == "table"
    assert sql_out[0]["total_rows"] == 2
    conn.close()


def test_run_notebook_cell_error_does_not_stop_execution():
    """An error in one cell doesn't prevent subsequent cells from running."""
    conn = duckdb.connect(":memory:")
    nb = {
        "title": "Error Test",
        "cells": [
            {"id": "c1", "type": "sql", "source": "SELECT * FROM nonexistent", "outputs": []},
            {"id": "c2", "type": "sql", "source": "SELECT 42 AS answer", "outputs": []},
        ],
    }
    result = run_notebook(conn, nb)
    # First cell should have error
    assert any(o["type"] == "error" for o in result["cells"][0]["outputs"])
    # Second cell should still execute
    assert result["cells"][1]["outputs"][0]["type"] == "table"
    assert result["cells"][1]["outputs"][0]["rows"] == [[42]]
    # cell_results should reflect both
    assert result["cell_results"][0]["has_error"] is True
    assert result["cell_results"][1]["has_error"] is False
    conn.close()


def test_run_notebook_with_all_cell_types(tmp_path):
    """Run notebook with code, sql, ingest, and markdown cells."""
    csv_path = tmp_path / "test.csv"
    csv_path.write_text("a,b\n1,2\n")

    conn = duckdb.connect(":memory:")
    nb = {
        "title": "All Types",
        "cells": [
            {"id": "c1", "type": "markdown", "source": "# Title"},
            {"id": "c2", "type": "code", "source": "x = 10", "outputs": []},
            {"id": "c3", "type": "sql", "source": "SELECT 1 AS val", "outputs": []},
            {
                "id": "c4",
                "type": "ingest",
                "source": json.dumps({
                    "source_type": "csv",
                    "source_path": str(csv_path),
                    "target_table": "test_data",
                }),
                "outputs": [],
            },
        ],
    }
    result = run_notebook(conn, nb, project_dir=tmp_path)
    # Markdown cells are skipped — only 3 cell_results
    assert len(result["cell_results"]) == 3
    # All should succeed
    assert all(not cr["has_error"] for cr in result["cell_results"])
    assert result["last_run_ms"] >= 0
    conn.close()
