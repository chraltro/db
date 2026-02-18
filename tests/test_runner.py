"""Tests for the Python script runner."""

import json
from pathlib import Path

import duckdb

from dp.engine.database import ensure_meta_table
from dp.engine.runner import run_script, run_scripts_in_dir


def test_run_script_success(tmp_path):
    """A valid script with run(db) should execute successfully (backward compat)."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))

    script = tmp_path / "test_ingest.py"
    script.write_text(
        'import duckdb\n\n'
        'def run(db):\n'
        '    db.execute("CREATE SCHEMA IF NOT EXISTS landing")\n'
        '    db.execute("CREATE TABLE landing.test AS SELECT 42 AS val")\n'
        '    print("done")\n'
    )

    result = run_script(conn, script, "ingest")
    assert result["status"] == "success"
    assert result["duration_ms"] >= 0
    assert "done" in result["log_output"]

    # Verify the table was created
    row = conn.execute("SELECT val FROM landing.test").fetchone()
    assert row[0] == 42
    conn.close()


def test_run_script_error(tmp_path):
    """A script that raises an exception should be captured."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))

    script = tmp_path / "bad_script.py"
    script.write_text(
        'def run(db):\n'
        '    raise ValueError("something went wrong")\n'
    )

    result = run_script(conn, script, "ingest")
    assert result["status"] == "error"
    assert "something went wrong" in result["error"]
    conn.close()


def test_run_script_no_run_function(tmp_path):
    """A script without run() should succeed as top-level code."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))

    script = tmp_path / "no_run.py"
    script.write_text('x = 1\n')

    result = run_script(conn, script, "ingest")
    assert result["status"] == "success"
    conn.close()


def test_run_script_top_level(tmp_path):
    """A top-level script using db should execute and create tables."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))

    script = tmp_path / "top_level.py"
    script.write_text(
        'db.execute("CREATE SCHEMA IF NOT EXISTS landing")\n'
        'db.execute("CREATE TABLE landing.top AS SELECT 99 AS val")\n'
        'print("top-level done")\n'
    )

    result = run_script(conn, script, "ingest")
    assert result["status"] == "success"
    assert "top-level done" in result["log_output"]

    row = conn.execute("SELECT val FROM landing.top").fetchone()
    assert row[0] == 99
    conn.close()


def test_run_notebook_as_script(tmp_path):
    """A .dpnb notebook should run as a pipeline step."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))

    notebook = {
        "title": "Test Notebook",
        "cells": [
            {
                "id": "cell_1",
                "type": "code",
                "source": 'db.execute("CREATE SCHEMA IF NOT EXISTS landing")',
                "outputs": [],
            },
            {
                "id": "cell_2",
                "type": "code",
                "source": 'db.execute("CREATE TABLE landing.nb_test AS SELECT 77 AS val")',
                "outputs": [],
            },
        ],
    }

    nb_path = tmp_path / "ingest_nb.dpnb"
    nb_path.write_text(json.dumps(notebook))

    result = run_script(conn, nb_path, "ingest")
    assert result["status"] == "success"

    row = conn.execute("SELECT val FROM landing.nb_test").fetchone()
    assert row[0] == 77
    conn.close()


def test_run_scripts_in_dir_discovers_notebooks(tmp_path):
    """run_scripts_in_dir should discover both .py and .dpnb files."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))

    ingest_dir = tmp_path / "ingest"
    ingest_dir.mkdir()

    # A .py script
    (ingest_dir / "a_script.py").write_text(
        'db.execute("CREATE SCHEMA IF NOT EXISTS landing")\n'
    )

    # A .dpnb notebook
    notebook = {
        "title": "NB",
        "cells": [
            {
                "id": "c1",
                "type": "code",
                "source": 'db.execute("CREATE SCHEMA IF NOT EXISTS landing")',
                "outputs": [],
            },
        ],
    }
    (ingest_dir / "b_notebook.dpnb").write_text(json.dumps(notebook))

    # A skipped file
    (ingest_dir / "_skip.py").write_text('x = 1\n')

    results = run_scripts_in_dir(conn, ingest_dir, "ingest")
    assert len(results) == 2
    assert all(r["status"] == "success" for r in results)
    conn.close()
