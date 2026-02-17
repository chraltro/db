"""Tests for the Python script runner."""

from pathlib import Path

import duckdb

from dp.engine.database import ensure_meta_table
from dp.engine.runner import run_script


def test_run_script_success(tmp_path):
    """A valid script with run(db) should execute successfully."""
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
    """A script without run() should fail with a clear error."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))

    script = tmp_path / "no_run.py"
    script.write_text('x = 1\n')

    result = run_script(conn, script, "ingest")
    assert result["status"] == "error"
    assert "no run()" in result["error"]
    conn.close()
