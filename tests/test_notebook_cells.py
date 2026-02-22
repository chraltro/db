from __future__ import annotations

import json

import duckdb
import pytest

from dp.engine.notebook import (
    execute_cell,
    execute_ingest_cell,
    execute_sql_cell,
)


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


def test_execute_sql_cell_select():
    """SQL cell executes a SELECT and returns table output."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE test AS SELECT 1 AS id, 'hello' AS name")
    result = execute_sql_cell(conn, "SELECT * FROM test")
    assert result["duration_ms"] >= 0
    assert len(result["outputs"]) == 1
    out = result["outputs"][0]
    assert out["type"] == "table"
    assert out["columns"] == ["id", "name"]
    assert out["rows"] == [[1, "hello"]]
    assert out["total_rows"] == 1
    conn.close()


def test_execute_sql_cell_ddl():
    """SQL cell handles CREATE TABLE (DDL)."""
    conn = duckdb.connect(":memory:")
    result = execute_sql_cell(conn, "CREATE TABLE new_table AS SELECT 42 AS val")
    assert len(result["outputs"]) == 1
    # DuckDB returns a result (Count column) for CREATE TABLE ... AS SELECT
    assert result["outputs"][0]["type"] in ("text", "table")
    # Verify table exists
    row = conn.execute("SELECT val FROM new_table").fetchone()
    assert row[0] == 42
    conn.close()


def test_execute_sql_cell_with_config():
    """SQL cell parses config comments."""
    conn = duckdb.connect(":memory:")
    sql = (
        "-- config: materialized=table, schema=bronze\n"
        "-- depends_on: landing.raw_data\n"
        "\n"
        "SELECT 1 AS id"
    )
    result = execute_sql_cell(conn, sql)
    assert result["config"]["materialized"] == "table"
    assert result["config"]["schema"] == "bronze"
    # Should still execute the query
    assert len(result["outputs"]) == 1
    assert result["outputs"][0]["type"] == "table"
    conn.close()


def test_execute_sql_cell_error():
    """SQL cell captures errors without raising."""
    conn = duckdb.connect(":memory:")
    result = execute_sql_cell(conn, "SELECT * FROM nonexistent_table")
    assert len(result["outputs"]) == 1
    assert result["outputs"][0]["type"] == "error"
    conn.close()


def test_execute_sql_cell_multi_statement():
    """SQL cell handles multiple statements separated by semicolons."""
    conn = duckdb.connect(":memory:")
    sql = "CREATE TABLE t1 AS SELECT 1 AS a; SELECT * FROM t1"
    result = execute_sql_cell(conn, sql)
    # Should have output from the SELECT
    assert any(o["type"] == "table" for o in result["outputs"])
    conn.close()


def test_execute_sql_cell_empty():
    """Empty SQL cell returns no outputs."""
    conn = duckdb.connect(":memory:")
    result = execute_sql_cell(conn, "")
    assert result["outputs"] == []
    conn.close()


def test_execute_sql_cell_insert():
    """SQL cell handles INSERT statements."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE dest (id INTEGER, name VARCHAR)")
    result = execute_sql_cell(conn, "INSERT INTO dest VALUES (1, 'test')")
    assert len(result["outputs"]) == 1
    # DuckDB returns a result (Count column) for INSERT
    assert result["outputs"][0]["type"] in ("text", "table")
    # Verify the insert worked
    row = conn.execute("SELECT * FROM dest").fetchone()
    assert row == (1, "test")
    conn.close()


def test_execute_sql_cell_semicolon_in_string():
    """SQL cell correctly handles semicolons inside string literals."""
    conn = duckdb.connect(":memory:")
    result = execute_sql_cell(conn, "SELECT 'hello;world' AS msg")
    assert len(result["outputs"]) == 1
    assert result["outputs"][0]["type"] == "table"
    assert result["outputs"][0]["rows"] == [["hello;world"]]
    conn.close()


def test_execute_sql_cell_truncation():
    """SQL cell uses fetchmany and reports truncation for large results."""
    conn = duckdb.connect(":memory:")
    # Generate a table with 600 rows (more than 500 display limit)
    conn.execute("CREATE TABLE big AS SELECT range AS id FROM range(600)")
    result = execute_sql_cell(conn, "SELECT * FROM big")
    out = result["outputs"][0]
    assert out["type"] == "table"
    assert out["total_rows"] == 500
    assert out["truncated"] is True
    conn.close()


def test_execute_sql_cell_no_truncation():
    """SQL cell doesn't report truncation for small results."""
    conn = duckdb.connect(":memory:")
    result = execute_sql_cell(conn, "SELECT 1 AS id")
    out = result["outputs"][0]
    assert out["truncated"] is False
    conn.close()


def test_execute_ingest_cell_csv(tmp_path):
    """Ingest cell loads a CSV file."""
    # Create a test CSV
    csv_path = tmp_path / "test.csv"
    csv_path.write_text("id,name\n1,alice\n2,bob\n")

    conn = duckdb.connect(":memory:")
    spec = json.dumps({
        "source_type": "csv",
        "source_path": str(csv_path),
        "target_schema": "landing",
        "target_table": "people",
    })
    result = execute_ingest_cell(conn, spec, project_dir=tmp_path)

    # Should not have errors
    assert not any(o["type"] == "error" for o in result["outputs"])
    # Should have a success message and preview
    assert any("2" in o.get("text", "") for o in result["outputs"] if o["type"] == "text")
    assert any(o["type"] == "table" for o in result["outputs"])

    # Verify data was loaded
    rows = conn.execute("SELECT * FROM landing.people").fetchall()
    assert len(rows) == 2
    conn.close()


def test_execute_ingest_cell_parquet(tmp_path):
    """Ingest cell loads a Parquet file."""
    # Create a test parquet file
    parquet_path = tmp_path / "test.parquet"
    conn_temp = duckdb.connect(":memory:")
    conn_temp.execute(
        f"COPY (SELECT 1 AS id, 'hello' AS msg) TO '{parquet_path}' (FORMAT PARQUET)"
    )
    conn_temp.close()

    conn = duckdb.connect(":memory:")
    spec = json.dumps({
        "source_type": "parquet",
        "source_path": str(parquet_path),
        "target_schema": "landing",
        "target_table": "msgs",
    })
    result = execute_ingest_cell(conn, spec, project_dir=tmp_path)
    assert not any(o["type"] == "error" for o in result["outputs"])
    row = conn.execute("SELECT * FROM landing.msgs").fetchone()
    assert row == (1, "hello")
    conn.close()


def test_execute_ingest_cell_missing_fields():
    """Ingest cell validates required fields."""
    conn = duckdb.connect(":memory:")

    # Missing source_type
    result = execute_ingest_cell(conn, json.dumps({"target_table": "t"}))
    assert any(o["type"] == "error" for o in result["outputs"])

    # Missing target_table
    result = execute_ingest_cell(conn, json.dumps({"source_type": "csv", "source_path": "/x.csv"}))
    assert any(o["type"] == "error" for o in result["outputs"])

    # Invalid JSON
    result = execute_ingest_cell(conn, "not json")
    assert any(o["type"] == "error" for o in result["outputs"])
    conn.close()


def test_execute_ingest_cell_unsupported_type():
    """Ingest cell rejects unknown source types."""
    conn = duckdb.connect(":memory:")
    spec = json.dumps({
        "source_type": "ftp",
        "source_path": "ftp://example.com/data",
        "target_table": "data",
    })
    result = execute_ingest_cell(conn, spec)
    assert any(o["type"] == "error" for o in result["outputs"])
    conn.close()


def test_ingest_cell_json_file(tmp_path):
    """Ingest cell loads a JSON file."""
    json_path = tmp_path / "data.json"
    json_path.write_text('[{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]')

    conn = duckdb.connect(":memory:")
    spec = json.dumps({
        "source_type": "json",
        "source_path": str(json_path),
        "target_schema": "landing",
        "target_table": "people",
    })
    result = execute_ingest_cell(conn, spec, project_dir=tmp_path)
    assert not any(o["type"] == "error" for o in result["outputs"])
    rows = conn.execute("SELECT * FROM landing.people").fetchall()
    assert len(rows) == 2
    conn.close()


def test_ingest_cell_default_schema():
    """Ingest cell defaults to 'landing' schema when not specified."""
    conn = duckdb.connect(":memory:")
    # This will fail because the file doesn't exist, but the error should NOT be
    # about invalid identifiers — the default schema should be valid
    spec = json.dumps({
        "source_type": "csv",
        "source_path": "/nonexistent/file.csv",
        "target_table": "test_data",
    })
    result = execute_ingest_cell(conn, spec)
    # Should get a file-not-found error, not an identifier error
    errors = [o for o in result["outputs"] if o["type"] == "error"]
    assert len(errors) == 1
    assert "Invalid" not in errors[0]["text"]
    conn.close()
