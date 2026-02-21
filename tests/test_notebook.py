"""Tests for notebook execution."""

import json
from pathlib import Path

import duckdb

from dp.engine.notebook import (
    create_notebook,
    execute_cell,
    execute_ingest_cell,
    execute_sql_cell,
    extract_notebook_outputs,
    generate_debug_notebook,
    load_notebook,
    model_to_notebook,
    promote_sql_to_model,
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


# --- SQL cell tests ---


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


# --- Ingest cell tests ---


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


# --- Promote to model tests ---


def test_promote_sql_to_model(tmp_path):
    """Promote a SQL cell to a transform model file."""
    transform_dir = tmp_path / "transform"
    sql = "SELECT c.id, c.name FROM landing.customers c JOIN landing.orders o ON c.id = o.cust_id"

    model_path = promote_sql_to_model(
        sql_source=sql,
        model_name="customer_orders",
        schema="bronze",
        transform_dir=transform_dir,
        description="Customer orders summary",
    )

    assert model_path.exists()
    assert model_path.name == "customer_orders.sql"
    assert model_path.parent.name == "bronze"

    content = model_path.read_text()
    assert "-- config: materialized=table, schema=bronze" in content
    assert "-- depends_on: landing.customers, landing.orders" in content
    assert "-- description: Customer orders summary" in content
    assert "SELECT c.id, c.name FROM landing.customers c" in content


def test_promote_sql_with_existing_config(tmp_path):
    """Promote respects existing config comments in the SQL."""
    transform_dir = tmp_path / "transform"
    sql = (
        "-- config: materialized=view, schema=silver\n"
        "-- depends_on: bronze.customers\n"
        "\n"
        "SELECT * FROM bronze.customers WHERE active = true"
    )

    model_path = promote_sql_to_model(
        sql_source=sql,
        model_name="active_customers",
        schema="bronze",  # Should be overridden by config
        transform_dir=transform_dir,
    )

    content = model_path.read_text()
    assert "materialized=view" in content
    assert "schema=silver" in content
    # File should be in the silver directory due to config override
    assert model_path.parent.name == "silver"


def test_promote_sql_no_deps(tmp_path):
    """Promote SQL with no table references."""
    transform_dir = tmp_path / "transform"
    sql = "SELECT 1 AS one, 2 AS two"

    model_path = promote_sql_to_model(
        sql_source=sql,
        model_name="constants",
        schema="gold",
        transform_dir=transform_dir,
    )

    content = model_path.read_text()
    assert "-- config:" in content
    assert "-- depends_on:" not in content


# --- Model to notebook tests ---


def test_model_to_notebook(tmp_path):
    """Create a notebook from a transform model."""
    transform_dir = tmp_path / "transform" / "bronze"
    transform_dir.mkdir(parents=True)
    (transform_dir / "test.sql").write_text(
        "-- config: materialized=table, schema=bronze\n"
        "-- depends_on: landing.raw\n\n"
        "SELECT * FROM landing.raw"
    )

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS landing")
    conn.execute("CREATE TABLE landing.raw AS SELECT 1 AS id")

    nb = model_to_notebook(
        conn, "bronze.test",
        tmp_path / "transform",
        tmp_path / "notebooks",
    )

    assert nb["title"] == "Debug: bronze.test"
    cell_types = [c["type"] for c in nb["cells"]]
    assert "markdown" in cell_types
    assert "sql" in cell_types

    # Should have upstream data query
    sql_sources = [c["source"] for c in nb["cells"] if c["type"] == "sql"]
    assert any("landing.raw" in s for s in sql_sources)
    # Should have the model SQL
    assert any("SELECT * FROM landing.raw" in s for s in sql_sources)
    conn.close()


# --- Debug notebook tests ---


def test_generate_debug_notebook(tmp_path):
    """Generate a debug notebook for a failed model."""
    transform_dir = tmp_path / "transform" / "silver"
    transform_dir.mkdir(parents=True)
    (transform_dir / "bad_model.sql").write_text(
        "-- config: materialized=table, schema=silver\n"
        "-- depends_on: bronze.data\n"
        "-- assert: row_count > 0\n"
        "-- assert: unique(id)\n\n"
        "SELECT id, name FROM bronze.data"
    )
    (tmp_path / "transform" / "bronze").mkdir(parents=True)
    (tmp_path / "transform" / "bronze" / "data.sql").write_text(
        "-- config: materialized=table, schema=bronze\n\n"
        "SELECT 1 AS id, 'test' AS name"
    )

    conn = duckdb.connect(":memory:")
    nb = generate_debug_notebook(
        conn, "silver.bad_model",
        tmp_path / "transform",
        error_message="Column 'id' not found in table 'bronze.data'",
        assertion_failures=[
            {"expression": "unique(id)", "detail": "duplicate_count=5"},
        ],
    )

    assert nb["title"] == "Debug: silver.bad_model"
    # Should contain error explanation
    md_cells = [c for c in nb["cells"] if c["type"] == "markdown"]
    assert any("Column 'id' not found" in c["source"] for c in md_cells)
    # Should contain upstream queries
    sql_cells = [c for c in nb["cells"] if c["type"] == "sql"]
    assert any("bronze.data" in c["source"] for c in sql_cells)
    # Should contain assertion diagnostics
    assert any("duplicate" in c["source"].lower() for c in sql_cells)
    conn.close()


def test_generate_debug_notebook_no_error(tmp_path):
    """Debug notebook can be generated without specific error info."""
    transform_dir = tmp_path / "transform" / "bronze"
    transform_dir.mkdir(parents=True)
    (transform_dir / "test.sql").write_text(
        "-- config: materialized=table, schema=bronze\n\n"
        "SELECT 1 AS id"
    )

    conn = duckdb.connect(":memory:")
    nb = generate_debug_notebook(conn, "bronze.test", tmp_path / "transform")
    assert nb["title"] == "Debug: bronze.test"
    assert len(nb["cells"]) >= 2
    conn.close()


# --- Extract notebook outputs tests ---


def test_extract_notebook_outputs_explicit():
    """Extract explicitly declared outputs."""
    nb = {
        "title": "Test",
        "outputs": ["landing.earthquakes", "landing.weather"],
        "cells": [],
    }
    outputs = extract_notebook_outputs(nb)
    assert outputs == ["landing.earthquakes", "landing.weather"]


def test_extract_notebook_outputs_from_sql_cells():
    """Extract outputs inferred from SQL cells."""
    nb = {
        "title": "Test",
        "cells": [
            {"type": "sql", "source": "CREATE TABLE landing.data AS SELECT 1 AS id"},
            {"type": "sql", "source": "SELECT * FROM landing.data"},
        ],
    }
    outputs = extract_notebook_outputs(nb)
    assert "landing.data" in outputs


def test_extract_notebook_outputs_from_ingest_cells():
    """Extract outputs inferred from ingest cells."""
    nb = {
        "title": "Test",
        "cells": [
            {
                "type": "ingest",
                "source": json.dumps({
                    "source_type": "csv",
                    "source_path": "/data/test.csv",
                    "target_schema": "landing",
                    "target_table": "raw_data",
                }),
            },
        ],
    }
    outputs = extract_notebook_outputs(nb)
    assert "landing.raw_data" in outputs


def test_extract_notebook_outputs_from_code_cells():
    """Extract outputs inferred from code cell patterns."""
    nb = {
        "title": "Test",
        "cells": [
            {
                "type": "code",
                "source": "db.execute('CREATE OR REPLACE TABLE landing.events AS SELECT 1 AS id')",
            },
        ],
    }
    outputs = extract_notebook_outputs(nb)
    assert "landing.events" in outputs
