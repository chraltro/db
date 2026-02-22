from __future__ import annotations

import duckdb
import pytest

from dp.engine.notebook import (
    generate_debug_notebook,
    model_to_notebook,
    promote_sql_to_model,
)


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


def test_model_to_notebook_no_deps(tmp_path):
    """Model-to-notebook works for models with no dependencies."""
    transform_dir = tmp_path / "transform" / "gold"
    transform_dir.mkdir(parents=True)
    (transform_dir / "constants.sql").write_text(
        "-- config: materialized=table, schema=gold\n\n"
        "SELECT 1 AS one, 2 AS two"
    )

    conn = duckdb.connect(":memory:")
    nb = model_to_notebook(
        conn, "gold.constants",
        tmp_path / "transform",
        tmp_path / "notebooks",
    )

    assert nb["title"] == "Debug: gold.constants"
    sql_cells = [c for c in nb["cells"] if c["type"] == "sql"]
    # Should have the model SQL and current output query
    assert any("SELECT 1 AS one, 2 AS two" in c["source"] for c in sql_cells)
    assert any("gold.constants" in c["source"] for c in sql_cells)
    conn.close()


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


def test_debug_notebook_with_multiple_assertions(tmp_path):
    """Debug notebook handles multiple assertion failures."""
    transform_dir = tmp_path / "transform" / "silver"
    transform_dir.mkdir(parents=True)
    (transform_dir / "report.sql").write_text(
        "-- config: materialized=table, schema=silver\n"
        "-- assert: unique(id)\n"
        "-- assert: no_nulls(name)\n"
        "-- assert: row_count > 0\n\n"
        "SELECT 1 AS id, 'test' AS name"
    )

    conn = duckdb.connect(":memory:")
    nb = generate_debug_notebook(
        conn, "silver.report",
        tmp_path / "transform",
        assertion_failures=[
            {"expression": "unique(id)", "detail": "duplicates=3"},
            {"expression": "no_nulls(name)", "detail": "null_count=5"},
            {"expression": "row_count > 0", "detail": "row_count=0"},
        ],
    )

    sql_cells = [c for c in nb["cells"] if c["type"] == "sql"]
    # Should have diagnostic SQL for each assertion type
    assert any("duplicate" in c["source"].lower() for c in sql_cells)
    assert any("IS NULL" in c["source"] for c in sql_cells)
    assert any("row_count" in c["source"] for c in sql_cells)
    conn.close()


def test_promote_rejects_overwrite_by_default(tmp_path):
    """Promote raises FileExistsError when model file already exists."""
    transform_dir = tmp_path / "transform"

    # Create the model first
    promote_sql_to_model(
        sql_source="SELECT 1 AS id",
        model_name="existing_model",
        schema="bronze",
        transform_dir=transform_dir,
    )

    # Attempting to promote again should fail
    with pytest.raises(FileExistsError, match="already exists"):
        promote_sql_to_model(
            sql_source="SELECT 2 AS id",
            model_name="existing_model",
            schema="bronze",
            transform_dir=transform_dir,
        )


def test_promote_overwrite_flag(tmp_path):
    """Promote with overwrite=True replaces existing model file."""
    transform_dir = tmp_path / "transform"

    promote_sql_to_model(
        sql_source="SELECT 1 AS id",
        model_name="my_model",
        schema="bronze",
        transform_dir=transform_dir,
    )

    # Overwrite should succeed
    model_path = promote_sql_to_model(
        sql_source="SELECT 2 AS id",
        model_name="my_model",
        schema="bronze",
        transform_dir=transform_dir,
        overwrite=True,
    )

    content = model_path.read_text()
    assert "SELECT 2 AS id" in content


def test_promote_rejects_invalid_model_name(tmp_path):
    """Promote rejects model names with path traversal or invalid chars."""
    transform_dir = tmp_path / "transform"

    with pytest.raises(ValueError, match="Invalid"):
        promote_sql_to_model(
            sql_source="SELECT 1",
            model_name="../../../evil",
            schema="bronze",
            transform_dir=transform_dir,
        )

    with pytest.raises(ValueError, match="Invalid"):
        promote_sql_to_model(
            sql_source="SELECT 1",
            model_name="DROP TABLE users--",
            schema="bronze",
            transform_dir=transform_dir,
        )


def test_promote_rejects_invalid_schema(tmp_path):
    """Promote rejects invalid schema names."""
    transform_dir = tmp_path / "transform"

    with pytest.raises(ValueError, match="Invalid"):
        promote_sql_to_model(
            sql_source="SELECT 1",
            model_name="ok_model",
            schema="silver; DROP TABLE--",
            transform_dir=transform_dir,
        )
