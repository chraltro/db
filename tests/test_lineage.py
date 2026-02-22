"""Tests for column-level lineage (sqlglot AST-based)."""

from __future__ import annotations

import textwrap
from pathlib import Path

import duckdb
import pytest

from dp.engine.database import ensure_meta_table
from dp.engine.transform import (
    SQLModel,
    extract_column_lineage,
)


@pytest.fixture
def db(tmp_path):
    """Create a DuckDB connection with metadata tables."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    ensure_meta_table(conn)
    conn.execute("CREATE SCHEMA IF NOT EXISTS landing")
    return conn


class TestColumnLineage:
    def test_simple_lineage(self):
        model = SQLModel(
            path=Path("test.sql"), name="customers", schema="gold",
            full_name="gold.customers", sql="",
            query="SELECT c.customer_id, c.name, COUNT(o.order_id) AS order_count FROM bronze.customers c LEFT JOIN bronze.orders o ON c.customer_id = o.customer_id GROUP BY 1, 2",
            materialized="table",
            depends_on=["bronze.customers", "bronze.orders"],
        )
        lineage = extract_column_lineage(model)
        assert "customer_id" in lineage
        assert any(s["source_table"] == "bronze.customers" for s in lineage["customer_id"])
        assert "order_count" in lineage
        assert any(s["source_table"] == "bronze.orders" for s in lineage["order_count"])

    def test_lineage_with_aliases(self):
        model = SQLModel(
            path=Path("test.sql"), name="summary", schema="gold",
            full_name="gold.summary", sql="",
            query="SELECT e.event_id, e.magnitude AS mag FROM silver.earthquake_events AS e",
            materialized="table",
            depends_on=["silver.earthquake_events"],
        )
        lineage = extract_column_lineage(model)
        assert "event_id" in lineage
        assert "mag" in lineage
        # mag should trace to silver.earthquake_events.magnitude
        assert any(
            s["source_table"] == "silver.earthquake_events" and s["source_column"] == "magnitude"
            for s in lineage["mag"]
        )

    def test_lineage_star_select(self):
        model = SQLModel(
            path=Path("test.sql"), name="test", schema="bronze",
            full_name="bronze.test", sql="",
            query="SELECT * FROM landing.raw_data",
            materialized="view",
            depends_on=["landing.raw_data"],
        )
        lineage = extract_column_lineage(model)
        # * doesn't give us column names without a db connection
        assert isinstance(lineage, dict)

    def test_lineage_computed_column(self):
        model = SQLModel(
            path=Path("test.sql"), name="test", schema="silver",
            full_name="silver.test", sql="",
            query="SELECT d.id, d.amount * 1.1 AS amount_with_tax FROM bronze.data d",
            materialized="table",
            depends_on=["bronze.data"],
        )
        lineage = extract_column_lineage(model)
        assert "amount_with_tax" in lineage
        assert any(s["source_column"] == "amount" for s in lineage["amount_with_tax"])

    def test_lineage_with_cte(self):
        """CTEs should be traced through to the source tables."""
        model = SQLModel(
            path=Path("test.sql"), name="test", schema="gold",
            full_name="gold.test", sql="",
            query=textwrap.dedent("""\
                WITH filtered AS (
                    SELECT id, name FROM bronze.customers WHERE active = true
                )
                SELECT f.id, f.name FROM filtered f
            """),
            materialized="table",
            depends_on=["bronze.customers"],
        )
        lineage = extract_column_lineage(model)
        assert "id" in lineage
        assert "name" in lineage

    def test_lineage_with_case(self):
        """CASE expressions should trace all column references."""
        model = SQLModel(
            path=Path("test.sql"), name="test", schema="silver",
            full_name="silver.test", sql="",
            query="SELECT e.id, CASE WHEN e.magnitude >= 5.0 THEN 'strong' ELSE 'weak' END AS strength FROM bronze.events e",
            materialized="table",
            depends_on=["bronze.events"],
        )
        lineage = extract_column_lineage(model)
        assert "strength" in lineage
        assert any(s["source_column"] == "magnitude" for s in lineage["strength"])

    def test_lineage_with_window_function(self):
        """Window functions should trace column references."""
        model = SQLModel(
            path=Path("test.sql"), name="test", schema="silver",
            full_name="silver.test", sql="",
            query="SELECT e.id, ROW_NUMBER() OVER (PARTITION BY e.region ORDER BY e.magnitude DESC) AS rn FROM bronze.events e",
            materialized="table",
            depends_on=["bronze.events"],
        )
        lineage = extract_column_lineage(model)
        assert "rn" in lineage
        # rn references region and magnitude
        source_cols = {s["source_column"] for s in lineage["rn"]}
        assert "region" in source_cols
        assert "magnitude" in source_cols

    def test_lineage_with_subquery(self):
        """Subqueries in SELECT should trace sources."""
        model = SQLModel(
            path=Path("test.sql"), name="test", schema="gold",
            full_name="gold.test", sql="",
            query="SELECT c.id, (SELECT COUNT(*) FROM bronze.orders o WHERE o.customer_id = c.id) AS order_count FROM bronze.customers c",
            materialized="table",
            depends_on=["bronze.customers", "bronze.orders"],
        )
        lineage = extract_column_lineage(model)
        assert "id" in lineage
        assert "order_count" in lineage

    def test_lineage_union_all(self):
        """UNION ALL should trace from the first SELECT."""
        model = SQLModel(
            path=Path("test.sql"), name="test", schema="silver",
            full_name="silver.test", sql="",
            query="SELECT a.id, a.name FROM bronze.customers_a a UNION ALL SELECT b.id, b.name FROM bronze.customers_b b",
            materialized="table",
            depends_on=["bronze.customers_a", "bronze.customers_b"],
        )
        lineage = extract_column_lineage(model)
        assert "id" in lineage
        assert "name" in lineage

    def test_lineage_star_with_connection(self, db):
        """SELECT * with a connection should resolve columns from information_schema."""
        db.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        db.execute("CREATE TABLE bronze.src AS SELECT 1 AS id, 'x' AS name, 3.14 AS val")
        model = SQLModel(
            path=Path("test.sql"), name="test", schema="silver",
            full_name="silver.test", sql="",
            query="SELECT * FROM bronze.src",
            materialized="view",
            depends_on=["bronze.src"],
        )
        lineage = extract_column_lineage(model, conn=db)
        assert "id" in lineage
        assert "name" in lineage
        assert "val" in lineage
        assert lineage["id"][0]["source_table"] == "bronze.src"

    def test_lineage_unparseable_sql(self):
        """Unparseable SQL should return empty lineage, not crash."""
        model = SQLModel(
            path=Path("test.sql"), name="test", schema="bronze",
            full_name="bronze.test", sql="",
            query="THIS IS NOT VALID SQL AT ALL",
            materialized="view",
        )
        lineage = extract_column_lineage(model)
        assert lineage == {}
