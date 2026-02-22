"""Tests for the SQL transformation engine."""

import textwrap
from pathlib import Path

import duckdb
import pytest

from dp.engine.database import ensure_meta_table
from dp.engine.sql_analysis import (
    parse_config as _parse_config,
    parse_depends as _parse_depends,
    strip_config_comments as _strip_config_comments,
)
from dp.engine.transform import (
    SQLModel,
    build_dag,
    discover_models,
    run_transform,
)


def test_parse_config():
    sql = "-- config: materialized=table, schema=gold\nSELECT 1"
    config = _parse_config(sql)
    assert config == {"materialized": "table", "schema": "gold"}


def test_parse_config_empty():
    assert _parse_config("SELECT 1") == {}


def test_parse_depends():
    sql = "-- depends_on: bronze.customers, bronze.orders\nSELECT 1"
    deps = _parse_depends(sql)
    assert deps == ["bronze.customers", "bronze.orders"]


def test_parse_depends_empty():
    assert _parse_depends("SELECT 1") == []


def test_strip_config_comments():
    sql = "-- config: materialized=view\n-- depends_on: landing.x\n\nSELECT 1"
    query = _strip_config_comments(sql)
    assert query.strip() == "SELECT 1"


def test_discover_models(tmp_path):
    bronze = tmp_path / "transform" / "bronze"
    bronze.mkdir(parents=True)
    (bronze / "customers.sql").write_text(
        "-- config: materialized=view, schema=bronze\n"
        "-- depends_on: landing.customers\n\n"
        "SELECT * FROM landing.customers\n"
    )
    models = discover_models(tmp_path / "transform")
    assert len(models) == 1
    m = models[0]
    assert m.name == "customers"
    assert m.schema == "bronze"
    assert m.full_name == "bronze.customers"
    assert m.materialized == "view"
    assert m.depends_on == ["landing.customers"]


def test_discover_models_convention(tmp_path):
    """Folder name becomes schema when no config override."""
    silver = tmp_path / "transform" / "silver"
    silver.mkdir(parents=True)
    (silver / "test_model.sql").write_text("SELECT 1 AS x\n")
    models = discover_models(tmp_path / "transform")
    assert len(models) == 1
    assert models[0].schema == "silver"
    assert models[0].materialized == "view"  # default


def test_build_dag():
    models = [
        SQLModel(
            path=Path("a.sql"), name="a", schema="bronze", full_name="bronze.a",
            sql="", query="SELECT 1", materialized="view", depends_on=[],
        ),
        SQLModel(
            path=Path("b.sql"), name="b", schema="silver", full_name="silver.b",
            sql="", query="SELECT 1", materialized="view", depends_on=["bronze.a"],
        ),
        SQLModel(
            path=Path("c.sql"), name="c", schema="gold", full_name="gold.c",
            sql="", query="SELECT 1", materialized="table", depends_on=["silver.b"],
        ),
    ]
    ordered = build_dag(models)
    names = [m.full_name for m in ordered]
    assert names.index("bronze.a") < names.index("silver.b")
    assert names.index("silver.b") < names.index("gold.c")


def test_run_transform_end_to_end(tmp_path):
    """Full end-to-end: create landing data, discover models, run transform."""
    # Create a DuckDB database with landing data
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS landing")
    conn.execute("CREATE TABLE landing.users AS SELECT 1 AS id, 'Alice' AS name")

    # Create transform SQL
    bronze = tmp_path / "transform" / "bronze"
    bronze.mkdir(parents=True)
    (bronze / "users.sql").write_text(
        "-- config: materialized=view, schema=bronze\n"
        "-- depends_on: landing.users\n\n"
        "SELECT id, UPPER(name) AS name FROM landing.users\n"
    )

    gold = tmp_path / "transform" / "gold"
    gold.mkdir(parents=True)
    (gold / "dim_users.sql").write_text(
        "-- config: materialized=table, schema=gold\n"
        "-- depends_on: bronze.users\n\n"
        "SELECT id, name, 'active' AS status FROM bronze.users\n"
    )

    # Run transform
    results = run_transform(conn, tmp_path / "transform", force=True)
    assert results["bronze.users"] == "built"
    assert results["gold.dim_users"] == "built"

    # Verify data
    row = conn.execute("SELECT * FROM gold.dim_users").fetchone()
    assert row == (1, "ALICE", "active")

    # Run again — should skip (no changes)
    results2 = run_transform(conn, tmp_path / "transform")
    assert results2["bronze.users"] == "skipped"
    assert results2["gold.dim_users"] == "skipped"

    conn.close()


def test_change_detection(tmp_path):
    """Changing a SQL file should trigger a rebuild."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS landing")
    conn.execute("CREATE TABLE landing.data AS SELECT 1 AS val")

    bronze = tmp_path / "transform" / "bronze"
    bronze.mkdir(parents=True)
    sql_file = bronze / "data.sql"
    sql_file.write_text(
        "-- config: materialized=view, schema=bronze\n"
        "-- depends_on: landing.data\n\n"
        "SELECT val FROM landing.data\n"
    )

    # First run
    results = run_transform(conn, tmp_path / "transform")
    assert results["bronze.data"] == "built"

    # No change — skip
    results = run_transform(conn, tmp_path / "transform")
    assert results["bronze.data"] == "skipped"

    # Modify SQL
    sql_file.write_text(
        "-- config: materialized=view, schema=bronze\n"
        "-- depends_on: landing.data\n\n"
        "SELECT val, val * 2 AS doubled FROM landing.data\n"
    )

    # Should rebuild
    results = run_transform(conn, tmp_path / "transform")
    assert results["bronze.data"] == "built"


def test_transform_nonexistent_target(tmp_path):
    """Targeting a model that doesn't exist should return empty results."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS landing")
    conn.execute("CREATE TABLE landing.data AS SELECT 1 AS val")

    bronze = tmp_path / "transform" / "bronze"
    bronze.mkdir(parents=True)
    (bronze / "data.sql").write_text(
        "-- config: materialized=view, schema=bronze\n"
        "-- depends_on: landing.data\n\n"
        "SELECT val FROM landing.data\n"
    )

    # Target a model that doesn't exist
    results = run_transform(conn, tmp_path / "transform", targets=["nonexistent"])
    assert results == {}

    # Target an existing model by full_name should work
    results = run_transform(conn, tmp_path / "transform", targets=["bronze.data"], force=True)
    assert results["bronze.data"] == "built"

    conn.close()

    conn.close()
