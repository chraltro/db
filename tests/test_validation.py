"""Tests for compile-time SQL validation and impact analysis."""

from __future__ import annotations

import textwrap
from pathlib import Path

import duckdb
import pytest

from dp.engine.database import ensure_meta_table
from dp.engine.transform import (
    SQLModel,
    ValidationError,
    discover_models,
    impact_analysis,
    validate_models,
)


@pytest.fixture
def db(tmp_path):
    """Create a DuckDB connection with metadata tables."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    ensure_meta_table(conn)
    conn.execute("CREATE SCHEMA IF NOT EXISTS landing")
    return conn


@pytest.fixture
def transform_dir(tmp_path):
    """Create a basic transform directory."""
    t = tmp_path / "transform"
    t.mkdir()
    for sub in ("bronze", "silver", "gold"):
        (t / sub).mkdir()
    return t


class TestValidation:
    def test_validate_valid_models(self, db, transform_dir):
        db.execute("CREATE TABLE landing.data AS SELECT 1 AS id, 'test' AS name")
        (transform_dir / "bronze" / "data.sql").write_text(textwrap.dedent("""\
            -- config: materialized=table, schema=bronze
            -- depends_on: landing.data

            SELECT id, name FROM landing.data
        """))
        models = discover_models(transform_dir)
        errors = validate_models(db, models)
        assert len(errors) == 0

    def test_validate_bad_sql(self, transform_dir):
        (transform_dir / "bronze" / "bad.sql").write_text(textwrap.dedent("""\
            -- config: materialized=table, schema=bronze

            SELECTT id FRUM landing.data WHEREE
        """))
        models = discover_models(transform_dir)
        errors = validate_models(None, models)
        parse_errors = [e for e in errors if "parse error" in e.message.lower()]
        assert len(parse_errors) >= 1

    def test_validate_missing_table(self, db, transform_dir):
        (transform_dir / "bronze" / "missing.sql").write_text(textwrap.dedent("""\
            -- config: materialized=table, schema=bronze
            -- depends_on: landing.nonexistent

            SELECT id FROM landing.nonexistent
        """))
        models = discover_models(transform_dir)
        errors = validate_models(db, models)
        table_errors = [e for e in errors if "does not exist" in e.message]
        assert len(table_errors) >= 1

    def test_validate_bad_column(self, db, transform_dir):
        """Qualified column reference to a non-existent column should be caught."""
        db.execute("CREATE TABLE landing.users AS SELECT 1 AS id, 'alice' AS name")
        (transform_dir / "bronze" / "users.sql").write_text(textwrap.dedent("""\
            -- config: materialized=table, schema=bronze
            -- depends_on: landing.users

            SELECT u.id, u.nonexistent_column FROM landing.users u
        """))
        models = discover_models(transform_dir)
        errors = validate_models(db, models)
        col_errors = [e for e in errors if "not found" in e.message.lower()]
        assert len(col_errors) >= 1

    def test_validate_model_references_other_model(self, db, transform_dir):
        """Referencing another model (not yet built) should not error."""
        (transform_dir / "bronze" / "a.sql").write_text(textwrap.dedent("""\
            -- config: materialized=table, schema=bronze

            SELECT 1 AS id
        """))
        (transform_dir / "silver" / "b.sql").write_text(textwrap.dedent("""\
            -- config: materialized=table, schema=silver
            -- depends_on: bronze.a

            SELECT id FROM bronze.a
        """))
        models = discover_models(transform_dir)
        errors = validate_models(db, models)
        # bronze.a is a known model name, so it should not error
        table_errors = [e for e in errors if "bronze.a" in e.message and "does not exist" in e.message]
        assert len(table_errors) == 0

    def test_validate_no_connection(self, transform_dir):
        """Validation without a connection should still check SQL parsing."""
        (transform_dir / "bronze" / "ok.sql").write_text("SELECT 1 AS id\n")
        models = discover_models(transform_dir)
        errors = validate_models(None, models)
        assert len(errors) == 0


class TestImpactAnalysis:
    def test_basic_impact(self):
        models = [
            SQLModel(
                path=Path("a.sql"), name="a", schema="bronze", full_name="bronze.a",
                sql="", query="SELECT 1", materialized="table", depends_on=[],
            ),
            SQLModel(
                path=Path("b.sql"), name="b", schema="silver", full_name="silver.b",
                sql="", query="SELECT 1", materialized="table",
                depends_on=["bronze.a"],
            ),
            SQLModel(
                path=Path("c.sql"), name="c", schema="gold", full_name="gold.c",
                sql="", query="SELECT 1", materialized="table",
                depends_on=["silver.b"],
            ),
        ]
        result = impact_analysis(models, "bronze.a")
        assert "silver.b" in result["downstream_models"]
        assert "gold.c" in result["downstream_models"]
        assert len(result["downstream_models"]) == 2

    def test_impact_no_downstream(self):
        models = [
            SQLModel(
                path=Path("a.sql"), name="a", schema="gold", full_name="gold.a",
                sql="", query="SELECT 1", materialized="table", depends_on=[],
            ),
        ]
        result = impact_analysis(models, "gold.a")
        assert result["downstream_models"] == []

    def test_impact_chain(self):
        models = [
            SQLModel(path=Path("a.sql"), name="a", schema="bronze", full_name="bronze.a",
                     sql="", query="SELECT 1", materialized="table", depends_on=[]),
            SQLModel(path=Path("b.sql"), name="b", schema="silver", full_name="silver.b",
                     sql="", query="SELECT 1", materialized="table", depends_on=["bronze.a"]),
            SQLModel(path=Path("c.sql"), name="c", schema="gold", full_name="gold.c",
                     sql="", query="SELECT 1", materialized="table", depends_on=["silver.b"]),
        ]
        result = impact_analysis(models, "bronze.a")
        chain = result["impact_chain"]
        assert "bronze.a" in chain
        assert "silver.b" in chain["bronze.a"]
        assert "silver.b" in chain
        assert "gold.c" in chain["silver.b"]

    def test_impact_with_column(self, db):
        db.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        db.execute("CREATE TABLE bronze.src AS SELECT 1 AS id, 'x' AS name")

        models = [
            SQLModel(path=Path("a.sql"), name="src", schema="bronze", full_name="bronze.src",
                     sql="", query="SELECT 1 AS id, 'x' AS name", materialized="table", depends_on=[]),
            SQLModel(path=Path("b.sql"), name="users", schema="silver", full_name="silver.users",
                     sql="", query="SELECT s.id, s.name FROM bronze.src s",
                     materialized="table", depends_on=["bronze.src"]),
        ]
        result = impact_analysis(models, "bronze.src", column="name", conn=db)
        assert result["column"] == "name"
        affected = result["affected_columns"]
        assert any(a["model"] == "silver.users" and a["column"] == "name" for a in affected)

    def test_impact_diamond_dependency(self):
        """Diamond dependency: A -> B, A -> C, B -> D, C -> D."""
        models = [
            SQLModel(path=Path("a.sql"), name="a", schema="bronze", full_name="bronze.a",
                     sql="", query="SELECT 1", materialized="table", depends_on=[]),
            SQLModel(path=Path("b.sql"), name="b", schema="silver", full_name="silver.b",
                     sql="", query="SELECT 1", materialized="table", depends_on=["bronze.a"]),
            SQLModel(path=Path("c.sql"), name="c", schema="silver", full_name="silver.c",
                     sql="", query="SELECT 1", materialized="table", depends_on=["bronze.a"]),
            SQLModel(path=Path("d.sql"), name="d", schema="gold", full_name="gold.d",
                     sql="", query="SELECT 1", materialized="table",
                     depends_on=["silver.b", "silver.c"]),
        ]
        result = impact_analysis(models, "bronze.a")
        assert set(result["downstream_models"]) == {"silver.b", "silver.c", "gold.d"}
