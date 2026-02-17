"""Tests for documentation generation."""

from pathlib import Path

import duckdb

from dp.engine.docs import generate_docs


def test_generate_docs_empty(tmp_path):
    """Docs for empty database."""
    conn = duckdb.connect(":memory:")
    md = generate_docs(conn, tmp_path / "transform")
    assert "No tables found" in md
    conn.close()


def test_generate_docs_with_data(tmp_path):
    """Full docs with tables and models."""
    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    conn.execute("CREATE SCHEMA IF NOT EXISTS landing")
    conn.execute("CREATE TABLE landing.users AS SELECT 1 AS id, 'Alice' AS name")

    # Create a model file
    bronze = tmp_path / "transform" / "bronze"
    bronze.mkdir(parents=True)
    (bronze / "users.sql").write_text(
        "-- config: materialized=view, schema=bronze\n"
        "-- depends_on: landing.users\n\n"
        "SELECT id, UPPER(name) AS name FROM landing.users\n"
    )
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    conn.execute("CREATE VIEW bronze.users AS SELECT id, UPPER(name) AS name FROM landing.users")

    md = generate_docs(conn, tmp_path / "transform")

    assert "landing.users" in md
    assert "bronze.users" in md
    assert "| `id` |" in md
    assert "| `name` |" in md
    assert "Depends on:" in md
    assert "Lineage" in md

    conn.close()
