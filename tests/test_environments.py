"""Tests for environment management, seeds, sources, exposures, and dp check."""

from pathlib import Path

import duckdb
import pytest

from dp.config import load_project


# --- Environment management tests ---


def test_environments_not_defined(tmp_path):
    """When no environments section exists, behavior is unchanged."""
    (tmp_path / "project.yml").write_text(
        """
name: test
database:
  path: warehouse.duckdb
"""
    )
    config = load_project(tmp_path)
    assert config.database.path == "warehouse.duckdb"
    assert config.environments == {}
    assert config.active_environment is None


def test_environments_default_to_dev(tmp_path):
    """When environments are defined and no env is specified, defaults to dev."""
    (tmp_path / "project.yml").write_text(
        """
name: test
database:
  path: warehouse.duckdb
environments:
  dev:
    database:
      path: dev.duckdb
  prod:
    database:
      path: prod.duckdb
"""
    )
    config = load_project(tmp_path)
    assert config.active_environment == "dev"
    assert config.database.path == "dev.duckdb"


def test_environments_explicit_prod(tmp_path):
    """When --env=prod is specified, uses prod database path."""
    (tmp_path / "project.yml").write_text(
        """
name: test
database:
  path: warehouse.duckdb
environments:
  dev:
    database:
      path: dev.duckdb
  prod:
    database:
      path: prod.duckdb
"""
    )
    config = load_project(tmp_path, env="prod")
    assert config.active_environment == "prod"
    assert config.database.path == "prod.duckdb"


def test_environments_connection_override(tmp_path):
    """Environment overrides connection parameters."""
    (tmp_path / "project.yml").write_text(
        """
name: test
database:
  path: warehouse.duckdb
connections:
  pg:
    type: postgres
    host: localhost
    port: 5432
environments:
  prod:
    connections:
      pg:
        host: prod-db.example.com
        port: 5433
"""
    )
    config = load_project(tmp_path, env="prod")
    assert config.connections["pg"].params["host"] == "prod-db.example.com"
    assert config.connections["pg"].params["port"] == 5433


def test_environments_fallback_no_envs(tmp_path):
    """When no environments are defined, specifying --env has no effect."""
    (tmp_path / "project.yml").write_text(
        """
name: test
database:
  path: warehouse.duckdb
"""
    )
    config = load_project(tmp_path, env="prod")
    assert config.database.path == "warehouse.duckdb"
    assert config.active_environment is None


# --- Seeds tests ---


def test_seeds_basic_loading(tmp_path):
    """Test basic CSV seed loading."""
    from dp.engine.database import connect, ensure_meta_table
    from dp.engine.seeds import load_seed, run_seeds

    seeds_dir = tmp_path / "seeds"
    seeds_dir.mkdir()
    (seeds_dir / "customers.csv").write_text("id,name,email\n1,Alice,a@e.com\n2,Bob,b@e.com\n")

    conn = connect(tmp_path / "test.duckdb")
    ensure_meta_table(conn)
    try:
        result = load_seed(conn, seeds_dir / "customers.csv")
        assert result["status"] == "built"
        assert result["full_name"] == "seeds.customers"
        assert result["row_count"] == 2

        # Verify data
        rows = conn.execute("SELECT * FROM seeds.customers").fetchall()
        assert len(rows) == 2
    finally:
        conn.close()


def test_seeds_change_detection(tmp_path):
    """Test that unchanged seeds are skipped."""
    from dp.engine.database import connect, ensure_meta_table
    from dp.engine.seeds import load_seed

    seeds_dir = tmp_path / "seeds"
    seeds_dir.mkdir()
    csv_path = seeds_dir / "data.csv"
    csv_path.write_text("id,value\n1,hello\n")

    conn = connect(tmp_path / "test.duckdb")
    ensure_meta_table(conn)
    try:
        r1 = load_seed(conn, csv_path)
        assert r1["status"] == "built"

        r2 = load_seed(conn, csv_path)
        assert r2["status"] == "skipped"

        # Modify the CSV
        csv_path.write_text("id,value\n1,hello\n2,world\n")
        r3 = load_seed(conn, csv_path)
        assert r3["status"] == "built"
        assert r3["row_count"] == 2
    finally:
        conn.close()


def test_seeds_force_reload(tmp_path):
    """Test that --force reloads all seeds."""
    from dp.engine.database import connect, ensure_meta_table
    from dp.engine.seeds import load_seed

    seeds_dir = tmp_path / "seeds"
    seeds_dir.mkdir()
    csv_path = seeds_dir / "data.csv"
    csv_path.write_text("id,value\n1,hello\n")

    conn = connect(tmp_path / "test.duckdb")
    ensure_meta_table(conn)
    try:
        load_seed(conn, csv_path)
        r2 = load_seed(conn, csv_path, force=True)
        assert r2["status"] == "built"
    finally:
        conn.close()


def test_seeds_empty_csv(tmp_path):
    """Test handling of empty CSV files."""
    from dp.engine.database import connect, ensure_meta_table
    from dp.engine.seeds import load_seed

    seeds_dir = tmp_path / "seeds"
    seeds_dir.mkdir()
    csv_path = seeds_dir / "empty.csv"
    csv_path.write_text("")

    conn = connect(tmp_path / "test.duckdb")
    ensure_meta_table(conn)
    try:
        result = load_seed(conn, csv_path)
        assert result["status"] == "built"
        assert result["row_count"] == 0
    finally:
        conn.close()


def test_seeds_discover(tmp_path):
    """Test seed discovery."""
    from dp.engine.seeds import discover_seeds

    seeds_dir = tmp_path / "seeds"
    seeds_dir.mkdir()
    (seeds_dir / "customers.csv").write_text("id\n1\n")
    (seeds_dir / "orders.csv").write_text("id\n1\n")
    (seeds_dir / "not_a_csv.txt").write_text("hello")

    seeds = discover_seeds(seeds_dir)
    assert len(seeds) == 2
    names = {s["name"] for s in seeds}
    assert names == {"customers", "orders"}


def test_seeds_run_all(tmp_path):
    """Test run_seeds loads all CSVs."""
    from dp.engine.database import connect, ensure_meta_table
    from dp.engine.seeds import run_seeds

    seeds_dir = tmp_path / "seeds"
    seeds_dir.mkdir()
    (seeds_dir / "a.csv").write_text("x\n1\n")
    (seeds_dir / "b.csv").write_text("y\n2\n")

    conn = connect(tmp_path / "test.duckdb")
    ensure_meta_table(conn)
    try:
        results = run_seeds(conn, seeds_dir)
        assert results["seeds.a"] == "built"
        assert results["seeds.b"] == "built"
    finally:
        conn.close()


def test_seeds_in_dag(tmp_path):
    """Test that seeds integrate with the transform DAG."""
    from dp.engine.seeds import discover_seeds

    seeds_dir = tmp_path / "seeds"
    seeds_dir.mkdir()
    (seeds_dir / "products.csv").write_text("id,name\n1,Widget\n")

    seeds = discover_seeds(seeds_dir)
    assert seeds[0]["full_name"] == "seeds.products"

    # This seed can be referenced by models as seeds.products


# --- Sources tests ---


def test_sources_parsing(tmp_path):
    """Test sources.yml parsing."""
    (tmp_path / "project.yml").write_text("name: test\n")
    (tmp_path / "sources.yml").write_text(
        """
sources:
  - name: postgres_app
    schema: landing
    description: "Production Postgres"
    freshness_hours: 6
    connection: prod_postgres
    tables:
      - name: customers
        description: "Customer records"
        columns:
          - name: id
            description: "Primary key"
          - name: email
            description: "Email address"
      - name: orders
        description: "Order records"
        loaded_at_column: updated_at
"""
    )

    config = load_project(tmp_path)
    assert len(config.sources) == 1
    src = config.sources[0]
    assert src.name == "postgres_app"
    assert src.schema == "landing"
    assert src.freshness_hours == 6
    assert src.connection == "prod_postgres"
    assert len(src.tables) == 2
    assert src.tables[0].name == "customers"
    assert len(src.tables[0].columns) == 2
    assert src.tables[0].columns[0].name == "id"
    assert src.tables[1].loaded_at_column == "updated_at"


def test_sources_empty(tmp_path):
    """Test missing sources.yml returns empty list."""
    (tmp_path / "project.yml").write_text("name: test\n")
    config = load_project(tmp_path)
    assert config.sources == []


# --- Exposures tests ---


def test_exposures_parsing(tmp_path):
    """Test exposures.yml parsing."""
    (tmp_path / "project.yml").write_text("name: test\n")
    (tmp_path / "exposures.yml").write_text(
        """
exposures:
  - name: executive_dashboard
    description: "Main exec dashboard"
    owner: analytics_team
    type: dashboard
    url: https://looker.example.com/dashboard/1
    depends_on:
      - gold.revenue_summary
      - gold.customer_segments
  - name: ml_churn_model
    description: "Customer churn prediction"
    owner: ml_team
    type: ml_model
    depends_on:
      - gold.customer_features
"""
    )

    config = load_project(tmp_path)
    assert len(config.exposures) == 2
    exp = config.exposures[0]
    assert exp.name == "executive_dashboard"
    assert exp.owner == "analytics_team"
    assert exp.type == "dashboard"
    assert len(exp.depends_on) == 2


def test_exposures_empty(tmp_path):
    """Test missing exposures.yml returns empty list."""
    (tmp_path / "project.yml").write_text("name: test\n")
    config = load_project(tmp_path)
    assert config.exposures == []


# --- dp check with sources integration ---


def test_check_with_known_tables(tmp_path):
    """Test validate_models accepts known_tables for seeds/sources."""
    from dp.engine.transform import SQLModel, ValidationError, validate_models

    model = SQLModel(
        name="test_model",
        schema="silver",
        full_name="silver.test_model",
        materialized="table",
        depends_on=["seeds.products"],
        sql="-- config: materialized=table, schema=silver\nSELECT * FROM seeds.products",
        query="SELECT * FROM seeds.products",
        path=tmp_path / "transform" / "silver" / "test_model.sql",
        content_hash="abc123",
    )

    # Without known_tables, should report error
    errors = validate_models(None, [model])
    has_error = any(e.severity == "error" and "seeds.products" in e.message for e in errors)
    assert has_error

    # With known_tables including seeds.products, should pass
    errors = validate_models(None, [model], known_tables={"seeds.products"})
    has_error = any(e.severity == "error" and "seeds.products" in e.message for e in errors)
    assert not has_error


def test_check_with_source_columns(tmp_path):
    """Test validate_models validates columns from sources.yml."""
    from dp.engine.transform import SQLModel, validate_models

    model = SQLModel(
        name="test_model",
        schema="silver",
        full_name="silver.test_model",
        materialized="table",
        depends_on=["landing.customers"],
        sql="-- config: materialized=table, schema=silver\nSELECT c.id, c.nonexistent FROM landing.customers c",
        query="SELECT c.id, c.nonexistent FROM landing.customers c",
        path=tmp_path / "transform" / "silver" / "test_model.sql",
        content_hash="abc123",
    )

    source_columns = {"landing.customers": {"id", "email", "name"}}
    errors = validate_models(
        None, [model],
        known_tables={"landing.customers"},
        source_columns=source_columns,
    )
    # Should find that "nonexistent" is not in landing.customers
    col_errors = [e for e in errors if "nonexistent" in e.message]
    assert len(col_errors) > 0


# --- Stream with seed step ---


def test_stream_seed_step_parsing(tmp_path):
    """Test that seed is a valid stream step."""
    (tmp_path / "project.yml").write_text(
        """
name: test
streams:
  pipeline:
    steps:
      - seed: [all]
      - transform: [all]
"""
    )
    config = load_project(tmp_path)
    steps = config.streams["pipeline"].steps
    assert steps[0].action == "seed"
    assert steps[0].targets == ["all"]


# --- Docs with sources and exposures ---


def test_docs_include_sources(tmp_path):
    """Test that generate_docs includes sources section."""
    from dp.config import SourceColumn, SourceConfig, SourceTable
    from dp.engine.docs import generate_docs

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    conn.execute("CREATE TABLE bronze.test (id INTEGER)")

    sources = [
        SourceConfig(
            name="app_db",
            schema="landing",
            description="Production database",
            freshness_hours=6,
            tables=[
                SourceTable(
                    name="users",
                    description="User records",
                    columns=[SourceColumn(name="id", description="Primary key")],
                ),
            ],
        ),
    ]

    transform_dir = tmp_path / "transform"
    transform_dir.mkdir()
    md = generate_docs(conn, transform_dir, sources=sources)
    conn.close()

    assert "## Sources" in md
    assert "app_db" in md
    assert "landing.users" in md
    assert "Primary key" in md


def test_docs_include_exposures(tmp_path):
    """Test that generate_docs includes exposures section."""
    from dp.config import ExposureConfig
    from dp.engine.docs import generate_docs

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
    conn.execute("CREATE TABLE gold.revenue (amount DOUBLE)")

    exposures = [
        ExposureConfig(
            name="revenue_dashboard",
            description="Revenue tracking",
            owner="analytics",
            depends_on=["gold.revenue"],
            type="dashboard",
        ),
    ]

    transform_dir = tmp_path / "transform"
    transform_dir.mkdir()
    md = generate_docs(conn, transform_dir, exposures=exposures)
    conn.close()

    assert "## Exposures" in md
    assert "revenue_dashboard" in md
    assert "analytics" in md
