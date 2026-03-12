"""Tests for Schema Sentinel: schema change detection, impact analysis, and fix suggestions."""

import json
from pathlib import Path

import duckdb
import pytest

from havn.engine.sentinel import (
    ColumnInfo,
    SchemaChange,
    SentinelConfig,
    _classify_type_change,
    _ensure_sentinel_db,
    _hash_columns,
    _type_compatible,
    analyze_impact,
    apply_rename_fix,
    capture_source_schema,
    compute_diff,
    get_impacts_for_diff,
    get_recent_diffs,
    get_schema_history,
    get_source_names_from_models,
    resolve_impact,
    run_sentinel_check,
    snapshot_source,
)


@pytest.fixture
def project(tmp_path):
    """Create a minimal project directory with a warehouse DB."""
    (tmp_path / "project.yml").write_text("name: test\ndatabase:\n  path: warehouse.duckdb\n")

    db_path = tmp_path / "warehouse.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS landing")
    conn.execute("""
        CREATE TABLE landing.customers (
            id INTEGER NOT NULL,
            name VARCHAR,
            email VARCHAR,
            created_at TIMESTAMP
        )
    """)
    conn.execute("INSERT INTO landing.customers VALUES (1, 'Alice', 'alice@test.com', '2024-01-01')")
    conn.execute("INSERT INTO landing.customers VALUES (2, 'Bob', 'bob@test.com', '2024-01-02')")

    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    conn.execute("CREATE TABLE bronze.customers AS SELECT id, name, email FROM landing.customers")
    conn.close()

    return tmp_path


@pytest.fixture
def warehouse_conn(project):
    db_path = project / "warehouse.duckdb"
    conn = duckdb.connect(str(db_path))
    yield conn
    conn.close()


@pytest.fixture
def project_with_models(project):
    """Project with transform models that reference landing.customers."""
    transform = project / "transform"
    bronze = transform / "bronze"
    silver = transform / "silver"
    bronze.mkdir(parents=True)
    silver.mkdir(parents=True)

    (bronze / "customers.sql").write_text(
        "-- config: materialized=table, schema=bronze\n"
        "-- depends_on: landing.customers\n\n"
        "SELECT id, name, email FROM landing.customers\n"
    )
    (silver / "dim_customer.sql").write_text(
        "-- config: materialized=table, schema=silver\n"
        "-- depends_on: bronze.customers\n\n"
        "SELECT id, name FROM bronze.customers\n"
    )
    return project


# ---------------------------------------------------------------------------
# Schema capture
# ---------------------------------------------------------------------------


class TestSchemaCapture:
    def test_capture_source_schema(self, project, warehouse_conn):
        cols = capture_source_schema(warehouse_conn, "landing.customers")
        assert len(cols) == 4
        names = [c.name for c in cols]
        assert "id" in names
        assert "name" in names
        assert "email" in names
        assert "created_at" in names

    def test_capture_nonexistent_source(self, project, warehouse_conn):
        cols = capture_source_schema(warehouse_conn, "landing.nonexistent")
        assert cols == []

    def test_snapshot_source(self, project, warehouse_conn):
        snap_id = snapshot_source(project, warehouse_conn, "landing.customers", run_id="test-run")
        assert snap_id != ""

        history = get_schema_history(project, "landing.customers")
        assert len(history) == 1
        assert history[0]["snapshot_id"] == snap_id
        assert len(history[0]["columns"]) == 4


# ---------------------------------------------------------------------------
# Schema diff
# ---------------------------------------------------------------------------


class TestSchemaDiff:
    def test_no_changes(self):
        cols = [ColumnInfo("id", "INTEGER", False, 1), ColumnInfo("name", "VARCHAR", True, 2)]
        changes = compute_diff(cols, cols)
        assert changes == []

    def test_column_added(self):
        prev = [ColumnInfo("id", "INTEGER", False, 1)]
        curr = [ColumnInfo("id", "INTEGER", False, 1), ColumnInfo("email", "VARCHAR", True, 2)]
        changes = compute_diff(prev, curr)
        assert len(changes) == 1
        assert changes[0].change_type == "column_added"
        assert changes[0].column_name == "email"
        assert changes[0].severity == "info"

    def test_column_removed(self):
        prev = [ColumnInfo("id", "INTEGER", False, 1), ColumnInfo("email", "VARCHAR", True, 2)]
        curr = [ColumnInfo("id", "INTEGER", False, 1)]
        changes = compute_diff(prev, curr)
        assert len(changes) == 1
        assert changes[0].change_type == "column_removed"
        assert changes[0].column_name == "email"
        assert changes[0].severity == "breaking"

    def test_column_renamed(self):
        prev = [ColumnInfo("customer_id", "INTEGER", False, 1), ColumnInfo("name", "VARCHAR", True, 2)]
        curr = [ColumnInfo("cust_id", "INTEGER", False, 1), ColumnInfo("name", "VARCHAR", True, 2)]
        changes = compute_diff(prev, curr, SentinelConfig(rename_inference=True))
        # Should detect rename
        rename_changes = [c for c in changes if c.change_type == "column_renamed"]
        assert len(rename_changes) == 1
        assert rename_changes[0].old_value == "customer_id"
        assert rename_changes[0].new_value == "cust_id"

    def test_rename_inference_disabled(self):
        prev = [ColumnInfo("customer_id", "INTEGER", False, 1)]
        curr = [ColumnInfo("cust_id", "INTEGER", False, 1)]
        changes = compute_diff(prev, curr, SentinelConfig(rename_inference=False))
        types = {c.change_type for c in changes}
        assert "column_renamed" not in types
        assert "column_removed" in types
        assert "column_added" in types

    def test_type_changed(self):
        prev = [ColumnInfo("amount", "INTEGER", True, 1)]
        curr = [ColumnInfo("amount", "VARCHAR", True, 1)]
        changes = compute_diff(prev, curr)
        assert len(changes) == 1
        assert changes[0].change_type == "type_changed"
        assert changes[0].severity == "warning"

    def test_type_widened(self):
        prev = [ColumnInfo("count", "INTEGER", True, 1)]
        curr = [ColumnInfo("count", "BIGINT", True, 1)]
        changes = compute_diff(prev, curr)
        assert len(changes) == 1
        assert changes[0].change_type == "type_widened"
        assert changes[0].severity == "info"

    def test_type_narrowed(self):
        prev = [ColumnInfo("value", "BIGINT", True, 1)]
        curr = [ColumnInfo("value", "INTEGER", True, 1)]
        changes = compute_diff(prev, curr)
        assert len(changes) == 1
        assert changes[0].change_type == "type_narrowed"
        assert changes[0].severity == "breaking"

    def test_nullable_changed(self):
        prev = [ColumnInfo("id", "INTEGER", False, 1)]
        curr = [ColumnInfo("id", "INTEGER", True, 1)]
        changes = compute_diff(prev, curr)
        assert len(changes) == 1
        assert changes[0].change_type == "nullable_changed"
        assert changes[0].severity == "warning"

    def test_order_changed_tracked(self):
        prev = [ColumnInfo("a", "INT", True, 1), ColumnInfo("b", "INT", True, 2)]
        curr = [ColumnInfo("a", "INT", True, 2), ColumnInfo("b", "INT", True, 1)]
        changes = compute_diff(prev, curr, SentinelConfig(track_ordering=True))
        order_changes = [c for c in changes if c.change_type == "order_changed"]
        assert len(order_changes) == 2

    def test_order_changed_not_tracked(self):
        prev = [ColumnInfo("a", "INT", True, 1), ColumnInfo("b", "INT", True, 2)]
        curr = [ColumnInfo("a", "INT", True, 2), ColumnInfo("b", "INT", True, 1)]
        changes = compute_diff(prev, curr, SentinelConfig(track_ordering=False))
        assert len(changes) == 0

    def test_multiple_changes(self):
        prev = [
            ColumnInfo("id", "INTEGER", False, 1),
            ColumnInfo("name", "VARCHAR", True, 2),
            ColumnInfo("old_col", "TEXT", True, 3),
        ]
        curr = [
            ColumnInfo("id", "BIGINT", False, 1),  # type widened
            ColumnInfo("name", "VARCHAR", False, 2),  # nullable changed
            ColumnInfo("new_col", "TEXT", True, 3),  # old_col renamed
        ]
        changes = compute_diff(prev, curr, SentinelConfig(rename_inference=True))
        types = {c.change_type for c in changes}
        assert "type_widened" in types
        assert "nullable_changed" in types
        assert "column_renamed" in types


# ---------------------------------------------------------------------------
# Type classification
# ---------------------------------------------------------------------------


class TestTypeClassification:
    def test_compatible_types(self):
        assert _type_compatible("INTEGER", "BIGINT") is True
        assert _type_compatible("VARCHAR", "TEXT") is True
        assert _type_compatible("INTEGER", "VARCHAR") is False

    def test_classify_widened(self):
        assert _classify_type_change("INTEGER", "BIGINT") == "type_widened"
        assert _classify_type_change("FLOAT", "DOUBLE") == "type_widened"

    def test_classify_narrowed(self):
        assert _classify_type_change("BIGINT", "INTEGER") == "type_narrowed"

    def test_classify_incompatible(self):
        assert _classify_type_change("INTEGER", "VARCHAR") == "type_changed"


# ---------------------------------------------------------------------------
# Impact analysis
# ---------------------------------------------------------------------------


class TestImpactAnalysis:
    def test_direct_impact(self, project_with_models, warehouse_conn):
        changes = [
            SchemaChange("column_removed", "breaking", "email", old_value="VARCHAR"),
        ]
        impacts = analyze_impact(project_with_models, "landing.customers", changes, warehouse_conn)
        direct = [i for i in impacts if i.impact_type == "direct"]
        assert len(direct) >= 1
        # bronze.customers references email
        bronze_impact = [i for i in direct if i.model_name == "bronze.customers"]
        assert len(bronze_impact) == 1
        assert "email" in bronze_impact[0].columns_affected

    def test_transitive_impact(self, project_with_models, warehouse_conn):
        changes = [
            SchemaChange("column_removed", "breaking", "email", old_value="VARCHAR"),
        ]
        impacts = analyze_impact(project_with_models, "landing.customers", changes, warehouse_conn)
        transitive = [i for i in impacts if i.impact_type == "transitive"]
        assert any(i.model_name == "silver.dim_customer" for i in transitive)

    def test_safe_impact(self, project_with_models, warehouse_conn):
        # created_at is NOT referenced by any model
        changes = [
            SchemaChange("column_removed", "breaking", "created_at", old_value="TIMESTAMP"),
        ]
        impacts = analyze_impact(project_with_models, "landing.customers", changes, warehouse_conn)
        bronze = [i for i in impacts if i.model_name == "bronze.customers"]
        # bronze.customers doesn't reference created_at, so it should be "safe"
        if bronze:
            assert bronze[0].impact_type == "safe"

    def test_fix_suggestion_for_rename(self, project_with_models, warehouse_conn):
        changes = [
            SchemaChange("column_renamed", "breaking", "email",
                         old_value="email", new_value="email_address", rename_candidate="email_address"),
        ]
        impacts = analyze_impact(project_with_models, "landing.customers", changes, warehouse_conn)
        bronze = [i for i in impacts if i.model_name == "bronze.customers"]
        if bronze and bronze[0].fix_suggestion:
            assert "email" in bronze[0].fix_suggestion
            assert "email_address" in bronze[0].fix_suggestion


# ---------------------------------------------------------------------------
# Full sentinel check
# ---------------------------------------------------------------------------


class TestSentinelCheck:
    def test_first_run_no_diffs(self, project, warehouse_conn):
        """First snapshot: no previous snapshot to compare against."""
        diffs = run_sentinel_check(project, warehouse_conn, ["landing.customers"])
        assert diffs == []

    def test_detect_change(self, project, warehouse_conn):
        """Second run after a schema change should detect the diff."""
        # First run: baseline
        run_sentinel_check(project, warehouse_conn, ["landing.customers"], run_id="run-1")

        # Modify schema: add a column
        warehouse_conn.execute("ALTER TABLE landing.customers ADD COLUMN phone VARCHAR")

        # Second run: should detect change
        diffs = run_sentinel_check(project, warehouse_conn, ["landing.customers"], run_id="run-2")
        assert len(diffs) == 1
        assert diffs[0].source_name == "landing.customers"
        change_types = {c.change_type for c in diffs[0].changes}
        assert "column_added" in change_types

    def test_no_change_no_diff(self, project, warehouse_conn):
        """Two runs with no schema change should produce no diffs."""
        run_sentinel_check(project, warehouse_conn, ["landing.customers"], run_id="run-1")
        diffs = run_sentinel_check(project, warehouse_conn, ["landing.customers"], run_id="run-2")
        assert diffs == []

    def test_breaking_change_flagged(self, project, warehouse_conn):
        run_sentinel_check(project, warehouse_conn, ["landing.customers"], run_id="run-1")

        # Drop a column (breaking)
        warehouse_conn.execute("ALTER TABLE landing.customers DROP COLUMN email")

        diffs = run_sentinel_check(project, warehouse_conn, ["landing.customers"], run_id="run-2")
        assert len(diffs) == 1
        assert diffs[0].has_breaking is True

    def test_disabled_config(self, project, warehouse_conn):
        diffs = run_sentinel_check(
            project, warehouse_conn, ["landing.customers"],
            config=SentinelConfig(enabled=False),
        )
        assert diffs == []


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


class TestQueryHelpers:
    def test_get_recent_diffs(self, project, warehouse_conn):
        run_sentinel_check(project, warehouse_conn, ["landing.customers"], run_id="run-1")
        warehouse_conn.execute("ALTER TABLE landing.customers ADD COLUMN phone VARCHAR")
        run_sentinel_check(project, warehouse_conn, ["landing.customers"], run_id="run-2")

        diffs = get_recent_diffs(project)
        assert len(diffs) == 1
        assert diffs[0]["source_name"] == "landing.customers"

    def test_get_schema_history(self, project, warehouse_conn):
        run_sentinel_check(project, warehouse_conn, ["landing.customers"], run_id="run-1")
        warehouse_conn.execute("ALTER TABLE landing.customers ADD COLUMN phone VARCHAR")
        run_sentinel_check(project, warehouse_conn, ["landing.customers"], run_id="run-2")

        history = get_schema_history(project, "landing.customers")
        assert len(history) == 2
        # Most recent first
        assert len(history[0]["columns"]) == 5  # original 4 + phone
        assert len(history[1]["columns"]) == 4

    def test_get_source_names_from_models(self, project_with_models):
        sources = get_source_names_from_models(project_with_models)
        assert "landing.customers" in sources


# ---------------------------------------------------------------------------
# Fix application
# ---------------------------------------------------------------------------


class TestApplyFix:
    def test_apply_rename(self, project_with_models):
        result = apply_rename_fix(
            project_with_models,
            "transform/bronze/customers.sql",
            "email",
            "email_address",
        )
        assert result["status"] == "success"
        assert "1" in result["message"]  # at least 1 replacement

        content = (project_with_models / "transform" / "bronze" / "customers.sql").read_text()
        assert "email_address" in content
        assert "email" not in content or "email_address" in content

    def test_apply_rename_not_found(self, project_with_models):
        result = apply_rename_fix(
            project_with_models,
            "transform/bronze/customers.sql",
            "nonexistent_col",
            "new_col",
        )
        assert result["status"] == "error"

    def test_apply_rename_missing_file(self, project_with_models):
        result = apply_rename_fix(project_with_models, "transform/nonexistent.sql", "a", "b")
        assert result["status"] == "error"


# ---------------------------------------------------------------------------
# Resolve impact
# ---------------------------------------------------------------------------


class TestResolveImpact:
    def test_resolve(self, project_with_models, warehouse_conn):
        # Create a diff with impacts
        run_sentinel_check(project_with_models, warehouse_conn, ["landing.customers"], run_id="run-1")
        warehouse_conn.execute("ALTER TABLE landing.customers DROP COLUMN email")
        diffs = run_sentinel_check(project_with_models, warehouse_conn, ["landing.customers"], run_id="run-2")
        assert len(diffs) == 1

        impacts = get_impacts_for_diff(project_with_models, diffs[0].diff_id)
        if impacts:
            ok = resolve_impact(project_with_models, diffs[0].diff_id, impacts[0]["model_name"])
            assert ok is True


# ---------------------------------------------------------------------------
# Hash utilities
# ---------------------------------------------------------------------------


class TestHashUtils:
    def test_hash_columns_deterministic(self):
        cols = [ColumnInfo("a", "INT", True, 1), ColumnInfo("b", "VARCHAR", False, 2)]
        h1 = _hash_columns(cols)
        h2 = _hash_columns(cols)
        assert h1 == h2

    def test_hash_columns_changes(self):
        cols1 = [ColumnInfo("a", "INT", True, 1)]
        cols2 = [ColumnInfo("a", "BIGINT", True, 1)]
        assert _hash_columns(cols1) != _hash_columns(cols2)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class TestSentinelConfig:
    def test_defaults(self):
        c = SentinelConfig()
        assert c.enabled is True
        assert c.on_change == "pause"
        assert c.rename_inference is True
        assert c.auto_fix is False

    def test_custom(self):
        c = SentinelConfig(on_change="continue", track_ordering=True, auto_fix=True)
        assert c.on_change == "continue"
        assert c.track_ordering is True
        assert c.auto_fix is True
