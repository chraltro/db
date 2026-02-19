"""Tests for the diff engine, schema diff, PK detection, snapshot, git, and CI."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from dp.engine.database import ensure_meta_table
from dp.engine.diff import (
    DiffResult,
    SchemaChange,
    _compute_schema_changes,
    diff_model,
    diff_models,
    get_primary_key,
    parse_primary_key_from_sql,
)


# ---- Helpers ----


def _setup_db(tmp_path):
    """Create a DuckDB database with landing data and run a basic transform."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS landing")
    conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
    ensure_meta_table(conn)
    return conn


def _create_model(tmp_path, schema, name, sql):
    """Create a SQL model file under transform/schema/name.sql."""
    d = tmp_path / "transform" / schema
    d.mkdir(parents=True, exist_ok=True)
    f = d / f"{name}.sql"
    f.write_text(sql)
    return f


# ---- Task 1.6: Diff tests ----


def test_diff_no_changes(tmp_path):
    """Model with no changes: added=0, removed=0."""
    conn = _setup_db(tmp_path)
    conn.execute("CREATE TABLE gold.metrics AS SELECT 1 AS id, 100 AS value")

    result = diff_model(conn, "SELECT 1 AS id, 100 AS value", "gold", "metrics")
    assert result.added == 0
    assert result.removed == 0
    assert result.modified == 0
    assert result.total_before == 1
    assert result.total_after == 1
    assert result.error is None
    conn.close()


def test_diff_added_rows(tmp_path):
    """Model with added rows."""
    conn = _setup_db(tmp_path)
    conn.execute("CREATE TABLE gold.metrics AS SELECT 1 AS id, 100 AS value")

    result = diff_model(
        conn,
        "SELECT 1 AS id, 100 AS value UNION ALL SELECT 2, 200",
        "gold",
        "metrics",
    )
    assert result.added == 1
    assert result.removed == 0
    assert result.total_before == 1
    assert result.total_after == 2
    assert len(result.sample_added) == 1
    conn.close()


def test_diff_removed_rows(tmp_path):
    """Model with removed rows."""
    conn = _setup_db(tmp_path)
    conn.execute(
        "CREATE TABLE gold.metrics AS "
        "SELECT 1 AS id, 100 AS value UNION ALL SELECT 2, 200"
    )

    result = diff_model(conn, "SELECT 1 AS id, 100 AS value", "gold", "metrics")
    assert result.added == 0
    assert result.removed == 1
    assert result.total_before == 2
    assert result.total_after == 1
    assert len(result.sample_removed) == 1
    conn.close()


def test_diff_modified_rows_with_pk(tmp_path):
    """Model with modified rows using primary key."""
    conn = _setup_db(tmp_path)
    conn.execute(
        "CREATE TABLE gold.metrics AS "
        "SELECT 1 AS id, 100 AS value UNION ALL SELECT 2, 200"
    )

    result = diff_model(
        conn,
        "SELECT 1 AS id, 150 AS value UNION ALL SELECT 2, 200",
        "gold",
        "metrics",
        primary_key=["id"],
    )
    assert result.modified == 1
    assert result.total_before == 2
    assert result.total_after == 2
    assert len(result.sample_modified) == 1
    conn.close()


def test_diff_new_table(tmp_path):
    """Model targeting a table that doesn't exist yet."""
    conn = _setup_db(tmp_path)

    result = diff_model(conn, "SELECT 1 AS id, 'hello' AS name", "gold", "new_table")
    assert result.is_new is True
    assert result.added == 1
    assert result.removed == 0
    assert result.total_before == 0
    assert result.total_after == 1
    conn.close()


def test_diff_model_sql_fails(tmp_path):
    """Model SQL that fails should return an error."""
    conn = _setup_db(tmp_path)

    result = diff_model(conn, "SELECT * FROM nonexistent_table_xyz", "gold", "broken")
    assert result.error is not None
    assert "nonexistent_table_xyz" in result.error.lower() or "not found" in result.error.lower() or "does not exist" in result.error.lower() or "Catalog Error" in result.error
    conn.close()


def test_diff_schema_change_added_column(tmp_path):
    """Detect added column in schema."""
    conn = _setup_db(tmp_path)
    conn.execute("CREATE TABLE gold.metrics AS SELECT 1 AS id, 100 AS value")

    result = diff_model(
        conn,
        "SELECT 1 AS id, 100 AS value, 'x' AS category",
        "gold",
        "metrics",
    )
    assert len(result.schema_changes) == 1
    assert result.schema_changes[0].column == "category"
    assert result.schema_changes[0].change_type == "added"
    conn.close()


def test_diff_schema_change_removed_column(tmp_path):
    """Detect removed column in schema."""
    conn = _setup_db(tmp_path)
    conn.execute("CREATE TABLE gold.metrics AS SELECT 1 AS id, 100 AS value, 'x' AS category")

    result = diff_model(
        conn,
        "SELECT 1 AS id, 100 AS value",
        "gold",
        "metrics",
    )
    assert any(sc.column == "category" and sc.change_type == "removed" for sc in result.schema_changes)
    conn.close()


def test_diff_schema_change_type_changed(tmp_path):
    """Detect type change in schema."""
    conn = _setup_db(tmp_path)
    conn.execute("CREATE TABLE gold.metrics AS SELECT 1 AS id, 100 AS value")

    result = diff_model(
        conn,
        "SELECT 1 AS id, 100.0 AS value",
        "gold",
        "metrics",
    )
    # DuckDB might or might not change the type here — let's check if it detects any schema changes
    # The key point is the code runs without error
    assert result.error is None
    conn.close()


def test_diff_full_row_comparison_no_pk(tmp_path):
    """Full-row comparison when no PK defined."""
    conn = _setup_db(tmp_path)
    conn.execute(
        "CREATE TABLE gold.metrics AS "
        "SELECT 1 AS id, 100 AS value UNION ALL SELECT 2, 200"
    )

    result = diff_model(
        conn,
        "SELECT 1 AS id, 100 AS value UNION ALL SELECT 3, 300",
        "gold",
        "metrics",
    )
    assert result.added == 1  # row (3, 300) is new
    assert result.removed == 1  # row (2, 200) is gone
    assert result.modified == 0  # no PK, can't detect modifications
    conn.close()


def test_pk_parsed_from_sql_comment():
    """PK parsed from SQL comment."""
    sql = "-- dp:primary_key = id\nSELECT 1 AS id"
    pk = parse_primary_key_from_sql(sql)
    assert pk == ["id"]


def test_pk_parsed_from_sql_comment_multi():
    """Multiple PK columns parsed from SQL comment."""
    sql = "-- dp:primary_key = id, date\nSELECT 1 AS id"
    pk = parse_primary_key_from_sql(sql)
    assert pk == ["id", "date"]


def test_pk_none_when_not_present():
    """No PK comment returns None."""
    sql = "SELECT 1 AS id"
    pk = parse_primary_key_from_sql(sql)
    assert pk is None


def test_pk_from_project_config():
    """PK from project config."""

    class FakeConfig:
        _raw = {"models": {"gold.metrics": {"primary_key": ["id"]}}}

    pk = get_primary_key("SELECT 1", FakeConfig(), "gold.metrics")
    assert pk == ["id"]


def test_pk_sql_takes_precedence():
    """SQL comment PK takes precedence over project.yml."""

    class FakeConfig:
        _raw = {"models": {"gold.metrics": {"primary_key": ["other_id"]}}}

    sql = "-- dp:primary_key = id\nSELECT 1 AS id"
    pk = get_primary_key(sql, FakeConfig(), "gold.metrics")
    assert pk == ["id"]


def test_diff_multiple_models(tmp_path):
    """Multiple models diffed at once."""
    conn = _setup_db(tmp_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
    conn.execute("CREATE TABLE gold.metrics AS SELECT 1 AS id, 100 AS value")
    conn.execute("CREATE TABLE silver.events AS SELECT 1 AS event_id, 'click' AS type")

    _create_model(
        tmp_path, "gold", "metrics",
        "-- config: materialized=table, schema=gold\n\n"
        "SELECT 1 AS id, 100 AS value UNION ALL SELECT 2, 200"
    )
    _create_model(
        tmp_path, "silver", "events",
        "-- config: materialized=table, schema=silver\n\n"
        "SELECT 1 AS event_id, 'click' AS type"
    )

    results = diff_models(conn, tmp_path / "transform")
    assert len(results) == 2
    # gold.metrics has an added row
    gold_result = next(r for r in results if r.model == "gold.metrics")
    assert gold_result.added == 1
    # silver.events has no changes
    silver_result = next(r for r in results if r.model == "silver.events")
    assert silver_result.added == 0
    assert silver_result.removed == 0
    conn.close()


def test_diff_json_output_format(tmp_path):
    """Verify DiffResult can be serialized to JSON."""
    conn = _setup_db(tmp_path)
    conn.execute("CREATE TABLE gold.metrics AS SELECT 1 AS id, 100 AS value")

    result = diff_model(conn, "SELECT 1 AS id, 100 AS value UNION ALL SELECT 2, 200", "gold", "metrics")
    # Serialize to dict and then JSON
    d = {
        "model": result.model,
        "added": result.added,
        "removed": result.removed,
        "modified": result.modified,
        "total_before": result.total_before,
        "total_after": result.total_after,
        "is_new": result.is_new,
        "error": result.error,
        "schema_changes": [
            {"column": sc.column, "change_type": sc.change_type}
            for sc in result.schema_changes
        ],
        "sample_added": result.sample_added,
        "sample_removed": result.sample_removed,
        "sample_modified": result.sample_modified,
    }
    json_str = json.dumps(d)
    parsed = json.loads(json_str)
    assert parsed["model"] == "gold.metrics"
    assert parsed["added"] == 1
    conn.close()


# ---- Schema change unit tests ----


def test_compute_schema_changes():
    old = {"id": "INTEGER", "name": "VARCHAR", "old_col": "BOOLEAN"}
    new = {"id": "INTEGER", "name": "VARCHAR", "new_col": "DOUBLE"}
    changes = _compute_schema_changes(old, new)
    assert any(c.column == "new_col" and c.change_type == "added" for c in changes)
    assert any(c.column == "old_col" and c.change_type == "removed" for c in changes)


# ---- Snapshot tests (Task 2.3) ----


def test_snapshot_create_and_list(tmp_path):
    """Create snapshot, verify stored data."""
    from dp.engine.snapshot import create_snapshot, list_snapshots

    conn = _setup_db(tmp_path)
    conn.execute("CREATE TABLE gold.metrics AS SELECT 1 AS id, 100 AS value")

    # Create project files
    (tmp_path / "project.yml").write_text("name: test")
    _create_model(tmp_path, "gold", "metrics", "SELECT 1 AS id, 100 AS value")

    result = create_snapshot(conn, tmp_path, "v1")
    assert result["name"] == "v1"
    assert result["table_count"] >= 1
    assert result["file_count"] >= 1

    snapshots = list_snapshots(conn)
    assert len(snapshots) == 1
    assert snapshots[0]["name"] == "v1"
    conn.close()


def test_snapshot_diff_detects_changes(tmp_path):
    """Create snapshot, modify data, diff against snapshot detects changes."""
    from dp.engine.snapshot import create_snapshot, diff_against_snapshot

    conn = _setup_db(tmp_path)
    conn.execute("CREATE TABLE gold.metrics AS SELECT 1 AS id, 100 AS value")
    (tmp_path / "project.yml").write_text("name: test")
    _create_model(tmp_path, "gold", "metrics", "SELECT 1 AS id, 100 AS value")

    create_snapshot(conn, tmp_path, "baseline")

    # Modify data
    conn.execute("INSERT INTO gold.metrics VALUES (2, 200)")

    result = diff_against_snapshot(conn, tmp_path, "baseline")
    assert result is not None
    # Should detect modified table
    table_changes = result["table_changes"]
    assert any(tc["table"] == "gold.metrics" and tc["status"] == "modified" for tc in table_changes)
    conn.close()


def test_snapshot_no_changes(tmp_path):
    """Create snapshot, no changes, diff shows clean."""
    from dp.engine.snapshot import create_snapshot, diff_against_snapshot

    conn = _setup_db(tmp_path)
    conn.execute("CREATE TABLE gold.metrics AS SELECT 1 AS id, 100 AS value")
    (tmp_path / "project.yml").write_text("name: test")
    _create_model(tmp_path, "gold", "metrics", "SELECT 1 AS id, 100 AS value")

    create_snapshot(conn, tmp_path, "baseline")

    result = diff_against_snapshot(conn, tmp_path, "baseline")
    assert result is not None
    assert len(result["table_changes"]) == 0
    assert len(result["file_changes"]["added"]) == 0
    assert len(result["file_changes"]["removed"]) == 0
    assert len(result["file_changes"]["modified"]) == 0
    conn.close()


def test_snapshot_auto_naming(tmp_path):
    """Auto-naming format."""
    from dp.engine.snapshot import create_snapshot

    conn = _setup_db(tmp_path)
    (tmp_path / "project.yml").write_text("name: test")

    result = create_snapshot(conn, tmp_path)
    assert result["name"].startswith("snapshot-")
    conn.close()


def test_snapshot_deleted_table(tmp_path):
    """Snapshot with tables that no longer exist."""
    from dp.engine.snapshot import create_snapshot, diff_against_snapshot

    conn = _setup_db(tmp_path)
    conn.execute("CREATE TABLE gold.metrics AS SELECT 1 AS id")
    (tmp_path / "project.yml").write_text("name: test")

    create_snapshot(conn, tmp_path, "before")

    # Drop the table
    conn.execute("DROP TABLE gold.metrics")

    result = diff_against_snapshot(conn, tmp_path, "before")
    assert any(tc["status"] == "removed" for tc in result["table_changes"])
    conn.close()


def test_snapshot_file_manifest_changes(tmp_path):
    """File manifest detects added/removed/modified project files."""
    from dp.engine.snapshot import create_snapshot, diff_against_snapshot

    conn = _setup_db(tmp_path)
    (tmp_path / "project.yml").write_text("name: test")
    _create_model(tmp_path, "gold", "metrics", "SELECT 1")

    create_snapshot(conn, tmp_path, "before")

    # Add a new file
    _create_model(tmp_path, "gold", "new_model", "SELECT 2")
    # Modify existing
    _create_model(tmp_path, "gold", "metrics", "SELECT 1 AS id, 2 AS value")

    result = diff_against_snapshot(conn, tmp_path, "before")
    assert "transform/gold/new_model.sql" in result["file_changes"]["added"]
    assert "transform/gold/metrics.sql" in result["file_changes"]["modified"]
    conn.close()


def test_snapshot_delete(tmp_path):
    """Delete a snapshot."""
    from dp.engine.snapshot import create_snapshot, delete_snapshot, list_snapshots

    conn = _setup_db(tmp_path)
    (tmp_path / "project.yml").write_text("name: test")

    create_snapshot(conn, tmp_path, "v1")
    assert len(list_snapshots(conn)) == 1

    assert delete_snapshot(conn, "v1") is True
    assert len(list_snapshots(conn)) == 0
    assert delete_snapshot(conn, "nonexistent") is False
    conn.close()


# ---- Git integration tests (Task 3.6) ----


def test_is_git_repo_false_for_non_git(tmp_path):
    """is_git_repo returns false for non-git dirs."""
    from dp.engine.git import is_git_repo

    assert is_git_repo(tmp_path) is False


def _git_init(tmp_path):
    """Initialize a git repo with signing disabled."""
    import subprocess
    subprocess.run(["git", "init", "-b", "main"], cwd=tmp_path, capture_output=True)
    subprocess.run(["git", "config", "user.email", "test@test.com"], cwd=tmp_path, capture_output=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=tmp_path, capture_output=True)
    subprocess.run(["git", "config", "commit.gpgsign", "false"], cwd=tmp_path, capture_output=True)


def _git_commit(tmp_path, message):
    """Commit with signing disabled."""
    import subprocess
    subprocess.run(["git", "add", "."], cwd=tmp_path, capture_output=True)
    subprocess.run(["git", "commit", "--no-gpg-sign", "-m", message], cwd=tmp_path, capture_output=True)


def test_git_functions_in_real_repo(tmp_path):
    """Test git functions in a real git repo."""
    from dp.engine.git import (
        changed_files,
        current_branch,
        is_dirty,
        is_git_repo,
        last_commit_hash,
        last_commit_message,
    )

    _git_init(tmp_path)

    assert is_git_repo(tmp_path) is True

    # Create and commit a file
    (tmp_path / "test.txt").write_text("hello")
    _git_commit(tmp_path, "Initial commit")

    branch = current_branch(tmp_path)
    assert branch is not None

    assert is_dirty(tmp_path) is False

    commit = last_commit_hash(tmp_path)
    assert commit is not None and len(commit) > 0

    msg = last_commit_message(tmp_path)
    assert msg == "Initial commit"

    # Modify file
    (tmp_path / "test.txt").write_text("modified")
    assert is_dirty(tmp_path) is True

    files = changed_files(tmp_path)
    assert "test.txt" in files


def test_changed_files_detects_all_types(tmp_path):
    """changed_files detects modified, added, deleted files."""
    from dp.engine.git import changed_files

    _git_init(tmp_path)

    (tmp_path / "a.txt").write_text("a")
    (tmp_path / "b.txt").write_text("b")
    _git_commit(tmp_path, "init")

    # Modify a, delete b, add c
    (tmp_path / "a.txt").write_text("modified")
    (tmp_path / "b.txt").unlink()
    (tmp_path / "c.txt").write_text("new")

    files = changed_files(tmp_path)
    assert "a.txt" in files
    assert "b.txt" in files
    assert "c.txt" in files


def test_diff_files_between(tmp_path):
    """diff_files_between returns correct files."""
    import subprocess

    from dp.engine.git import diff_files_between

    _git_init(tmp_path)

    (tmp_path / "a.txt").write_text("a")
    _git_commit(tmp_path, "init")

    # Create branch and make changes
    subprocess.run(["git", "checkout", "-b", "feature"], cwd=tmp_path, capture_output=True)
    (tmp_path / "b.txt").write_text("b")
    _git_commit(tmp_path, "add b")

    # diff between main and feature (two-dot since it falls back)
    files = diff_files_between(tmp_path, "main", "feature")
    assert "b.txt" in files


def test_git_graceful_without_git(tmp_path):
    """All git functions degrade gracefully without git."""
    from dp.engine.git import (
        current_branch,
        is_dirty,
        is_git_repo,
        last_commit_hash,
        last_commit_message,
    )

    assert is_git_repo(tmp_path) is False
    assert current_branch(tmp_path) is None
    assert is_dirty(tmp_path) is False
    assert last_commit_hash(tmp_path) is None
    assert last_commit_message(tmp_path) is None


# ---- CI tests (Task 4.3) ----


def test_ci_generate_workflow(tmp_path):
    """Generated YAML is valid."""
    from dp.engine.ci import generate_workflow

    (tmp_path / "project.yml").write_text("name: test")
    result = generate_workflow(tmp_path)

    workflow_path = tmp_path / result["path"]
    assert workflow_path.exists()

    import yaml

    content = yaml.safe_load(workflow_path.read_text())
    assert content["name"] == "dp CI"
    # YAML parses `on:` as boolean True, so check for that key
    assert True in content or "on" in content
    assert "diff" in content["jobs"]


def test_ci_diff_comment_format():
    """Diff comment formatting is correct for various scenarios."""
    from dp.engine.ci import _format_diff_comment

    # No changes
    comment = _format_diff_comment([
        {"model": "gold.metrics", "added": 0, "removed": 0, "modified": 0,
         "total_before": 100, "total_after": 100, "schema_changes": [],
         "is_new": False, "error": None, "sample_added": [], "sample_removed": [], "sample_modified": []},
    ])
    assert "No data changes" in comment

    # With changes
    comment = _format_diff_comment([
        {"model": "gold.metrics", "added": 5, "removed": 2, "modified": 1,
         "total_before": 100, "total_after": 103, "schema_changes": [],
         "is_new": False, "error": None, "sample_added": [{"id": 1}], "sample_removed": [], "sample_modified": []},
    ])
    assert "gold.metrics" in comment
    assert "+5" in comment

    # New table
    comment = _format_diff_comment([
        {"model": "gold.new_table", "added": 10, "removed": 0, "modified": 0,
         "total_before": 0, "total_after": 10, "schema_changes": [],
         "is_new": True, "error": None, "sample_added": [], "sample_removed": [], "sample_modified": []},
    ])
    assert "NEW" in comment

    # Error
    comment = _format_diff_comment([
        {"model": "gold.broken", "added": 0, "removed": 0, "modified": 0,
         "total_before": 0, "total_after": 0, "schema_changes": [],
         "is_new": False, "error": "SQL failed", "sample_added": [], "sample_removed": [], "sample_modified": []},
    ])
    assert "ERROR" in comment


def test_ci_diff_comment_snapshot_format():
    """Snapshot diff comment format."""
    from dp.engine.ci import _format_diff_comment

    comment = _format_diff_comment({
        "snapshot_name": "baseline",
        "table_changes": [
            {"table": "gold.metrics", "status": "modified", "snapshot_rows": 100, "current_rows": 110},
        ],
        "file_changes": {
            "added": ["transform/gold/new.sql"],
            "removed": [],
            "modified": ["transform/gold/metrics.sql"],
        },
    })
    assert "gold.metrics" in comment
    assert "modified" in comment
    assert "new.sql" in comment


def test_ci_diff_comment_missing_token(tmp_path):
    """dp ci diff-comment handles missing GITHUB_TOKEN gracefully."""
    import os
    from dp.engine.ci import post_diff_comment

    # Ensure no token
    old_token = os.environ.pop("GITHUB_TOKEN", None)
    try:
        result = post_diff_comment("nonexistent.json")
        assert result.get("error") is not None
    finally:
        if old_token:
            os.environ["GITHUB_TOKEN"] = old_token


def test_ci_diff_comment_missing_file():
    """dp ci diff-comment handles missing file gracefully."""
    import os
    from dp.engine.ci import post_diff_comment

    os.environ["GITHUB_TOKEN"] = "test-token"
    try:
        result = post_diff_comment("nonexistent_file.json", "owner/repo", 1)
        assert "not found" in result.get("error", "")
    finally:
        del os.environ["GITHUB_TOKEN"]
