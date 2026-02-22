from __future__ import annotations

import json

import duckdb
import pytest

from dp.engine.notebook import (
    _split_sql_statements,
    _validate_identifier,
    execute_ingest_cell,
)


def test_validate_identifier_valid():
    """Valid identifiers pass validation."""
    assert _validate_identifier("landing") == "landing"
    assert _validate_identifier("my_table") == "my_table"
    assert _validate_identifier("_private") == "_private"
    assert _validate_identifier("Table123") == "Table123"


def test_validate_identifier_invalid():
    """Invalid identifiers raise ValueError."""
    with pytest.raises(ValueError, match="Invalid"):
        _validate_identifier("DROP TABLE users--")
    with pytest.raises(ValueError, match="Invalid"):
        _validate_identifier("landing.data")
    with pytest.raises(ValueError, match="Invalid"):
        _validate_identifier("1bad_start")
    with pytest.raises(ValueError, match="Invalid"):
        _validate_identifier("has space")
    with pytest.raises(ValueError, match="Invalid"):
        _validate_identifier("")


def test_ingest_cell_rejects_injection_in_schema():
    """Ingest cell rejects SQL injection in target_schema."""
    conn = duckdb.connect(":memory:")
    spec = json.dumps({
        "source_type": "csv",
        "source_path": "/data/test.csv",
        "target_schema": "landing; DROP TABLE users--",
        "target_table": "data",
    })
    result = execute_ingest_cell(conn, spec)
    assert any(o["type"] == "error" for o in result["outputs"])
    assert any("Invalid" in o.get("text", "") for o in result["outputs"])
    conn.close()


def test_ingest_cell_rejects_injection_in_table():
    """Ingest cell rejects SQL injection in target_table."""
    conn = duckdb.connect(":memory:")
    spec = json.dumps({
        "source_type": "csv",
        "source_path": "/data/test.csv",
        "target_schema": "landing",
        "target_table": "data; DROP TABLE users--",
    })
    result = execute_ingest_cell(conn, spec)
    assert any(o["type"] == "error" for o in result["outputs"])
    conn.close()


def test_resolve_path_prevents_traversal(tmp_path):
    """_resolve_path prevents path traversal outside project dir."""
    from dp.engine.notebook import _resolve_path

    # Valid relative path should work
    result = _resolve_path("data/test.csv", tmp_path)
    assert str(tmp_path) in result

    # Path traversal should raise ValueError
    with pytest.raises(ValueError, match="Path traversal"):
        _resolve_path("../../etc/passwd", tmp_path)

    with pytest.raises(ValueError, match="Path traversal"):
        _resolve_path("data/../../../etc/passwd", tmp_path)


def test_split_sql_statements_basic():
    """Split basic multi-statement SQL."""
    stmts = _split_sql_statements("SELECT 1; SELECT 2")
    assert stmts == ["SELECT 1", "SELECT 2"]


def test_split_sql_statements_preserves_quoted_semicolons():
    """Semicolons inside single-quoted strings are preserved."""
    stmts = _split_sql_statements("SELECT 'hello;world' AS msg")
    assert len(stmts) == 1
    assert "hello;world" in stmts[0]


def test_split_sql_statements_escaped_quotes():
    """Escaped quotes (doubled) inside strings are handled."""
    stmts = _split_sql_statements("SELECT 'it''s a test;yes' AS msg; SELECT 2")
    assert len(stmts) == 2
    assert "it''s a test;yes" in stmts[0]
    assert stmts[1] == "SELECT 2"


def test_split_sql_statements_line_comments():
    """Line comments don't interfere with splitting."""
    sql = "-- Create table\nCREATE TABLE t (id INT); -- done\nSELECT * FROM t"
    stmts = _split_sql_statements(sql)
    assert len(stmts) == 2


def test_split_sql_statements_no_trailing_semicolon():
    """Handles SQL without a trailing semicolon."""
    stmts = _split_sql_statements("SELECT 1")
    assert stmts == ["SELECT 1"]


def test_split_sql_statements_empty():
    """Empty SQL returns no statements."""
    assert _split_sql_statements("") == []
    assert _split_sql_statements("  ") == []
    assert _split_sql_statements(";") == []
