"""Tests for data import engine."""

import csv
import json
from pathlib import Path

import duckdb

from dp.engine.importer import import_file, preview_file


def _make_csv(tmp_path: Path, name: str = "test.csv") -> Path:
    """Create a sample CSV file."""
    path = tmp_path / name
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "value"])
        writer.writerow([1, "Alice", 100])
        writer.writerow([2, "Bob", 200])
        writer.writerow([3, "Charlie", 300])
    return path


def _make_json(tmp_path: Path, name: str = "test.json") -> Path:
    """Create a sample JSON file."""
    path = tmp_path / name
    data = [
        {"id": 1, "city": "London"},
        {"id": 2, "city": "Paris"},
    ]
    path.write_text(json.dumps(data))
    return path


def test_preview_csv(tmp_path):
    """Preview a CSV file."""
    csv_path = _make_csv(tmp_path)
    result = preview_file(str(csv_path))
    assert result["columns"] == ["id", "name", "value"]
    assert len(result["rows"]) == 3
    assert result["source_type"] == "csv"


def test_preview_json(tmp_path):
    """Preview a JSON file."""
    json_path = _make_json(tmp_path)
    result = preview_file(str(json_path))
    assert "id" in result["columns"]
    assert "city" in result["columns"]
    assert len(result["rows"]) == 2


def test_import_csv(tmp_path):
    """Import a CSV file into the warehouse."""
    csv_path = _make_csv(tmp_path)
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        result = import_file(conn, str(csv_path), "landing", "customers")
        assert result["status"] == "success"
        assert result["rows"] == 3
        assert result["table"] == "landing.customers"

        # Verify data
        rows = conn.execute("SELECT * FROM landing.customers ORDER BY id").fetchall()
        assert len(rows) == 3
        assert rows[0][1] == "Alice"
    finally:
        conn.close()


def test_import_json(tmp_path):
    """Import a JSON file into the warehouse."""
    json_path = _make_json(tmp_path)
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        result = import_file(conn, str(json_path), "landing")
        assert result["status"] == "success"
        assert result["rows"] == 2
    finally:
        conn.close()


def test_import_missing_file(tmp_path):
    """Import non-existent file raises error."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        try:
            import_file(conn, "/nonexistent/file.csv", "landing")
            assert False, "Should have raised"
        except FileNotFoundError:
            pass
    finally:
        conn.close()
