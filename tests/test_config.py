"""Tests for project configuration."""

from pathlib import Path

from dp.config import load_project


def test_load_missing_config(tmp_path):
    """Loading from a dir without project.yml returns defaults."""
    config = load_project(tmp_path)
    assert config.database.path == "warehouse.duckdb"
    assert config.streams == {}


def test_load_config(tmp_path):
    (tmp_path / "project.yml").write_text(
        """
name: test-project
description: "A test project"

database:
  path: my.duckdb

connections:
  pg:
    type: postgres
    host: localhost
    port: 5432

streams:
  pipeline:
    description: "Test pipeline"
    steps:
      - ingest: [customers]
      - transform: [all]
      - export: [to_csv]
    schedule: "0 6 * * *"

lint:
  dialect: duckdb
"""
    )

    config = load_project(tmp_path)
    assert config.name == "test-project"
    assert config.database.path == "my.duckdb"
    assert "pg" in config.connections
    assert config.connections["pg"].type == "postgres"
    assert "pipeline" in config.streams
    assert len(config.streams["pipeline"].steps) == 3
    assert config.streams["pipeline"].schedule == "0 6 * * *"
    assert config.lint.dialect == "duckdb"


def test_stream_steps_parsing(tmp_path):
    (tmp_path / "project.yml").write_text(
        """
name: test
streams:
  s1:
    steps:
      - ingest: [a, b]
      - transform: all
      - export: [x]
"""
    )
    config = load_project(tmp_path)
    steps = config.streams["s1"].steps
    assert steps[0].action == "ingest"
    assert steps[0].targets == ["a", "b"]
    assert steps[1].action == "transform"
    assert steps[1].targets == ["all"]
    assert steps[2].action == "export"
    assert steps[2].targets == ["x"]
