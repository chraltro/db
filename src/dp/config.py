"""Project configuration: project.yml parsing and defaults."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class DatabaseConfig:
    path: str = "warehouse.duckdb"


@dataclass
class ConnectionConfig:
    type: str = ""
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class StreamStep:
    """A single step in a stream: ingest, transform, or export."""

    action: str  # "ingest", "transform", "export"
    targets: list[str]  # script names or model paths, ["all"] for everything


@dataclass
class StreamConfig:
    description: str = ""
    steps: list[StreamStep] = field(default_factory=list)
    schedule: str | None = None  # cron expression or None for on-demand


@dataclass
class LintConfig:
    dialect: str = "duckdb"
    rules: list[str] = field(default_factory=list)


@dataclass
class ProjectConfig:
    name: str = "default"
    description: str = ""
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    connections: dict[str, ConnectionConfig] = field(default_factory=dict)
    streams: dict[str, StreamConfig] = field(default_factory=dict)
    lint: LintConfig = field(default_factory=LintConfig)
    project_dir: Path = field(default_factory=Path.cwd)


def _expand_env_vars(value: Any) -> Any:
    """Expand ${ENV_VAR} references in string values."""
    if isinstance(value, str):
        return re.sub(
            r"\$\{(\w+)\}",
            lambda m: os.environ.get(m.group(1), m.group(0)),
            value,
        )
    if isinstance(value, dict):
        return {k: _expand_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env_vars(v) for v in value]
    return value


def _parse_stream_steps(raw_steps: list[dict]) -> list[StreamStep]:
    steps = []
    for step_dict in raw_steps:
        for action, targets in step_dict.items():
            if isinstance(targets, str):
                targets = [targets]
            steps.append(StreamStep(action=action, targets=targets))
    return steps


def load_project(project_dir: Path | None = None) -> ProjectConfig:
    """Load project.yml from the given directory (or cwd)."""
    project_dir = Path(project_dir) if project_dir else Path.cwd()
    config_path = project_dir / "project.yml"

    if not config_path.exists():
        return ProjectConfig(project_dir=project_dir)

    raw = yaml.safe_load(config_path.read_text()) or {}
    raw = _expand_env_vars(raw)

    # Database
    db_raw = raw.get("database", {})
    database = DatabaseConfig(path=db_raw.get("path", "warehouse.duckdb"))

    # Connections
    connections = {}
    for name, conn_raw in raw.get("connections", {}).items():
        conn_type = conn_raw.pop("type", "")
        connections[name] = ConnectionConfig(type=conn_type, params=conn_raw)

    # Streams
    streams = {}
    for name, stream_raw in raw.get("streams", {}).items():
        streams[name] = StreamConfig(
            description=stream_raw.get("description", ""),
            steps=_parse_stream_steps(stream_raw.get("steps", [])),
            schedule=stream_raw.get("schedule"),
        )

    # Lint
    lint_raw = raw.get("lint", {})
    lint = LintConfig(
        dialect=lint_raw.get("dialect", "duckdb"),
        rules=lint_raw.get("rules", []),
    )

    return ProjectConfig(
        name=raw.get("name", project_dir.name),
        description=raw.get("description", ""),
        database=database,
        connections=connections,
        streams=streams,
        lint=lint,
        project_dir=project_dir,
    )


# --- Scaffold templates ---

PROJECT_YML_TEMPLATE = """\
name: {name}
description: ""

database:
  path: warehouse.duckdb

connections: {{}}
  # postgres_prod:
  #   type: postgres
  #   host: localhost
  #   port: 5432
  #   database: production
  #   user: ${{POSTGRES_USER}}
  #   password: ${{POSTGRES_PASSWORD}}

streams:
  full-refresh:
    description: "Full data pipeline: ingest, transform, export"
    steps:
      - ingest: [all]
      - transform: [all]
      - export: [all]
    schedule: null  # on-demand only

lint:
  dialect: duckdb
"""

SAMPLE_INGEST_SCRIPT = '''\
"""Sample ingest script.

The platform calls run(db) with a DuckDB connection.
The script should create/replace tables in the landing schema.
"""

import duckdb


def run(db: duckdb.DuckDBPyConnection) -> None:
    db.execute("CREATE SCHEMA IF NOT EXISTS landing")

    # Example: load a CSV file
    # db.execute("""
    #     CREATE OR REPLACE TABLE landing.customers AS
    #     SELECT * FROM read_csv('data/customers.csv', auto_detect=true)
    # """)

    # Example: load from a dataframe
    # import pandas as pd
    # df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    # db.execute("CREATE OR REPLACE TABLE landing.example AS SELECT * FROM df")
'''

SAMPLE_BRONZE_SQL = """\
-- config: materialized=view, schema=bronze
-- depends_on: landing.example

SELECT
    *
FROM landing.example
"""

SAMPLE_EXPORT_SCRIPT = '''\
"""Sample export script.

The platform calls run(db) with a DuckDB connection.
Read from the warehouse and write to an external destination.
"""

import duckdb


def run(db: duckdb.DuckDBPyConnection) -> None:
    # Example: export a gold table to CSV
    # db.execute("""
    #     COPY gold.dim_customers TO 'output/customers.csv'
    #     (HEADER, DELIMITER ',')
    # """)
    pass
'''
