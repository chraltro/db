"""Project configuration: project.yml parsing and defaults."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr


class DatabaseConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    path: str = "warehouse.duckdb"


class ConnectionConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    type: str = ""
    params: dict[str, Any] = Field(default_factory=dict)


class StreamStep(BaseModel):
    """A single step in a stream: ingest, transform, or export."""
    model_config = ConfigDict(extra="ignore")

    action: str  # "ingest", "transform", "export"
    targets: list[str]  # script names or model paths, ["all"] for everything


class StreamConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    description: str = ""
    steps: list[StreamStep] = Field(default_factory=list)
    schedule: str | None = None  # cron expression or None for on-demand
    retries: int = 0  # number of retry attempts for failed steps
    retry_delay: int = 5  # seconds between retries
    webhook_url: str | None = None  # POST notification on completion/failure


class LintConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    dialect: str = "duckdb"
    rules: list[str] = Field(default_factory=list)


class AlertsConfig(BaseModel):
    """Configuration for pipeline alerts and notifications."""
    model_config = ConfigDict(extra="ignore")

    slack_webhook_url: str | None = None
    webhook_url: str | None = None
    channels: list[str] = Field(default_factory=list)  # ["slack", "webhook", "log"]
    on_success: bool = True
    on_failure: bool = True
    on_assertion_failure: bool = True
    on_stale: bool = True
    freshness_hours: float = 24.0  # Max hours before a model is considered stale


class EnvironmentConfig(BaseModel):
    """A single environment override (e.g. dev, prod)."""
    model_config = ConfigDict(extra="ignore")

    database: dict[str, Any] = Field(default_factory=dict)  # {"path": "dev.duckdb"}
    connections: dict[str, dict[str, Any]] = Field(default_factory=dict)


class SourceColumn(BaseModel):
    """A column in a source table."""
    model_config = ConfigDict(extra="ignore")

    name: str
    description: str = ""


class SourceTable(BaseModel):
    """A declared external source table."""
    model_config = ConfigDict(extra="ignore")

    name: str
    description: str = ""
    columns: list[SourceColumn] = Field(default_factory=list)
    loaded_at_column: str | None = None


class SourceConfig(BaseModel):
    """An external data source declaration."""
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    name: str
    schema_name: str = Field(default="landing", alias="schema")
    description: str = ""
    tables: list[SourceTable] = Field(default_factory=list)
    freshness_hours: float | None = None  # max age SLA
    connection: str | None = None

    @property
    def schema(self) -> str:
        return self.schema_name


class ExposureConfig(BaseModel):
    """A downstream consumer declaration."""
    model_config = ConfigDict(extra="ignore")

    name: str
    description: str = ""
    owner: str = ""
    depends_on: list[str] = Field(default_factory=list)
    type: str = ""  # "dashboard", "report", "ml_model", etc.
    url: str = ""


class ProjectConfig(BaseModel):
    model_config = ConfigDict(extra="ignore", arbitrary_types_allowed=True)

    name: str = "default"
    description: str = ""
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    connections: dict[str, ConnectionConfig] = Field(default_factory=dict)
    streams: dict[str, StreamConfig] = Field(default_factory=dict)
    lint: LintConfig = Field(default_factory=LintConfig)
    alerts: AlertsConfig = Field(default_factory=AlertsConfig)
    environments: dict[str, EnvironmentConfig] = Field(default_factory=dict)
    active_environment: str | None = None
    sources: list[SourceConfig] = Field(default_factory=list)
    exposures: list[ExposureConfig] = Field(default_factory=list)
    project_dir: Path = Field(default_factory=Path.cwd)
    _raw: dict[str, Any] = PrivateAttr(default_factory=dict)


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


def _parse_sources(project_dir: Path) -> list[SourceConfig]:
    """Parse sources.yml if it exists."""
    sources_path = project_dir / "sources.yml"
    if not sources_path.exists():
        return []
    raw = yaml.safe_load(sources_path.read_text()) or {}
    raw = _expand_env_vars(raw)
    sources = []
    for src_raw in raw.get("sources", []):
        tables = []
        for t_raw in src_raw.get("tables", []):
            columns = [
                SourceColumn(name=c.get("name", ""), description=c.get("description", ""))
                for c in t_raw.get("columns", [])
            ]
            tables.append(SourceTable(
                name=t_raw.get("name", ""),
                description=t_raw.get("description", ""),
                columns=columns,
                loaded_at_column=t_raw.get("loaded_at_column"),
            ))
        sources.append(SourceConfig(
            name=src_raw.get("name", ""),
            schema=src_raw.get("schema", "landing"),
            description=src_raw.get("description", ""),
            tables=tables,
            freshness_hours=float(src_raw["freshness_hours"]) if "freshness_hours" in src_raw else None,
            connection=src_raw.get("connection"),
        ))
    return sources


def _parse_exposures(project_dir: Path) -> list[ExposureConfig]:
    """Parse exposures.yml if it exists."""
    exposures_path = project_dir / "exposures.yml"
    if not exposures_path.exists():
        return []
    raw = yaml.safe_load(exposures_path.read_text()) or {}
    raw = _expand_env_vars(raw)
    exposures = []
    for exp_raw in raw.get("exposures", []):
        exposures.append(ExposureConfig(
            name=exp_raw.get("name", ""),
            description=exp_raw.get("description", ""),
            owner=exp_raw.get("owner", ""),
            depends_on=exp_raw.get("depends_on", []),
            type=exp_raw.get("type", ""),
            url=exp_raw.get("url", ""),
        ))
    return exposures


def load_project(project_dir: Path | None = None, env: str | None = None) -> ProjectConfig:
    """Load project.yml from the given directory (or cwd).

    Args:
        project_dir: Path to the project directory.
        env: Environment name to activate (e.g. "dev", "prod").
             If environments are defined and env is None, defaults to "dev".
    """
    from dp.engine.secrets import load_env

    project_dir = Path(project_dir) if project_dir else Path.cwd()
    config_path = project_dir / "project.yml"

    # Load .env secrets into environment before expanding vars
    load_env(project_dir)

    if not config_path.exists():
        return ProjectConfig(project_dir=project_dir)

    raw = yaml.safe_load(config_path.read_text()) or {}
    raw = _expand_env_vars(raw)

    # Database
    db_raw = raw.get("database", {})
    database = DatabaseConfig(path=db_raw.get("path", "warehouse.duckdb"))

    # Connections (make a deep copy of each dict to avoid mutating raw)
    connections = {}
    for name, conn_raw in raw.get("connections", {}).items():
        conn_raw_copy = dict(conn_raw)
        conn_type = conn_raw_copy.pop("type", "")
        connections[name] = ConnectionConfig(type=conn_type, params=conn_raw_copy)

    # Streams
    streams = {}
    for name, stream_raw in raw.get("streams", {}).items():
        streams[name] = StreamConfig(
            description=stream_raw.get("description", ""),
            steps=_parse_stream_steps(stream_raw.get("steps", [])),
            schedule=stream_raw.get("schedule"),
            retries=int(stream_raw.get("retries", 0)),
            retry_delay=int(stream_raw.get("retry_delay", 5)),
            webhook_url=stream_raw.get("webhook_url"),
        )

    # Lint
    lint_raw = raw.get("lint", {})
    lint = LintConfig(
        dialect=lint_raw.get("dialect", "duckdb"),
        rules=lint_raw.get("rules", []),
    )

    # Alerts
    alerts_raw = raw.get("alerts", {})
    alerts = AlertsConfig(
        slack_webhook_url=alerts_raw.get("slack_webhook_url"),
        webhook_url=alerts_raw.get("webhook_url"),
        channels=alerts_raw.get("channels", []),
        on_success=alerts_raw.get("on_success", True),
        on_failure=alerts_raw.get("on_failure", True),
        on_assertion_failure=alerts_raw.get("on_assertion_failure", True),
        on_stale=alerts_raw.get("on_stale", True),
        freshness_hours=float(alerts_raw.get("freshness_hours", 24.0)),
    )

    # Environments
    environments: dict[str, EnvironmentConfig] = {}
    for env_name, env_raw in raw.get("environments", {}).items():
        environments[env_name] = EnvironmentConfig(
            database=env_raw.get("database", {}),
            connections=env_raw.get("connections", {}),
        )

    # Apply environment overrides
    active_env = env
    if environments and active_env is None:
        active_env = "dev" if "dev" in environments else None
    if active_env and active_env in environments:
        env_cfg = environments[active_env]
        if env_cfg.database:
            if "path" in env_cfg.database:
                database = DatabaseConfig(path=env_cfg.database["path"])
        for conn_name, conn_overrides in env_cfg.connections.items():
            if conn_name in connections:
                connections[conn_name].params.update(conn_overrides)
            else:
                conn_type = conn_overrides.pop("type", "") if isinstance(conn_overrides, dict) else ""
                connections[conn_name] = ConnectionConfig(type=conn_type, params=conn_overrides)

    # Sources and exposures
    sources = _parse_sources(project_dir)
    exposures = _parse_exposures(project_dir)

    config = ProjectConfig(
        name=raw.get("name", project_dir.name),
        description=raw.get("description", ""),
        database=database,
        connections=connections,
        streams=streams,
        lint=lint,
        alerts=alerts,
        environments=environments,
        active_environment=active_env if active_env and active_env in environments else None,
        sources=sources,
        exposures=exposures,
        project_dir=project_dir,
    )
    config._raw = raw
    return config


# --- Scaffold templates (re-exported from dp.templates for backward compat) ---

from dp.templates import (  # noqa: E402, F401
    CLAUDE_MD_TEMPLATE,
    PROJECT_YML_TEMPLATE,
    SAMPLE_BRONZE_SQL,
    SAMPLE_EXPORT_SCRIPT,
    SAMPLE_GOLD_REGIONS_SQL,
    SAMPLE_GOLD_SUMMARY_SQL,
    SAMPLE_GOLD_TOP_SQL,
    SAMPLE_INGEST_NOTEBOOK,
    SAMPLE_SILVER_DAILY_SQL,
    SAMPLE_SILVER_EVENTS_SQL,
)
