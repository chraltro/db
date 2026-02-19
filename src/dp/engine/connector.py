"""Data connector framework.

Connectors auto-generate ingest scripts from templates, test connections,
and schedule syncs. Community-contributed connectors implement a simple
contract: a class that can test, discover, and generate scripts.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from dp.engine.database import connect, ensure_meta_table, log_run


# ---------------------------------------------------------------------------
# Connector contract
# ---------------------------------------------------------------------------


@dataclass
class ParamSpec:
    """Describes a single configuration parameter for a connector."""

    name: str
    description: str
    required: bool = True
    default: Any = None
    secret: bool = False  # stored in .env instead of project.yml


@dataclass
class DiscoveredResource:
    """A table, endpoint, or sheet discovered by a connector."""

    name: str
    schema: str = ""
    description: str = ""


class BaseConnector:
    """Base class for all data connectors.

    Community contributors implement a subclass and register it with
    ``@register_connector``.  The minimum contract is:

    - ``name``: short identifier (e.g. ``"postgres"``)
    - ``display_name``: human-readable name (e.g. ``"PostgreSQL"``)
    - ``description``: one-liner
    - ``params``: list of ``ParamSpec`` describing configuration
    - ``test_connection(config)``: verify the connection works
    - ``discover(config)``: list available tables / resources
    - ``generate_script(config, tables, target_schema)``: emit a Python ingest script
    """

    name: str = ""
    display_name: str = ""
    description: str = ""
    params: list[ParamSpec] = []
    default_schedule: str | None = None  # cron expression or None

    def test_connection(self, config: dict[str, Any]) -> dict:
        """Test the connection.

        Returns ``{"success": True}`` or ``{"success": False, "error": "..."}``
        """
        raise NotImplementedError

    def discover(self, config: dict[str, Any]) -> list[DiscoveredResource]:
        """Discover available tables / resources."""
        raise NotImplementedError

    def generate_script(
        self,
        config: dict[str, Any],
        tables: list[str],
        target_schema: str = "landing",
    ) -> str:
        """Return the contents of a Python ingest script."""
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

CONNECTORS: dict[str, type[BaseConnector]] = {}


def register_connector(cls: type[BaseConnector]) -> type[BaseConnector]:
    """Class decorator that registers a connector."""
    CONNECTORS[cls.name] = cls
    return cls


def get_connector(name: str) -> BaseConnector:
    """Instantiate a connector by name."""
    if name not in CONNECTORS:
        raise ValueError(
            f"Unknown connector: {name!r}. "
            f"Available: {', '.join(sorted(CONNECTORS))}"
        )
    return CONNECTORS[name]()


def list_connectors() -> list[dict[str, Any]]:
    """Return metadata for every registered connector."""
    results = []
    for name in sorted(CONNECTORS):
        cls = CONNECTORS[name]
        inst = cls()
        results.append({
            "name": inst.name,
            "display_name": inst.display_name,
            "description": inst.description,
            "params": [
                {
                    "name": p.name,
                    "description": p.description,
                    "required": p.required,
                    "secret": p.secret,
                    "default": p.default,
                }
                for p in inst.params
            ],
            "default_schedule": inst.default_schedule,
        })
    return results


# ---------------------------------------------------------------------------
# High-level operations (used by CLI and API)
# ---------------------------------------------------------------------------


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(value: str, label: str = "identifier") -> str:
    """Validate that a value is a safe SQL/Python identifier.

    Raises ``ValueError`` if the value contains unsafe characters.
    """
    if not value or not _IDENTIFIER_RE.match(value):
        raise ValueError(
            f"Invalid {label}: {value!r}. "
            "Must start with a letter or underscore and contain only "
            "letters, digits, and underscores."
        )
    return value


def _sanitize_name(raw: str) -> str:
    """Turn an arbitrary string into a safe Python/SQL identifier."""
    s = re.sub(r"[^a-zA-Z0-9_]", "_", raw.lower())
    s = re.sub(r"_+", "_", s).strip("_")
    if not s or s[0].isdigit():
        s = "_" + s
    return s


def setup_connector(
    project_dir: Path,
    connector_type: str,
    connection_name: str,
    config: dict[str, Any],
    tables: list[str] | None = None,
    target_schema: str = "landing",
    schedule: str | None = None,
) -> dict[str, Any]:
    """Full connector setup: test, discover, generate script, update project.yml.

    Returns a summary dict with keys: status, connection_name, script_path,
    tables, schedule.
    """
    connector = get_connector(connector_type)

    # 0. Validate identifiers before anything touches the filesystem or DB
    try:
        validate_identifier(target_schema, "target schema")
        validate_identifier(_sanitize_name(connection_name), "connection name")
    except ValueError as e:
        return {"status": "error", "error": str(e)}

    # 1. Test connection
    test_result = connector.test_connection(config)
    if not test_result.get("success"):
        return {
            "status": "error",
            "error": test_result.get("error", "Connection test failed"),
        }

    # 2. Discover resources if no tables specified
    if not tables:
        discovered = connector.discover(config)
        tables = [r.name for r in discovered]

    if not tables:
        return {
            "status": "error",
            "error": "No tables or resources found to sync",
        }

    # 3. Validate all table names are safe identifiers
    sanitized_tables = []
    for t in tables:
        safe = _sanitize_name(t)
        try:
            validate_identifier(safe, f"table name derived from '{t}'")
        except ValueError as e:
            return {"status": "error", "error": str(e)}
        sanitized_tables.append(safe)
    tables = sanitized_tables

    # 4. Generate ingest script
    script_content = connector.generate_script(config, tables, target_schema)
    safe_name = _sanitize_name(connection_name)
    script_filename = f"connector_{safe_name}.py"
    ingest_dir = project_dir / "ingest"
    ingest_dir.mkdir(parents=True, exist_ok=True)
    script_path = ingest_dir / script_filename
    script_path.write_text(script_content)

    # 5. Separate secrets from non-secret params
    secret_params = {}
    yml_params: dict[str, Any] = {"type": connector_type}
    for pspec in connector.params:
        val = config.get(pspec.name)
        if val is None:
            continue
        if pspec.secret:
            env_key = f"{safe_name.upper()}_{pspec.name.upper()}"
            secret_params[env_key] = str(val)
            yml_params[pspec.name] = f"${{{env_key}}}"
        else:
            yml_params[pspec.name] = val

    # 6. Write secrets to .env
    if secret_params:
        from dp.engine.secrets import set_secret

        for key, value in secret_params.items():
            set_secret(project_dir, key, value)

    # 7. Update project.yml — add connection and optionally a sync stream
    _update_project_yml(
        project_dir,
        connection_name=connection_name,
        connection_params=yml_params,
        script_filename=script_filename,
        schedule=schedule or connector.default_schedule,
    )

    return {
        "status": "success",
        "connection_name": connection_name,
        "connector_type": connector_type,
        "script_path": str(script_path.relative_to(project_dir)),
        "tables": tables,
        "schedule": schedule or connector.default_schedule,
    }


def test_connector(
    connector_type: str,
    config: dict[str, Any],
) -> dict[str, Any]:
    """Test a connector without setting anything up."""
    connector = get_connector(connector_type)
    return connector.test_connection(config)


def discover_connector(
    connector_type: str,
    config: dict[str, Any],
) -> list[dict[str, str]]:
    """Discover available resources for a connector."""
    connector = get_connector(connector_type)
    resources = connector.discover(config)
    return [
        {"name": r.name, "schema": r.schema, "description": r.description}
        for r in resources
    ]


def sync_connector(
    project_dir: Path,
    connection_name: str,
) -> dict[str, Any]:
    """Run the ingest script for a configured connector."""
    from dp.config import load_project
    from dp.engine.runner import run_script

    config = load_project(project_dir)
    db_path = project_dir / config.database.path

    safe_name = _sanitize_name(connection_name)
    script_path = project_dir / "ingest" / f"connector_{safe_name}.py"
    if not script_path.exists():
        return {
            "status": "error",
            "error": f"Ingest script not found: {script_path.name}",
        }

    conn = connect(db_path)
    try:
        return run_script(conn, script_path, "ingest")
    finally:
        conn.close()


def regenerate_connector(
    project_dir: Path,
    connection_name: str,
    config_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Re-generate the ingest script for an existing connector.

    Reads the current config from project.yml, applies any overrides,
    and rewrites the ingest script.  Useful after connector code is
    updated or when config values change.
    """
    from dp.config import load_project

    project_config = load_project(project_dir)

    # Find the connection in project.yml
    conn_config = project_config.connections.get(connection_name)
    if conn_config is None:
        return {"status": "error", "error": f"Connection '{connection_name}' not found in project.yml"}

    connector_type = conn_config.type
    try:
        connector = get_connector(connector_type)
    except ValueError as e:
        return {"status": "error", "error": str(e)}

    # Build config from stored params, overlaying overrides
    config: dict[str, Any] = dict(conn_config.params)
    config.pop("type", None)
    if config_overrides:
        config.update(config_overrides)

    # Resolve env-var references (${VAR} → look up in .env)
    from dp.engine.secrets import load_env

    env_vars = load_env(project_dir)
    for key, val in list(config.items()):
        if isinstance(val, str) and val.startswith("${") and val.endswith("}"):
            env_key = val[2:-1]
            resolved = env_vars.get(env_key)
            if resolved:
                config[key] = resolved

    # Discover resources for regeneration
    try:
        discovered = connector.discover(config)
        tables = [r.name for r in discovered]
    except Exception:
        tables = []

    if not tables:
        # Fallback: extract tables from existing script if possible
        tables = []

    # Determine target schema from existing stream or default
    target_schema = "landing"
    safe_name = _sanitize_name(connection_name)

    # Validate
    try:
        validate_identifier(target_schema, "target schema")
    except ValueError as e:
        return {"status": "error", "error": str(e)}

    # Sanitize tables
    if tables:
        sanitized = []
        for t in tables:
            safe = _sanitize_name(t)
            try:
                validate_identifier(safe, f"table name derived from '{t}'")
            except ValueError as e:
                return {"status": "error", "error": str(e)}
            sanitized.append(safe)
        tables = sanitized

    # Generate new script
    script_content = connector.generate_script(config, tables, target_schema)
    script_filename = f"connector_{safe_name}.py"
    script_path = project_dir / "ingest" / script_filename
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text(script_content)

    return {
        "status": "success",
        "connection_name": connection_name,
        "connector_type": connector_type,
        "script_path": str(script_path.relative_to(project_dir)),
        "tables": tables,
    }


def remove_connector(
    project_dir: Path,
    connection_name: str,
) -> dict[str, Any]:
    """Remove a connector: delete script, remove from project.yml."""
    safe_name = _sanitize_name(connection_name)
    script_path = project_dir / "ingest" / f"connector_{safe_name}.py"

    removed_script = False
    if script_path.exists():
        script_path.unlink()
        removed_script = True

    # Remove from project.yml
    removed_config = _remove_from_project_yml(project_dir, connection_name)

    if not removed_script and not removed_config:
        return {"status": "error", "error": f"Connector '{connection_name}' not found"}

    return {
        "status": "success",
        "removed_script": removed_script,
        "removed_config": removed_config,
    }


def list_configured_connectors(project_dir: Path) -> list[dict[str, Any]]:
    """List connectors that are configured in project.yml."""
    from dp.config import load_project

    config = load_project(project_dir)
    results = []
    for name, conn_config in config.connections.items():
        safe_name = _sanitize_name(name)
        script_path = project_dir / "ingest" / f"connector_{safe_name}.py"
        results.append({
            "name": name,
            "type": conn_config.type,
            "has_script": script_path.exists(),
            "script_path": f"ingest/connector_{safe_name}.py",
            "params": {k: "***" if k in ("password", "api_key", "token", "secret", "credentials") else v for k, v in conn_config.params.items()},
        })
    return results


# ---------------------------------------------------------------------------
# project.yml helpers
# ---------------------------------------------------------------------------


def _update_project_yml(
    project_dir: Path,
    connection_name: str,
    connection_params: dict[str, Any],
    script_filename: str,
    schedule: str | None = None,
) -> None:
    """Add a connection and sync stream to project.yml."""
    config_path = project_dir / "project.yml"
    if config_path.exists():
        raw = yaml.safe_load(config_path.read_text()) or {}
    else:
        raw = {"name": project_dir.name, "database": {"path": "warehouse.duckdb"}}

    # Add connection
    if "connections" not in raw or raw["connections"] is None:
        raw["connections"] = {}
    raw["connections"][connection_name] = connection_params

    # Add sync stream
    stream_name = f"sync-{_sanitize_name(connection_name)}"
    if "streams" not in raw or raw["streams"] is None:
        raw["streams"] = {}
    raw["streams"][stream_name] = {
        "description": f"Sync data from {connection_name}",
        "steps": [
            {"ingest": [script_filename.replace(".py", "")]},
            {"transform": ["all"]},
        ],
        "schedule": schedule,
    }

    config_path.write_text(yaml.dump(raw, default_flow_style=False, sort_keys=False))


def _remove_from_project_yml(project_dir: Path, connection_name: str) -> bool:
    """Remove a connection and its sync stream from project.yml."""
    config_path = project_dir / "project.yml"
    if not config_path.exists():
        return False

    raw = yaml.safe_load(config_path.read_text()) or {}
    removed = False

    # Remove connection
    connections = raw.get("connections")
    if isinstance(connections, dict) and connection_name in connections:
        del connections[connection_name]
        removed = True

    # Remove sync stream
    stream_name = f"sync-{_sanitize_name(connection_name)}"
    streams = raw.get("streams")
    if isinstance(streams, dict) and stream_name in streams:
        del streams[stream_name]
        removed = True

    if removed:
        config_path.write_text(yaml.dump(raw, default_flow_style=False, sort_keys=False))

    return removed
