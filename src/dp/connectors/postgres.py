"""PostgreSQL connector — syncs tables via DuckDB's postgres extension."""

from __future__ import annotations

from typing import Any

from dp.engine.connector import (
    BaseConnector,
    DiscoveredResource,
    ParamSpec,
    register_connector,
)


@register_connector
class PostgresConnector(BaseConnector):
    name = "postgres"
    display_name = "PostgreSQL"
    description = "Import tables from a PostgreSQL database."
    default_schedule = "0 6 * * *"  # daily at 6 AM

    params = [
        ParamSpec("host", "Database host", default="localhost"),
        ParamSpec("port", "Database port", required=False, default=5432),
        ParamSpec("database", "Database name"),
        ParamSpec("user", "Username", default="postgres"),
        ParamSpec("password", "Password", secret=True),
        ParamSpec("schema", "Schema to import from", required=False, default="public"),
    ]

    def _conn_string(self, config: dict[str, Any]) -> str:
        host = config.get("host", "localhost")
        port = config.get("port", 5432)
        database = config.get("database", "postgres")
        user = config.get("user", "postgres")
        password = config.get("password", "")
        return f"host={host} port={port} dbname={database} user={user} password={password}"

    def test_connection(self, config: dict[str, Any]) -> dict:
        import duckdb

        conn = duckdb.connect(":memory:")
        try:
            conn.execute("INSTALL postgres; LOAD postgres;")
            conn_str = self._conn_string(config)
            conn.execute(f"ATTACH '{conn_str}' AS ext_db (TYPE POSTGRES, READ_ONLY)")
            conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_catalog = 'ext_db'"
            )
            return {"success": True}
        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            conn.close()

    def discover(self, config: dict[str, Any]) -> list[DiscoveredResource]:
        import duckdb

        schema = config.get("schema", "public")
        conn = duckdb.connect(":memory:")
        try:
            conn.execute("INSTALL postgres; LOAD postgres;")
            conn_str = self._conn_string(config)
            conn.execute(f"ATTACH '{conn_str}' AS ext_db (TYPE POSTGRES, READ_ONLY)")
            rows = conn.execute(
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_catalog = 'ext_db' AND table_schema = ?",
                [schema],
            ).fetchall()
            return [
                DiscoveredResource(name=r[1], schema=r[0]) for r in rows
            ]
        except Exception:
            return []
        finally:
            conn.close()

    def generate_script(
        self,
        config: dict[str, Any],
        tables: list[str],
        target_schema: str = "landing",
    ) -> str:
        host = config.get("host", "localhost")
        port = config.get("port", 5432)
        database = config.get("database", "postgres")
        user = config.get("user", "postgres")
        src_schema = config.get("schema", "public")

        # Password comes from .env via the connector framework
        password_env = config.get("password", "")
        if password_env.startswith("${") and password_env.endswith("}"):
            env_var = password_env[2:-1]
            password_line = f'password = os.environ.get("{env_var}", "")'
        else:
            password_line = f'password = os.environ.get("POSTGRES_PASSWORD", "")'

        table_list = ", ".join(f'"{t}"' for t in tables)

        return f'''\
"""Auto-generated PostgreSQL ingest script.

Syncs tables from {database} into {target_schema}.* via DuckDB's postgres extension.
"""

import os

{password_line}
conn_str = f"host={host} port={port} dbname={database} user={user} password={{password}}"

db.execute("INSTALL postgres; LOAD postgres;")
db.execute(f"ATTACH '{{conn_str}}' AS pg_src (TYPE POSTGRES, READ_ONLY)")
db.execute("CREATE SCHEMA IF NOT EXISTS {target_schema}")

tables = [{table_list}]

total_rows = 0
for table in tables:
    src = f"pg_src.{src_schema}.{{table}}"
    dest = f"{target_schema}.{{table}}"
    db.execute(f"CREATE OR REPLACE TABLE {{dest}} AS SELECT * FROM {{src}}")
    rows = db.execute(f"SELECT COUNT(*) FROM {{dest}}").fetchone()[0]
    total_rows += rows
    print(f"Loaded {{rows}} rows into {{dest}}")

db.execute("DETACH pg_src")
print(f"Loaded {{total_rows}} rows total from PostgreSQL")
'''
