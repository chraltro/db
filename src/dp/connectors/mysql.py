"""MySQL connector — syncs tables via DuckDB's mysql extension."""

from __future__ import annotations

from typing import Any

from dp.engine.connector import (
    BaseConnector,
    DiscoveredResource,
    ParamSpec,
    register_connector,
)


@register_connector
class MySQLConnector(BaseConnector):
    name = "mysql"
    display_name = "MySQL"
    description = "Import tables from a MySQL database."
    default_schedule = "0 6 * * *"

    params = [
        ParamSpec("host", "Database host", default="localhost"),
        ParamSpec("port", "Database port", required=False, default=3306),
        ParamSpec("database", "Database name"),
        ParamSpec("user", "Username", default="root"),
        ParamSpec("password", "Password", secret=True),
    ]

    def _conn_string(self, config: dict[str, Any]) -> str:
        host = config.get("host", "localhost")
        port = config.get("port", 3306)
        database = config.get("database", "")
        user = config.get("user", "root")
        password = config.get("password", "")
        return f"host={host} port={port} database={database} user={user} password={password}"

    def test_connection(self, config: dict[str, Any]) -> dict:
        import duckdb

        conn = duckdb.connect(":memory:")
        try:
            conn.execute("INSTALL mysql; LOAD mysql;")
            conn_str = self._conn_string(config)
            conn.execute(f"ATTACH '{conn_str}' AS ext_db (TYPE MYSQL, READ_ONLY)")
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

        conn = duckdb.connect(":memory:")
        try:
            conn.execute("INSTALL mysql; LOAD mysql;")
            conn_str = self._conn_string(config)
            conn.execute(f"ATTACH '{conn_str}' AS ext_db (TYPE MYSQL, READ_ONLY)")
            rows = conn.execute(
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_catalog = 'ext_db'"
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
        port = config.get("port", 3306)
        database = config.get("database", "")
        user = config.get("user", "root")

        password_env = config.get("password", "")
        if password_env.startswith("${") and password_env.endswith("}"):
            env_var = password_env[2:-1]
            password_line = f'password = os.environ.get("{env_var}", "")'
        else:
            password_line = f'password = os.environ.get("MYSQL_PASSWORD", "")'

        table_list = ", ".join(f'"{t}"' for t in tables)

        return f'''\
"""Auto-generated MySQL ingest script.

Syncs tables from {database} into {target_schema}.* via DuckDB's mysql extension.
"""

import os

{password_line}
conn_str = f"host={host} port={port} database={database} user={user} password={{password}}"

db.execute("INSTALL mysql; LOAD mysql;")
db.execute(f"ATTACH '{{conn_str}}' AS mysql_src (TYPE MYSQL, READ_ONLY)")
db.execute("CREATE SCHEMA IF NOT EXISTS {target_schema}")

tables = [{table_list}]

total_rows = 0
for table in tables:
    src = f"mysql_src.{database}.{{table}}"
    dest = f"{target_schema}.{{table}}"
    db.execute(f"CREATE OR REPLACE TABLE {{dest}} AS SELECT * FROM {{src}}")
    rows = db.execute(f"SELECT COUNT(*) FROM {{dest}}").fetchone()[0]
    total_rows += rows
    print(f"Loaded {{rows}} rows into {{dest}}")

db.execute("DETACH mysql_src")
print(f"Loaded {{total_rows}} rows total from MySQL")
'''
