"""MySQL connector — syncs tables via DuckDB's mysql extension."""

from __future__ import annotations

from typing import Any

from dp.engine.connector import (
    BaseConnector,
    DiscoveredResource,
    ParamSpec,
    register_connector,
    validate_identifier,
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
        ParamSpec("cdc_column", "Column for incremental sync (e.g. updated_at)", required=False),
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
        validate_identifier(target_schema, "target schema")
        for t in tables:
            validate_identifier(t, "table name")

        host = config.get("host", "localhost")
        port = config.get("port", 3306)
        database = config.get("database", "")
        user = config.get("user", "root")

        cdc_column = config.get("cdc_column", "")
        if cdc_column:
            validate_identifier(cdc_column, "cdc_column")

        password_env = config.get("password", "")
        if isinstance(password_env, str) and password_env.startswith("${") and password_env.endswith("}"):
            env_var = password_env[2:-1]
            password_line = f'password = os.environ.get("{env_var}", "")'
        else:
            password_line = 'password = os.environ.get("MYSQL_PASSWORD", "")'

        table_list = ", ".join(f'"{t}"' for t in tables)

        if cdc_column:
            sync_block = _incremental_sync_block(
                target_schema, database, cdc_column, "mysql_src",
            )
        else:
            sync_block = _full_refresh_sync_block(
                target_schema, database, "mysql_src",
            )

        return f'''\
"""Auto-generated MySQL ingest script.

Syncs tables from {database} into {target_schema}.* via DuckDB's mysql extension.
Includes retry logic and per-table error handling.
"""

import os
import time

{password_line}
conn_str = f"host={host} port={port} database={database} user={user} password={{password}}"

db.execute("INSTALL mysql; LOAD mysql;")


def _attach_with_retry(conn_str, max_retries=3):
    """Attach MySQL with retry and exponential backoff."""
    for attempt in range(max_retries + 1):
        try:
            db.execute(f"ATTACH '{{conn_str}}' AS mysql_src (TYPE MYSQL, READ_ONLY)")
            return
        except Exception as e:
            if attempt == max_retries:
                raise
            wait = 2 ** attempt
            print(f"  Connection failed ({{e}}), retrying in {{wait}}s... ({{attempt + 1}}/{{max_retries}})")
            time.sleep(wait)


_attach_with_retry(conn_str)
db.execute("CREATE SCHEMA IF NOT EXISTS {target_schema}")

tables = [{table_list}]

{sync_block}

db.execute("DETACH mysql_src")

if errors:
    print(f"\\nCompleted with {{len(errors)}} error(s):")
    for table, err in errors:
        print(f"  {{table}}: {{err}}")
    raise RuntimeError(f"{{len(errors)}} table(s) failed to sync")
else:
    print(f"Loaded {{total_rows}} rows total from MySQL ({{len(tables)}} tables)")
'''


def _full_refresh_sync_block(
    target_schema: str,
    database: str,
    attach_alias: str,
) -> str:
    return f'''\
total_rows = 0
errors = []
for table in tables:
    src = f"{attach_alias}.{database}.{{table}}"
    dest = f"{target_schema}.{{table}}"
    try:
        db.execute(f"CREATE OR REPLACE TABLE {{dest}} AS SELECT * FROM {{src}}")
        rows = db.execute(f"SELECT COUNT(*) FROM {{dest}}").fetchone()[0]
        total_rows += rows
        print(f"Loaded {{rows}} rows into {{dest}}")
    except Exception as e:
        print(f"  ERROR syncing {{table}}: {{e}}")
        errors.append((table, str(e)))'''


def _incremental_sync_block(
    target_schema: str,
    database: str,
    cdc_column: str,
    attach_alias: str,
) -> str:
    return f'''\
# Incremental sync via high-watermark on "{cdc_column}"
from dp.engine.cdc import ensure_cdc_table, get_watermark, update_watermark
ensure_cdc_table(db)

CONNECTOR_NAME = "mysql_sync"

total_rows = 0
errors = []
for table in tables:
    src = f"{attach_alias}.{database}.{{table}}"
    dest = f"{target_schema}.{{table}}"
    full_name = f"{target_schema}.{{table}}"
    try:
        watermark = get_watermark(db, CONNECTOR_NAME, full_name)

        if watermark:
            safe_wm = watermark.replace("'", "''")
            query = f"SELECT * FROM {{src}} WHERE \\"{cdc_column}\\" > '{{safe_wm}}'"

            exists = db.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = '{target_schema}' AND table_name = '" + table + "'"
            ).fetchone()[0] > 0

            if exists:
                db.execute(f"INSERT INTO {{dest}} {{query}}")
            else:
                db.execute(f"CREATE TABLE {{dest}} AS {{query}}")
        else:
            db.execute(f"CREATE OR REPLACE TABLE {{dest}} AS SELECT * FROM {{src}}")

        rows = db.execute(f"SELECT COUNT(*) FROM {{dest}}").fetchone()[0]
        total_rows += rows

        new_wm = db.execute(
            f'SELECT MAX(\\"{cdc_column}\\")::VARCHAR FROM {{dest}}'
        ).fetchone()
        if new_wm and new_wm[0]:
            update_watermark(db, CONNECTOR_NAME, full_name, "high_watermark", new_wm[0], rows_synced=rows)

        suffix = " (incremental)" if watermark else " (full)"
        print(f"Loaded {{rows}} rows into {{dest}}{{suffix}}")
    except Exception as e:
        print(f"  ERROR syncing {{table}}: {{e}}")
        errors.append((table, str(e)))'''
