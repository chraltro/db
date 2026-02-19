"""Webhook connector — receives data via HTTP POST to a local endpoint."""

from __future__ import annotations

from typing import Any

from dp.engine.connector import (
    BaseConnector,
    DiscoveredResource,
    ParamSpec,
    register_connector,
)


@register_connector
class WebhookConnector(BaseConnector):
    name = "webhook"
    display_name = "Webhook"
    description = "Receive data via HTTP POST webhook endpoint. dp serve exposes /api/webhook/<name>."
    default_schedule = None  # event-driven, no schedule

    params = [
        ParamSpec("table_name", "Target table name for incoming data"),
        ParamSpec("secret", "Shared secret for webhook verification", required=False, secret=True),
        ParamSpec("append", "Append to table instead of replace (true/false)", required=False, default="true"),
    ]

    def test_connection(self, config: dict[str, Any]) -> dict:
        table_name = config.get("table_name", "")
        if not table_name:
            return {"success": False, "error": "table_name is required"}
        return {"success": True}

    def discover(self, config: dict[str, Any]) -> list[DiscoveredResource]:
        table_name = config.get("table_name", "webhook_data")
        return [
            DiscoveredResource(
                name=table_name,
                description="Incoming webhook data",
            )
        ]

    def generate_script(
        self,
        config: dict[str, Any],
        tables: list[str],
        target_schema: str = "landing",
    ) -> str:
        table_name = tables[0] if tables else config.get("table_name", "webhook_data")
        append = config.get("append", "true").lower() == "true"

        return f'''\
"""Auto-generated webhook ingest script.

Processes webhook data stored in {target_schema}.{table_name}_inbox.
The dp server receives POSTs at /api/webhook/{table_name} and stores
them in the inbox table. This script processes the inbox into the
final table.

To send data: POST JSON to http://localhost:3000/api/webhook/{table_name}
"""

import json

db.execute("CREATE SCHEMA IF NOT EXISTS {target_schema}")

# Create inbox table if it doesn\'t exist (the webhook endpoint creates rows here)
db.execute("""
    CREATE TABLE IF NOT EXISTS {target_schema}.{table_name}_inbox (
        id VARCHAR DEFAULT gen_random_uuid()::VARCHAR,
        received_at TIMESTAMP DEFAULT current_timestamp,
        payload JSON
    )
""")

# Check for new data
count = db.execute(
    "SELECT COUNT(*) FROM {target_schema}.{table_name}_inbox"
).fetchone()[0]

if count == 0:
    print("No new webhook data to process")
else:
    print(f"Processing {{count}} webhook records...")

    # Flatten JSON payloads into a table
    {"" if append else "db.execute('DROP TABLE IF EXISTS {target_schema}.{table_name}')"}
    db.execute("""
        {"INSERT INTO" if append else "CREATE OR REPLACE TABLE"} {target_schema}.{table_name}
        {"" if not append else "SELECT * FROM ("}
        SELECT
            id,
            received_at,
            unnest(from_json(payload, \'[{{"dummy": true}}]\'::JSON), recursive := true)
        FROM {target_schema}.{table_name}_inbox
        {")" if append else ""}
    """)

    # Clear processed inbox
    db.execute("DELETE FROM {target_schema}.{table_name}_inbox")

    rows = db.execute(
        "SELECT COUNT(*) FROM {target_schema}.{table_name}"
    ).fetchone()[0]
    print(f"Loaded {{rows}} rows into {target_schema}.{table_name}")
'''
