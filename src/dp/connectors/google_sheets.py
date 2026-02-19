"""Google Sheets connector — imports spreadsheet data via CSV export."""

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
class GoogleSheetsConnector(BaseConnector):
    name = "google_sheets"
    display_name = "Google Sheets"
    description = "Import data from a public or shared Google Sheets spreadsheet."
    default_schedule = "0 */4 * * *"  # every 4 hours

    params = [
        ParamSpec("spreadsheet_id", "Google Sheets ID (from the URL)"),
        ParamSpec("sheet_name", "Sheet/tab name", required=False, default="Sheet1"),
        ParamSpec("table_name", "Target table name", required=False),
    ]

    def test_connection(self, config: dict[str, Any]) -> dict:
        from urllib.request import urlopen

        spreadsheet_id = config.get("spreadsheet_id", "")
        if not spreadsheet_id:
            return {"success": False, "error": "spreadsheet_id is required"}

        sheet = config.get("sheet_name", "Sheet1")
        url = (
            f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
            f"/gviz/tq?tqx=out:csv&sheet={sheet}"
        )
        try:
            with urlopen(url, timeout=15) as resp:
                # Read just the header to verify access
                first_line = resp.readline()
                if first_line:
                    return {"success": True}
                return {"success": False, "error": "Empty response"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def discover(self, config: dict[str, Any]) -> list[DiscoveredResource]:
        sheet = config.get("sheet_name", "Sheet1")
        table_name = config.get("table_name") or sheet.lower().replace(" ", "_")
        return [DiscoveredResource(name=table_name, description=f"Sheet: {sheet}")]

    def generate_script(
        self,
        config: dict[str, Any],
        tables: list[str],
        target_schema: str = "landing",
    ) -> str:
        validate_identifier(target_schema, "target schema")
        for t in tables:
            validate_identifier(t, "table name")

        spreadsheet_id = config.get("spreadsheet_id", "")
        sheet = config.get("sheet_name", "Sheet1")
        table_name = tables[0] if tables else config.get("table_name") or sheet.lower().replace(" ", "_")

        return f'''\
"""Auto-generated Google Sheets ingest script.

Imports data from spreadsheet {spreadsheet_id} (sheet: {sheet})
into {target_schema}.{table_name}.
"""

import os
import tempfile
from urllib.request import urlopen

spreadsheet_id = "{spreadsheet_id}"
sheet_name = "{sheet}"

url = (
    f"https://docs.google.com/spreadsheets/d/{{spreadsheet_id}}"
    f"/gviz/tq?tqx=out:csv&sheet={{sheet_name}}"
)

print(f"Fetching Google Sheet: {{sheet_name}}...")
with urlopen(url, timeout=30) as resp:
    csv_data = resp.read()

# Write to temp file for DuckDB
with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv", delete=False) as f:
    f.write(csv_data)
    tmp_path = f.name

db.execute("CREATE SCHEMA IF NOT EXISTS {target_schema}")
db.execute(f"""
    CREATE OR REPLACE TABLE {target_schema}.{table_name} AS
    SELECT * FROM read_csv('{{tmp_path}}', auto_detect=true)
""")

os.unlink(tmp_path)
rows = db.execute("SELECT COUNT(*) FROM {target_schema}.{table_name}").fetchone()[0]
print(f"Loaded {{rows}} rows into {target_schema}.{table_name}")
'''
