"""HubSpot connector — syncs CRM data (contacts, companies, deals)."""

from __future__ import annotations

from typing import Any

from dp.engine.connector import (
    BaseConnector,
    DiscoveredResource,
    ParamSpec,
    register_connector,
)

HUBSPOT_OBJECTS = [
    ("contacts", "/crm/v3/objects/contacts", "CRM contacts"),
    ("companies", "/crm/v3/objects/companies", "CRM companies"),
    ("deals", "/crm/v3/objects/deals", "CRM deals"),
    ("tickets", "/crm/v3/objects/tickets", "Support tickets"),
    ("products", "/crm/v3/objects/products", "Products"),
    ("line_items", "/crm/v3/objects/line_items", "Line items"),
]


@register_connector
class HubSpotConnector(BaseConnector):
    name = "hubspot"
    display_name = "HubSpot"
    description = "Import CRM data from HubSpot (contacts, companies, deals)."
    default_schedule = "0 */6 * * *"

    params = [
        ParamSpec("api_key", "HubSpot private app access token", secret=True),
        ParamSpec(
            "objects",
            "Comma-separated objects to sync (default: contacts,companies,deals)",
            required=False,
            default="contacts,companies,deals",
        ),
    ]

    def test_connection(self, config: dict[str, Any]) -> dict:
        import json
        from urllib.request import Request, urlopen

        api_key = config.get("api_key", "")
        if not api_key:
            return {"success": False, "error": "api_key is required"}

        try:
            req = Request(
                "https://api.hubapi.com/crm/v3/objects/contacts?limit=1",
                headers={"Authorization": f"Bearer {api_key}"},
            )
            with urlopen(req, timeout=15) as resp:
                if resp.status < 400:
                    return {"success": True}
                return {"success": False, "error": f"HTTP {resp.status}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def discover(self, config: dict[str, Any]) -> list[DiscoveredResource]:
        objects_str = config.get("objects", "contacts,companies,deals")
        selected = [o.strip() for o in objects_str.split(",")]
        return [
            DiscoveredResource(name=f"hubspot_{o[0]}", description=o[2])
            for o in HUBSPOT_OBJECTS
            if o[0] in selected
        ]

    def generate_script(
        self,
        config: dict[str, Any],
        tables: list[str],
        target_schema: str = "landing",
    ) -> str:
        api_key_env = config.get("api_key", "")
        if api_key_env.startswith("${") and api_key_env.endswith("}"):
            env_var = api_key_env[2:-1]
            key_line = f'api_key = os.environ.get("{env_var}", "")'
        else:
            key_line = 'api_key = os.environ.get("HUBSPOT_API_KEY", "")'

        objects_str = config.get("objects", "contacts,companies,deals")
        selected = [o.strip() for o in objects_str.split(",")]

        obj_entries = []
        for name, endpoint, _ in HUBSPOT_OBJECTS:
            if name in selected:
                obj_entries.append(f'    "{name}": "{endpoint}",')
        obj_map = "\n".join(obj_entries)

        return f'''\
"""Auto-generated HubSpot ingest script.

Syncs HubSpot CRM data into {target_schema}.hubspot_* tables.
"""

import json
import os
import tempfile
from urllib.request import Request, urlopen
from urllib.parse import urlencode

{key_line}

OBJECTS = {{
{obj_map}
}}

BASE = "https://api.hubapi.com"

db.execute("CREATE SCHEMA IF NOT EXISTS {target_schema}")

total_rows = 0
for obj_name, endpoint in OBJECTS.items():
    table = f"{target_schema}.hubspot_{{obj_name}}"
    all_records = []
    url = f"{{BASE}}{{endpoint}}?limit=100"

    print(f"Fetching HubSpot {{obj_name}}...")
    while url:
        req = Request(url, headers={{"Authorization": f"Bearer {{api_key}}"}})
        with urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())

        results = data.get("results", [])
        # Flatten properties into top-level fields
        for r in results:
            props = r.pop("properties", {{}})
            r.update(props)
        all_records.extend(results)

        # HubSpot pagination
        paging = data.get("paging", {{}})
        next_link = paging.get("next", {{}}).get("link")
        url = next_link if next_link else None

    if all_records:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(all_records, f)
            tmp_path = f.name

        db.execute(f"""
            CREATE OR REPLACE TABLE {{table}} AS
            SELECT * FROM read_json('{{tmp_path}}', auto_detect=true)
        """)
        os.unlink(tmp_path)

        rows = db.execute(f"SELECT COUNT(*) FROM {{table}}").fetchone()[0]
        total_rows += rows
        print(f"Loaded {{rows}} rows into {{table}}")
    else:
        print(f"No records for {{obj_name}}")

print(f"Loaded {{total_rows}} rows total from HubSpot")
'''
