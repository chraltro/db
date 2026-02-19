"""Stripe connector — syncs payments, customers, and charges data."""

from __future__ import annotations

from typing import Any

from dp.engine.connector import (
    BaseConnector,
    DiscoveredResource,
    ParamSpec,
    register_connector,
)

STRIPE_RESOURCES = [
    ("charges", "/v1/charges", "Payment charges"),
    ("customers", "/v1/customers", "Customer records"),
    ("invoices", "/v1/invoices", "Invoices"),
    ("subscriptions", "/v1/subscriptions", "Subscriptions"),
    ("payment_intents", "/v1/payment_intents", "Payment intents"),
    ("products", "/v1/products", "Products"),
    ("prices", "/v1/prices", "Prices"),
    ("balance_transactions", "/v1/balance_transactions", "Balance transactions"),
    ("refunds", "/v1/refunds", "Refunds"),
    ("disputes", "/v1/disputes", "Disputes"),
]


@register_connector
class StripeConnector(BaseConnector):
    name = "stripe"
    display_name = "Stripe"
    description = "Import payments, customers, and billing data from Stripe."
    default_schedule = "0 */6 * * *"

    params = [
        ParamSpec("api_key", "Stripe secret API key (sk_...)", secret=True),
        ParamSpec(
            "resources",
            "Comma-separated resources to sync (default: all)",
            required=False,
            default="charges,customers,invoices,subscriptions,payment_intents,products,prices,balance_transactions",
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
                "https://api.stripe.com/v1/charges?limit=1",
                headers={"Authorization": f"Bearer {api_key}"},
            )
            with urlopen(req, timeout=15) as resp:
                if resp.status < 400:
                    return {"success": True}
                return {"success": False, "error": f"HTTP {resp.status}"}
        except Exception as e:
            error = str(e)
            if "401" in error or "Unauthorized" in error:
                return {"success": False, "error": "Invalid API key"}
            return {"success": False, "error": error}

    def discover(self, config: dict[str, Any]) -> list[DiscoveredResource]:
        resources_str = config.get("resources", "")
        if resources_str:
            selected = [r.strip() for r in resources_str.split(",")]
        else:
            selected = [r[0] for r in STRIPE_RESOURCES]

        return [
            DiscoveredResource(name=f"stripe_{r[0]}", description=r[2])
            for r in STRIPE_RESOURCES
            if r[0] in selected
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
            key_line = 'api_key = os.environ.get("STRIPE_API_KEY", "")'

        resources_str = config.get("resources", "")
        if resources_str:
            selected = [r.strip() for r in resources_str.split(",")]
        else:
            selected = [r[0] for r in STRIPE_RESOURCES]

        # Build resource map for the script
        resource_entries = []
        for name, endpoint, _ in STRIPE_RESOURCES:
            if name in selected:
                resource_entries.append(f'    "{name}": "{endpoint}",')
        resource_map = "\n".join(resource_entries)

        return f'''\
"""Auto-generated Stripe ingest script.

Syncs Stripe data into {target_schema}.stripe_* tables.
"""

import json
import os
import tempfile
from urllib.request import Request, urlopen

{key_line}

RESOURCES = {{
{resource_map}
}}

BASE = "https://api.stripe.com"

db.execute("CREATE SCHEMA IF NOT EXISTS {target_schema}")

total_rows = 0
for resource_name, endpoint in RESOURCES.items():
    table = f"{target_schema}.stripe_{{resource_name}}"
    all_records = []
    url = f"{{BASE}}{{endpoint}}?limit=100"

    print(f"Fetching Stripe {{resource_name}}...")
    while url:
        req = Request(url, headers={{"Authorization": f"Bearer {{api_key}}"}})
        with urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())

        records = data.get("data", [])
        all_records.extend(records)

        # Stripe auto-pagination
        if data.get("has_more") and records:
            last_id = records[-1].get("id", "")
            url = f"{{BASE}}{{endpoint}}?limit=100&starting_after={{last_id}}"
        else:
            url = None

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
        print(f"No records for {{resource_name}}")

print(f"Loaded {{total_rows}} rows total from Stripe")
'''
