"""Shopify connector — syncs e-commerce data (orders, products, customers)."""

from __future__ import annotations

from typing import Any

from dp.engine.connector import (
    BaseConnector,
    DiscoveredResource,
    ParamSpec,
    register_connector,
)

SHOPIFY_RESOURCES = [
    ("orders", "/admin/api/2024-01/orders.json", "Orders"),
    ("products", "/admin/api/2024-01/products.json", "Products"),
    ("customers", "/admin/api/2024-01/customers.json", "Customers"),
    ("collections", "/admin/api/2024-01/custom_collections.json", "Collections"),
    ("inventory_items", "/admin/api/2024-01/inventory_items.json", "Inventory items"),
]


@register_connector
class ShopifyConnector(BaseConnector):
    name = "shopify"
    display_name = "Shopify"
    description = "Import e-commerce data from Shopify (orders, products, customers)."
    default_schedule = "0 */6 * * *"

    params = [
        ParamSpec("store", "Shopify store name (e.g. my-store from my-store.myshopify.com)"),
        ParamSpec("access_token", "Shopify Admin API access token", secret=True),
        ParamSpec(
            "resources",
            "Comma-separated resources to sync",
            required=False,
            default="orders,products,customers",
        ),
    ]

    def test_connection(self, config: dict[str, Any]) -> dict:
        import json
        from urllib.request import Request, urlopen

        store = config.get("store", "")
        token = config.get("access_token", "")
        if not store or not token:
            return {"success": False, "error": "store and access_token are required"}

        try:
            url = f"https://{store}.myshopify.com/admin/api/2024-01/shop.json"
            req = Request(url, headers={"X-Shopify-Access-Token": token})
            with urlopen(req, timeout=15) as resp:
                if resp.status < 400:
                    return {"success": True}
                return {"success": False, "error": f"HTTP {resp.status}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def discover(self, config: dict[str, Any]) -> list[DiscoveredResource]:
        resources_str = config.get("resources", "orders,products,customers")
        selected = [r.strip() for r in resources_str.split(",")]
        return [
            DiscoveredResource(name=f"shopify_{r[0]}", description=r[2])
            for r in SHOPIFY_RESOURCES
            if r[0] in selected
        ]

    def generate_script(
        self,
        config: dict[str, Any],
        tables: list[str],
        target_schema: str = "landing",
    ) -> str:
        store = config.get("store", "")
        token_env = config.get("access_token", "")
        if token_env.startswith("${") and token_env.endswith("}"):
            env_var = token_env[2:-1]
            token_line = f'access_token = os.environ.get("{env_var}", "")'
        else:
            token_line = 'access_token = os.environ.get("SHOPIFY_ACCESS_TOKEN", "")'

        resources_str = config.get("resources", "orders,products,customers")
        selected = [r.strip() for r in resources_str.split(",")]

        resource_entries = []
        for name, endpoint, _ in SHOPIFY_RESOURCES:
            if name in selected:
                resource_entries.append(f'    "{name}": "{endpoint}",')
        resource_map = "\n".join(resource_entries)

        return f'''\
"""Auto-generated Shopify ingest script.

Syncs Shopify data from {store}.myshopify.com into {target_schema}.shopify_* tables.
"""

import json
import os
import tempfile
from urllib.request import Request, urlopen
from urllib.parse import urlencode, urlparse, parse_qs

{token_line}

STORE = "{store}"
BASE = f"https://{{STORE}}.myshopify.com"

RESOURCES = {{
{resource_map}
}}

db.execute("CREATE SCHEMA IF NOT EXISTS {target_schema}")

total_rows = 0
for resource_name, endpoint in RESOURCES.items():
    table = f"{target_schema}.shopify_{{resource_name}}"
    all_records = []
    url = f"{{BASE}}{{endpoint}}?limit=250"

    print(f"Fetching Shopify {{resource_name}}...")
    while url:
        req = Request(url, headers={{"X-Shopify-Access-Token": access_token}})
        with urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())

            # Shopify returns {{ "orders": [...] }} etc.
            key = resource_name
            if key == "collections":
                key = "custom_collections"
            records = data.get(key, [])
            all_records.extend(records)

            # Link-header pagination
            link_header = resp.headers.get("Link", "")
            url = None
            if 'rel="next"' in link_header:
                for part in link_header.split(","):
                    if 'rel="next"' in part:
                        url = part.split("<")[1].split(">")[0]
                        break

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

print(f"Loaded {{total_rows}} rows total from Shopify")
'''
