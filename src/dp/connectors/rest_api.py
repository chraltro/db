"""REST API connector — fetches JSON data from HTTP endpoints."""

from __future__ import annotations

from typing import Any

from dp.engine.connector import (
    BaseConnector,
    DiscoveredResource,
    ParamSpec,
    register_connector,
)


@register_connector
class RESTAPIConnector(BaseConnector):
    name = "rest_api"
    display_name = "REST API"
    description = "Fetch JSON data from any REST API endpoint."
    default_schedule = "0 */6 * * *"  # every 6 hours

    params = [
        ParamSpec("url", "API base URL (e.g. https://api.example.com/v1/data)"),
        ParamSpec("method", "HTTP method", required=False, default="GET"),
        ParamSpec("headers", "Extra headers as JSON string", required=False, default="{}"),
        ParamSpec("auth_header", "Authorization header value", required=False, secret=True),
        ParamSpec("json_path", "JSONPath to the data array (e.g. $.data or $.results)", required=False, default="$"),
        ParamSpec("table_name", "Target table name", required=False, default="api_data"),
        ParamSpec("pagination_key", "Key for next-page URL in response", required=False),
    ]

    def test_connection(self, config: dict[str, Any]) -> dict:
        import json
        from urllib.request import Request, urlopen

        url = config.get("url", "")
        if not url:
            return {"success": False, "error": "URL is required"}

        try:
            headers = {"Content-Type": "application/json"}
            extra = config.get("headers")
            if extra:
                if isinstance(extra, str):
                    extra = json.loads(extra)
                headers.update(extra)
            auth = config.get("auth_header")
            if auth:
                headers["Authorization"] = auth

            req = Request(url, method=config.get("method", "GET"), headers=headers)
            with urlopen(req, timeout=15) as resp:
                if resp.status < 400:
                    return {"success": True}
                return {"success": False, "error": f"HTTP {resp.status}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def discover(self, config: dict[str, Any]) -> list[DiscoveredResource]:
        table_name = config.get("table_name", "api_data")
        return [DiscoveredResource(name=table_name, description=config.get("url", ""))]

    def generate_script(
        self,
        config: dict[str, Any],
        tables: list[str],
        target_schema: str = "landing",
    ) -> str:
        url = config.get("url", "")
        method = config.get("method", "GET")
        headers = config.get("headers", "{}")
        json_path = config.get("json_path", "$")
        table_name = tables[0] if tables else config.get("table_name", "api_data")
        pagination_key = config.get("pagination_key", "")

        auth_env = config.get("auth_header", "")
        if auth_env.startswith("${") and auth_env.endswith("}"):
            env_var = auth_env[2:-1]
            auth_line = f'auth_header = os.environ.get("{env_var}", "")'
        else:
            auth_line = 'auth_header = ""'

        pagination_block = ""
        if pagination_key:
            pagination_block = f'''
# Pagination support
next_url = data.get("{pagination_key}")
while next_url:
    req = Request(next_url, method="{method}", headers=req_headers)
    with urlopen(req, timeout=30) as resp:
        page = json.loads(resp.read())
    page_records = _extract(page, json_path)
    if not page_records:
        break
    all_records.extend(page_records)
    next_url = page.get("{pagination_key}")
'''

        return f'''\
"""Auto-generated REST API ingest script.

Fetches data from {url} into {target_schema}.{table_name}.
"""

import json
import os
from urllib.request import Request, urlopen

{auth_line}

url = "{url}"
json_path = "{json_path}"

req_headers = {{"Content-Type": "application/json"}}
extra_headers = {headers}
if isinstance(extra_headers, str):
    extra_headers = json.loads(extra_headers)
req_headers.update(extra_headers)
if auth_header:
    req_headers["Authorization"] = auth_header

req = Request(url, method="{method}", headers=req_headers)

print(f"Fetching data from {{url}}...")
with urlopen(req, timeout=30) as resp:
    data = json.loads(resp.read())


def _extract(obj, path):
    """Simple JSONPath-like extraction for $.key.subkey patterns."""
    if path == "$" or not path:
        return obj if isinstance(obj, list) else [obj]
    parts = path.lstrip("$.").split(".")
    current = obj
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part, [])
        else:
            break
    return current if isinstance(current, list) else [current]


all_records = _extract(data, json_path)
{pagination_block}
print(f"Got {{len(all_records)}} records")

if all_records:
    db.execute("CREATE SCHEMA IF NOT EXISTS {target_schema}")
    # Write to a temp JSON file for DuckDB to read
    import tempfile
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(all_records, f)
        tmp_path = f.name

    db.execute(f"""
        CREATE OR REPLACE TABLE {target_schema}.{table_name} AS
        SELECT * FROM read_json('{{tmp_path}}', auto_detect=true)
    """)

    os.unlink(tmp_path)
    rows = db.execute("SELECT COUNT(*) FROM {target_schema}.{table_name}").fetchone()[0]
    print(f"Loaded {{rows}} rows into {target_schema}.{table_name}")
else:
    print("No records found")
'''
