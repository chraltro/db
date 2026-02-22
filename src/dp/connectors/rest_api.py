"""REST API connector — fetches JSON data from HTTP endpoints."""

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
        ParamSpec("since_param", "Query param for incremental fetch (e.g. since, updated_after)", required=False),
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
        validate_identifier(target_schema, "target schema")
        for t in tables:
            validate_identifier(t, "table name")

        url = config.get("url", "")
        method = config.get("method", "GET")
        headers = config.get("headers", "{}")
        json_path = config.get("json_path", "$")
        table_name = tables[0] if tables else config.get("table_name", "api_data")
        pagination_key = config.get("pagination_key", "")
        since_param = config.get("since_param", "")

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
    page_data, _ = _fetch_with_retry(next_url, "{method}", req_headers)
    page_records = _extract(page_data, json_path)
    if not page_records:
        break
    all_records.extend(page_records)
    next_url = page_data.get("{pagination_key}")
'''

        # Incremental fetch block
        incremental_block = ""
        incremental_update = ""
        if since_param:
            incremental_block = f'''
# Incremental: append {since_param}=<last_value> to URL
from dp.engine.cdc import ensure_cdc_table, get_watermark, update_watermark
ensure_cdc_table(db)

_watermark = get_watermark(db, "rest_api", "{target_schema}.{table_name}")
if _watermark:
    _sep = "&" if "?" in url else "?"
    url = f"{{url}}{{_sep}}{since_param}={{_watermark}}"
    print(f"Incremental fetch: {since_param}={{_watermark}}")
'''
            incremental_update = f'''
    # Update watermark for next incremental fetch
    from datetime import datetime, timezone
    update_watermark(
        db, "rest_api", "{target_schema}.{table_name}",
        "high_watermark", datetime.now(timezone.utc).isoformat(),
        rows_synced=rows,
    )'''

        return f'''\
"""Auto-generated REST API ingest script.

Fetches data from {url} into {target_schema}.{table_name}.
Includes retry logic with exponential backoff.
"""

import json
import os
import time
from urllib.error import HTTPError
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


def _fetch_with_retry(fetch_url, method, headers, max_retries=3):
    """Fetch URL with retry on rate-limit (429) and server errors (5xx)."""
    for attempt in range(max_retries + 1):
        try:
            req = Request(fetch_url, method=method, headers=headers)
            with urlopen(req, timeout=30) as resp:
                return json.loads(resp.read()), resp.headers
        except HTTPError as e:
            if e.code == 429 or e.code >= 500:
                retry_after = int(e.headers.get("Retry-After", 2 ** attempt))
                print(f"  HTTP {{e.code}}, retrying in {{retry_after}}s... ({{attempt + 1}}/{{max_retries}})")
                time.sleep(retry_after)
            else:
                raise
        except (OSError, TimeoutError) as e:
            if attempt == max_retries:
                raise
            wait = 2 ** attempt
            print(f"  Network error ({{e}}), retrying in {{wait}}s... ({{attempt + 1}}/{{max_retries}})")
            time.sleep(wait)
    raise RuntimeError(f"Failed after {{max_retries}} retries: {{fetch_url}}")


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

{incremental_block}
print(f"Fetching data from {{url}}...")
data, _ = _fetch_with_retry(url, "{method}", req_headers)

all_records = _extract(data, json_path)
{pagination_block}
print(f"Got {{len(all_records)}} records")

if all_records:
    db.execute("CREATE SCHEMA IF NOT EXISTS {target_schema}")
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
{incremental_update}
else:
    print("No records found")
'''
