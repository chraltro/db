"""S3 / GCS connector — imports data from cloud storage buckets."""

from __future__ import annotations

from typing import Any

from dp.engine.connector import (
    BaseConnector,
    DiscoveredResource,
    ParamSpec,
    register_connector,
)


@register_connector
class S3GCSConnector(BaseConnector):
    name = "s3_gcs"
    display_name = "S3 / GCS"
    description = "Import files from Amazon S3 or Google Cloud Storage buckets."
    default_schedule = "0 6 * * *"

    params = [
        ParamSpec("path", "Bucket path (s3://bucket/prefix or gs://bucket/prefix)"),
        ParamSpec("format", "File format: csv, parquet, json (auto-detected if omitted)", required=False),
        ParamSpec("table_name", "Target table name", required=False),
        ParamSpec("aws_access_key_id", "AWS access key ID", required=False, secret=True),
        ParamSpec("aws_secret_access_key", "AWS secret access key", required=False, secret=True),
        ParamSpec("aws_region", "AWS region", required=False, default="us-east-1"),
    ]

    def test_connection(self, config: dict[str, Any]) -> dict:
        import duckdb

        path = config.get("path", "")
        if not path:
            return {"success": False, "error": "path is required"}

        conn = duckdb.connect(":memory:")
        try:
            self._setup_credentials(conn, config)

            # Try to list/read from the path
            fmt = self._detect_format(config)
            reader = self._reader_func(fmt)
            conn.execute(f"SELECT COUNT(*) FROM {reader}('{path}')")
            return {"success": True}
        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            conn.close()

    def _setup_credentials(self, conn: Any, config: dict[str, Any]) -> None:
        path = config.get("path", "")
        if path.startswith("s3://"):
            conn.execute("INSTALL httpfs; LOAD httpfs;")
            key_id = config.get("aws_access_key_id", "")
            secret_key = config.get("aws_secret_access_key", "")
            region = config.get("aws_region", "us-east-1")
            if key_id:
                conn.execute(f"SET s3_access_key_id = '{key_id}'")
            if secret_key:
                conn.execute(f"SET s3_secret_access_key = '{secret_key}'")
            if region:
                conn.execute(f"SET s3_region = '{region}'")
        elif path.startswith("gs://"):
            conn.execute("INSTALL httpfs; LOAD httpfs;")

    def _detect_format(self, config: dict[str, Any]) -> str:
        fmt = config.get("format", "")
        if fmt:
            return fmt
        path = config.get("path", "").lower()
        if ".parquet" in path or ".pq" in path:
            return "parquet"
        if ".json" in path or ".jsonl" in path:
            return "json"
        return "csv"

    def _reader_func(self, fmt: str) -> str:
        return {
            "csv": "read_csv",
            "parquet": "read_parquet",
            "json": "read_json",
        }.get(fmt, "read_csv")

    def discover(self, config: dict[str, Any]) -> list[DiscoveredResource]:
        path = config.get("path", "")
        table_name = config.get("table_name")
        if not table_name:
            # Derive from bucket path
            parts = path.replace("s3://", "").replace("gs://", "").strip("/").split("/")
            table_name = parts[-1].split(".")[0] if parts else "cloud_data"
            table_name = table_name.replace("-", "_").lower()
        return [DiscoveredResource(name=table_name, description=path)]

    def generate_script(
        self,
        config: dict[str, Any],
        tables: list[str],
        target_schema: str = "landing",
    ) -> str:
        path = config.get("path", "")
        fmt = self._detect_format(config)
        reader = self._reader_func(fmt)
        table_name = tables[0] if tables else "cloud_data"
        region = config.get("aws_region", "us-east-1")

        # Build credential setup
        cred_lines = []
        if path.startswith("s3://"):
            cred_lines.append('db.execute("INSTALL httpfs; LOAD httpfs;")')
            key_env = config.get("aws_access_key_id", "")
            secret_env = config.get("aws_secret_access_key", "")

            if key_env and key_env.startswith("${"):
                env_var = key_env[2:-1]
                cred_lines.append(f'key_id = os.environ.get("{env_var}", "")')
            else:
                cred_lines.append('key_id = os.environ.get("AWS_ACCESS_KEY_ID", "")')

            if secret_env and secret_env.startswith("${"):
                env_var = secret_env[2:-1]
                cred_lines.append(f'secret_key = os.environ.get("{env_var}", "")')
            else:
                cred_lines.append('secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")')

            cred_lines.append('if key_id:')
            cred_lines.append(f'    db.execute(f"SET s3_access_key_id = \'{{key_id}}\'")')
            cred_lines.append('if secret_key:')
            cred_lines.append(f'    db.execute(f"SET s3_secret_access_key = \'{{secret_key}}\'")')
            cred_lines.append(f'db.execute("SET s3_region = \'{region}\'")')
        elif path.startswith("gs://"):
            cred_lines.append('db.execute("INSTALL httpfs; LOAD httpfs;")')

        cred_block = "\n".join(cred_lines)

        return f'''\
"""Auto-generated S3/GCS ingest script.

Imports data from {path} into {target_schema}.{table_name}.
"""

import os

{cred_block}

bucket_path = "{path}"

print(f"Reading from {{bucket_path}}...")
db.execute("CREATE SCHEMA IF NOT EXISTS {target_schema}")
db.execute(f"""
    CREATE OR REPLACE TABLE {target_schema}.{table_name} AS
    SELECT * FROM {reader}('{{bucket_path}}')
""")

rows = db.execute("SELECT COUNT(*) FROM {target_schema}.{table_name}").fetchone()[0]
print(f"Loaded {{rows}} rows into {target_schema}.{table_name}")
'''
