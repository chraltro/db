"""CSV/file connector — imports data from local files or URLs."""

from __future__ import annotations

from typing import Any

from dp.engine.connector import (
    BaseConnector,
    DiscoveredResource,
    ParamSpec,
    register_connector,
)


@register_connector
class CSVConnector(BaseConnector):
    name = "csv"
    display_name = "CSV / File Upload"
    description = "Import data from CSV, Parquet, or JSON files (local path or URL)."
    default_schedule = None  # typically one-shot

    params = [
        ParamSpec("path", "File path or URL (supports CSV, Parquet, JSON)"),
        ParamSpec("format", "File format: csv, parquet, json (auto-detected if omitted)", required=False),
        ParamSpec("table_name", "Target table name", required=False),
    ]

    def test_connection(self, config: dict[str, Any]) -> dict:
        path = config.get("path", "")
        if not path:
            return {"success": False, "error": "path is required"}

        # URL test
        if path.startswith("http://") or path.startswith("https://"):
            from urllib.request import urlopen
            try:
                with urlopen(path, timeout=15) as resp:
                    if resp.status < 400:
                        return {"success": True}
                    return {"success": False, "error": f"HTTP {resp.status}"}
            except Exception as e:
                return {"success": False, "error": str(e)}

        # Local file test
        from pathlib import Path
        if Path(path).exists():
            return {"success": True}
        return {"success": False, "error": f"File not found: {path}"}

    def discover(self, config: dict[str, Any]) -> list[DiscoveredResource]:
        from pathlib import Path

        path = config.get("path", "")
        table_name = config.get("table_name")
        if not table_name:
            if path.startswith("http"):
                table_name = path.split("/")[-1].split("?")[0].split(".")[0]
            else:
                table_name = Path(path).stem
            table_name = table_name.replace("-", "_").replace(" ", "_").lower()
        return [DiscoveredResource(name=table_name, description=path)]

    def _detect_format(self, config: dict[str, Any]) -> str:
        fmt = config.get("format", "")
        if fmt:
            return fmt
        lower = config.get("path", "").lower()
        if lower.endswith(".parquet") or lower.endswith(".pq"):
            return "parquet"
        if lower.endswith(".json") or lower.endswith(".jsonl") or lower.endswith(".ndjson"):
            return "json"
        return "csv"

    def _reader_call(self, fmt: str, path_var: str) -> str:
        if fmt == "parquet":
            return f"read_parquet('{{{path_var}}}')"
        if fmt == "json":
            return f"read_json('{{{path_var}}}', auto_detect=true)"
        return f"read_csv('{{{path_var}}}', auto_detect=true)"

    def generate_script(
        self,
        config: dict[str, Any],
        tables: list[str],
        target_schema: str = "landing",
    ) -> str:
        path = config.get("path", "")
        fmt = self._detect_format(config)
        table_name = tables[0] if tables else "data"
        is_url = path.startswith("http://") or path.startswith("https://")

        if is_url:
            reader = self._reader_call(fmt, "tmp_path")
            lines = [
                f'"""Auto-generated CSV/file ingest script.',
                f"",
                f"Imports data from {path} into {target_schema}.{table_name}.",
                f'"""',
                f"",
                f"import os",
                f"import tempfile",
                f"from urllib.request import urlopen",
                f"",
                f'url = "{path}"',
                f"",
                f'print(f"Downloading {{url}}...")',
                f"with urlopen(url, timeout=60) as resp:",
                f"    data = resp.read()",
                f"",
                f'with tempfile.NamedTemporaryFile(mode="wb", suffix=".{fmt}", delete=False) as f:',
                f"    f.write(data)",
                f"    tmp_path = f.name",
                f"",
                f'db.execute("CREATE SCHEMA IF NOT EXISTS {target_schema}")',
                f'db.execute(f"""',
                f"    CREATE OR REPLACE TABLE {target_schema}.{table_name} AS",
                f"    SELECT * FROM {reader}",
                f'""")',
                f"",
                f"os.unlink(tmp_path)",
                f'rows = db.execute("SELECT COUNT(*) FROM {target_schema}.{table_name}").fetchone()[0]',
                f'print(f"Loaded {{rows}} rows into {target_schema}.{table_name}")',
                f"",
            ]
            return "\n".join(lines)
        else:
            reader = self._reader_call(fmt, "file_path")
            lines = [
                f'"""Auto-generated CSV/file ingest script.',
                f"",
                f"Imports data from {path} into {target_schema}.{table_name}.",
                f'"""',
                f"",
                f'file_path = "{path}"',
                f"",
                f'print(f"Reading {{file_path}}...")',
                f'db.execute("CREATE SCHEMA IF NOT EXISTS {target_schema}")',
                f'db.execute(f"""',
                f"    CREATE OR REPLACE TABLE {target_schema}.{table_name} AS",
                f"    SELECT * FROM {reader}",
                f'""")',
                f"",
                f'rows = db.execute("SELECT COUNT(*) FROM {target_schema}.{table_name}").fetchone()[0]',
                f'print(f"Loaded {{rows}} rows into {target_schema}.{table_name}")',
                f"",
            ]
            return "\n".join(lines)
