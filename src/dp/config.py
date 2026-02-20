"""Project configuration: project.yml parsing and defaults."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class DatabaseConfig:
    path: str = "warehouse.duckdb"


@dataclass
class ConnectionConfig:
    type: str = ""
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class StreamStep:
    """A single step in a stream: ingest, transform, or export."""

    action: str  # "ingest", "transform", "export"
    targets: list[str]  # script names or model paths, ["all"] for everything


@dataclass
class StreamConfig:
    description: str = ""
    steps: list[StreamStep] = field(default_factory=list)
    schedule: str | None = None  # cron expression or None for on-demand
    retries: int = 0  # number of retry attempts for failed steps
    retry_delay: int = 5  # seconds between retries
    webhook_url: str | None = None  # POST notification on completion/failure


@dataclass
class LintConfig:
    dialect: str = "duckdb"
    rules: list[str] = field(default_factory=list)


@dataclass
class ProjectConfig:
    name: str = "default"
    description: str = ""
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    connections: dict[str, ConnectionConfig] = field(default_factory=dict)
    streams: dict[str, StreamConfig] = field(default_factory=dict)
    lint: LintConfig = field(default_factory=LintConfig)
    project_dir: Path = field(default_factory=Path.cwd)
    _raw: dict[str, Any] = field(default_factory=dict)


def _expand_env_vars(value: Any) -> Any:
    """Expand ${ENV_VAR} references in string values."""
    if isinstance(value, str):
        return re.sub(
            r"\$\{(\w+)\}",
            lambda m: os.environ.get(m.group(1), m.group(0)),
            value,
        )
    if isinstance(value, dict):
        return {k: _expand_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env_vars(v) for v in value]
    return value


def _parse_stream_steps(raw_steps: list[dict]) -> list[StreamStep]:
    steps = []
    for step_dict in raw_steps:
        for action, targets in step_dict.items():
            if isinstance(targets, str):
                targets = [targets]
            steps.append(StreamStep(action=action, targets=targets))
    return steps


def load_project(project_dir: Path | None = None) -> ProjectConfig:
    """Load project.yml from the given directory (or cwd)."""
    from dp.engine.secrets import load_env

    project_dir = Path(project_dir) if project_dir else Path.cwd()
    config_path = project_dir / "project.yml"

    # Load .env secrets into environment before expanding vars
    load_env(project_dir)

    if not config_path.exists():
        return ProjectConfig(project_dir=project_dir)

    raw = yaml.safe_load(config_path.read_text()) or {}
    raw = _expand_env_vars(raw)

    # Database
    db_raw = raw.get("database", {})
    database = DatabaseConfig(path=db_raw.get("path", "warehouse.duckdb"))

    # Connections
    connections = {}
    for name, conn_raw in raw.get("connections", {}).items():
        conn_type = conn_raw.pop("type", "")
        connections[name] = ConnectionConfig(type=conn_type, params=conn_raw)

    # Streams
    streams = {}
    for name, stream_raw in raw.get("streams", {}).items():
        streams[name] = StreamConfig(
            description=stream_raw.get("description", ""),
            steps=_parse_stream_steps(stream_raw.get("steps", [])),
            schedule=stream_raw.get("schedule"),
            retries=int(stream_raw.get("retries", 0)),
            retry_delay=int(stream_raw.get("retry_delay", 5)),
            webhook_url=stream_raw.get("webhook_url"),
        )

    # Lint
    lint_raw = raw.get("lint", {})
    lint = LintConfig(
        dialect=lint_raw.get("dialect", "duckdb"),
        rules=lint_raw.get("rules", []),
    )

    return ProjectConfig(
        name=raw.get("name", project_dir.name),
        description=raw.get("description", ""),
        database=database,
        connections=connections,
        streams=streams,
        lint=lint,
        project_dir=project_dir,
        _raw=raw,
    )


# --- Scaffold templates ---

PROJECT_YML_TEMPLATE = """\
name: {name}
description: ""

database:
  path: warehouse.duckdb

connections: {{}}
  # postgres_prod:
  #   type: postgres
  #   host: localhost
  #   port: 5432
  #   database: production
  #   user: ${{POSTGRES_USER}}
  #   password: ${{POSTGRES_PASSWORD}}

streams:
  full-refresh:
    description: "Full data pipeline: ingest, transform, export"
    steps:
      - ingest: [all]
      - transform: [all]
      - export: [all]
    schedule: null  # on-demand only

lint:
  dialect: duckdb
"""

SAMPLE_INGEST_NOTEBOOK = json.dumps({
    "title": "Earthquake Ingestion",
    "cells": [
        {
            "id": "cell_1",
            "type": "markdown",
            "source": "# Earthquake Data Ingestion\n\nFetches M2.5+ earthquakes from the past 30 days via the [USGS GeoJSON API](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php).\n\nThis notebook demonstrates using a `.dpnb` notebook as an ingest step in the pipeline.",
        },
        {
            "id": "cell_2",
            "type": "code",
            "source": 'import json\nfrom urllib.request import urlopen\n\nFEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_month.geojson"\n\nprint("Fetching earthquakes from USGS...")\nwith urlopen(FEED_URL, timeout=30) as resp:\n    data = json.loads(resp.read())\n\nfeatures = data.get("features", [])\nprint(f"Got {len(features)} earthquakes (M2.5+, last 30 days)")',
            "outputs": [],
        },
        {
            "id": "cell_3",
            "type": "markdown",
            "source": "## Create landing table\n\nCreate the `landing.earthquakes` table and load the raw data.",
        },
        {
            "id": "cell_4",
            "type": "code",
            "source": 'db.execute("CREATE SCHEMA IF NOT EXISTS landing")\n\ndb.execute("""\n    CREATE OR REPLACE TABLE landing.earthquakes (\n        id VARCHAR, magnitude DOUBLE, place VARCHAR,\n        event_time BIGINT, updated BIGINT,\n        latitude DOUBLE, longitude DOUBLE, depth_km DOUBLE,\n        felt INTEGER, tsunami INTEGER, sig INTEGER,\n        mag_type VARCHAR, event_type VARCHAR, status VARCHAR,\n        detail_url VARCHAR\n    )\n""")\n\nrows = []\nfor f in features:\n    p = f.get("properties", {})\n    c = f.get("geometry", {}).get("coordinates", [0, 0, 0])\n    rows.append((\n        str(f.get("id", "")),\n        p.get("mag"),\n        str(p.get("place", "")),\n        p.get("time"),\n        p.get("updated"),\n        c[1] if len(c) > 1 else None,\n        c[0] if len(c) > 0 else None,\n        c[2] if len(c) > 2 else None,\n        p.get("felt"),\n        p.get("tsunami"),\n        p.get("sig"),\n        str(p.get("magType", "")),\n        str(p.get("type", "")),\n        str(p.get("status", "")),\n        str(p.get("detail", "")),\n    ))\n\nif rows:\n    db.executemany(\n        "INSERT INTO landing.earthquakes VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",\n        rows,\n    )\n\nprint(f"Loaded {len(rows)} earthquakes into landing.earthquakes")',
            "outputs": [],
        },
        {
            "id": "cell_5",
            "type": "markdown",
            "source": "## Preview\n\nQuick look at the data we just loaded.",
        },
        {
            "id": "cell_6",
            "type": "code",
            "source": 'db.execute("SELECT * FROM landing.earthquakes ORDER BY magnitude DESC LIMIT 10")',
            "outputs": [],
        },
    ],
}, indent=2) + "\n"

SAMPLE_BRONZE_SQL = """\
-- config: materialized=table, schema=bronze
-- depends_on: landing.earthquakes

SELECT
    id AS event_id,
    magnitude,
    mag_type,
    place,
    latitude,
    longitude,
    depth_km,
    sig AS significance,
    event_type,
    status,
    epoch_ms(event_time) AS event_time,
    epoch_ms(updated) AS updated_at,
    coalesce(felt, 0) AS felt_reports,
    tsunami = 1 AS tsunami_alert
FROM landing.earthquakes
WHERE magnitude IS NOT NULL
"""

SAMPLE_SILVER_EVENTS_SQL = """\
-- config: materialized=table, schema=silver
-- depends_on: bronze.earthquakes

SELECT
    event_id,
    event_time,
    magnitude,
    mag_type,
    place,
    latitude,
    longitude,
    depth_km,
    felt_reports,
    tsunami_alert,
    significance,
    cast(event_time AS DATE) AS event_date,
    CASE
        WHEN magnitude >= 8.0 THEN 'Great'
        WHEN magnitude >= 7.0 THEN 'Major'
        WHEN magnitude >= 6.0 THEN 'Strong'
        WHEN magnitude >= 5.0 THEN 'Moderate'
        WHEN magnitude >= 4.0 THEN 'Light'
        ELSE 'Minor'
    END AS magnitude_class,
    CASE
        WHEN place LIKE '% of %'
            THEN trim(split_part(place, ' of ', 2))
        ELSE place
    END AS region,
    CASE
        WHEN depth_km < 70 THEN 'Shallow'
        WHEN depth_km < 300 THEN 'Intermediate'
        ELSE 'Deep'
    END AS depth_class,
    hour(event_time) AS event_hour
FROM bronze.earthquakes
"""

SAMPLE_SILVER_DAILY_SQL = """\
-- config: materialized=table, schema=silver
-- depends_on: silver.earthquake_events

SELECT
    event_date,
    count(*) AS total_events,
    round(avg(magnitude), 2) AS avg_magnitude,
    max(magnitude) AS max_magnitude,
    round(avg(depth_km), 1) AS avg_depth_km,
    sum(
        CASE
            WHEN
                magnitude_class IN (
                    'Strong', 'Major', 'Great'
                )
                THEN 1
            ELSE 0
        END
    ) AS significant_count,
    sum(
        CASE WHEN tsunami_alert THEN 1 ELSE 0 END
    ) AS tsunami_alerts,
    sum(felt_reports) AS total_felt_reports,
    arg_max(place, magnitude) AS strongest_location
FROM silver.earthquake_events
GROUP BY event_date
ORDER BY event_date DESC
"""

SAMPLE_GOLD_SUMMARY_SQL = """\
-- config: materialized=table, schema=gold
-- depends_on: silver.earthquake_events, silver.earthquake_daily

SELECT
    d.event_date,
    d.total_events,
    d.avg_magnitude,
    d.max_magnitude,
    d.avg_depth_km,
    d.significant_count,
    d.tsunami_alerts,
    d.total_felt_reports,
    d.strongest_location,
    sum(CASE
        WHEN e.magnitude_class = 'Minor' THEN 1
        ELSE 0
    END) AS minor_count,
    sum(CASE
        WHEN e.magnitude_class = 'Light' THEN 1
        ELSE 0
    END) AS light_count,
    sum(CASE
        WHEN e.magnitude_class = 'Moderate' THEN 1
        ELSE 0
    END) AS moderate_count,
    sum(CASE
        WHEN e.magnitude_class = 'Strong' THEN 1
        ELSE 0
    END) AS strong_count,
    sum(CASE
        WHEN e.magnitude_class = 'Major' THEN 1
        ELSE 0
    END) AS major_count,
    sum(CASE
        WHEN e.magnitude_class = 'Great' THEN 1
        ELSE 0
    END) AS great_count
FROM silver.earthquake_daily AS d
INNER JOIN silver.earthquake_events AS e
    ON d.event_date = e.event_date
GROUP BY ALL
ORDER BY d.event_date DESC
"""

SAMPLE_GOLD_TOP_SQL = """\
-- config: materialized=table, schema=gold
-- depends_on: silver.earthquake_events

SELECT
    event_id,
    event_time,
    magnitude,
    magnitude_class,
    place,
    region,
    latitude,
    longitude,
    depth_km,
    depth_class,
    felt_reports,
    tsunami_alert,
    significance
FROM silver.earthquake_events
WHERE magnitude >= 4.5
ORDER BY magnitude DESC, event_time DESC
"""

SAMPLE_GOLD_REGIONS_SQL = """\
-- config: materialized=table, schema=gold
-- depends_on: silver.earthquake_events

SELECT
    region,
    count(*) AS total_events,
    round(avg(magnitude), 2) AS avg_magnitude,
    max(magnitude) AS max_magnitude,
    round(avg(depth_km), 1) AS avg_depth_km,
    sum(CASE
        WHEN magnitude >= 5.0 THEN 1 ELSE 0
    END) AS significant_events,
    sum(CASE
        WHEN tsunami_alert THEN 1 ELSE 0
    END) AS tsunami_alerts,
    min(event_date) AS first_event,
    max(event_date) AS last_event
FROM silver.earthquake_events
GROUP BY region
HAVING count(*) >= 3
ORDER BY total_events DESC
"""

SAMPLE_EXPORT_SCRIPT = '''\
"""Export earthquake analytics to CSV files."""

from pathlib import Path

output_dir = Path(__file__).parent.parent / "output"
output_dir.mkdir(exist_ok=True)

db.execute(f"""
    COPY gold.earthquake_summary
    TO \\'{output_dir / "earthquake_summary.csv"}\\'
    (HEADER, DELIMITER \\',\\')
""")
rows = db.execute("SELECT COUNT(*) FROM gold.earthquake_summary").fetchone()[0]
print(f"Exported {rows} rows to output/earthquake_summary.csv")

db.execute(f"""
    COPY gold.top_earthquakes
    TO \\'{output_dir / "top_earthquakes.csv"}\\'
    (HEADER, DELIMITER \\',\\')
""")
rows = db.execute("SELECT COUNT(*) FROM gold.top_earthquakes").fetchone()[0]
print(f"Exported {rows} rows to output/top_earthquakes.csv")

db.execute(f"""
    COPY gold.region_risk
    TO \\'{output_dir / "region_risk.csv"}\\'
    (HEADER, DELIMITER \\',\\')
""")
rows = db.execute("SELECT COUNT(*) FROM gold.region_risk").fetchone()[0]
print(f"Exported {rows} rows to output/region_risk.csv")
'''

CLAUDE_MD_TEMPLATE = """\
# CLAUDE.md — Agent Instructions for {name}

This is a dp data platform project. dp uses DuckDB for analytics, plain SQL for transforms, and Python for ingest/export.

## Commands

```bash
dp transform              # build SQL models in dependency order
dp transform --force      # force rebuild all
dp run ingest/script.py   # run a single script
dp stream full-refresh    # run full pipeline (ingest -> transform -> export)
dp query "SELECT 1"       # ad-hoc SQL query
dp tables                 # list warehouse objects
dp lint                   # lint SQL (SQLFluff, DuckDB dialect)
dp lint --fix             # auto-fix lint violations
dp serve                  # start web UI on :3000
dp history                # show run log
```

## Project Layout

```
ingest/           Python scripts that load data into DuckDB (landing schema)
transform/
  bronze/         Light cleanup SQL (views/tables)
  silver/         Business logic, joins
  gold/           Consumption-ready models
export/           Python scripts that export data out
notebooks/        Interactive .dpnb notebooks
project.yml       Streams, connections, schedules
.env              Secrets (never committed)
warehouse.duckdb  The database (single file)
```

## SQL Model Convention

```sql
-- config: materialized=table, schema=silver
-- depends_on: bronze.customers, bronze.orders

SELECT c.customer_id, c.name, COUNT(o.order_id) AS order_count
FROM bronze.customers c
LEFT JOIN bronze.orders o ON c.customer_id = o.customer_id
GROUP BY 1, 2
```

- `-- config:` sets materialization (view/table) and schema
- `-- depends_on:` declares upstream dependencies for DAG ordering
- Folder name = default schema (e.g., transform/bronze/ -> schema=bronze)
- No Jinja, no templating — plain SQL only

## Python Script Convention

```python
# A DuckDB connection is available as `db` — just write top-level code
db.execute("CREATE SCHEMA IF NOT EXISTS landing")
db.execute("CREATE OR REPLACE TABLE landing.data AS SELECT * FROM ...")
```

- Scripts run as top-level code with `db` (DuckDB connection) pre-injected
- Legacy `def run(db)` scripts are still supported (backward compatible)
- `.dpnb` notebooks can also be used as ingest/export steps
- Scripts prefixed with `_` are skipped

## Schemas

- `landing` — raw data from ingest scripts
- `bronze` — cleaned, deduplicated
- `silver` — business logic, joins
- `gold` — consumption-ready
- `_dp_internal` — metadata (do not modify directly)

## Things You Can Ask Your AI Assistant

Here are example prompts that work well with this project:

**Adding data:**
- "Load CSV files from data/customers.csv into the warehouse"
- "Create an ingest script that pulls data from our Postgres database"
- "Add a new data source for our Stripe payments API"

**Transforming data:**
- "Create a silver model that joins customers with their orders"
- "Add a gold table that shows monthly revenue by product category"
- "Fix the SQL error in transform/silver/dim_customer.sql"

**Querying & exploring:**
- "Show me the top 10 customers by order count"
- "What tables are in the warehouse and what columns do they have?"
- "Write a query to find duplicate records in landing.customers"

**Operations:**
- "Run the full pipeline and show me what happened"
- "Why did the last transform fail?"
- "Set up a daily schedule for the full-refresh stream"
- "Add a new export that writes the gold.revenue table to CSV"

**Tip:** Run `dp context` to generate a summary of your project that you can
paste into any AI chat (ChatGPT, Claude, etc.) for instant context.
"""
