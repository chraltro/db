"""Scaffold templates for `dp init`.

These are large string constants used to create sample projects.
Separated from config.py to keep configuration logic focused.
"""

from __future__ import annotations

import json


# ---------------------------------------------------------------------------
# project.yml
# ---------------------------------------------------------------------------

PROJECT_YML_TEMPLATE = """\
name: {name}
description: "Earthquake analytics pipeline — a dp sample project"

database:
  path: warehouse.duckdb

connections: {{}}
  # Example: connect to a PostgreSQL database
  # prod_postgres:
  #   type: postgres
  #   host: localhost
  #   port: 5432
  #   database: production
  #   user: ${{POSTGRES_USER}}
  #   password: ${{POSTGRES_PASSWORD}}

streams:
  full-refresh:
    description: "Full pipeline: seed reference data, ingest live data, transform, export"
    steps:
      - seed: [all]
      - ingest: [all]
      - transform: [all]
      - export: [all]
    schedule: null  # on-demand; use cron for scheduled runs, e.g. "0 6 * * *"

lint:
  dialect: duckdb
"""

# ---------------------------------------------------------------------------
# Ingest notebook — fetches USGS earthquake data, falls back to sample data
# ---------------------------------------------------------------------------

# Build the ingest cell source — this is the core data loading logic
_INGEST_FETCH_SOURCE = """\
import json
from pathlib import Path
from urllib.request import urlopen

FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_month.geojson"

try:
    print("Fetching live earthquake data from USGS...")
    with urlopen(FEED_URL, timeout=15) as resp:
        data = json.loads(resp.read())
    features = data.get("features", [])
    print(f"Fetched {len(features)} earthquakes (M2.5+, last 30 days)")
except Exception as e:
    print(f"USGS API unavailable ({type(e).__name__}), using sample data...")
    features = [
        {"id":"us7000m1a1","properties":{"mag":7.1,"place":"154km SSW of Kainantu, Papua New Guinea","time":1735776000000,"updated":1735862400000,"felt":120,"tsunami":0,"sig":776,"magType":"mww","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[146.49,-6.45,90.2]}},
        {"id":"us7000m2b2","properties":{"mag":6.4,"place":"67km SW of Dili, Timor Leste","time":1735862400000,"updated":1735948800000,"felt":30,"tsunami":1,"sig":450,"magType":"mb","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[125.30,-9.02,45.0]}},
        {"id":"us7000m3c3","properties":{"mag":5.8,"place":"12km SE of Ridgecrest, CA","time":1735948800000,"updated":1736035200000,"felt":892,"tsunami":0,"sig":518,"magType":"mww","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[-117.58,35.60,8.0]}},
        {"id":"us7000m4d4","properties":{"mag":5.2,"place":"95km NNE of Hualien City, Taiwan","time":1736035200000,"updated":1736121600000,"felt":45,"tsunami":0,"sig":416,"magType":"mb","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[121.80,24.59,18.5]}},
        {"id":"us7000m5e5","properties":{"mag":5.0,"place":"24km W of Challis, Idaho","time":1736121600000,"updated":1736208000000,"felt":210,"tsunami":0,"sig":385,"magType":"ml","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[-114.45,44.50,10.0]}},
        {"id":"us7000m6f6","properties":{"mag":4.9,"place":"8km S of Indios, Puerto Rico","time":1736208000000,"updated":1736294400000,"felt":65,"tsunami":0,"sig":370,"magType":"md","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[-66.83,17.93,12.0]}},
        {"id":"us7000m7g7","properties":{"mag":4.7,"place":"130km ESE of Adak, Alaska","time":1736294400000,"updated":1736380800000,"felt":0,"tsunami":0,"sig":340,"magType":"mb","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[-175.12,51.64,35.0]}},
        {"id":"us7000m8h8","properties":{"mag":4.5,"place":"78km SSW of Molibagu, Indonesia","time":1736380800000,"updated":1736467200000,"felt":15,"tsunami":0,"sig":312,"magType":"mb","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[123.73,0.05,120.0]}},
        {"id":"us7000m9i9","properties":{"mag":4.3,"place":"32km NW of Salta, Argentina","time":1736467200000,"updated":1736553600000,"felt":8,"tsunami":0,"sig":284,"magType":"mb","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[-65.62,-24.58,180.0]}},
        {"id":"us7000m0j0","properties":{"mag":4.1,"place":"15km ENE of Norcia, Italy","time":1736553600000,"updated":1736640000000,"felt":52,"tsunami":0,"sig":259,"magType":"ml","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[13.15,42.82,9.0]}},
        {"id":"us7000mak1","properties":{"mag":3.9,"place":"52km S of Whites City, New Mexico","time":1736640000000,"updated":1736726400000,"felt":4,"tsunami":0,"sig":234,"magType":"ml","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[-104.37,31.72,5.0]}},
        {"id":"us7000mbl2","properties":{"mag":3.7,"place":"47km N of Vanuatu","time":1736726400000,"updated":1736812800000,"felt":0,"tsunami":0,"sig":211,"magType":"mb","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[167.65,-14.80,200.0]}},
        {"id":"us7000mcm3","properties":{"mag":3.5,"place":"65km WSW of Tonga","time":1736812800000,"updated":1736899200000,"felt":0,"tsunami":0,"sig":188,"magType":"mb","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[-176.40,-21.30,310.0]}},
        {"id":"us7000mdn4","properties":{"mag":3.3,"place":"22km SE of Pahala, Hawaii","time":1736899200000,"updated":1736985600000,"felt":12,"tsunami":0,"sig":167,"magType":"ml","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[-155.38,19.10,32.0]}},
        {"id":"us7000meo5","properties":{"mag":3.1,"place":"8km NE of Magna, Utah","time":1736985600000,"updated":1737072000000,"felt":22,"tsunami":0,"sig":148,"magType":"ml","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[-112.02,40.74,11.0]}},
        {"id":"us7000mfp6","properties":{"mag":2.9,"place":"5km W of Ridgecrest, CA","time":1737072000000,"updated":1737158400000,"felt":3,"tsunami":0,"sig":129,"magType":"md","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[-117.65,35.63,2.5]}},
        {"id":"us7000mgq7","properties":{"mag":2.7,"place":"18km SE of Anchorage, Alaska","time":1737158400000,"updated":1737244800000,"felt":7,"tsunami":0,"sig":112,"magType":"ml","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[-149.72,61.10,40.0]}},
        {"id":"us7000mhr8","properties":{"mag":2.6,"place":"18km NE of Ridgecrest, CA","time":1737244800000,"updated":1737331200000,"felt":0,"tsunami":0,"sig":104,"magType":"md","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[-117.55,35.65,1.8]}},
        {"id":"us7000mis9","properties":{"mag":6.8,"place":"34km E of Noto, Japan","time":1735689600000,"updated":1735776000000,"felt":540,"tsunami":1,"sig":712,"magType":"mww","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[137.35,37.50,10.0]}},
        {"id":"us7000mjt0","properties":{"mag":4.6,"place":"200km S of Fiji","time":1737331200000,"updated":1737417600000,"felt":0,"tsunami":0,"sig":326,"magType":"mb","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[179.20,-19.80,550.0]}},
        {"id":"us7000mku1","properties":{"mag":3.8,"place":"40km NW of Guam","time":1737417600000,"updated":1737504000000,"felt":5,"tsunami":0,"sig":222,"magType":"mb","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[144.45,13.72,60.0]}},
        {"id":"us7000mlv2","properties":{"mag":5.5,"place":"80km SE of Kainantu, Papua New Guinea","time":1737504000000,"updated":1737590400000,"felt":18,"tsunami":0,"sig":465,"magType":"mb","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[146.55,-6.50,70.0]}},
        {"id":"us7000mmw3","properties":{"mag":3.0,"place":"15km N of Pahala, Hawaii","time":1737590400000,"updated":1737676800000,"felt":8,"tsunami":0,"sig":138,"magType":"ml","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[-155.40,19.15,5.0]}},
        {"id":"us7000mnx4","properties":{"mag":4.0,"place":"55km NE of Arica, Chile","time":1737676800000,"updated":1737763200000,"felt":25,"tsunami":0,"sig":246,"magType":"mb","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[-69.80,-18.20,100.0]}},
        {"id":"us7000moy5","properties":{"mag":2.8,"place":"8km N of Ridgecrest, CA","time":1737763200000,"updated":1737849600000,"felt":42,"tsunami":0,"sig":120,"magType":"md","type":"earthquake","status":"reviewed","detail":""},"geometry":{"coordinates":[-117.60,35.70,14.0]}},
    ]
    print(f"Loaded {len(features)} sample earthquakes")\
"""

_INGEST_LOAD_SOURCE = """\
db.execute("CREATE SCHEMA IF NOT EXISTS landing")

db.execute(\"\"\"
    CREATE OR REPLACE TABLE landing.earthquakes (
        id VARCHAR, magnitude DOUBLE, place VARCHAR,
        event_time BIGINT, updated BIGINT,
        latitude DOUBLE, longitude DOUBLE, depth_km DOUBLE,
        felt INTEGER, tsunami INTEGER, sig INTEGER,
        mag_type VARCHAR, event_type VARCHAR, status VARCHAR,
        detail_url VARCHAR
    )
\"\"\")

rows = []
for f in features:
    p = f.get("properties", {})
    c = f.get("geometry", {}).get("coordinates", [0, 0, 0])
    rows.append((
        str(f.get("id", "")),
        p.get("mag"),
        str(p.get("place", "")),
        p.get("time"),
        p.get("updated"),
        c[1] if len(c) > 1 else None,
        c[0] if len(c) > 0 else None,
        c[2] if len(c) > 2 else None,
        p.get("felt"),
        p.get("tsunami"),
        p.get("sig"),
        str(p.get("magType", "")),
        str(p.get("type", "")),
        str(p.get("status", "")),
        str(p.get("detail", "")),
    ))

if rows:
    db.executemany(
        "INSERT INTO landing.earthquakes VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
        rows,
    )

print(f"Loaded {len(rows)} earthquakes into landing.earthquakes")\
"""

SAMPLE_INGEST_NOTEBOOK = json.dumps({
    "title": "Earthquake Ingestion",
    "cells": [
        {
            "id": "cell_1",
            "type": "markdown",
            "source": (
                "# Earthquake Data Ingestion\n\n"
                "Fetches M2.5+ earthquakes from the past 30 days via the "
                "[USGS GeoJSON API](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php).\n\n"
                "If the API is unavailable (offline, firewall, timeout), the notebook "
                "falls back to 25 sample earthquakes so the pipeline always works."
            ),
        },
        {
            "id": "cell_2",
            "type": "code",
            "source": _INGEST_FETCH_SOURCE,
            "outputs": [],
        },
        {
            "id": "cell_3",
            "type": "markdown",
            "source": "## Load into landing table\n\nCreate `landing.earthquakes` and insert the raw data.",
        },
        {
            "id": "cell_4",
            "type": "code",
            "source": _INGEST_LOAD_SOURCE,
            "outputs": [],
        },
        {
            "id": "cell_5",
            "type": "markdown",
            "source": "## Preview\n\nTop 10 earthquakes by magnitude.",
        },
        {
            "id": "cell_6",
            "type": "code",
            "source": 'db.execute("SELECT id, magnitude, place FROM landing.earthquakes ORDER BY magnitude DESC LIMIT 10")',
            "outputs": [],
        },
    ],
}, indent=2) + "\n"

# ---------------------------------------------------------------------------
# SQL transform models — bronze / silver / gold with assertions & docs
# ---------------------------------------------------------------------------

SAMPLE_BRONZE_SQL = """\
-- config: materialized=table, schema=bronze
-- depends_on: landing.earthquakes
-- description: Cleaned earthquake records with proper types and readable column names
-- assert: row_count > 0
-- assert: no_nulls(event_id)

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
-- description: Enriched earthquake events with magnitude class, region, and depth classification
-- assert: row_count > 0
-- assert: unique(event_id)
-- assert: accepted_values(magnitude_class, ['Minor', 'Light', 'Moderate', 'Strong', 'Major', 'Great'])
-- assert: accepted_values(depth_class, ['Shallow', 'Intermediate', 'Deep'])

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
-- description: Daily earthquake aggregates for dashboard and trend analysis
-- assert: row_count > 0
-- assert: unique(event_date)
-- assert: total_events > 0

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
-- description: Daily earthquake dashboard with magnitude class breakdown
-- col: event_date: Calendar date
-- col: total_events: Number of earthquakes recorded
-- col: significant_count: Events with magnitude >= 6.0 (Strong, Major, Great)
-- assert: row_count > 0
-- assert: unique(event_date)

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
-- description: Significant earthquakes (M4.5+) ranked by magnitude
-- col: event_id: USGS earthquake identifier
-- col: magnitude: Richter scale magnitude
-- col: magnitude_class: Human-readable severity (Moderate/Strong/Major/Great)
-- assert: row_count > 0
-- assert: no_nulls(event_id)

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
-- description: Regional seismic activity summary for risk assessment
-- col: region: Geographic region extracted from USGS place description
-- col: significant_events: Events with magnitude >= 5.0
-- assert: row_count > 0

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
HAVING count(*) >= 2
ORDER BY total_events DESC
"""

# ---------------------------------------------------------------------------
# Export script
# ---------------------------------------------------------------------------

SAMPLE_EXPORT_SCRIPT = '''\
"""Export earthquake analytics to CSV files."""

from pathlib import Path

output_dir = Path(__file__).parent.parent / "output"
output_dir.mkdir(exist_ok=True)

tables = [
    ("gold.earthquake_summary", "earthquake_summary.csv"),
    ("gold.top_earthquakes", "top_earthquakes.csv"),
    ("gold.region_risk", "region_risk.csv"),
]

for table, filename in tables:
    dest = str(output_dir / filename).replace("'", "''")
    db.execute(f"COPY {table} TO '{dest}' (HEADER, DELIMITER ',')")
    rows = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"Exported {rows} rows to output/{filename}")
'''

# ---------------------------------------------------------------------------
# Seed data — magnitude scale lookup table
# ---------------------------------------------------------------------------

SAMPLE_SEED_CSV = """\
class,min_magnitude,max_magnitude,description,risk_level,typical_effects
Minor,2.5,3.9,Minor earthquake,low,Often felt; rarely causes damage
Light,4.0,4.9,Light earthquake,low,Noticeable shaking of indoor objects
Moderate,5.0,5.9,Moderate earthquake,medium,Can damage poorly constructed buildings
Strong,6.0,6.9,Strong earthquake,high,Destructive in populated areas up to 160km
Major,7.0,7.9,Major earthquake,critical,Serious damage over large areas
Great,8.0,10.0,Great earthquake,critical,Can totally destroy communities near epicenter
"""

# ---------------------------------------------------------------------------
# Contracts — data quality rules
# ---------------------------------------------------------------------------

SAMPLE_CONTRACTS_YML = """\
contracts:
  - name: pipeline_produces_data
    description: "The pipeline must produce earthquake summary data"
    model: gold.earthquake_summary
    severity: error
    assertions:
      - row_count > 0
      - no_nulls(event_date)
      - unique(event_date)

  - name: significant_earthquakes_valid
    description: "Top earthquakes table must contain only M4.5+ events"
    model: gold.top_earthquakes
    severity: warn
    assertions:
      - row_count > 0
      - no_nulls(event_id)
      - unique(event_id)

  - name: regions_have_data
    description: "Region risk table must have aggregated data"
    model: gold.region_risk
    severity: warn
    assertions:
      - row_count > 0
      - no_nulls(region)
"""

# ---------------------------------------------------------------------------
# Interactive notebook — explore the data after a pipeline run
# ---------------------------------------------------------------------------

SAMPLE_EXPLORE_NOTEBOOK = json.dumps({
    "title": "Earthquake Explorer",
    "cells": [
        {
            "id": "cell_1",
            "type": "markdown",
            "source": (
                "# Earthquake Explorer\n\n"
                "Interactive analysis of earthquake data. "
                "Run `dp stream full-refresh` first to populate the warehouse."
            ),
        },
        {
            "id": "cell_2",
            "type": "sql",
            "source": (
                "-- Overview: date range, event count, magnitude stats\n"
                "SELECT\n"
                "    count(*) AS total_events,\n"
                "    min(event_date) AS earliest,\n"
                "    max(event_date) AS latest,\n"
                "    round(avg(magnitude), 2) AS avg_magnitude,\n"
                "    max(magnitude) AS max_magnitude\n"
                "FROM silver.earthquake_events"
            ),
            "outputs": [],
        },
        {
            "id": "cell_3",
            "type": "sql",
            "source": (
                "-- Strongest earthquakes\n"
                "SELECT magnitude, magnitude_class, place, event_time, depth_class\n"
                "FROM silver.earthquake_events\n"
                "ORDER BY magnitude DESC\n"
                "LIMIT 10"
            ),
            "outputs": [],
        },
        {
            "id": "cell_4",
            "type": "sql",
            "source": (
                "-- Events by magnitude class\n"
                "SELECT\n"
                "    magnitude_class,\n"
                "    count(*) AS events,\n"
                "    round(avg(depth_km), 1) AS avg_depth_km,\n"
                "    sum(CASE WHEN tsunami_alert THEN 1 ELSE 0 END) AS tsunami_alerts\n"
                "FROM silver.earthquake_events\n"
                "GROUP BY magnitude_class\n"
                "ORDER BY min(magnitude) DESC"
            ),
            "outputs": [],
        },
        {
            "id": "cell_5",
            "type": "sql",
            "source": (
                "-- Most active regions\n"
                "SELECT region, total_events, max_magnitude, avg_magnitude\n"
                "FROM gold.region_risk\n"
                "ORDER BY total_events DESC\n"
                "LIMIT 10"
            ),
            "outputs": [],
        },
        {
            "id": "cell_6",
            "type": "markdown",
            "source": (
                "## Next steps\n\n"
                "- Add new SQL models in `transform/` to explore different angles\n"
                "- Use `dp query \"SELECT ...\"` for quick ad-hoc queries\n"
                "- Check `dp tables` to see all available tables\n"
                "- Run `dp contracts` to validate data quality rules"
            ),
        },
    ],
}, indent=2) + "\n"

# ---------------------------------------------------------------------------
# CLAUDE.md — agent instructions
# ---------------------------------------------------------------------------

CLAUDE_MD_TEMPLATE = """\
# CLAUDE.md — Agent Instructions for {name}

This is a dp data platform project. dp uses DuckDB for analytics, plain SQL for transforms, and Python for ingest/export.

## Commands

```bash
dp transform              # build SQL models in dependency order
dp transform --force      # force rebuild all
dp run ingest/script.py   # run a single script
dp stream full-refresh    # run full pipeline (seed -> ingest -> transform -> export)
dp seed                   # load CSV files from seeds/ into DuckDB
dp query "SELECT 1"       # ad-hoc SQL query
dp tables                 # list warehouse objects
dp lint                   # lint SQL (SQLFluff, DuckDB dialect)
dp lint --fix             # auto-fix lint violations
dp serve                  # start web UI on :3000
dp history                # show run log
dp contracts              # evaluate data quality contracts
dp validate               # check project structure and DAG
dp context                # generate AI-friendly project summary
```

## Project Layout

```
ingest/           Python scripts / .dpnb notebooks that load data into DuckDB
transform/
  bronze/         Light cleanup SQL (views/tables)
  silver/         Business logic, joins, enrichment
  gold/           Consumption-ready models
export/           Python scripts that export data out
seeds/            CSV reference data (loaded with dp seed)
contracts/        YAML data quality rules (evaluated with dp contracts)
notebooks/        Interactive .dpnb notebooks for exploration
project.yml       Streams, connections, schedules
.env              Secrets (never committed)
warehouse.duckdb  The database (single file)
```

## SQL Model Convention

```sql
-- config: materialized=table, schema=silver
-- depends_on: bronze.customers, bronze.orders
-- description: Customer order summary with aggregates
-- col: customer_id: Unique customer identifier
-- col: order_count: Total orders including cancelled
-- assert: row_count > 0
-- assert: unique(customer_id)
-- assert: no_nulls(customer_id)

SELECT c.customer_id, c.name, COUNT(o.order_id) AS order_count
FROM bronze.customers c
LEFT JOIN bronze.orders o ON c.customer_id = o.customer_id
GROUP BY 1, 2
```

- `-- config:` sets materialization (view/table/incremental) and schema
- `-- depends_on:` declares upstream dependencies for DAG ordering
- `-- description:` documents what the model does
- `-- col: name: desc` documents individual columns
- `-- assert:` defines data quality checks (row_count, unique, no_nulls, accepted_values)
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

## Data Quality

**Inline assertions** (in SQL models):
```sql
-- assert: row_count > 0
-- assert: unique(order_id)
-- assert: no_nulls(customer_id)
-- assert: accepted_values(status, ['pending', 'shipped', 'delivered'])
```

**Contracts** (in contracts/*.yml):
```yaml
contracts:
  - name: orders_valid
    model: gold.orders
    severity: error
    assertions:
      - row_count > 0
      - unique(order_id)
```

## Schemas

- `landing` — raw data from ingest scripts
- `bronze` — cleaned, deduplicated
- `silver` — business logic, joins
- `gold` — consumption-ready
- `seeds` — reference data from CSV files
- `_dp_internal` — metadata (do not modify directly)

## Things You Can Ask Your AI Assistant

**Adding data:**
- "Load CSV files from data/customers.csv into the warehouse"
- "Create an ingest script that pulls data from our Postgres database"
- "Add a new data source for our Stripe payments API"

**Transforming data:**
- "Create a silver model that joins customers with their orders"
- "Add a gold table that shows monthly revenue by product category"
- "Fix the SQL error in transform/silver/dim_customer.sql"

**Data quality:**
- "Add assertions to check that order amounts are positive"
- "Create a contract that validates customer data completeness"
- "Why did the last assertion fail?"

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
