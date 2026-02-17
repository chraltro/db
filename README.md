# dp — Self-Hosted Data Platform

A lightweight, self-hosted data platform for companies who find Databricks/Snowflake too complex and too expensive. Runs on a single machine. No data leaves your infrastructure.

**DuckDB** for OLAP. **Plain SQL** for transforms. **Python** for ingest/export. **No Jinja**, no compilation step, no profiles.yml.

## Quick Start

```bash
pip install -e .
dp init my-project
cd my-project
dp transform
dp serve
```

## Architecture

```
ingest/           Python scripts that load data into DuckDB
transform/
  bronze/         Light cleanup, type casting, dedup
  silver/         Business logic, joins, conforming
  gold/           Consumption-ready facts and dimensions
export/           Python scripts that export data from DuckDB
warehouse.duckdb  The whole database, one file
project.yml       Streams, schedules, connections
```

## SQL Transform Convention

```sql
-- config: materialized=view, schema=bronze
-- depends_on: landing.customers

SELECT
    customer_id,
    UPPER(name) AS name
FROM landing.customers
```

Config is a comment. SQL is just SQL. No templating.

## Commands

- `dp init <name>` — scaffold a new project
- `dp run <script>` — run an ingest or export script
- `dp transform` — build all SQL models in dependency order
- `dp stream <name>` — run a full pipeline (ingest + transform + export)
- `dp lint` — lint SQL with SQLFluff
- `dp query "<sql>"` — run ad-hoc queries
- `dp tables` — list warehouse objects
- `dp history` — show run history
- `dp serve` — start the web UI
