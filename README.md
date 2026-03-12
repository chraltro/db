<p align="center">
  <br />
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/chraltro/db/main/.github/assets/logo-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/chraltro/db/main/.github/assets/logo-light.svg">
    <img alt="havn" src="https://raw.githubusercontent.com/chraltro/db/main/.github/assets/logo-dark.svg" width="160">
  </picture>
  <br />
  <strong>Data in safe waters.</strong>
  <br />
  The open-source data platform that runs on your machine. DuckDB + SQL + Python. No cloud required.
  <br />
  <br />
  <a href="#quick-start">Quick Start</a> &middot; <a href="#features">Features</a> &middot; <a href="#why-havn">Why havn?</a> &middot; <a href="#documentation">Docs</a> &middot; <a href="CONTRIBUTING.md">Contributing</a>
  <br />
  <br />

  [![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
  [![Python 3.10+](https://img.shields.io/badge/Python-3.10+-3776AB.svg)](https://python.org)
  [![DuckDB](https://img.shields.io/badge/Powered%20by-DuckDB-FFF000.svg)](https://duckdb.org)
  [![Tests](https://img.shields.io/github/actions/workflow/status/chraltro/db/ci.yml?label=tests)](https://github.com/chraltro/db/actions)

</p>

---

**havn** (Norwegian for *harbour*) is a self-hosted data platform — a Nordic alternative to Databricks and Snowflake for teams that want analytics without the complexity, cost, or data leaving their infrastructure.

Your entire warehouse lives in a single DuckDB file. Transforms are plain SQL. Ingest and export scripts are Python. There's no Jinja, no compilation step, no profiles.yml, and no YAML spaghetti.

```
git clone https://github.com/chraltro/db.git && cd db && pip install -e . && cd frontend && npm install && npm run build && cd .. && havn init my-project && cd my-project && havn stream full-refresh && havn serve
```

<!-- Screenshot placeholder: replace with actual screenshot of havn web UI -->
<!--
<p align="center">
  <img src="https://raw.githubusercontent.com/chraltro/db/main/.github/assets/screenshot.png" width="800" alt="havn web UI" />
</p>
-->

## Why havn?

Most data tools force a choice: **powerful but complex** (Databricks, Snowflake, dbt + Airflow) or **simple but limited** (CSVs in a folder).

havn gives you the analytical power of a modern data stack in something you can install in one command and run on a laptop.

| Pain point | havn's answer |
|---|---|
| Cloud costs spiraling | **Runs locally.** DuckDB on your machine. $0/month. |
| Data leaving your infrastructure | **Self-hosted.** Your data stays on your hardware. Full stop. |
| Jinja-templated SQL nobody understands | **Plain SQL.** Config is a comment. Dependencies are a comment. SQL is just SQL. |
| 30-minute onboarding | **30-second onboarding.** Install from source and `havn init` gives you a working pipeline with sample data. |
| Separate tools for ingest, transform, orchestration, UI | **One tool does it all.** CLI, web UI, scheduler, connectors — included. |
| LLMs can't write your DSL | **AI-native.** Plain SQL + simple conventions = LLMs write correct transforms on the first try. |

## Features

### SQL Transform Engine
Write plain SQL with comment-based config. havn resolves dependencies, builds a DAG, and executes in the right order — with change detection that only rebuilds what changed.

```sql
-- config: materialized=table, schema=gold
-- depends_on: silver.customers, silver.orders

SELECT
    c.customer_id,
    c.name,
    COUNT(o.order_id) AS order_count,
    SUM(o.amount)     AS lifetime_value
FROM silver.customers c
LEFT JOIN silver.orders o ON c.customer_id = o.customer_id
GROUP BY 1, 2
```

### Web UI
Full-featured browser interface with Monaco code editor, interactive SQL runner, DAG visualization, data table browser, chart builder, and pipeline monitoring. Dark and light themes included.

```bash
havn serve          # http://localhost:3000
havn serve --auth   # with role-based access control
```

### 20+ Data Connectors
Connect to Postgres, MySQL, SQLite, Stripe, HubSpot, Google Sheets, S3, REST APIs, and more — from the CLI or the web UI.

```bash
havn connect postgres --host localhost --database mydb --user admin
havn connect stripe --api-key sk_live_xxx
havn connect csv --path /data/customers.csv
```

### Notebooks
Interactive `.dpnb` notebooks with code cells, markdown, and inline results. Use them for exploration, or wire them into your pipeline as ingest/export steps.

### Pipeline Orchestration
Define multi-step pipelines in `project.yml`. Schedule them with cron. Get webhook notifications on completion.

```yaml
streams:
  daily-refresh:
    schedule: "0 6 * * *"
    steps:
      - ingest: [all]
      - transform: [all]
      - export: [all]
    webhook_url: https://hooks.slack.com/...
```

### Git Integration & CI
Track changes with `havn diff`, create snapshots with `havn snapshot`, and generate GitHub Actions workflows with `havn ci generate` that post data diff comments on PRs.

```bash
havn diff                          # what would change?
havn diff --against main           # changes vs a branch
havn snapshot create before-deploy # save state
havn ci generate                   # create GitHub Actions workflow
```

### AI-Native Design
Every project scaffolded with `havn init` includes LLM context files. Plain SQL + simple conventions means AI assistants write correct code on the first try.

```bash
havn context   # generate project summary, paste into any AI chat
```

| Tool | Config file | Auto-included |
|------|-------------|:---:|
| [Claude Code](https://docs.anthropic.com/en/docs/claude-code) | `CLAUDE.md` | Yes |
| [Cursor](https://cursor.sh) | `.cursorrules` | Yes |
| [GitHub Copilot](https://github.com/features/copilot) | `.github/copilot-instructions.md` | Yes |
| Any LLM | `havn context` | Yes |

## Quick Start

### Install

From source (recommended):

```bash
git clone https://github.com/chraltro/db.git
cd db
pip install -e .
cd frontend && npm install && npm run build && cd ..
```

> **Note:** `pip install havn` will be available once the package is published to PyPI. For now, install from source.

### Create a project

```bash
havn init my-project
cd my-project
```

This scaffolds a complete project with a sample pipeline that fetches earthquake data from the USGS API, transforms it through bronze/silver/gold layers, and exports a report.

### Run the pipeline

```bash
havn stream full-refresh
```

### Explore your data

```bash
havn serve                              # open web UI at localhost:3000
havn query "SELECT * FROM gold.earthquake_summary"
havn tables                             # list all tables
```

## Architecture

```
my-project/
├── ingest/              Python scripts + notebooks that load raw data
│   └── earthquakes.dpnb
├── transform/
│   ├── bronze/          Light cleanup (type casting, dedup)
│   ├── silver/          Business logic (joins, aggregations)
│   └── gold/            Consumption-ready tables
├── export/              Python scripts that push data out
├── notebooks/           Interactive .dpnb notebooks
├── project.yml          Pipelines, connections, schedules
├── .env                 Secrets (never committed)
└── warehouse.duckdb     Your entire database, one file
```

Data flows through four schemas:

```
landing/  →  bronze/  →  silver/  →  gold/
 (raw)      (cleaned)   (modeled)   (ready)
```

The warehouse is a single DuckDB file. Copy it, back it up, version it — it's just a file.

## All Commands

| Command | Description |
|---|---|
| `havn init <name>` | Scaffold a new project |
| `havn stream <name>` | Run a full pipeline (ingest → transform → export) |
| `havn transform` | Build SQL models in dependency order |
| `havn run <script>` | Run a single ingest/export script or notebook |
| `havn query "<sql>"` | Run ad-hoc SQL queries |
| `havn tables` | List warehouse tables and views |
| `havn serve` | Start the web UI |
| `havn diff` | Preview what would change before running transforms |
| `havn lint` | Lint SQL files with SQLFluff |
| `havn history` | Show pipeline run log |
| `havn status` | Project health: git info, warehouse stats, last run |
| `havn validate` | Check project structure, config, and DAG for errors |
| `havn snapshot create` | Save a named snapshot of project + data state |
| `havn backup` | Back up the warehouse database |
| `havn connect <type>` | Set up a data connector |
| `havn watch` | Watch files and auto-rebuild on change |
| `havn schedule` | Start the cron scheduler |
| `havn checkpoint` | Smart git commit with auto-generated messages |
| `havn docs` | Generate markdown documentation from warehouse schema |
| `havn context` | Generate project summary for AI assistants |
| `havn ci generate` | Generate GitHub Actions workflow |
| `havn secrets list/set/delete` | Manage .env secrets |
| `havn users create/list/delete` | Manage platform users and roles |

## Comparison

| | **havn** | **dbt + Airflow** | **Databricks** | **Snowflake** |
|---|:---:|:---:|:---:|:---:|
| Self-hosted | Yes | Partial | No | No |
| Setup time | 1 min | Hours | Hours | Hours |
| Monthly cost | $0 | $100s+ | $1000s+ | $1000s+ |
| SQL dialect | Plain SQL | Jinja SQL | Spark SQL | Snowflake SQL |
| Ingest built-in | Yes | No (need Airbyte etc.) | Yes | Yes |
| Web UI | Yes | Separate (Airflow UI) | Yes | Yes |
| Single-file database | Yes | No | No | No |
| AI-native | Yes | No | Partial | No |
| Data stays on your machine | Yes | Depends | No | No |

havn is the right choice when you want a complete data platform without the infrastructure overhead. It's not trying to replace Snowflake at 10TB scale — it's the best tool for teams working with data that fits on a single machine (which is most teams).

## Documentation

- **[CLAUDE.md](CLAUDE.md)** — Full technical reference (architecture, conventions, development workflow)
- **[CONTRIBUTING.md](CONTRIBUTING.md)** — How to contribute
- `havn docs` — Auto-generate documentation from your warehouse schema
- `havn context` — Generate a project summary to paste into any AI assistant

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

```bash
# Development setup
git clone https://github.com/chraltro/db.git
cd db
pip install -e ".[dev]"
cd frontend && npm install && npm run build && cd ..
pytest tests/
```

## License

[MIT](LICENSE) — use it however you want.
