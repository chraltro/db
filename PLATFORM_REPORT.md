# havn — Platform Summary Report

> **Date:** 2026-03-11
> **Version:** 0.1.0
> **Classification:** Self-hosted data platform (alternative to Databricks / Snowflake)

---

## Executive Summary

**havn** is a self-hosted, zero-cost data platform that consolidates the entire analytics stack — ingestion, transformation, quality, orchestration, collaboration, and serving — into a single tool backed by a single DuckDB file. No data leaves the machine. No cloud account required. No vendor lock-in.

Where Databricks requires a cloud account, Spark cluster, and dozens of services, havn runs on a laptop with `pip install havn` and delivers 80% of the capability at 0% of the cost.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      havn CLI / Web UI                    │
├─────────────┬─────────────┬─────────────┬───────────────┤
│  Ingest     │  Transform  │  Export     │  Notebooks    │
│  (Python)   │  (SQL DAG)  │  (Python)  │  (.dpnb)      │
├─────────────┴─────────────┴─────────────┴───────────────┤
│              DuckDB Engine (OLAP)                       │
├─────────────────────────────────────────────────────────┤
│              warehouse.duckdb (single file)             │
└─────────────────────────────────────────────────────────┘

Data flows through four schemas:
  landing/  →  bronze/  →  silver/  →  gold/
  (raw)        (cleaned)   (modeled)   (consumption)
```

All metadata (model state, run logs, profiles, users, tokens, alerts, CDC state, versions, contracts) lives in a `_dp_internal` schema inside the same warehouse file.

---

## Complete Feature Inventory

### 1. SQL Transform Engine

The core of havn. SQL files in `transform/` are automatically discovered, dependency-ordered, and executed.

| Capability | Description |
|---|---|
| **DAG execution** | Builds a dependency graph from `-- depends_on:` comments; executes models in topological order |
| **Change detection** | SHA256 hash of normalized SQL; only rebuilds models whose SQL actually changed |
| **Materializations** | `table` (default) or `view`; configured via `-- config: materialized=table` |
| **Incremental models** | Three strategies: `append`, `delete+insert` (default), `merge` (true upsert with matched updates) |
| **Schema evolution** | Automatically adds new columns to existing tables during incremental runs |
| **Inline assertions** | `-- assert: row_count > 0` comments run post-build and fail the pipeline on violation |
| **Column documentation** | `-- column: name: description` comments parsed and surfaced in docs |
| **Profile statistics** | Auto-computes row counts, null percentages, distinct counts after each build |
| **Freshness monitoring** | Configurable SLA thresholds; alerts when models go stale |
| **Force rebuild** | `havn transform --force` ignores cache and rebuilds everything |
| **Selective builds** | Build individual models or filtered subsets |
| **No Jinja** | Plain SQL only — config via comments, no templating language to learn |

### 2. Python Script Execution

Ingest and export scripts are plain Python with a DuckDB connection (`db`) pre-injected at runtime.

| Capability | Description |
|---|---|
| **Zero boilerplate** | Write top-level Python code; `db` is available immediately |
| **Legacy support** | `def run(db)` function signature still works (backward compatible) |
| **Timeout protection** | 5-minute default; runs in background thread to prevent process hangs |
| **Output capture** | stdout/stderr captured and logged; row counts auto-extracted |
| **Pipeline integration** | Scripts in `ingest/` and `export/` are orchestrated as pipeline steps |
| **Skip convention** | Files prefixed with `_` are silently skipped |
| **Failure semantics** | Ingest failures halt the pipeline (data integrity); export failures are non-blocking |

### 3. Interactive Notebooks (.dpnb)

A lightweight notebook format (JSON-based) supporting mixed-mode execution.

| Capability | Description |
|---|---|
| **Cell types** | Code (Python), SQL, Markdown, and Ingest (structured data loading) |
| **Shared namespace** | Variables persist across cells; `db` and `pd` (pandas) pre-loaded |
| **Auto-rendering** | Tables, charts, and inline output rendered automatically |
| **Pipeline steps** | `.dpnb` files can serve as ingest/export steps in streams |
| **Conversion** | Convert SQL models to notebooks; promote notebook SQL to full transforms |

### 4. Data Connectors & Import Wizard

Pluggable connectors for external data sources with auto-generated ingest scripts.

| Capability | Description |
|---|---|
| **File formats** | CSV, Parquet, JSON/JSONL, Excel — auto-inferred schema |
| **Database sources** | PostgreSQL, MySQL, SQLite via DuckDB extensions |
| **Connector framework** | `BaseConnector` abstract class for custom sources |
| **Auto-generation** | `setup_connector()` creates ingest scripts, updates project.yml, stores secrets |
| **Discovery** | Connectors expose `discover()` to list available tables/endpoints |
| **Connection testing** | `havn test-connection` verifies connectivity before committing config |
| **Preview** | Non-destructive preview before import |

### 5. Change Data Capture (CDC)

Incremental data extraction from external sources using watermarking and file tracking.

| Capability | Description |
|---|---|
| **High watermark** | Tracks last-seen value of a column (e.g., `updated_at`) for incremental pulls |
| **File tracking** | Monitors file modification timestamps; only re-ingests changed files |
| **Full refresh** | Fallback mode that replaces the entire table |
| **State management** | CDC state persisted in `_dp_internal.cdc_state` |
| **Status & reset** | Query sync status and reset watermarks per source |

### 6. Data Quality & Contracts

Two complementary systems for ensuring data correctness.

| Capability | Description |
|---|---|
| **Inline assertions** | `-- assert: expression` in SQL files; evaluated post-build |
| **YAML contracts** | Standalone contract files in `contracts/` for reusable quality rules |
| **Assertion types** | `row_count`, `no_nulls`, `unique`, `accepted_values`, custom SQL expressions |
| **Severity levels** | `error` (fails pipeline) or `warn` (logs warning, continues) |
| **History tracking** | Results stored in `_dp_internal.assertion_results` and `contract_results` |
| **Batch execution** | `havn contracts` evaluates all contracts; supports filtering |

### 7. Authentication & RBAC

Optional token-based security with role-based access control.

| Capability | Description |
|---|---|
| **Three roles** | `admin` (full), `editor` (read/write/execute), `viewer` (read-only) |
| **Token auth** | 30-day expiring tokens; PBKDF2 password hashing (100k iterations) |
| **Rate limiting** | 5 failed login attempts per 60 seconds per IP |
| **User management** | Create, update, delete users; revoke tokens via CLI or API |
| **Optional** | `havn serve` (no auth) vs `havn serve --auth` (enforced) |

### 8. Secrets Management

Secure handling of credentials and connection strings.

| Capability | Description |
|---|---|
| **`.env` file** | Standard dotenv format; never committed to git |
| **Variable expansion** | `${DB_PASSWORD}` in project.yml auto-resolved from .env |
| **Log masking** | Secret values masked in logs and API responses |
| **CLI management** | `havn secret set/get/delete` without manual file editing |

### 9. Scheduling & Orchestration

Built-in cron scheduler with multi-step pipeline orchestration.

| Capability | Description |
|---|---|
| **Cron expressions** | 5-field standard cron syntax (`0 6 * * *`) |
| **Streams** | Named multi-step pipelines: ingest → transform → export |
| **Retry logic** | Configurable retry count and delay per stream |
| **Webhooks** | POST notifications on completion or failure |
| **File watcher** | Auto-rebuild transforms when `.sql` or `.py` files change (30s poll, 2s debounce) |
| **Persistent queue** | Huey + SQLite backend; survives process restarts |

### 10. Alerting & Notifications

Multi-channel alerting for pipeline and data quality events.

| Capability | Description |
|---|---|
| **Slack** | Formatted webhook messages with block layout |
| **Generic webhooks** | POST to any HTTP endpoint |
| **Logging** | Python logger integration |
| **Alert types** | Pipeline success/failure, assertion failures, stale models |
| **History** | All alerts tracked in `_dp_internal.alert_log` |

### 11. Versioning & Time Travel

Parquet-based table snapshots for point-in-time queries and rollback.

| Capability | Description |
|---|---|
| **Create versions** | Snapshot all tables to `_snapshots/{id}/` as Parquet files |
| **Diff versions** | Compare two versions or version vs. current state |
| **Restore** | Roll back tables from Parquet snapshots; auto-snapshots before restore |
| **Table timeline** | Track a single table's changes across versions |
| **Cleanup** | Retain N most recent versions; purge old snapshots |
| **Trigger tagging** | Manual, transform, or restore triggers recorded |

### 12. Snapshot & Diff Engine

Lightweight project state comparison without full versioning.

| Capability | Description |
|---|---|
| **Project snapshots** | Hash file contents + table schemas for baseline comparison |
| **Model diff** | Row-level diff with added/removed/modified counts and sample rows |
| **Schema diff** | Detects column additions, removals, and type changes |
| **Primary key support** | `-- havn:primary_key = col1, col2` enables modified-row detection |
| **Non-destructive** | Compares SQL output against materialized table without modifying warehouse |

### 13. SQL Analysis & Lineage

AST-based SQL parsing for dependency resolution and impact analysis.

| Capability | Description |
|---|---|
| **Table references** | Extracts all upstream tables from CTEs, subqueries, JOINs, UNION ALL |
| **Column lineage** | Traces column provenance through CTEs and subqueries |
| **Impact analysis** | Identifies downstream models affected by a change |
| **AST parsing** | Uses sqlglot for correct handling of complex SQL |
| **Regex fallback** | Graceful degradation when AST parsing fails |

### 14. Documentation Generator

Auto-generated project documentation from metadata and SQL files.

| Capability | Description |
|---|---|
| **Markdown output** | Human-readable docs with TOC, column metadata, lineage diagrams |
| **JSON output** | Structured docs for the web UI's two-pane layout |
| **Sources & exposures** | External source declarations and downstream consumer metadata |
| **Lineage diagrams** | Text-based dependency visualization |
| **Row counts** | Auto-included for tables |

### 15. SQL Linting

SQLFluff integration for consistent SQL style.

| Capability | Description |
|---|---|
| **DuckDB dialect** | Defaults to DuckDB syntax rules |
| **Auto-fix** | `havn lint --fix` applies fixable violations |
| **Header preservation** | Config comment headers preserved during fixes |
| **Configurable** | Rules and dialect configurable in project.yml or `.sqlfluff` |

### 16. CI/CD Integration

GitHub Actions workflow generation with PR-level data diff feedback.

| Capability | Description |
|---|---|
| **Workflow generation** | Auto-creates `.github/workflows/havn-ci.yml` |
| **PR comments** | Posts formatted data diff results as PR comments |
| **Full pipeline** | Checkout → install → transform → snapshot → diff → comment |

### 17. Live Collaboration

WebSocket-based real-time shared query sessions.

| Capability | Description |
|---|---|
| **Session management** | Create/join/leave shared sessions (max 100 concurrent) |
| **Shared SQL editor** | Real-time content sync across participants (100KB limit) |
| **Cursor tracking** | Live cursor position visibility |
| **Query history** | Shared execution history (max 200 entries) |
| **Auto-cleanup** | Stale sessions evicted after 24 hours |

### 18. Seed Data

Static CSV reference data loading with change detection.

| Capability | Description |
|---|---|
| **CSV loading** | Files in `seeds/` auto-loaded into DuckDB tables |
| **Change detection** | SHA256 hashing; skips unchanged seeds |
| **Force reload** | Override change detection when needed |
| **Schema inference** | Auto-detects column types from CSV content |

### 19. Environment Management

Multi-environment configuration for dev/staging/prod workflows.

| Capability | Description |
|---|---|
| **Environment overrides** | Different databases, connections, and credentials per environment |
| **CLI flag** | `--env prod` switches configuration context |
| **project.yml** | `environments:` section defines overrides |

### 20. Web UI

Full-featured React 19 + Vite single-page application.

| Tab | Description |
|---|---|
| **Overview** | Pipeline health, stream status, alert summary |
| **Editor** | Monaco code editor with syntax highlighting and formatting |
| **Query** | Interactive SQL runner with tabular results |
| **Tables** | Browse warehouse tables grouped by schema (landing/bronze/silver/gold) |
| **Data Sources** | Connector setup and data import wizard |
| **Notebooks** | Interactive notebook viewer and editor |
| **DAG** | Visual dependency graph of all models |
| **Diff** | Preview model changes before applying |
| **Docs** | Generated documentation viewer |
| **History** | Pipeline run log with execution details |
| **Settings** | Theme (dark/light), user settings, guided tour |

Additional UI features: resizable panels, keyboard shortcuts (Alt+1..5), auth context, pipeline context.

### 21. API

FastAPI backend with 40+ endpoints covering every platform capability.

| Category | Examples |
|---|---|
| **Pipeline** | Run ingest/transform/export, stream execution |
| **Query** | Ad-hoc SQL execution with results |
| **Models** | List, inspect, diff, profile models |
| **Tables** | Schema browsing, data preview |
| **Auth** | Login, token management, user CRUD |
| **Notebooks** | Execute cells, save/load notebooks |
| **Connectors** | Setup, test, discover, sync |
| **Contracts** | Evaluate data quality rules |
| **Versioning** | Create/restore/diff versions |
| **Collaboration** | WebSocket sessions |
| **Files** | Read/write/delete project files |

---

## Unique Selling Points

### 1. Single-File Warehouse
The entire database — data, metadata, history, user accounts — lives in one `warehouse.duckdb` file. Copy it, back it up, version-control it, email it. No server, no cluster, no cloud.

### 2. Zero Cost
$0/month. Runs on a laptop. No cloud subscription, no per-query pricing, no data egress charges. Total cost of ownership is the hardware you already own.

### 3. No Data Leaves the Machine
All processing is local. No telemetry, no cloud sync, no third-party data access. Full data sovereignty by default.

### 4. Plain SQL, No Jinja
Configuration via SQL comments (`-- config:`, `-- depends_on:`, `-- assert:`). No templating language to learn, no macro system to debug, no compile step. Every `.sql` file is valid SQL that runs directly in DuckDB.

### 5. AI-Native Simplicity
The comment-based convention system means LLMs can write correct havn SQL models on the first attempt. No proprietary DSL or macro system to hallucinate about.

### 6. Batteries Included
Ingestion, transformation, quality, orchestration, serving, authentication, documentation, linting, versioning, CI/CD, collaboration — all in one `pip install`. No ecosystem of plugins to assemble.

### 7. DuckDB Performance
Columnar OLAP engine that handles analytical queries at speeds rivaling cloud warehouses, on local hardware. Parquet, CSV, JSON native support. No ETL into a separate system.

### 8. Incremental by Default
Change detection (SHA256 hashing) means `havn transform` only rebuilds what changed. Incremental models support append, delete+insert, and merge strategies with automatic schema evolution.

### 9. Data Quality as Code
Inline assertions and YAML contracts provide two complementary quality gates. Assertions fail the pipeline; contracts with `warn` severity log issues without blocking.

### 10. Time Travel Without Git LFS
Parquet-based versioning lets you snapshot, diff, and restore warehouse state at any point. No need to version-control large binary database files.

### 11. From Laptop to Production
Same tool for local development and production deployment. Environment overrides in project.yml switch database paths and credentials. GitHub Actions CI integration posts data diffs on pull requests.

### 12. Interactive Notebooks
`.dpnb` notebooks serve double duty: exploratory analysis and pipeline steps. No separate notebook server needed.

---

## Technology Stack

| Layer | Technology |
|---|---|
| Database | DuckDB >= 1.2.0 |
| Backend | Python 3.10+, FastAPI, Typer |
| Frontend | React 19, Vite, Monaco Editor |
| SQL Parsing | sqlglot >= 26.0 |
| SQL Linting | SQLFluff >= 3.0 |
| Task Queue | Huey >= 2.5 (SQLite backend) |
| File Watching | watchdog >= 4.0 |
| Auth | PBKDF2 (hashlib), token-based |
| Testing | pytest >= 8.0, httpx >= 0.28.0 |

---

## CLI Command Reference

| Command | Description |
|---|---|
| `havn init` | Scaffold a new project |
| `havn transform` | Build SQL models (DAG-ordered, change-detected) |
| `havn transform --force` | Force rebuild all models |
| `havn run <script>` | Execute an ingest/export Python script |
| `havn stream <name>` | Run a named multi-step pipeline |
| `havn query "<sql>"` | Ad-hoc SQL execution |
| `havn tables` | List warehouse objects |
| `havn lint` | Check SQL style |
| `havn lint --fix` | Auto-fix SQL style violations |
| `havn seed` | Load CSV seed data |
| `havn serve` | Start web UI (port 3000) |
| `havn serve --auth` | Start web UI with authentication |
| `havn history` | Show pipeline run log |
| `havn docs` | Generate documentation |
| `havn diff` | Compare model output vs. materialized state |
| `havn contracts` | Evaluate data quality contracts |
| `havn snapshot` | Create a project checkpoint |
| `havn version create` | Create a warehouse version |
| `havn version restore` | Restore from a version |
| `havn watch` | Auto-rebuild on file changes |
| `havn schedule` | Start the cron scheduler |
| `havn secret set/get/delete` | Manage secrets |
| `havn user create/list/delete` | Manage users |
| `havn ci generate` | Generate GitHub Actions workflow |
| `havn test-connection` | Verify external source connectivity |
| `havn validate` | Validate project configuration |
| `havn status` | Show project status |
| `havn context` | Show project context for AI assistants |

---

*This report covers all features and unique selling points of havn v0.1.0 as of 2026-03-11.*
