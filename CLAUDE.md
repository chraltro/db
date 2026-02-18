# CLAUDE.md — Agent Instructions for dp

## What is dp?

dp is a self-hosted data platform — a lightweight alternative to Databricks/Snowflake. It uses **DuckDB** for OLAP analytics, **plain SQL** for transforms, and **Python** for ingest/export scripts. All data lives in a single `warehouse.duckdb` file. No data leaves the machine.

## Quick Reference

```bash
# Install
pip install -e .              # from source
pip install -e ".[dev]"       # with test deps (pytest, httpx)

# Build frontend
cd frontend && npm install && npm run build

# Run tests
pytest tests/

# Lint SQL
dp lint                       # check
dp lint --fix                 # auto-fix

# Common commands
dp init my-project            # scaffold new project
dp transform                  # build all SQL models
dp transform --force          # force rebuild (ignore cache)
dp query "SELECT 1"           # ad-hoc SQL
dp tables                     # list warehouse objects
dp serve                      # start web UI on :3000
dp serve --auth               # with authentication
dp run ingest/example.py      # run a script
dp stream full-refresh        # run full pipeline
dp history                    # show run log
```

## Project Structure

```
src/dp/                       # Python package (the platform itself)
  cli.py                      # Typer CLI — all commands defined here
  config.py                   # project.yml parsing, scaffold templates
  engine/
    database.py               # DuckDB connection, metadata tables
    transform.py              # SQL DAG engine with change detection
    runner.py                 # Python script executor (ingest/export)
    auth.py                   # Token auth, RBAC (admin/editor/viewer)
    secrets.py                # .env secrets management
    scheduler.py              # Cron scheduler (Huey) + file watcher
    importer.py               # Data import wizard (CSV, Parquet, DB)
    notebook.py               # .dpnb notebook execution
    docs.py                   # Markdown doc generator
  lint/
    linter.py                 # SQLFluff integration
  server/
    app.py                    # FastAPI backend (40+ endpoints)

frontend/                     # React + Vite SPA
  src/
    App.jsx                   # Main app, tab routing
    api.js                    # API client (fetch wrapper)
    Editor.jsx                # Monaco code editor
    FileTree.jsx              # Project file browser
    QueryPanel.jsx            # Ad-hoc SQL runner
    TablesPanel.jsx           # Table browser
    DAGPanel.jsx              # Model dependency graph
    ...                       # ~15 components total

tests/                        # pytest test suite
  test_transform.py           # SQL DAG + change detection
  test_runner.py              # Script execution
  test_api.py                 # FastAPI endpoints (uses httpx)
  test_auth.py                # Authentication + RBAC
  test_config.py              # Config parsing
  test_secrets.py             # Secrets management
  test_scheduler.py           # Scheduler
  test_docs.py                # Doc generation
  test_importer.py            # Data import
  test_notebook.py            # Notebook execution
```

## Architecture

```
User project layout (created by `dp init`):
  ingest/         Python scripts (or .dpnb notebooks)
  transform/
    bronze/       Light cleanup SQL
    silver/       Business logic SQL
    gold/         Consumption-ready SQL
  export/         Python scripts (or .dpnb notebooks)
  notebooks/      .dpnb interactive notebooks
  project.yml     Config: streams, connections, schedules
  .env            Secrets (never committed)
  warehouse.duckdb   Single-file DuckDB database

Internal DuckDB schemas:
  landing/        Raw data from ingest scripts
  bronze/         Cleaned data
  silver/         Business logic
  gold/           Consumption-ready
  _dp_internal/   Metadata (model_state, run_log, users, tokens)
```

## Key Conventions

### SQL Transform Files

Every `.sql` file in `transform/` follows this convention:

```sql
-- config: materialized=table, schema=silver
-- depends_on: bronze.customers, bronze.orders

SELECT
    c.customer_id,
    c.name,
    COUNT(o.order_id) AS order_count
FROM bronze.customers c
LEFT JOIN bronze.orders o ON c.customer_id = o.customer_id
GROUP BY 1, 2
```

- `-- config:` sets materialization (`view` or `table`) and schema
- `-- depends_on:` declares upstream dependencies (used for DAG ordering)
- Folder name is the default schema (e.g., `transform/bronze/` → `schema=bronze`)
- No Jinja, no templating — just plain SQL
- Change detection uses SHA256 hash of normalized SQL content

### Python Script Convention

Ingest and export scripts are plain Python with a `db` (DuckDB connection) pre-injected:

```python
# A DuckDB connection is available as `db` — just write top-level code
db.execute("CREATE SCHEMA IF NOT EXISTS landing")
db.execute("CREATE OR REPLACE TABLE landing.data AS SELECT * FROM ...")
```

- Scripts run as top-level code with `db` pre-injected (no wrapper function needed)
- Legacy `def run(db)` scripts are still supported (backward compatible)
- `.dpnb` notebooks can also be used as ingest/export pipeline steps
- Scripts prefixed with `_` are skipped
- Ingest failures stop the pipeline (data integrity)
- `stdout`/`stderr` are captured and logged

### project.yml

```yaml
name: my-project
database:
  path: warehouse.duckdb
connections:
  prod_postgres:
    type: postgres
    host: ${DB_HOST}          # env var expansion via .env
    password: ${DB_PASSWORD}
streams:
  daily-refresh:
    description: "Daily ETL"
    schedule: "0 6 * * *"     # 5-field cron
    steps:
      - ingest: [all]
      - transform: [all]
      - export: [all]
lint:
  dialect: duckdb
```

## Development Workflow

### Running Tests

```bash
pytest tests/                    # all tests
pytest tests/test_transform.py   # specific file
pytest tests/ -v                 # verbose
pytest tests/ -x                 # stop on first failure
```

Tests use temporary DuckDB databases (in-memory or tmp files). No external services needed.

### Making Backend Changes

1. Source is in `src/dp/`
2. CLI commands are in `cli.py` — each `@app.command()` function maps to a `dp <command>`
3. Engine logic is in `engine/` — transform.py is the core SQL DAG engine
4. API endpoints are in `server/app.py` — FastAPI with Pydantic models
5. Run `pytest tests/` after changes

### Making Frontend Changes

1. Source is in `frontend/src/`
2. React 19 + Vite, no TypeScript
3. Monaco editor for code editing
4. API client in `api.js` (thin fetch wrapper)
5. Dev server: `cd frontend && npm run dev` (port 5173, proxies /api to 3000)
6. Build: `cd frontend && npm run build`

### Adding a New CLI Command

1. Add `@app.command()` function in `src/dp/cli.py`
2. Import engine modules lazily (inside the function body)
3. Use `_resolve_project()` for project dir resolution
4. Use `rich` Console for output formatting
5. Add corresponding API endpoint in `server/app.py` if needed
6. Add tests in `tests/`

### Adding a New SQL Model

Create a `.sql` file in the appropriate `transform/` subdirectory:

```sql
-- config: materialized=table, schema=gold
-- depends_on: silver.dim_customer

SELECT * FROM silver.dim_customer WHERE active = true
```

Run `dp transform` to build it.

## Code Style

- Python 3.10+, type hints used throughout
- `from __future__ import annotations` in all modules
- Imports: stdlib → third-party → local (standard Python convention)
- Rich library for terminal formatting
- Lazy imports in CLI commands (faster startup)
- SQLFluff config in `pyproject.toml` — DuckDB dialect, keywords UPPER, identifiers lower

## Testing Patterns

- Tests use `tmp_path` fixture for temp databases
- API tests use `httpx.AsyncClient` with FastAPI's `TestClient`
- No mocking of DuckDB — tests use real (temporary) databases
- Test files mirror source structure: `test_transform.py` tests `engine/transform.py`

## Common Tasks for Agents

### "Add a new ingest source"
1. Create `ingest/source_name.py` with `run(db)` function
2. Load data into `landing.table_name`
3. Add SQL transforms in `transform/bronze/` → `silver/` → `gold/`
4. Update `project.yml` streams if needed
5. Test with `dp run ingest/source_name.py` then `dp transform`

### "Add a new API endpoint"
1. Add Pydantic request/response models in `server/app.py`
2. Add `@app.post("/api/...")` or `@app.get("/api/...")` handler
3. Use `_require_permission(request, "read"|"write"|"execute")` for auth
4. Always use `connect()`/`conn.close()` pattern with try/finally
5. Add test in `tests/test_api.py`

### "Fix a SQL model"
1. Edit the `.sql` file in `transform/`
2. Run `dp transform` — change detection will rebuild only changed models
3. Use `dp transform --force` to rebuild everything
4. Validate with `dp query "SELECT * FROM schema.table LIMIT 10"`

### "Debug a failed pipeline"
1. Check `dp history` for recent failures
2. Look at error messages in the run log
3. Run individual steps: `dp run ingest/script.py`, then `dp transform`
4. Use `dp query` to inspect data at each layer
