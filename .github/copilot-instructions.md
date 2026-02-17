## dp — Self-Hosted Data Platform

dp uses DuckDB + plain SQL transforms + Python ingest/export. All data in a single `warehouse.duckdb` file.

### SQL models go in `transform/` with comment-based config:

```sql
-- config: materialized=table, schema=silver
-- depends_on: bronze.customers
SELECT * FROM bronze.customers WHERE active = true
```

Folder name = default schema. No Jinja — plain SQL only.

### Python scripts go in `ingest/` or `export/` with a `run(db)` function:

```python
def run(db: duckdb.DuckDBPyConnection) -> None:
    db.execute("CREATE OR REPLACE TABLE landing.x AS SELECT * FROM ...")
```

### Key commands: `dp transform`, `dp run <script>`, `dp query "<sql>"`, `dp lint`, `dp tables`

### Code patterns:
- `from __future__ import annotations` in all Python files
- Lazy imports in CLI commands (`src/dp/cli.py`)
- DuckDB connections: always `try/finally` with `conn.close()`
- Tests: `pytest tests/` — uses real temp DuckDB, no mocks
- API: FastAPI in `src/dp/server/app.py`, auth via `_require_permission()`

### Don't:
- Add Jinja/templating to SQL
- Add TypeScript to the frontend
- Mock DuckDB in tests
- Modify `_dp_internal` schema from user-facing code
