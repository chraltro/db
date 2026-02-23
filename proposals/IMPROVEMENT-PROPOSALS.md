# dp Platform — 10 Improvement Proposals

After a thorough review of the entire codebase (backend, frontend, tests, configuration), here are 10 concrete improvement proposals ranked by impact.

---

## 1. Query Panel UX Redesign (UI/UX)

**Visual mockup:** [`proposals/ui-mockup-query-panel-redesign.html`](./ui-mockup-query-panel-redesign.html)
Open this file in a browser to see an interactive before/after comparison.

**Current problems:**
- The Query Panel uses a plain `<textarea>` with no syntax highlighting, while the Editor tab uses Monaco. This inconsistency is jarring — the most-used data exploration surface has the worst editing experience.
- No way to export query results. Users must manually copy-paste from the rendered table, which is unusable for anything beyond 10 rows.
- No EXPLAIN / query plan view. Users run expensive queries blind with no way to understand performance.
- No execution time shown. After a query runs, users see row/column counts but not how long it took.
- No pagination. `SortableTable` renders every row into the DOM. At 10K+ rows, this causes visible lag and high memory usage.
- No row numbers in results, making it hard to reference specific rows.

**Proposed changes (see mockup):**
1. Replace the plain textarea with Monaco editor (already loaded for the Editor tab — reuse it) for syntax highlighting, bracket matching, and better autocomplete.
2. Add an Export dropdown: CSV, JSON, clipboard (tab-separated), and Markdown table format.
3. Add an "Explain" button that runs `EXPLAIN ANALYZE <query>` and renders the query plan in a tree view.
4. Show execution time as a badge in the results status bar.
5. Paginate results (100 rows/page by default) instead of rendering all rows.
6. Add row numbers as a fixed first column.
7. Add a search/filter input to the schema sidebar.

**Files affected:**
- `frontend/src/QueryPanel.jsx` — main rewrite
- `frontend/src/SortableTable.jsx` — add pagination, row numbers
- `frontend/src/api.js` — add `explainQuery()` method
- `src/dp/server/routes/query.py` — add `/api/explain` endpoint

---

## 2. Thread Safety in Server Global State

**Problem:**
`src/dp/server/deps.py` has three module-level mutable caches — `_config_cache`, `_model_cache`, and `_login_attempts` — that are read and written by concurrent request handlers without any synchronization. FastAPI runs on uvicorn with thread-pool executors for sync endpoints, meaning concurrent requests can corrupt these dicts.

**Specific risks:**
- `_login_attempts` (line 255): Two concurrent login requests could race on the rate-limit counter, allowing bypasses or false positives.
- `_model_cache` (line 102): Concurrent SQL file mtime checks could produce inconsistent mtime snapshots, causing cache thrashing.
- `_config_cache` (line 56): Simultaneous config reloads could return half-updated config objects.

**Proposed fix:**
Add a `threading.Lock` for each cache. Wrap reads and writes in `with _cache_lock:` blocks. This is a minimal, low-risk change.

```python
import threading

_config_lock = threading.Lock()
_model_lock = threading.Lock()
_login_lock = threading.Lock()

def _get_config_cached():
    with _config_lock:
        # ... existing logic ...

def _discover_models_cached(transform_dir):
    with _model_lock:
        # ... existing logic ...

def _check_rate_limit(key):
    with _login_lock:
        # ... existing logic ...
```

**Files affected:**
- `src/dp/server/deps.py`

---

## 3. SQL Injection via File Path Interpolation in Importer

**Problem:**
`src/dp/engine/importer.py` constructs DuckDB queries by interpolating file paths directly into SQL strings using f-strings:

```python
# importer.py, around line 35-41
f"SELECT * FROM read_csv('{file_path}', ...)"
```

If a file path contains a single quote (e.g., `data/Bob's Report.csv`), this breaks the query. More critically, a crafted file name could inject arbitrary SQL. While dp is self-hosted and the attack surface is limited to local users, this is still a code correctness issue.

**Proposed fix:**
Use DuckDB's parameterized queries where possible, or at minimum escape single quotes in file paths:

```python
safe_path = str(file_path).replace("'", "''")
```

Better: For `read_csv` / `read_parquet` operations, DuckDB supports parameter binding in some contexts. Where it doesn't, use the escaping approach consistently across all file-reading functions.

**Files affected:**
- `src/dp/engine/importer.py` — `preview_csv()`, `preview_parquet()`, `import_csv()`, `import_parquet()`

---

## 4. Scheduler Silently Swallows Errors, Never Retries

**Problem:**
`src/dp/engine/scheduler.py` catches all exceptions during stream execution and only prints them to console (line 183-187). The `project.yml` schema already supports `retries` and `retry_delay` fields on streams, but these are never read or used. Failed pipelines are silently lost.

```python
# scheduler.py, line ~183
except Exception as e:
    console.print(f"[red]Stream '{stream_name}' failed: {e}")
```

**Proposed fix:**
1. Read `retries` and `retry_delay` from the stream config.
2. Implement retry logic with exponential backoff.
3. Log failures to `_dp_internal.run_log` via `log_run()` so they appear in `dp history` and the web UI.
4. If the alerts module is configured, fire an alert on final failure.

```python
max_retries = stream_config.retries or 0
retry_delay = stream_config.retry_delay or 30

for attempt in range(max_retries + 1):
    try:
        _run_stream_task(...)
        break
    except Exception as e:
        if attempt < max_retries:
            logger.warning(f"Retry {attempt+1}/{max_retries} for '{stream_name}': {e}")
            time.sleep(retry_delay * (2 ** attempt))
        else:
            logger.error(f"Stream '{stream_name}' failed after {max_retries+1} attempts: {e}")
            log_run(conn, stream_name, "error", str(e))
```

**Files affected:**
- `src/dp/engine/scheduler.py`
- `src/dp/config.py` (ensure retries/retry_delay fields are parsed)

---

## 5. Resource Leaks in Importer — DuckDB Connections Not Cleaned Up

**Problem:**
`src/dp/engine/importer.py` creates in-memory DuckDB connections (`duckdb.connect(":memory:")`) in multiple functions (`preview_csv`, `preview_parquet`, `import_from_database`) without `try/finally` blocks. If an exception occurs mid-function (e.g., malformed CSV), the connection is never closed.

```python
# importer.py ~line 31
mem = duckdb.connect(":memory:")
# ... multiple operations that can throw ...
# mem.close() only called at end, not in finally
```

DuckDB connections hold file locks and memory. Leaked connections accumulate over time, especially when users import many files via the web UI.

**Proposed fix:**
Use context managers or `try/finally`:

```python
mem = duckdb.connect(":memory:")
try:
    # ... operations ...
finally:
    mem.close()
```

Or better, if DuckDB supports it as a context manager:
```python
with duckdb.connect(":memory:") as mem:
    # ... operations ...
```

**Files affected:**
- `src/dp/engine/importer.py` — 4 functions need cleanup

---

## 6. No Virtual Scrolling for Large Result Sets in Frontend

**Problem:**
`SortableTable.jsx` renders every row as a DOM `<tr>` element. When a query returns 10,000+ rows (common for `SELECT * FROM large_table`), React creates 10,000+ DOM nodes, causing:
- Multi-second render freezes
- High memory usage (each row ~1KB of DOM, so 10K rows = ~10MB DOM)
- Sluggish scroll performance

**Proposed fix:**
Implement windowed rendering using `@tanstack/react-virtual` (already used widely in the React ecosystem). Only render rows visible in the viewport + a small buffer:

```jsx
import { useVirtualizer } from '@tanstack/react-virtual';

function SortableTable({ columns, rows }) {
  const parentRef = useRef(null);
  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 28, // row height in px
    overscan: 20,
  });

  return (
    <div ref={parentRef} style={{ height: '100%', overflow: 'auto' }}>
      <table>
        <thead>...</thead>
        <tbody style={{ height: virtualizer.getTotalSize() }}>
          {virtualizer.getVirtualItems().map(virtualRow => (
            <tr key={virtualRow.index} style={{
              position: 'absolute',
              top: virtualRow.start,
              height: virtualRow.size,
            }}>
              {rows[virtualRow.index].map(...)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
```

This reduces DOM nodes from N to ~50 regardless of dataset size.

**Files affected:**
- `frontend/src/SortableTable.jsx`
- `frontend/package.json` — add `@tanstack/react-virtual`

---

## 7. Missing Request Timeouts and Abort Controllers on Frontend API Calls

**Problem:**
All API calls in `frontend/src/api.js` use plain `fetch()` without timeouts or `AbortController`. If the backend hangs (DuckDB lock, slow query, network issue), the frontend waits indefinitely with no feedback to the user. The "Running..." state persists forever.

This affects every panel: QueryPanel, TablesPanel, HistoryPanel, OverviewPanel, etc.

**Proposed fix:**
Add a timeout wrapper to the API client:

```javascript
// api.js
async function fetchWithTimeout(url, options = {}, timeoutMs = 30000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal });
    return response;
  } catch (e) {
    if (e.name === 'AbortError') {
      throw new Error(`Request timed out after ${timeoutMs / 1000}s`);
    }
    throw e;
  } finally {
    clearTimeout(timer);
  }
}
```

Additionally, for the Query panel specifically, add a "Cancel" button that calls `controller.abort()`, giving users a way to stop long-running queries.

**Files affected:**
- `frontend/src/api.js` — add timeout wrapper
- `frontend/src/QueryPanel.jsx` — add cancel button

---

## 8. Incremental Models Don't Handle Schema Removal or NULL Unique Keys

**Problem:**
`src/dp/engine/transform/execution.py` `_execute_incremental()` has two gaps:

1. **Schema evolution is additive-only** (line ~74-90): When the source query adds new columns, they're auto-added to the target table. But when columns are *removed* from the source query, the target retains stale columns with NULL values forever. Over time, the target accumulates ghost columns.

2. **NULL values in unique_key columns** (line ~68): The delete+insert and merge strategies use `WHERE key1 = staging.key1 AND key2 = staging.key2`. In SQL, `NULL = NULL` is `NULL` (not `TRUE`), so rows with NULL keys are never matched for deletion — they accumulate as duplicates on every incremental run.

**Proposed fix:**

For schema evolution:
```python
# After adding new columns, optionally drop columns not in staging
if model.drop_missing_columns:
    staging_cols = {r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'temp' AND table_name = ?", [staging_name]
    ).fetchall()}
    for col in target_cols - staging_cols:
        conn.execute(f"ALTER TABLE {model.full_name} DROP COLUMN {validate_identifier(col)}")
```

For NULL keys:
```python
# Use IS NOT DISTINCT FROM instead of = for key matching
join_clause = " AND ".join(
    f"target.{k} IS NOT DISTINCT FROM staging.{k}" for k in keys
)
```

**Files affected:**
- `src/dp/engine/transform/execution.py`
- `src/dp/engine/transform/models.py` — add `drop_missing_columns` config option

---

## 9. Editor Autocomplete Cache Grows Unbounded

**Problem:**
`frontend/src/Editor.jsx` has two caches that grow indefinitely:

```javascript
const schemaCache = new Map();  // line ~15 — never cleared
let tablesCache = null;          // line ~16 — 30s TTL but entries never evicted
```

`schemaCache` stores column definitions for every `schema.table` pair the user has ever used in autocomplete. In a project with hundreds of tables, this accumulates significant memory. Long sessions (common for data analysts who keep dp open all day) can cause the tab to slow down.

Similarly in `QueryPanel.jsx`, `colCacheRef` (line 141) caches column info per table without any eviction.

**Proposed fix:**
Replace the unbounded `Map` with a simple LRU cache (max ~200 entries):

```javascript
class LRUCache {
  constructor(maxSize = 200) {
    this.max = maxSize;
    this.cache = new Map();
  }
  get(key) {
    if (!this.cache.has(key)) return undefined;
    const val = this.cache.get(key);
    this.cache.delete(key);
    this.cache.set(key, val); // move to end (most recent)
    return val;
  }
  set(key, val) {
    this.cache.delete(key);
    this.cache.set(key, val);
    if (this.cache.size > this.max) {
      this.cache.delete(this.cache.keys().next().value); // evict oldest
    }
  }
}
```

**Files affected:**
- `frontend/src/Editor.jsx`
- `frontend/src/QueryPanel.jsx`

---

## 10. Incomplete Alert/Notification System — Slack and Webhook Stubs

**Problem:**
`src/dp/engine/alerts.py` defines `_send_slack()` and `_send_webhook()` functions, and `project.yml` supports alert configuration:

```yaml
streams:
  daily-refresh:
    on_failure:
      slack: "#data-alerts"
      webhook: "https://hooks.example.com/..."
```

However, the alert implementations appear incomplete. The `_send_slack` and `_send_webhook` functions exist but may not be fully wired up. The freshness monitoring in `analysis.py` detects stale models but never sends alerts. The scheduler swallows exceptions (see #4) without triggering alerts.

This means pipeline failures go unnoticed unless someone manually checks `dp history` or the web UI.

**Proposed fix:**
1. Complete the Slack integration using a simple `httpx.post()` to the Slack webhook URL (no SDK needed).
2. Complete the webhook integration with configurable payload format.
3. Wire the alert system into the scheduler's error handling (connects to improvement #4).
4. Wire freshness monitoring to send alerts when models go stale beyond configured thresholds.
5. Add a "Test Alert" button in the Settings panel to verify configuration.

```python
async def _send_slack(webhook_url: str, message: str) -> bool:
    import httpx
    payload = {
        "text": message,
        "blocks": [{
            "type": "section",
            "text": {"type": "mrkdwn", "text": message}
        }]
    }
    async with httpx.AsyncClient() as client:
        resp = await client.post(webhook_url, json=payload, timeout=10)
        return resp.status_code == 200
```

**Files affected:**
- `src/dp/engine/alerts.py` — complete implementations
- `src/dp/engine/scheduler.py` — integrate alert calls
- `src/dp/engine/transform/analysis.py` — wire freshness → alerts
- `src/dp/server/routes/pipeline.py` — add test-alert endpoint

---

## Summary

| # | Improvement | Category | Severity | Effort |
|---|------------|----------|----------|--------|
| 1 | Query Panel UX Redesign | UI/UX | High | Large |
| 2 | Thread Safety in Server Globals | Security/Correctness | High | Small |
| 3 | SQL Injection via File Path Interpolation | Security | High | Small |
| 4 | Scheduler Error Handling + Retries | Reliability | High | Medium |
| 5 | Resource Leaks in Importer | Correctness | Medium | Small |
| 6 | Virtual Scrolling for Large Tables | Performance | Medium | Medium |
| 7 | Request Timeouts + Abort Controllers | Reliability | Medium | Small |
| 8 | Incremental Model Schema + NULL Handling | Correctness | Medium | Medium |
| 9 | Unbounded Autocomplete Cache | Performance | Low | Small |
| 10 | Complete Alert/Notification System | Feature | Medium | Large |

**Recommended priority order:** 2 → 3 → 5 → 7 → 4 → 8 → 9 → 6 → 10 → 1

Start with the quick security/correctness fixes (2, 3, 5, 7) since they're small changes with high impact. Then tackle reliability (4, 8) and performance (9, 6). The UI redesign (1) and alert system (10) are larger efforts best done as separate sprints.
