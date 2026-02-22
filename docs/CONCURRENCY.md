# Connection Pooling and DuckDB Concurrency

## How dp Uses DuckDB Connections

dp uses DuckDB as an **embedded database** — there is no database server. Every operation opens a direct connection to the `.duckdb` file.

### Connection lifecycle

```
CLI command / API request
    → duckdb.connect("warehouse.duckdb")
    → execute queries
    → conn.close()
```

There is **no connection pool**. Each operation creates a fresh connection and closes it when done. This is intentional:

1. DuckDB connections are fast to create (~1ms)
2. DuckDB's file-based locking handles concurrency
3. Connection pools add complexity without meaningful benefit for embedded databases
4. dp is designed for batch workloads, not thousands of concurrent connections

### Where connections are opened

| Component | Pattern | Lifetime |
|-----------|---------|----------|
| CLI commands (`dp transform`, `dp query`) | Open → run → close | Duration of the command |
| Stream execution (`dp stream`) | Open → run all steps → close | Duration of the stream |
| Web UI API endpoints | Open → handle request → close | Duration of the HTTP request |
| Script execution (`dp run`) | Open → pass to script as `db` → close | Duration of script + timeout |
| Connector test/discover | Open in-memory → test → close | Seconds |

### The `connect()` function

```python
# src/dp/engine/database.py
def connect(db_path, read_only=False):
    conn = duckdb.connect(str(db_path), read_only=read_only)
    conn.execute("SET enable_progress_bar = true")
    return conn
```

That's it. No pool, no wrapper, no retry logic. DuckDB handles locking internally.

## DuckDB Concurrency Model

### Single-writer, multiple-reader

DuckDB uses a **WAL (Write-Ahead Log)** for concurrency:

- **Multiple readers** can query simultaneously
- **Only one writer** can modify the database at a time
- Write operations acquire an exclusive lock
- Readers do not block writers, and writers do not block readers (MVCC)

### What this means for dp

| Scenario | Works? | Notes |
|----------|--------|-------|
| Run `dp query` while `dp serve` is running | Yes | Read queries don't conflict |
| Run `dp transform` while browsing the web UI | Mostly | Transforms write; API reads still work, but API writes will wait |
| Run two `dp transform` simultaneously | No | Second one will fail or wait for the lock |
| Run `dp stream` while `dp serve` handles a write request | Depends | The write request will wait until the stream step releases the lock |
| Browse tables in web UI during a long export | Yes | Export writes, UI reads — no conflict |

### Lock contention in practice

For typical dp usage (batch transforms, occasional queries, web UI browsing), you'll rarely hit lock contention. Problems arise when:

1. **Long-running transforms** hold the write lock for minutes
2. **Concurrent CLI commands** try to write at the same time
3. **API write endpoints** (file writes, query execution) run during transforms

DuckDB will **wait** for the lock (with a timeout), not immediately fail. The default timeout is configurable but dp doesn't override it.

### Error handling

If a write operation can't acquire the lock within the timeout:

```
IOException: Could not set lock on file "warehouse.duckdb": Resource temporarily unavailable
```

This is not a dp bug — it's DuckDB's concurrency model working as designed. Solutions:

- Wait for the running operation to finish
- Use `read_only=True` for operations that only need to read
- Don't run multiple write operations concurrently

## Why No Connection Pool?

Connection pools solve a specific problem: reusing expensive TCP connections to remote databases. DuckDB doesn't have this problem:

| Feature | Remote DB (Postgres) | Embedded DB (DuckDB) |
|---------|---------------------|---------------------|
| Connection cost | ~50-200ms (TCP + auth) | ~1ms (file open) |
| Concurrent connections | Hundreds | Limited by file lock |
| Connection state | Per-connection settings | Per-connection settings |
| Pool benefit | High | Negligible |

Adding a connection pool to dp would:

- Add complexity (pool size config, health checks, leak detection)
- Not improve performance (connections are already cheap)
- Risk stale connections holding the write lock
- Complicate error handling (is the connection dead or just locked?)

### What we do instead

- **CLI commands**: Open, run, close. Simple and correct.
- **Web UI**: Each API request gets a fresh connection. The request handler closes it in a `finally` block.
- **Streams**: One connection for the entire stream. All steps (seed, ingest, transform, export) share the same connection to avoid lock conflicts between steps.
- **Scripts**: The `db` connection is created by the runner and passed to the script. The runner closes it after the script finishes (or times out).

## External Database Connections

When dp connects to **external databases** (PostgreSQL, MySQL) via DuckDB's extension system, the pattern is different:

```sql
-- DuckDB opens a connection to the external database
ATTACH 'host=... dbname=...' AS pg_src (TYPE POSTGRES, READ_ONLY)

-- Data is read through the attachment
SELECT * FROM pg_src.public.users

-- Connection is closed when detached
DETACH pg_src
```

These external connections are:

- Managed by DuckDB's extension (not dp)
- Read-only by default (safer, no accidental writes to production)
- Not pooled — each `ATTACH` creates a new connection
- Closed on `DETACH` (or when the DuckDB connection closes)

### Retry logic for external connections

The hardened connectors (PostgreSQL, MySQL, REST API) include **retry with exponential backoff** for external connection failures:

```python
def _attach_with_retry(conn_str, max_retries=3):
    for attempt in range(max_retries + 1):
        try:
            db.execute(f"ATTACH '{conn_str}' AS pg_src ...")
            return
        except Exception as e:
            if attempt == max_retries:
                raise
            wait = 2 ** attempt  # 1s, 2s, 4s
            time.sleep(wait)
```

This handles transient network failures, DNS resolution delays, and database server restarts.

## Parallel Transform Execution

`dp transform` supports a `--parallel` flag that runs independent models concurrently:

```bash
dp transform --parallel --workers 4
```

### How it works

1. Models are organized into **tiers** based on the dependency DAG
2. Models in the same tier have no interdependencies
3. Each tier runs its models in parallel using `ThreadPoolExecutor`
4. The next tier starts only after the current tier finishes

```
Tier 0: [bronze.events, bronze.users]        ← parallel
Tier 1: [silver.enriched_events, silver.dim_users]  ← parallel (after tier 0)
Tier 2: [gold.dashboard, gold.report]         ← parallel (after tier 1)
```

### Thread safety

All parallel execution uses the **same DuckDB connection**. This is safe because:

- DuckDB connections support concurrent reads from multiple threads
- Each model execution is a single `CREATE TABLE AS ...` statement
- DuckDB handles the write serialization internally
- Thread pool workers wait for the write lock when needed

### Limitations

- Parallel execution is **thread-based** (GIL applies to Python code, but DuckDB releases the GIL during query execution)
- The write lock means parallel writes are effectively serialized — the benefit comes from **parallel query planning and I/O**, not parallel writes
- For small models (<1000 rows), parallel execution may be slower than sequential due to thread overhead
- Assertion failures in one model do **not** cancel other running models in the same tier

## Best Practices

1. **Don't worry about connection pooling.** DuckDB connections are cheap. Open and close them freely.

2. **Avoid concurrent writes.** Run one `dp transform` or `dp stream` at a time. The web UI's read endpoints will continue working fine during transforms.

3. **Use `--parallel` for large projects.** If you have 20+ independent models per tier, parallel execution can significantly reduce total build time.

4. **Close connections in `finally` blocks.** If you write custom scripts that open DuckDB connections, always close them:
   ```python
   conn = duckdb.connect("warehouse.duckdb")
   try:
       conn.execute("...")
   finally:
       conn.close()
   ```

5. **Use read-only connections when possible.** The `connect(path, read_only=True)` function opens a read-only connection that won't block writers.
