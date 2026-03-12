# Change Data Capture (CDC)

CDC enables incremental data extraction from external sources. Instead of re-fetching all data on every sync, havn tracks what has changed and only fetches new or modified records. This reduces load on source systems and speeds up pipelines.

## CDC Modes

havn supports three CDC modes:

### high_watermark

Tracks the maximum value of a column (typically `updated_at` or `id`) and only fetches rows where the column exceeds the stored watermark.

```yaml
connectors:
  prod_users:
    type: postgres
    connection: prod_postgres
    target_schema: landing
    tables:
      - name: users
        cdc_mode: high_watermark
        cdc_column: updated_at
```

How it works:

1. On first sync, all rows are fetched (no watermark stored yet)
2. After sync, the maximum value of `updated_at` is stored as the watermark
3. On subsequent syncs, only rows where `updated_at > <stored_watermark>` are fetched
4. New rows are appended to the target table (not replaced)

Best for: Database tables with a monotonically increasing timestamp or ID column.

### file_tracking

Tracks last-modified timestamps for file sources and only re-ingests when files have changed.

```yaml
connectors:
  data_files:
    type: csv
    connection: local_files
    target_schema: landing
    tables:
      - name: transactions
        cdc_mode: file_tracking
```

How it works:

1. On first sync, the file is read and its modification time (`mtime`) is stored
2. On subsequent syncs, the current file `mtime` is compared against the stored value
3. If the file has been modified, it is re-read (full replacement of the target table)
4. If unchanged, the sync is **skipped**

Supported file types: CSV, Parquet, JSON, NDJSON.

### full_refresh

Always fetches all data. No CDC tracking.

```yaml
connectors:
  reference_data:
    type: postgres
    connection: prod_postgres
    target_schema: landing
    tables:
      - name: country_codes
        cdc_mode: full_refresh
```

Use this for small reference tables that change infrequently and are cheap to fully reload.

## Configuration

### In project.yml

```yaml
connectors:
  prod_users:
    type: postgres
    connection: prod_postgres
    target_schema: landing
    schedule: "*/30 * * * *"
    tables:
      - name: users
        cdc_mode: high_watermark
        cdc_column: updated_at
      - name: roles
        cdc_mode: full_refresh
      - name: permissions
        cdc_mode: high_watermark
        cdc_column: id
        source_query: "SELECT id, name, level FROM permissions"
```

Table options:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `name` | string | required | Source table name |
| `cdc_mode` | string | `full_refresh` | CDC mode: `high_watermark`, `file_tracking`, `full_refresh` |
| `cdc_column` | string | `null` | Column to track (required for `high_watermark`) |
| `source_query` | string | `null` | Custom SELECT query (default: `SELECT * FROM table`) |

## CDC State

CDC state is stored in `_dp_internal.cdc_state`:

| Column | Type | Description |
|--------|------|-------------|
| `connector_name` | VARCHAR | Connector identifier |
| `table_name` | VARCHAR | Table being tracked |
| `cdc_mode` | VARCHAR | Active CDC mode |
| `watermark_value` | VARCHAR | Current high-watermark value |
| `file_mtime` | DOUBLE | File modification timestamp |
| `last_sync_at` | TIMESTAMP | Last successful sync time |
| `rows_synced` | BIGINT | Rows synced in last run |

## Managing CDC State

### View CDC Status

```bash
havn cdc status
```

Shows all tracked tables with their watermarks and last sync times.

Filter by connector:

```bash
havn cdc status --connector prod_users
```

### Reset Watermarks

Reset all watermarks for a connector (forces full re-sync):

```bash
havn cdc reset --connector prod_users
```

Reset a specific table:

```bash
havn cdc reset --connector prod_users --table users
```

### API Access

```bash
# View all CDC state
curl http://localhost:3000/api/cdc

# View state for a specific connector
curl http://localhost:3000/api/cdc/prod_users

# Reset watermarks
curl -X POST http://localhost:3000/api/cdc/prod_users/reset
```

## Sync Results

Each sync returns a `CDCSyncResult` with:

| Field | Description |
|-------|-------------|
| `table` | Table name |
| `status` | `success`, `skipped`, or `error` |
| `rows_synced` | Number of rows synced |
| `duration_ms` | Sync duration in milliseconds |
| `cdc_mode` | CDC mode used |
| `watermark_before` | Watermark value before sync |
| `watermark_after` | Watermark value after sync |
| `error` | Error message (if failed) |

## Best Practices

1. **Use `high_watermark` for large, append-heavy tables** -- Only new rows are fetched, reducing query time and network transfer.

2. **Ensure the watermark column is indexed** -- The source database should have an index on the CDC column for efficient filtering.

3. **Use `full_refresh` for small reference tables** -- The overhead of CDC tracking is not worth it for tables with fewer than a few thousand rows.

4. **Reset watermarks after schema changes** -- If the source table schema changes, reset the watermark to re-sync all data.

5. **Monitor CDC state** -- Use `havn cdc status` regularly to verify syncs are completing and watermarks are advancing.

## Related Pages

- [Connectors](connectors) -- Setting up data connectors
- [Pipelines](pipelines) -- Running CDC as part of pipelines
- [Configuration](configuration) -- Connector configuration in project.yml
- [Scheduler](scheduler) -- Automating CDC syncs on a schedule
