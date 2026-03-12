# Versioning and Time Travel

havn provides two versioning systems: **snapshots** for lightweight project state comparison, and **versions** for full data time travel with Parquet-backed table recovery.

## Snapshots

Snapshots capture a fingerprint of the current project and data state. They are fast to create and useful for comparing changes over time.

### Creating Snapshots

```bash
havn snapshot
```

Or with a custom name:

```bash
havn snapshot --name before-refactor
```

Via API:

```bash
curl -X POST http://localhost:3000/api/snapshot \
  -H "Content-Type: application/json" \
  -d '{"name": "before-refactor"}'
```

### What Snapshots Capture

- **File manifest** -- SHA256 hashes of all files in `transform/`, `ingest/`, `export/`, and `project.yml`
- **Table signatures** -- Row counts and column schema hashes for every user table
- **Project hash** -- Combined hash of all project files

Snapshots do **not** store actual data -- they are metadata-only.

### Listing Snapshots

```bash
# Via API
curl http://localhost:3000/api/snapshots
```

### Comparing Against a Snapshot

```bash
havn diff --snapshot before-refactor
```

Shows:

```
Snapshot: before-refactor
Created: 2025-01-15 06:00:00

File changes:
  + transform/gold/new_model.sql
  ~ transform/silver/dim_customer.sql
  - transform/bronze/old_cleanup.sql

Table Changes
  Table                    Status     Before Rows   After Rows
  gold.new_model          added                        1,234
  silver.dim_customer     modified        5,000         5,432
  bronze.old_cleanup      removed         2,100
```

### Deleting Snapshots

```bash
# Via API
curl -X DELETE http://localhost:3000/api/snapshots/before-refactor
```

## Versions (Time Travel)

Versions create full Parquet snapshots of table data, enabling point-in-time recovery and data comparison.

### Creating Versions

```bash
# Via API
curl -X POST http://localhost:3000/api/versions \
  -H "Content-Type: application/json"
```

Versions are stored in the `_snapshots/` directory:

```
_snapshots/
  v1-20250115-060000/
    gold.customers.parquet
    gold.orders.parquet
    _manifest.json
  v2-20250116-060000/
    ...
```

### Version Triggers

Versions can be created:

- **Manually** -- Via API or CLI
- **Before restores** -- Auto-snapshot created before any restore operation
- **After transforms** -- Optionally snapshot after successful pipeline runs

### Listing Versions

```bash
# Via API
curl http://localhost:3000/api/versions
```

Returns:

```json
[
  {
    "version_id": "v2-20250116-060000",
    "created_at": "2025-01-16 06:00:00",
    "description": "",
    "trigger": "manual",
    "table_count": 8,
    "total_rows": 45678
  }
]
```

### Version Details

```bash
curl http://localhost:3000/api/versions/v2-20250116-060000
```

Returns detailed information including per-table row counts and column schemas.

## Diff Between Versions

Compare two versions or a version against current state:

```bash
# Compare two versions
curl "http://localhost:3000/api/versions/v1-20250115-060000/diff?to_version=v2-20250116-060000"

# Compare a version against current state
curl http://localhost:3000/api/versions/v1-20250115-060000/diff
```

Returns:

```json
{
  "from_version": "v1-20250115-060000",
  "to_version": "current",
  "tables_compared": 8,
  "changes": [
    {
      "table": "gold.customers",
      "change": "modified",
      "rows_before": 5000,
      "rows_after": 5432,
      "row_diff": 432
    },
    {
      "table": "gold.new_report",
      "change": "added",
      "rows": 1234
    },
    {
      "table": "gold.deprecated",
      "change": "removed",
      "rows_before": 100
    }
  ]
}
```

Changes include:
- **added** -- Table exists in the "to" version but not the "from" version
- **removed** -- Table exists in the "from" version but not the "to" version
- **modified** -- Row count or schema changed between versions
- **Schema changes** -- Columns added or removed between versions

## Restoring Versions

Restore tables from a version snapshot:

```bash
curl -X POST http://localhost:3000/api/versions/v1-20250115-060000/restore
```

The restore process:

1. **Auto-snapshots current state** -- Creates a version before restore (trigger: `restore`)
2. **Reads Parquet files** -- Loads each table from the version's Parquet snapshots
3. **Replaces current tables** -- Uses `CREATE OR REPLACE TABLE` from Parquet data
4. **Reports results** -- Returns per-table status (success/error/skipped)

### Selective Restore

Restore specific tables by specifying a table list in the request body.

### Path Safety

Parquet file paths are validated to prevent path traversal attacks. Files must reside within the project's `_snapshots/` directory.

## Table Timeline

View the history of a specific table across all versions:

```bash
curl http://localhost:3000/api/versions/timeline/gold.customers
```

Returns:

```json
[
  {
    "version_id": "v3-20250117-060000",
    "created_at": "2025-01-17 06:00:00",
    "row_count": 5500,
    "columns": [
      {"name": "customer_id", "type": "INTEGER"},
      {"name": "name", "type": "VARCHAR"}
    ]
  },
  {
    "version_id": "v2-20250116-060000",
    "created_at": "2025-01-16 06:00:00",
    "row_count": 5432,
    "columns": [...]
  }
]
```

## Cleanup

Remove old versions to save disk space:

```bash
# Keep only the 10 most recent versions (via API or engine function)
```

The cleanup function:
- Deletes Parquet files from `_snapshots/`
- Removes metadata from `_dp_internal.version_history`
- Validates paths before deletion (safety)

## Model-Level Diff

In addition to version-based diff, havn supports model-level diff that compares the current SQL output against the materialized table:

```bash
havn diff
```

This re-executes each model's SQL and compares the result against the existing table:

```
Diff Summary
  Model                    Before   After   Added   Removed   Modified   Schema
  gold.customer_summary     5,000   5,432    +432         0          0     --
  silver.dim_customer       3,200   3,200       0         0         15     --
```

### Git-Aware Diff

Compare only models whose SQL files changed relative to a git branch:

```bash
havn diff --against main
```

This finds SQL files that differ between HEAD and `main`, then diffs only those models.

## Backup and Restore

havn also provides database-level backup and restore:

```bash
# Create a backup
havn backup

# Restore from a backup
havn restore warehouse.duckdb.backup_20250115_060000
```

Backups are full copies of the DuckDB file. The backup command flushes the WAL (Write-Ahead Log) first to ensure consistency.

## Related Pages

- [Transforms](transforms) -- SQL models and change detection
- [Pipelines](pipelines) -- Running transforms
- [CLI Reference](cli-reference) -- Diff, snapshot, and version commands
- [API Reference](api-reference) -- Versioning API endpoints
