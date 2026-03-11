# Pipelines

Pipelines in dp are called **streams**. A stream is an ordered sequence of steps (ingest, seed, transform, export) defined in `project.yml`. Streams provide a single command to run your entire data pipeline or any subset of it.

## Defining Streams

Streams are configured in `project.yml` under the `streams:` key:

```yaml
streams:
  full-refresh:
    description: "Full pipeline rebuild"
    steps:
      - seed: [all]
      - ingest: [all]
      - transform: [all]
      - export: [all]

  daily-etl:
    description: "Daily incremental ETL"
    schedule: "0 6 * * *"
    steps:
      - ingest: [all]
      - transform: [all]

  export-only:
    description: "Re-export without rebuilding"
    steps:
      - export: [all]
```

## Stream Steps

Each step specifies an **action** and a list of **targets**:

### Ingest

Runs Python scripts (`.py`) and notebooks (`.dpnb`) from the `ingest/` directory:

```yaml
- ingest: [all]                    # Run all ingest scripts
- ingest: [customers, orders]     # Run specific scripts
```

Scripts prefixed with `_` (e.g., `_helpers.py`) are skipped. The `db` DuckDB connection is pre-injected into each script.

### Seed

Loads CSV files from the `seeds/` directory into DuckDB tables:

```yaml
- seed: [all]                     # Load all seeds
```

Seeds use change detection -- only modified CSVs are reloaded. See [Seeds](seeds).

### Transform

Builds SQL models from the `transform/` directory in dependency order:

```yaml
- transform: [all]               # Build all models
- transform: [gold.summary]      # Build specific models
```

Uses SHA256 change detection to skip unchanged models. See [Transforms](transforms).

### Export

Runs Python scripts from the `export/` directory:

```yaml
- export: [all]                   # Run all export scripts
- export: [daily_report]         # Run specific scripts
```

## Running Streams

### Basic Execution

```bash
dp stream full-refresh
```

Executes each step in order. If any ingest step fails, the pipeline stops immediately to preserve data integrity.

### Force Rebuild

```bash
dp stream full-refresh --force
```

Forces all transform models to rebuild regardless of change detection.

### With Environment

```bash
dp stream daily-etl --env prod
```

Uses the production database and environment settings.

## Error Handling

Streams have built-in error handling:

- **Ingest failures stop the pipeline** -- If an ingest script fails, subsequent steps (transform, export) are not executed. This prevents building models on incomplete data.
- **Transform failures are reported** -- Failed models are logged but other independent models continue.
- **Export failures are logged** -- Export errors do not affect upstream data.

### Retry Support

Streams support automatic retries for transient failures:

```yaml
streams:
  daily-etl:
    description: "Daily ETL with retries"
    retries: 3
    retry_delay: 10
    steps:
      - ingest: [all]
      - transform: [all]
```

- `retries` -- Number of retry attempts per failed step (default: 0)
- `retry_delay` -- Seconds to wait between retries (default: 5)

### Webhook Notifications

Get notified when a stream completes or fails:

```yaml
streams:
  daily-etl:
    webhook_url: "https://hooks.slack.com/services/T.../B.../..."
    steps:
      - ingest: [all]
      - transform: [all]
```

The webhook receives a JSON POST with:

```json
{
  "stream": "daily-etl",
  "status": "success",
  "duration_seconds": 12.3,
  "timestamp": "2025-01-15T06:00:00"
}
```

## Scheduling

Streams can be scheduled with cron expressions:

```yaml
streams:
  daily-etl:
    schedule: "0 6 * * *"    # 6 AM daily
    steps:
      - ingest: [all]
      - transform: [all]
```

Start the scheduler:

```bash
dp schedule
```

See [Scheduler](scheduler) for the full cron reference.

## Python Ingest Scripts

Ingest scripts are plain Python files. A DuckDB connection is pre-injected as `db`:

```python
# ingest/customers.py
import requests

response = requests.get("https://api.example.com/customers")
data = response.json()

db.execute("CREATE SCHEMA IF NOT EXISTS landing")
db.execute("CREATE OR REPLACE TABLE landing.customers AS SELECT * FROM ?", [data])
```

### Legacy Format

The older `def run(db)` function format is still supported for backward compatibility:

```python
def run(db):
    db.execute("CREATE SCHEMA IF NOT EXISTS landing")
    db.execute("CREATE OR REPLACE TABLE landing.data AS SELECT 1")
```

### Script Execution

- Scripts run as top-level code with `db` available in the namespace
- `stdout` and `stderr` are captured and logged
- Scripts prefixed with `_` are skipped
- Script output is masked to prevent leaking secrets from `.env`

## Running Individual Steps

You can run steps independently without a stream:

```bash
dp run ingest/customers.py       # Run a single script
dp seed                          # Load all seeds
dp transform                     # Build all models
dp run export/daily_report.py    # Run a single export
```

## Pipeline Monitoring

### Run History

```bash
dp history
```

Shows all pipeline runs with type, target, status, duration, and row counts.

### Freshness

```bash
dp freshness --hours 24
```

Checks which models were last built more than 24 hours ago.

### Project Status

```bash
dp status
```

Shows project health: git info, warehouse stats, and last run status.

## Related Pages

- [Transforms](transforms) -- SQL model details
- [Configuration](configuration) -- Full `project.yml` reference
- [Scheduler](scheduler) -- Cron scheduling details
- [Connectors](connectors) -- Automated data ingestion
- [CLI Reference](cli-reference) -- All pipeline-related commands
