# Scheduler

havn includes a built-in scheduler for running streams on cron schedules. It also provides a file watcher that automatically rebuilds transforms when SQL files change.

## Cron Scheduling

### Defining Schedules

Add a `schedule:` field to any stream in `project.yml`:

```yaml
streams:
  daily-etl:
    description: "Daily ETL pipeline"
    schedule: "0 6 * * *"         # 6 AM every day
    steps:
      - ingest: [all]
      - transform: [all]

  hourly-sync:
    description: "Hourly connector sync"
    schedule: "0 * * * *"         # Every hour on the hour
    steps:
      - ingest: [connector_stripe]
      - transform: [all]

  weekly-report:
    description: "Weekly summary export"
    schedule: "0 9 * * 1"         # 9 AM every Monday
    steps:
      - transform: [all]
      - export: [weekly_report]
```

### Cron Syntax

havn uses standard 5-field cron expressions:

```
┌───────────── minute (0 - 59)
│ ┌───────────── hour (0 - 23)
│ │ ┌───────────── day of month (1 - 31)
│ │ │ ┌───────────── month (1 - 12)
│ │ │ │ ┌───────────── day of week (0 - 6, 0 = Monday)
│ │ │ │ │
* * * * *
```

### Cron Patterns

| Pattern | Meaning |
|---------|---------|
| `*` | Every unit |
| `*/5` | Every 5 units |
| `1,15` | At 1 and 15 |
| `1-5` | From 1 through 5 |
| `30` | At exactly 30 |

### Common Schedules

```yaml
"0 6 * * *"      # Daily at 6 AM
"0 */2 * * *"    # Every 2 hours
"*/15 * * * *"   # Every 15 minutes
"0 9 * * 1"      # Monday at 9 AM
"0 0 1 * *"      # First day of every month at midnight
"30 8 * * 1-5"   # Weekdays at 8:30 AM
"0 6,18 * * *"   # Twice daily at 6 AM and 6 PM
```

## Starting the Scheduler

### CLI

```bash
havn schedule
```

This shows all scheduled streams and starts the scheduler:

```
Scheduled Streams
  Stream       Schedule       Description
  daily-etl    0 6 * * *      Daily ETL pipeline
  hourly-sync  0 * * * *      Hourly connector sync

Starting scheduler... (Ctrl+C to stop)
```

The scheduler runs as a foreground process. Press Ctrl+C to stop it.

### How It Works

1. The scheduler thread starts and reads schedules from `project.yml`
2. Every 30 seconds, it checks all cron expressions against the current time
3. When a schedule matches the current minute, the corresponding stream is executed
4. Each stream runs at most once per minute (deduplication)
5. Config is re-read on each check, so schedule changes take effect without restart

### With the Web Server

When you run `havn serve`, the scheduler is not started automatically. Start it separately in another terminal:

```bash
havn schedule
```

Or configure your deployment to run both processes.

## File Watcher

The file watcher monitors `transform/` and `ingest/` for changes and automatically triggers rebuilds.

### Starting the Watcher

```bash
havn watch
```

Output:

```
Watching for changes... (Ctrl+C to stop)
  transform/  -> auto-rebuild SQL models
```

### How It Works

1. Uses the `watchdog` library to monitor filesystem events
2. Watches `transform/` and `ingest/` directories recursively
3. When a `.sql` or `.py` file is modified, it triggers an action
4. For `transform/` changes: runs `havn transform`
5. Includes 2-second debouncing to avoid multiple rebuilds for rapid saves

### Example Session

```
Watching for changes... (Ctrl+C to stop)

Watcher: transform/silver/dim_customer.sql changed
Watcher: Running transform...
  skip  bronze.customers
  done  silver.dim_customer (1,234 rows, 45ms)
  done  gold.customer_summary (892 rows, 12ms)
Watcher: Transform completed
```

## Scheduler Architecture

### SchedulerThread

The scheduler runs as a daemon thread with a simple polling loop:

- Checks cron expressions every 30 seconds
- Reloads `project.yml` on each iteration (picks up config changes)
- Executes matching streams in the same thread (sequential)
- Logs all activity to the console and Python logging

### Huey Integration

havn also includes a Huey-based scheduler (SqliteHuey) for more robust task queuing. The SQLite-backed Huey instance stores task state in `.dp_scheduler.db` at the project root.

### FileWatcher

The file watcher runs as a separate daemon thread using the `watchdog` library. It:

- Monitors `transform/` and `ingest/` for `.sql` and `.py` changes
- Debounces rapid changes (2-second window)
- Creates a fresh database connection for each rebuild
- Reports results to the console

## Scheduler Status

### Via API

```bash
curl http://localhost:3000/api/scheduler
```

Returns:

```json
{
  "scheduled_streams": [
    {
      "name": "daily-etl",
      "description": "Daily ETL pipeline",
      "schedule": "0 6 * * *",
      "steps": [
        {"action": "ingest", "targets": ["all"]},
        {"action": "transform", "targets": ["all"]}
      ]
    }
  ]
}
```

## Deployment Considerations

### Running as a Service

For production deployments, run the scheduler as a background service:

```bash
# Using systemd (Linux)
# Create /etc/systemd/system/havn-scheduler.service

# Using nohup
nohup havn schedule > /var/log/havn-scheduler.log 2>&1 &
```

### Multiple Workers

The scheduler runs streams sequentially within a single thread. For parallel execution of independent streams, consider running multiple scheduler instances or using external orchestration tools.

### Monitoring

- Check `havn history` to verify scheduled runs are completing
- Use `havn freshness` to detect if scheduled pipelines have stopped running
- Configure webhook notifications on streams for immediate failure alerts

## Related Pages

- [Pipelines](pipelines) -- Stream configuration and execution
- [Configuration](configuration) -- Schedule configuration in project.yml
- [Connectors](connectors) -- Scheduling connector syncs
- [CLI Reference](cli-reference) -- Schedule and watch commands
