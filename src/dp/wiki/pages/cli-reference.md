# CLI Reference

Complete reference for all `dp` CLI commands. Run `dp --help` or `dp <command> --help` for built-in help.

## Project Management

### dp init

Scaffold a new data platform project.

```bash
dp init [NAME] [--dir PATH]
```

| Argument/Flag | Default | Description |
|---------------|---------|-------------|
| `NAME` | `my-project` | Project name |
| `--dir, -d` | `./<NAME>` | Target directory |

Creates project structure with sample earthquake data pipeline, seeds, contracts, and notebooks.

### dp validate

Validate project structure, config, and SQL model dependencies.

```bash
dp validate [--project PATH]
```

Checks `project.yml` parsing, directory structure, stream actions, model dependencies, circular dependencies, and environment variable references.

### dp status

Show project health: git info, warehouse stats, last run.

```bash
dp status [--project PATH]
```

### dp context

Generate a project summary to paste into any AI assistant.

```bash
dp context [--project PATH]
```

Outputs a comprehensive markdown summary of the project including configuration, models, scripts, warehouse tables, and recent history.

### dp checkpoint

Smart git commit: stages files, auto-generates commit message.

```bash
dp checkpoint [--message TEXT] [--project PATH]
```

Automatically stages all files except `.env`, generates a descriptive commit message from changed file paths, and commits.

### dp backup

Create a backup of the warehouse database.

```bash
dp backup [--output PATH] [--project PATH]
```

Flushes the DuckDB WAL and copies the database file.

### dp restore

Restore the warehouse database from a backup.

```bash
dp restore BACKUP_PATH [--project PATH]
```

## Pipeline Execution

### dp run

Run a single ingest or export script.

```bash
dp run SCRIPT [--project PATH]
```

Examples:
```bash
dp run ingest/customers.py
dp run ingest/earthquakes.dpnb
dp run export/daily_report.py
```

### dp seed

Load CSV files from seeds/ directory.

```bash
dp seed [--force] [--schema NAME] [--env NAME] [--project PATH]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--force, -f` | false | Reload all seeds (ignore change detection) |
| `--schema, -s` | `seeds` | Target schema |
| `--env, -e` | none | Environment override |

### dp transform

Build SQL models in dependency order.

```bash
dp transform [TARGETS...] [--force] [--parallel] [--workers N] [--env NAME] [--skip-check] [--project PATH]
```

| Flag | Default | Description |
|------|---------|-------------|
| `TARGETS` | all | Specific models to build |
| `--force, -f` | false | Rebuild all (ignore change detection) |
| `--parallel` | false | Run independent models concurrently |
| `--workers, -w` | 4 | Max parallel workers |
| `--env, -e` | none | Environment override |
| `--skip-check` | false | Skip pre-transform validation |

### dp stream

Run a full stream from project.yml.

```bash
dp stream NAME [--force] [--env NAME] [--project PATH]
```

### dp lint

Lint SQL files with SQLFluff.

```bash
dp lint [--fix] [--project PATH]
```

| Flag | Description |
|------|-------------|
| `--fix` | Auto-fix violations |

## Querying and Inspection

### dp query

Run an ad-hoc SQL query.

```bash
dp query "SQL" [--csv] [--json] [--limit N] [--env NAME] [--project PATH]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--csv` | false | Output as CSV |
| `--json` | false | Output as JSON |
| `--limit, -n` | 0 (all) | Max rows to return |
| `--env, -e` | none | Environment override |

### dp tables

List tables and views in the warehouse.

```bash
dp tables [SCHEMA] [--env NAME] [--project PATH]
```

### dp history

Show recent run history.

```bash
dp history [--limit N] [--project PATH]
```

## Data Quality

### dp check

Validate SQL models, run assertions, and run contracts.

```bash
dp check [TARGETS...] [--env NAME] [--project PATH]
```

Runs model validation, inline assertions, and YAML contracts.

### dp freshness

Check model and source freshness.

```bash
dp freshness [--hours N] [--alert] [--sources] [--env NAME] [--project PATH]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--hours, -h` | 24.0 | Max age before a model is stale |
| `--alert` | false | Send alerts for stale models |
| `--sources` | false | Check source freshness from sources.yml |

### dp profile

Show model profile statistics.

```bash
dp profile [MODEL] [--project PATH]
```

Without a model name, shows summary for all models. With a model name, shows detailed column statistics.

### dp assertions

Show recent assertion results.

```bash
dp assertions [--project PATH]
```

### dp contracts

Run data contracts from the contracts/ directory.

```bash
dp contracts [TARGETS...] [--history] [--project PATH]
```

| Flag | Description |
|------|-------------|
| `TARGETS` | Contract names or model names to run |
| `--history` | Show contract history instead of running |

## Model Analysis

### dp lineage

Show column-level lineage for a model.

```bash
dp lineage MODEL [--json] [--project PATH]
```

### dp impact

Analyze downstream impact of changing a model or column.

```bash
dp impact MODEL [--column NAME] [--json] [--project PATH]
```

### dp promote

Promote SQL to a transform model file.

```bash
dp promote SQL_SOURCE [--name NAME] [--schema NAME] [--desc TEXT] [--file PATH] [--overwrite] [--project PATH]
```

### dp debug

Generate a debug notebook for a failed model.

```bash
dp debug MODEL [--project PATH]
```

Creates a `.dpnb` notebook pre-populated with error info, upstream queries, and the failing SQL.

## Diff and Versioning

### dp diff

Compare model SQL output against materialized tables.

```bash
dp diff [TARGETS...] [--target SCHEMA] [--format FMT] [--rows] [--full] [--against REF] [--snapshot NAME] [--project PATH]
```

| Flag | Default | Description |
|------|---------|-------------|
| `TARGETS` | all | Models to diff |
| `--target, -t` | none | Diff all models in a schema |
| `--format, -f` | `table` | Output format: `table` or `json` |
| `--rows` | false | Include sample rows |
| `--full` | false | Show all changed rows |
| `--against` | none | Git-aware: only diff models changed vs a branch |
| `--snapshot` | none | Compare against a named snapshot |

## Connectors

### dp connect

Set up a data connector.

```bash
dp connect TYPE [--name NAME] [--tables LIST] [--schema NAME] [--schedule CRON] [--test] [--discover] [--config JSON] [--set KEY=VALUE] [--host H] [--port P] [--database D] [--user U] [--password P] [--url U] [--api-key K] [--token T] [--path P] [--project PATH]
```

Use `dp connect list` to show available connector types.

### dp connectors list

List configured connectors.

```bash
dp connectors list [--project PATH]
```

### dp connectors test

Test a configured connector.

```bash
dp connectors test CONNECTION_NAME [--project PATH]
```

### dp connectors sync

Run sync for a connector.

```bash
dp connectors sync CONNECTION_NAME [--project PATH]
```

### dp connectors regenerate

Regenerate the ingest script for a connector.

```bash
dp connectors regenerate CONNECTION_NAME [--project PATH]
```

### dp connectors remove

Remove a connector (script and config).

```bash
dp connectors remove CONNECTION_NAME [--project PATH]
```

### dp connectors available

List all available connector types.

```bash
dp connectors available
```

## CDC

### dp cdc

View and manage CDC state.

```bash
dp cdc ACTION [--connector NAME] [--table NAME] [--project PATH]
```

Actions:
- `status` -- Show CDC state for all connectors
- `reset` -- Reset watermarks (requires `--connector`)

## Scheduling

### dp schedule

Start the cron scheduler.

```bash
dp schedule [--project PATH]
```

### dp watch

Watch for file changes and auto-rebuild.

```bash
dp watch [--project PATH]
```

## Masking

### dp masking create

Create a masking policy.

```bash
dp masking create --schema S --table T --column C --method M [--exempt ROLES] [--project PATH]
```

### dp masking list

List all masking policies.

```bash
dp masking list [--project PATH]
```

### dp masking delete

Delete a masking policy.

```bash
dp masking delete POLICY_ID [--project PATH]
```

## Server

### dp serve

Start the web UI server.

```bash
dp serve [--port PORT] [--host HOST] [--auth] [--env NAME] [--project PATH]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | 3000 | Server port |
| `--host` | 127.0.0.1 | Server host |
| `--auth` | false | Enable authentication |
| `--env` | none | Environment to use |

## Version

### dp version

Show dp version.

```bash
dp version
```
