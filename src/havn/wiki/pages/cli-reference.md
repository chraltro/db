# CLI Reference

Complete reference for all `havn` CLI commands. Run `havn --help` or `havn <command> --help` for built-in help.

## Project Management

### havn init

Scaffold a new data platform project.

```bash
havn init [NAME] [--dir PATH]
```

| Argument/Flag | Default | Description |
|---------------|---------|-------------|
| `NAME` | `my-project` | Project name |
| `--dir, -d` | `./<NAME>` | Target directory |

Creates project structure with sample earthquake data pipeline, seeds, contracts, and notebooks.

### havn validate

Validate project structure, config, and SQL model dependencies.

```bash
havn validate [--project PATH]
```

Checks `project.yml` parsing, directory structure, stream actions, model dependencies, circular dependencies, and environment variable references.

### havn status

Show project health: git info, warehouse stats, last run.

```bash
havn status [--project PATH]
```

### havn context

Generate a project summary to paste into any AI assistant.

```bash
havn context [--project PATH]
```

Outputs a comprehensive markdown summary of the project including configuration, models, scripts, warehouse tables, and recent history.

### havn checkpoint

Smart git commit: stages files, auto-generates commit message.

```bash
havn checkpoint [--message TEXT] [--project PATH]
```

Automatically stages all files except `.env`, generates a descriptive commit message from changed file paths, and commits.

### havn backup

Create a backup of the warehouse database.

```bash
havn backup [--output PATH] [--project PATH]
```

Flushes the DuckDB WAL and copies the database file.

### havn restore

Restore the warehouse database from a backup.

```bash
havn restore BACKUP_PATH [--project PATH]
```

## Pipeline Execution

### havn run

Run a single ingest or export script.

```bash
havn run SCRIPT [--project PATH]
```

Examples:
```bash
havn run ingest/customers.py
havn run ingest/earthquakes.dpnb
havn run export/daily_report.py
```

### havn seed

Load CSV files from seeds/ directory.

```bash
havn seed [--force] [--schema NAME] [--env NAME] [--project PATH]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--force, -f` | false | Reload all seeds (ignore change detection) |
| `--schema, -s` | `seeds` | Target schema |
| `--env, -e` | none | Environment override |

### havn transform

Build SQL models in dependency order.

```bash
havn transform [TARGETS...] [--force] [--parallel] [--workers N] [--env NAME] [--skip-check] [--project PATH]
```

| Flag | Default | Description |
|------|---------|-------------|
| `TARGETS` | all | Specific models to build |
| `--force, -f` | false | Rebuild all (ignore change detection) |
| `--parallel` | false | Run independent models concurrently |
| `--workers, -w` | 4 | Max parallel workers |
| `--env, -e` | none | Environment override |
| `--skip-check` | false | Skip pre-transform validation |

### havn stream

Run a full stream from project.yml.

```bash
havn stream NAME [--force] [--env NAME] [--project PATH]
```

### havn lint

Lint SQL files with SQLFluff.

```bash
havn lint [--fix] [--project PATH]
```

| Flag | Description |
|------|-------------|
| `--fix` | Auto-fix violations |

## Querying and Inspection

### havn query

Run an ad-hoc SQL query.

```bash
havn query "SQL" [--csv] [--json] [--limit N] [--env NAME] [--project PATH]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--csv` | false | Output as CSV |
| `--json` | false | Output as JSON |
| `--limit, -n` | 0 (all) | Max rows to return |
| `--env, -e` | none | Environment override |

### havn tables

List tables and views in the warehouse.

```bash
havn tables [SCHEMA] [--env NAME] [--project PATH]
```

### havn history

Show recent run history.

```bash
havn history [--limit N] [--project PATH]
```

## Data Quality

### havn check

Validate SQL models, run assertions, and run contracts.

```bash
havn check [TARGETS...] [--env NAME] [--project PATH]
```

Runs model validation, inline assertions, and YAML contracts.

### havn freshness

Check model and source freshness.

```bash
havn freshness [--hours N] [--alert] [--sources] [--env NAME] [--project PATH]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--hours, -h` | 24.0 | Max age before a model is stale |
| `--alert` | false | Send alerts for stale models |
| `--sources` | false | Check source freshness from sources.yml |

### havn profile

Show model profile statistics.

```bash
havn profile [MODEL] [--project PATH]
```

Without a model name, shows summary for all models. With a model name, shows detailed column statistics.

### havn assertions

Show recent assertion results.

```bash
havn assertions [--project PATH]
```

### havn contracts

Run data contracts from the contracts/ directory.

```bash
havn contracts [TARGETS...] [--history] [--project PATH]
```

| Flag | Description |
|------|-------------|
| `TARGETS` | Contract names or model names to run |
| `--history` | Show contract history instead of running |

## Model Analysis

### havn lineage

Show column-level lineage for a model.

```bash
havn lineage MODEL [--json] [--project PATH]
```

### havn impact

Analyze downstream impact of changing a model or column.

```bash
havn impact MODEL [--column NAME] [--json] [--project PATH]
```

### havn promote

Promote SQL to a transform model file.

```bash
havn promote SQL_SOURCE [--name NAME] [--schema NAME] [--desc TEXT] [--file PATH] [--overwrite] [--project PATH]
```

### havn debug

Generate a debug notebook for a failed model.

```bash
havn debug MODEL [--project PATH]
```

Creates a `.dpnb` notebook pre-populated with error info, upstream queries, and the failing SQL.

## Diff and Versioning

### havn diff

Compare model SQL output against materialized tables.

```bash
havn diff [TARGETS...] [--target SCHEMA] [--format FMT] [--rows] [--full] [--against REF] [--snapshot NAME] [--project PATH]
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

### havn connect

Set up a data connector.

```bash
havn connect TYPE [--name NAME] [--tables LIST] [--schema NAME] [--schedule CRON] [--test] [--discover] [--config JSON] [--set KEY=VALUE] [--host H] [--port P] [--database D] [--user U] [--password P] [--url U] [--api-key K] [--token T] [--path P] [--project PATH]
```

Use `havn connect list` to show available connector types.

### havn connectors list

List configured connectors.

```bash
havn connectors list [--project PATH]
```

### havn connectors test

Test a configured connector.

```bash
havn connectors test CONNECTION_NAME [--project PATH]
```

### havn connectors sync

Run sync for a connector.

```bash
havn connectors sync CONNECTION_NAME [--project PATH]
```

### havn connectors regenerate

Regenerate the ingest script for a connector.

```bash
havn connectors regenerate CONNECTION_NAME [--project PATH]
```

### havn connectors remove

Remove a connector (script and config).

```bash
havn connectors remove CONNECTION_NAME [--project PATH]
```

### havn connectors available

List all available connector types.

```bash
havn connectors available
```

## CDC

### havn cdc

View and manage CDC state.

```bash
havn cdc ACTION [--connector NAME] [--table NAME] [--project PATH]
```

Actions:
- `status` -- Show CDC state for all connectors
- `reset` -- Reset watermarks (requires `--connector`)

## Scheduling

### havn schedule

Start the cron scheduler.

```bash
havn schedule [--project PATH]
```

### havn watch

Watch for file changes and auto-rebuild.

```bash
havn watch [--project PATH]
```

## Masking

### havn masking create

Create a masking policy.

```bash
havn masking create --schema S --table T --column C --method M [--exempt ROLES] [--project PATH]
```

### havn masking list

List all masking policies.

```bash
havn masking list [--project PATH]
```

### havn masking delete

Delete a masking policy.

```bash
havn masking delete POLICY_ID [--project PATH]
```

## Server

### havn serve

Start the web UI server.

```bash
havn serve [--port PORT] [--host HOST] [--auth] [--env NAME] [--project PATH]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | 3000 | Server port |
| `--host` | 127.0.0.1 | Server host |
| `--auth` | false | Enable authentication |
| `--env` | none | Environment to use |

## Version

### havn version

Show havn version.

```bash
havn version
```
