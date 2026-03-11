# Configuration

All dp project settings live in `project.yml` at the project root. This page documents every configuration option.

## Minimal Configuration

```yaml
name: my-project
database:
  path: warehouse.duckdb
```

## Full Reference

### Project Name

```yaml
name: my-project
```

Human-readable project name. Used in logging and documentation.

### Database

```yaml
database:
  path: warehouse.duckdb
```

- `path` -- Path to the DuckDB database file, relative to the project root.

### Connections

Define external database connections. Values support environment variable expansion via `${VAR}` syntax, resolved from `.env`:

```yaml
connections:
  prod_postgres:
    type: postgres
    host: ${DB_HOST}
    port: 5432
    database: ${DB_NAME}
    user: ${DB_USER}
    password: ${DB_PASSWORD}

  analytics_mysql:
    type: mysql
    host: localhost
    database: analytics
    user: reader
    password: ${MYSQL_PASSWORD}
```

Connection parameters vary by type. See [Connectors](connectors) for available types.

### Streams

Define data pipelines with ordered steps:

```yaml
streams:
  full-refresh:
    description: "Full pipeline rebuild"
    schedule: "0 6 * * *"
    retries: 2
    retry_delay: 10
    webhook_url: "https://hooks.slack.com/services/..."
    steps:
      - seed: [all]
      - ingest: [all]
      - transform: [all]
      - export: [all]

  quick-transform:
    description: "Just rebuild models"
    steps:
      - transform: [all]
```

Stream options:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `description` | string | `""` | Human-readable description |
| `schedule` | string | `null` | Cron expression (5-field) |
| `retries` | int | `0` | Retry attempts per failed step |
| `retry_delay` | int | `5` | Seconds between retries |
| `webhook_url` | string | `null` | URL for completion notifications |

Step actions: `ingest`, `seed`, `transform`, `export`. Each takes a list of targets or `[all]`.

### Lint

Configure SQLFluff SQL linting:

```yaml
lint:
  dialect: duckdb
  rules:
    - L001
    - L002
    - L003
```

- `dialect` -- SQL dialect for SQLFluff. Default: `duckdb`.
- `rules` -- List of SQLFluff rules to enable. Default: all rules.

### Sources

Declare external data sources with metadata and freshness SLAs:

```yaml
sources:
  - name: production_db
    schema: landing
    description: "Production PostgreSQL database"
    connection: prod_postgres
    freshness_hours: 24
    tables:
      - name: customers
        description: "Customer records"
        loaded_at_column: updated_at
        columns:
          - name: customer_id
            description: "Primary key"
          - name: email
            description: "Customer email address"
      - name: orders
        description: "Order records"
```

See [Sources](sources) for details.

### Exposures

Declare downstream consumers of your data:

```yaml
exposures:
  - name: sales_dashboard
    description: "Executive sales dashboard"
    owner: analytics-team
    type: dashboard
    url: "https://dashboard.internal/sales"
    depends_on:
      - gold.daily_revenue
      - gold.customer_summary
```

Exposures appear in the DAG visualization and documentation.

### Environments

Define environment-specific overrides:

```yaml
environments:
  dev:
    database:
      path: dev_warehouse.duckdb
  prod:
    database:
      path: prod_warehouse.duckdb
  test:
    database:
      path: ":memory:"
```

Switch environments with `--env`:

```bash
dp transform --env prod
dp serve --env dev
```

See [Environments](environments) for details.

### Alerts

Configure alerting for pipeline events:

```yaml
alerts:
  channels:
    - slack
    - webhook
  slack_webhook_url: ${SLACK_WEBHOOK_URL}
  webhook_url: "https://alerts.example.com/dp"
  on_success: true
  on_failure: true
```

- `channels` -- List of alert channels: `slack`, `webhook`, `log`
- `slack_webhook_url` -- Slack incoming webhook URL
- `webhook_url` -- Custom webhook URL for alerts
- `on_success` -- Send alerts on pipeline success (default: false)
- `on_failure` -- Send alerts on pipeline failure (default: true)

### Connectors (CDC)

Configure CDC-enabled connectors for incremental data extraction:

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
      - name: roles
        cdc_mode: full_refresh
    schedule: "*/30 * * * *"
```

See [CDC](cdc) for details.

## Environment Variable Expansion

Any value in `project.yml` can reference environment variables using `${VAR}` syntax:

```yaml
connections:
  prod:
    type: postgres
    host: ${DB_HOST}
    password: ${DB_PASSWORD}
```

Variables are resolved from the `.env` file at the project root:

```
DB_HOST=db.production.internal
DB_PASSWORD=s3cure_p@ssw0rd
```

The `.env` file is included in `.gitignore` by default and should never be committed.

### Managing Secrets

```bash
# Via the CLI (not yet available as a standalone command)
# Secrets are managed through the web UI or by editing .env directly
```

Via the API (when auth is enabled):

```bash
curl -X POST http://localhost:3000/api/secrets \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"key": "DB_PASSWORD", "value": "new_password"}'
```

## Related Pages

- [Getting Started](getting-started) -- Project setup walkthrough
- [Pipelines](pipelines) -- Stream configuration details
- [Environments](environments) -- Multi-environment support
- [Connectors](connectors) -- Connection types and parameters
- [Scheduler](scheduler) -- Cron scheduling reference
