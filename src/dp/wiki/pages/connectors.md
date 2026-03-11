# Connectors

Connectors automate data ingestion from external sources. dp includes pre-built connectors for databases, SaaS APIs, file storage, and webhooks. Each connector tests the connection, discovers available resources, generates an ingest script, and updates `project.yml`.

## Available Connectors

| Connector | Type | Description |
|-----------|------|-------------|
| PostgreSQL | `postgres` | PostgreSQL database tables |
| MySQL | `mysql` | MySQL/MariaDB database tables |
| CSV Files | `csv` | Local or remote CSV files |
| Stripe | `stripe` | Stripe payments data |
| Shopify | `shopify` | Shopify e-commerce data |
| HubSpot | `hubspot` | HubSpot CRM data |
| Google Sheets | `google_sheets` | Google Spreadsheets |
| REST API | `rest_api` | Generic REST API endpoints |
| S3/GCS | `s3_gcs` | Amazon S3 or Google Cloud Storage files |
| Webhook | `webhook` | Receive webhook data via HTTP POST |

List all available connectors:

```bash
dp connectors available
```

## Setting Up a Connector

### Interactive Setup

Use `dp connect` to set up a connector interactively:

```bash
# PostgreSQL
dp connect postgres --host localhost --database mydb --user admin --password secret

# Stripe
dp connect stripe --api-key sk_live_xxx

# CSV file
dp connect csv --path /data/customers.csv

# Google Sheets
dp connect google-sheets --set spreadsheet_id=1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms

# With JSON config
dp connect postgres --config '{"host":"db.prod","database":"app","user":"ro","password":"s3cret"}'

# From a config file
dp connect postgres --config ./postgres.json
```

The setup process:

1. **Tests the connection** -- Verifies credentials and connectivity
2. **Discovers resources** -- Lists available tables, endpoints, or sheets
3. **Generates an ingest script** -- Creates `ingest/connector_<name>.py`
4. **Updates project.yml** -- Adds the connection and creates a sync stream
5. **Stores secrets in .env** -- Passwords and API keys go to `.env`, not `project.yml`

### Configuration Options

```bash
dp connect <type> [OPTIONS]

Options:
  --name, -n          Connection name (default: auto-generated)
  --tables, -t        Comma-separated tables to sync
  --schema, -s        Target schema (default: landing)
  --schedule          Cron schedule for automatic sync
  --test              Only test the connection
  --discover          Only list available resources
  --config, -c        JSON string or file path with params
  --set key=value     Set individual parameters (repeatable)
```

Convenience shortcuts for common parameters:

```bash
  --host              Hostname
  --port              Port number
  --database          Database name
  --user              Username
  --password          Password
  --url               URL
  --api-key           API key
  --token             Access token
  --path              File or bucket path
```

## Managing Connectors

### List Configured Connectors

```bash
dp connectors list
```

Shows all connectors in `project.yml` with their type, script path, and status.

### Test a Connection

```bash
dp connectors test prod_postgres
```

Verifies that the connection still works with the stored credentials.

### Sync Data

```bash
dp connectors sync prod_postgres
```

Runs the generated ingest script for a connector.

### Regenerate Script

```bash
dp connectors regenerate prod_postgres
```

Re-discovers resources and regenerates the ingest script. Useful when the connector code is updated or configuration changes.

### Remove a Connector

```bash
dp connectors remove prod_postgres
```

Deletes the ingest script and removes the connection from `project.yml`.

## Connector Architecture

Each connector implements the `BaseConnector` contract:

- `test_connection(config)` -- Verify the connection works
- `discover(config)` -- List available tables/resources
- `generate_script(config, tables, target_schema)` -- Emit a Python ingest script

The generated ingest script is a standard dp Python script that uses the `db` DuckDB connection. You can customize it after generation.

### Secret Handling

Connector parameters marked as `secret` (passwords, API keys, tokens) are:

1. Stored in `.env` as environment variables (e.g., `PROD_POSTGRES_PASSWORD=...`)
2. Referenced in `project.yml` as `${ENV_VAR_NAME}` placeholders
3. Never written to `project.yml` in plaintext

## Webhook Connector

The webhook connector receives data via HTTP POST and stores it in a landing table:

```bash
dp connect webhook --name orders_webhook
```

Once configured, send data to the webhook endpoint:

```bash
curl -X POST http://localhost:3000/api/webhook/orders \
  -H "Content-Type: application/json" \
  -d '{"order_id": 123, "amount": 99.99}'
```

Data is stored in `landing.<webhook_name>_inbox` with columns: `id`, `received_at`, `payload` (JSON).

## Connector Health

Check the last sync status for each connector:

```bash
# Via API
curl http://localhost:3000/api/connectors/health
```

Returns the most recent run status, timestamp, and duration for each ingest script.

## Related Pages

- [CDC](cdc) -- Incremental sync with change data capture
- [Configuration](configuration) -- Connection configuration in project.yml
- [Pipelines](pipelines) -- Running connectors as pipeline steps
- [API Reference](api-reference) -- Connector API endpoints
