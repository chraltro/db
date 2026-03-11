# Sources

Sources are declarations of external data that your dp project depends on. They document where data comes from, define freshness SLAs, and provide metadata for column-level validation and documentation.

## Declaring Sources

Sources are defined in `project.yml` under the `sources:` key:

```yaml
sources:
  - name: production_db
    schema: landing
    description: "Production PostgreSQL database"
    connection: prod_postgres
    freshness_hours: 24
    tables:
      - name: customers
        description: "Customer records from the CRM"
        loaded_at_column: updated_at
        columns:
          - name: customer_id
            description: "Primary key, auto-incremented"
          - name: email
            description: "Customer email address"
          - name: created_at
            description: "Account creation timestamp"
      - name: orders
        description: "E-commerce order records"
        loaded_at_column: order_date
        columns:
          - name: order_id
            description: "Unique order identifier"
          - name: customer_id
            description: "Foreign key to customers"
          - name: total_amount
            description: "Order total in USD"
```

## Source Properties

### Source-Level

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `name` | string | yes | Identifier for the source |
| `schema` | string | yes | DuckDB schema where data lands (e.g., `landing`) |
| `description` | string | no | Human-readable description |
| `connection` | string | no | Reference to a connection in `project.yml` |
| `freshness_hours` | float | no | Maximum age in hours before data is considered stale |

### Table-Level

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `name` | string | yes | Table name |
| `description` | string | no | Table description |
| `loaded_at_column` | string | no | Timestamp column for freshness checks |
| `columns` | list | no | Column definitions |

### Column-Level

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `name` | string | yes | Column name |
| `description` | string | no | Column description |

## Freshness Monitoring

When `freshness_hours` is set, dp can check whether source data is stale:

### CLI

```bash
dp freshness --sources
```

### API

```bash
curl http://localhost:3000/api/sources/freshness
```

Returns:

```json
[
  {
    "source": "production_db",
    "table": "landing.customers",
    "sla_hours": 24,
    "last_loaded": "2025-01-14T06:00:00",
    "hours_ago": 28.5,
    "is_stale": true
  }
]
```

### How Freshness Is Determined

dp checks freshness in two ways:

1. **loaded_at_column** -- If specified, dp queries `MAX(loaded_at_column)` from the table to find the most recent data timestamp.
2. **Run log fallback** -- If no `loaded_at_column` is set, dp checks the most recent successful run in `_dp_internal.run_log` for that table.

A source is marked **stale** if the hours since the last load exceed `freshness_hours`.

### Freshness Alerts

Combine freshness checks with alerting:

```bash
dp freshness --hours 24 --alert
```

This sends a Slack or webhook notification for any stale sources (requires alerts configured in `project.yml`).

## Sources in Validation

Source declarations are used during `dp check` validation:

1. **Dependency resolution** -- Source tables are recognized as valid dependencies in `-- depends_on:` comments, preventing false "unknown table" warnings.

2. **Column validation** -- If columns are declared in a source, dp validates that SQL models referencing those tables use valid column names.

```bash
dp check
```

Example output when a model references a non-existent source column:

```
  warn  silver.customers: column 'non_existent' not found in source landing.customers
```

## Sources in the DAG

Source tables appear as special nodes in the DAG visualization. They are shown with a "source" type and include their description. This provides visibility into where data originates when viewing the dependency graph.

### Full DAG

The full DAG view (`/api/dag/full`) includes sources, seeds, models, and exposures:

```bash
curl http://localhost:3000/api/dag/full
```

## Sources in Documentation

Source metadata appears in the auto-generated documentation:

```bash
# Via API
curl http://localhost:3000/api/docs/markdown
```

This generates Markdown documentation that includes source tables, their descriptions, column definitions, and freshness status.

## Listing Sources

### CLI

Sources are displayed as part of the project context:

```bash
dp context
```

### API

```bash
curl http://localhost:3000/api/sources
```

Returns all declared sources with their tables and columns.

## Best Practices

1. **Declare all external dependencies** -- Every table in `landing` that comes from an external source should have a source declaration. This enables validation and documentation.

2. **Set freshness SLAs** -- Define `freshness_hours` for critical sources. Use `dp freshness` in CI/CD to catch data delivery issues.

3. **Document columns** -- Column descriptions flow into auto-generated documentation and help team members understand the data.

4. **Use loaded_at_column** -- When available, specify the timestamp column for more accurate freshness checks.

## Related Pages

- [Quality](quality) -- Data quality overview including freshness
- [Configuration](configuration) -- Full project.yml reference
- [Lineage](lineage) -- Sources in the dependency graph
- [Transforms](transforms) -- Referencing sources in SQL models
