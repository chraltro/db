# Data Quality

havn provides a comprehensive data quality framework with three complementary systems: inline assertions in SQL models, automatic profiling, and freshness monitoring. For standalone quality rules, see [Contracts](contracts).

## Inline Assertions

Add `-- assert:` comments to SQL model files. Assertions are evaluated after each model builds during `havn transform`:

```sql
-- config: materialized=table, schema=gold
-- depends_on: silver.customers
-- assert: row_count > 0
-- assert: unique(customer_id)
-- assert: no_nulls(customer_id)
-- assert: no_nulls(email)
-- assert: accepted_values(status, ['active', 'inactive', 'suspended'])
-- assert: "lifetime_value >= 0"

SELECT
    customer_id,
    email,
    status,
    SUM(order_total) AS lifetime_value
FROM silver.customers
GROUP BY 1, 2, 3
```

### Available Assertion Types

#### `row_count > N`

Checks that the table has more than N rows:

```sql
-- assert: row_count > 0
-- assert: row_count > 100
-- assert: row_count >= 1000
```

#### `unique(column)`

Checks that a column contains no duplicate values:

```sql
-- assert: unique(customer_id)
-- assert: unique(email)
```

#### `no_nulls(column)`

Checks that a column contains no NULL values:

```sql
-- assert: no_nulls(customer_id)
-- assert: no_nulls(email)
```

#### `accepted_values(column, [values])`

Checks that all values in a column are within the allowed set:

```sql
-- assert: accepted_values(status, ['active', 'inactive', 'suspended'])
-- assert: accepted_values(country_code, ['US', 'CA', 'GB', 'DE'])
```

#### Custom SQL Expressions

Any boolean SQL expression can be used as an assertion. Wrap complex expressions in quotes:

```sql
-- assert: "AVG(amount) > 0"
-- assert: "MAX(created_at) > CURRENT_DATE - INTERVAL '7 days'"
-- assert: "COUNT(DISTINCT region) > 1"
```

Custom expressions are evaluated as `SELECT (<expression>) FROM <table>` and must return a single truthy value.

### Assertion Behavior

- Assertions run **after** a model is built (after `CREATE OR REPLACE TABLE`)
- If an assertion **fails**, the model status is set to `assertion_failed`
- The model data is **not rolled back** -- the table exists but is flagged
- All assertions for a model are evaluated (not short-circuited)
- Results are stored in `_dp_internal.assertion_results`

### Viewing Assertion Results

```bash
havn assertions
```

Shows recent assertion results with pass/fail status and details.

Via API:

```bash
# All assertions
curl http://localhost:3000/api/assertions

# For a specific model
curl http://localhost:3000/api/assertions/gold.customer_summary
```

## Data Profiling

havn automatically computes column-level statistics for every model after it builds. Profiles include:

- **Row count** -- Total number of rows
- **Column count** -- Number of columns
- **Null percentages** -- Percentage of NULL values per column
- **Distinct counts** -- Number of distinct values per column

### Viewing Profiles

```bash
# Summary of all models
havn profile

# Detailed profile for one model
havn profile gold.earthquake_summary
```

The detailed view shows per-column statistics:

```
gold.earthquake_summary  (1,234 rows, 8 columns)
  Profiled at: 2025-01-15 06:00:12

  Column         Null %   Distinct   Status
  region             0%        42   ok
  magnitude          0%       156   ok
  location          12%       891   has nulls
  depth              0%       734   ok
```

### Profile via API

```bash
# All profiles
curl http://localhost:3000/api/profiles

# Specific model
curl http://localhost:3000/api/profiles/gold.earthquake_summary
```

### Table-Level Profiling

The table browser in the web UI provides interactive profiling for any table:

```bash
curl http://localhost:3000/api/tables/gold/earthquake_summary/profile
```

Returns detailed statistics including min/max values, averages (for numeric columns), and sample values.

## Freshness Monitoring

Freshness monitoring detects models that have not been rebuilt within a specified time window.

### Check Model Freshness

```bash
havn freshness --hours 24
```

Shows all models with their last run time and whether they are stale (not rebuilt within 24 hours).

### Check Source Freshness

```bash
havn freshness --sources
```

Checks source freshness against SLAs declared in `project.yml`. See [Sources](sources).

### Freshness Alerts

Send alerts for stale models:

```bash
havn freshness --hours 24 --alert
```

This sends notifications via Slack or webhook (requires alert configuration in `project.yml`).

### Freshness via API

```bash
curl "http://localhost:3000/api/freshness?max_hours=24"
```

## Combined Validation

The `havn check` command runs all quality checks in one pass:

```bash
havn check
```

This executes:

1. **Model validation** -- SQL syntax, dependency resolution, column references
2. **Inline assertions** -- `-- assert:` comments against live data
3. **YAML contracts** -- Rules from `contracts/` directory

### CI/CD Integration

Use `havn check` in your CI/CD pipeline:

```bash
havn check --env test
```

Exit code 1 if any validation or assertion fails, making it suitable for automated quality gates.

## Alerts

havn supports alerting for pipeline events:

### Configuration

```yaml
alerts:
  channels:
    - slack
    - webhook
  slack_webhook_url: ${SLACK_WEBHOOK_URL}
  webhook_url: "https://alerts.internal/havn"
  on_success: false
  on_failure: true
```

### Alert Types

- **Pipeline failure** -- Sent when a transform or stream fails
- **Assertion failure** -- Sent when data quality assertions fail
- **Stale data** -- Sent when models exceed freshness thresholds

### Test Alerts

```bash
curl -X POST http://localhost:3000/api/alerts/test \
  -H "Content-Type: application/json" \
  -d '{"channel": "slack", "slack_webhook_url": "https://hooks.slack.com/..."}'
```

### Alert History

```bash
curl http://localhost:3000/api/alerts
```

## Related Pages

- [Contracts](contracts) -- Standalone YAML data quality rules
- [Transforms](transforms) -- Adding assertions to SQL models
- [Sources](sources) -- Source freshness SLAs
- [Lineage](lineage) -- Understanding data dependencies
- [CLI Reference](cli-reference) -- Quality-related commands
