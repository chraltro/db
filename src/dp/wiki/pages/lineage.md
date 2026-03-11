# Lineage

dp provides column-level lineage tracking using AST-based SQL analysis. Lineage traces how each output column in a model maps back to its source columns in upstream tables, through CTEs, joins, subqueries, and expressions.

## How Lineage Works

dp uses sqlglot to parse SQL models into an Abstract Syntax Tree (AST) and traces column references through:

- Direct column references (`SELECT c.name FROM customers c`)
- Aliased expressions (`SELECT c.first_name || ' ' || c.last_name AS full_name`)
- CTEs (`WITH cte AS (SELECT ...) SELECT cte.col FROM cte`)
- Subqueries
- Window functions
- CASE expressions
- UNION ALL queries
- `SELECT *` expansion (when a database connection is available)

## Viewing Lineage

### CLI

```bash
dp lineage gold.earthquake_summary
```

Output:

```
Column lineage for gold.earthquake_summary:

  region <- silver.earthquake_events.region
  total_events <- (computed)
  avg_magnitude <- silver.earthquake_events.magnitude
  max_magnitude <- silver.earthquake_events.magnitude
  latest_event <- silver.earthquake_events.event_time
```

JSON output:

```bash
dp lineage gold.earthquake_summary --json
```

### API

Get lineage for a specific model:

```bash
curl http://localhost:3000/api/lineage/gold.earthquake_summary
```

Returns:

```json
{
  "model": "gold.earthquake_summary",
  "columns": {
    "region": [
      {"source_table": "silver.earthquake_events", "source_column": "region"}
    ],
    "total_events": [],
    "avg_magnitude": [
      {"source_table": "silver.earthquake_events", "source_column": "magnitude"}
    ]
  },
  "depends_on": ["silver.earthquake_events"]
}
```

Get lineage for all models:

```bash
curl http://localhost:3000/api/lineage
```

## Table-Level Dependencies

### `-- depends_on:` Declarations

Table-level dependencies are declared explicitly in SQL model headers:

```sql
-- depends_on: bronze.customers, bronze.orders
```

dp also auto-detects table references from SQL using AST parsing. Explicit declarations are recommended for clarity and are used for DAG ordering.

### DAG Visualization

The web UI displays an interactive dependency graph:

```bash
dp serve
# Navigate to the DAG tab
```

Or via API:

```bash
# Basic DAG (models only)
curl http://localhost:3000/api/dag

# Full DAG (models + seeds + sources + exposures + ingest scripts)
curl http://localhost:3000/api/dag/full
```

The DAG response includes:

- **Nodes** -- Models (table/view), sources, seeds, ingest scripts, exposures
- **Edges** -- Dependencies between nodes

## Impact Analysis

Impact analysis answers: "If I change this model or column, what downstream models are affected?"

### CLI

```bash
# Model-level impact
dp impact silver.customers

# Column-level impact
dp impact silver.customers --column email
```

Output:

```
Impact analysis for silver.customers
  Column: email

  3 downstream model(s) affected:
    gold.customer_summary
    gold.email_analytics
    gold.marketing_segments

  Affected columns:
    gold.customer_summary.email
    gold.email_analytics.email_domain
    gold.marketing_segments.contact_email

  Impact chain:
    silver.customers -> gold.customer_summary, gold.email_analytics
    gold.email_analytics -> gold.marketing_segments
```

JSON output:

```bash
dp impact silver.customers --column email --json
```

### API

```bash
# Model-level impact
curl http://localhost:3000/api/impact/silver.customers

# Column-level impact
curl "http://localhost:3000/api/impact/silver.customers?column=email"
```

Returns:

```json
{
  "model": "silver.customers",
  "column": "email",
  "downstream_models": [
    "gold.customer_summary",
    "gold.email_analytics"
  ],
  "affected_columns": [
    {"model": "gold.customer_summary", "column": "email"},
    {"model": "gold.email_analytics", "column": "email_domain"}
  ],
  "impact_chain": {
    "silver.customers": ["gold.customer_summary", "gold.email_analytics"]
  }
}
```

## CTE Tracing

dp traces lineage through CTEs correctly. For example:

```sql
WITH customer_orders AS (
    SELECT
        c.customer_id,
        c.name,
        COUNT(o.order_id) AS order_count
    FROM bronze.customers c
    LEFT JOIN bronze.orders o ON c.customer_id = o.customer_id
    GROUP BY 1, 2
)
SELECT
    customer_id,
    name,
    order_count,
    CASE WHEN order_count > 10 THEN 'VIP' ELSE 'regular' END AS tier
FROM customer_orders
```

Lineage for `name` correctly traces through the CTE back to `bronze.customers.name`.

## Full DAG Components

The full DAG (`/api/dag/full`) includes all project components:

| Node Type | Description |
|-----------|-------------|
| `source` | External data sources declared in `sources:` |
| `seed` | CSV files from `seeds/` |
| `ingest` | Python ingest scripts from `ingest/` |
| `import` | Data imported via the import wizard |
| `table` | SQL models materialized as tables |
| `view` | SQL models materialized as views |
| `exposure` | Downstream consumers declared in `exposures:` |

Ingest scripts are linked to their target tables by scanning the script content for `CREATE TABLE` and `INSERT INTO` patterns.

## Model Notebook View

The API provides a notebook-style view for each model that combines lineage, SQL source, sample data, and upstream/downstream relationships:

```bash
curl http://localhost:3000/api/models/gold.earthquake_summary/notebook-view
```

Returns the SQL source, sample data rows, column lineage, upstream dependencies, and downstream consumers in a single response.

## Limitations

- **Dynamic SQL** -- Lineage cannot trace through SQL built dynamically in Python ingest scripts.
- **DuckDB-specific syntax** -- Some DuckDB-specific functions may fall back to regex-based extraction when sqlglot cannot parse them.
- **SELECT \*** -- Star expansion requires a live database connection. Without it, `SELECT *` columns are attributed to the first upstream dependency.

## Related Pages

- [Transforms](transforms) -- SQL model format and `-- depends_on:`
- [Quality](quality) -- Using lineage for data quality
- [Sources](sources) -- Sources in the DAG
- [Seeds](seeds) -- Seeds in the DAG
- [API Reference](api-reference) -- Lineage and impact API endpoints
