# SQL Transforms

SQL transforms are the core of dp's data pipeline. Every `.sql` file in the `transform/` directory is a model that produces a table or view in DuckDB. Models are parsed, ordered by dependency, and executed automatically.

## File Structure

Models are organized into subdirectories that correspond to schemas:

```
transform/
  bronze/           # Schema: bronze
    customers.sql
    orders.sql
  silver/           # Schema: silver
    dim_customer.sql
    fact_orders.sql
  gold/             # Schema: gold
    customer_summary.sql
```

The folder name determines the default schema. A file at `transform/silver/dim_customer.sql` produces a table at `silver.dim_customer`.

## SQL Model Format

Every SQL model file starts with metadata comments followed by a SELECT statement:

```sql
-- config: materialized=table, schema=silver
-- depends_on: bronze.customers, bronze.orders
-- description: Customer dimension with order counts
-- col: customer_id: Unique customer identifier
-- col: order_count: Total number of orders
-- assert: row_count > 0
-- assert: unique(customer_id)
-- assert: no_nulls(customer_id)

SELECT
    c.customer_id,
    c.name,
    c.email,
    COUNT(o.order_id) AS order_count,
    SUM(o.total_amount) AS lifetime_value
FROM bronze.customers c
LEFT JOIN bronze.orders o ON c.customer_id = o.customer_id
GROUP BY 1, 2, 3
```

### Config Comments

#### `-- config:`

Sets materialization and schema:

```sql
-- config: materialized=table, schema=gold
```

- `materialized` -- Either `table` (persisted) or `view` (computed on read). Default: `table`.
- `schema` -- Override the schema derived from the folder name. Optional.

#### `-- depends_on:`

Declares upstream dependencies for DAG ordering:

```sql
-- depends_on: bronze.customers, bronze.orders
```

Dependencies are used to determine execution order. If omitted, dp auto-detects table references from the SQL using AST parsing (via sqlglot), but explicit declaration is recommended for clarity.

#### `-- description:`

Documents the model:

```sql
-- description: Customer dimension table with lifetime metrics
```

#### `-- col:`

Documents individual columns:

```sql
-- col: customer_id: Unique customer identifier
-- col: lifetime_value: Sum of all order amounts
```

#### `-- assert:`

Defines data quality assertions evaluated after the model builds:

```sql
-- assert: row_count > 0
-- assert: unique(customer_id)
-- assert: no_nulls(email)
-- assert: accepted_values(status, ['active', 'inactive'])
-- assert: "total_amount >= 0"
```

See [Quality](quality) for the full assertion reference.

## Change Detection

dp uses SHA256 hashing to detect when a model's SQL has changed. On each `dp transform` run:

1. The SQL content is normalized (whitespace-insensitive)
2. A SHA256 hash is computed from the normalized SQL
3. The hash is compared against the stored hash in `_dp_internal.model_state`
4. If the hash matches and upstream models haven't changed, the model is **skipped**
5. If the hash differs or any upstream dependency was rebuilt, the model is **rebuilt**

This means most `dp transform` runs only rebuild what has actually changed, making iterative development fast.

## DAG Ordering

Models are automatically sorted in topological order based on their `-- depends_on:` declarations. This ensures upstream tables exist before downstream models try to read from them.

```
bronze.customers ──┐
                   ├──> silver.dim_customer ──> gold.customer_summary
bronze.orders ─────┘
```

If a circular dependency is detected, `dp transform` will fail with an error. Use `dp validate` to check for circular dependencies without running transforms.

## Running Transforms

### Build All Models

```bash
dp transform
```

Only rebuilds models whose SQL has changed or whose upstream dependencies were rebuilt.

### Force Rebuild Everything

```bash
dp transform --force
```

Ignores change detection and rebuilds all models.

### Build Specific Models

```bash
dp transform gold.customer_summary silver.dim_customer
```

Builds only the specified models (and their upstream dependencies if needed).

### Parallel Execution

```bash
dp transform --parallel --workers 4
```

Runs independent models concurrently. Models at the same level in the DAG (no dependencies between them) execute in parallel.

### Environment Override

```bash
dp transform --env prod
```

Uses the database path and settings from the `prod` environment. See [Environments](environments).

## Materialization

### Table (Default)

```sql
-- config: materialized=table
```

Creates a persistent table using `CREATE OR REPLACE TABLE ... AS SELECT ...`. Data is stored on disk and queries are fast.

### View

```sql
-- config: materialized=view
```

Creates a view using `CREATE OR REPLACE VIEW ... AS SELECT ...`. The query runs on read, so data is always current but queries may be slower for complex logic.

## Plain SQL -- No Templating

dp uses plain SQL with no Jinja, no macros, and no templating language. This means:

- SQL files work directly in any DuckDB client
- No learning curve beyond standard SQL
- Full DuckDB syntax support (window functions, CTEs, UNNEST, etc.)
- Easy to test and debug

If you need dynamic behavior, use Python ingest/export scripts or parameterize via environment variables in `project.yml`.

## Validation

Check your models for errors without running them:

```bash
dp check
```

This validates:
- SQL syntax (via sqlglot AST parsing)
- `-- depends_on:` references exist in the DAG, seeds, or sources
- Column references against known upstream table schemas
- Inline assertions against live data (if warehouse exists)
- YAML contracts from `contracts/`

## Related Pages

- [Pipelines](pipelines) -- Run transforms as part of multi-step streams
- [Quality](quality) -- Data quality assertions and profiling
- [Lineage](lineage) -- Column-level lineage and impact analysis
- [Seeds](seeds) -- Load CSV reference data
- [CLI Reference](cli-reference) -- Full command reference for `dp transform`
