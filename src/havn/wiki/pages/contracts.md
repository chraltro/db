# Data Contracts

Data contracts are standalone YAML files that define data quality rules. They complement inline `-- assert:` comments in SQL models by providing reusable, centralized quality definitions with severity levels and historical tracking.

## Contract Format

Contracts live in the `contracts/` directory as YAML files:

```yaml
# contracts/orders.yml
contracts:
  - name: orders_not_empty
    description: "Orders table must have data"
    model: gold.orders
    severity: error
    assertions:
      - row_count > 0
      - no_nulls(order_id)
      - unique(order_id)
      - accepted_values(status, ['pending', 'shipped', 'delivered'])
      - "total_amount >= 0"

  - name: customers_fresh
    description: "Customers must be loaded within 24h"
    model: silver.customers
    severity: warn
    assertions:
      - row_count > 0
```

Each YAML file can contain multiple contracts under the `contracts:` key.

## Contract Properties

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `name` | string | yes | file stem | Unique contract identifier |
| `model` | string | yes | -- | Target model as `schema.table` |
| `description` | string | no | `""` | Human-readable description |
| `severity` | string | no | `error` | `error` or `warn` |
| `assertions` | list | yes | -- | List of assertion expressions |

## Assertion Types

Contracts support the same assertion expressions as inline `-- assert:` comments:

### `row_count > N`

```yaml
assertions:
  - row_count > 0
  - row_count > 1000
  - row_count >= 100
```

### `unique(column)`

```yaml
assertions:
  - unique(order_id)
  - unique(email)
```

### `no_nulls(column)`

```yaml
assertions:
  - no_nulls(customer_id)
  - no_nulls(email)
```

### `accepted_values(column, [values])`

```yaml
assertions:
  - accepted_values(status, ['pending', 'shipped', 'delivered'])
```

### Custom SQL Expressions

```yaml
assertions:
  - "AVG(amount) > 0"
  - "MAX(created_at) > CURRENT_DATE - INTERVAL '7 days'"
```

## Severity Levels

### `error` (default)

Contract failures with `error` severity cause `havn check` and `havn contracts` to exit with code 1. Use this for critical data quality rules that must be enforced.

### `warn`

Contract failures with `warn` severity are reported but do not cause command failure. Use this for aspirational quality goals or non-critical checks.

```yaml
contracts:
  - name: revenue_positive
    model: gold.daily_revenue
    severity: error                # Must pass
    assertions:
      - row_count > 0

  - name: high_cardinality
    model: gold.daily_revenue
    severity: warn                 # Nice to have
    assertions:
      - "COUNT(DISTINCT region) > 5"
```

## Running Contracts

### Run All Contracts

```bash
havn contracts
```

Discovers all YAML files in `contracts/`, evaluates each contract, and reports results:

```
Running 3 contract(s)...

  PASS  orders_not_empty (gold.orders) [12ms]
         pass  row_count > 0
         pass  unique(order_id)
         pass  no_nulls(order_id)

  FAIL  customers_fresh (silver.customers) [8ms]
         pass  row_count > 0
         FAIL  "MAX(updated_at) > CURRENT_DATE - INTERVAL '24 hours'" (value: false)

  PASS  revenue_check (gold.daily_revenue) [5ms]
         pass  row_count > 0

2 contract(s) passed, 1 failed.
```

### Run Specific Contracts

Filter by contract name or model name:

```bash
havn contracts orders_not_empty
havn contracts gold.orders
```

### View Contract History

```bash
havn contracts --history
```

Shows the pass/fail history of all contract evaluations, ordered by most recent.

## Contract History

Every contract evaluation is recorded in `_dp_internal.contract_results`:

| Column | Type | Description |
|--------|------|-------------|
| `id` | VARCHAR | Unique result ID |
| `contract_name` | VARCHAR | Contract identifier |
| `model` | VARCHAR | Target model |
| `passed` | BOOLEAN | Whether all assertions passed |
| `severity` | VARCHAR | `error` or `warn` |
| `detail` | JSON | Per-assertion results |
| `checked_at` | TIMESTAMP | Evaluation timestamp |

### History via API

```bash
curl http://localhost:3000/api/contracts/history
```

## Contracts via API

### List Contracts

```bash
curl http://localhost:3000/api/contracts
```

Returns all discovered contracts with their names, models, assertions, and severity.

### Run Contracts

```bash
curl -X POST http://localhost:3000/api/contracts/run
```

Returns:

```json
{
  "total": 3,
  "passed": 2,
  "failed": 1,
  "results": [
    {
      "contract_name": "orders_not_empty",
      "model": "gold.orders",
      "passed": true,
      "severity": "error",
      "duration_ms": 12,
      "error": null,
      "assertions": [
        {"expression": "row_count > 0", "passed": true, "detail": "1234"}
      ]
    }
  ]
}
```

## Contracts in havn check

The `havn check` command includes contract evaluation:

```bash
havn check
```

This runs:
1. SQL model validation
2. Inline assertions (`-- assert:`)
3. YAML contracts from `contracts/`

All three must pass for `havn check` to exit with code 0.

## Contracts vs Inline Assertions

| Feature | Inline Assertions | YAML Contracts |
|---------|-------------------|----------------|
| Location | Inside SQL files | Separate YAML files |
| Evaluated | During `havn transform` | During `havn contracts` or `havn check` |
| Severity | Always error | Configurable (error/warn) |
| History | Stored in `assertion_results` | Stored in `contract_results` |
| Use case | Model-specific checks | Cross-model, reusable rules |

Use inline assertions for model-specific quality checks that should run on every build. Use contracts for cross-cutting quality rules, SLA enforcement, and checks that should be run independently of transforms.

## Best Practices

1. **Start with row_count** -- Every critical table should have `row_count > 0` as a minimum quality check.

2. **Enforce primary keys** -- Use `unique(pk_column)` and `no_nulls(pk_column)` for every table that has a primary key.

3. **Use severity wisely** -- Reserve `error` for must-have guarantees. Use `warn` for aspirational goals or new checks that are not yet enforced.

4. **Organize by domain** -- Group related contracts in the same YAML file: `contracts/finance.yml`, `contracts/customers.yml`, etc.

5. **Run in CI/CD** -- Add `havn contracts` to your CI/CD pipeline to catch quality regressions before deployment.

## Related Pages

- [Quality](quality) -- Data quality overview
- [Transforms](transforms) -- Inline assertions in SQL models
- [CLI Reference](cli-reference) -- Contract-related commands
- [API Reference](api-reference) -- Contract API endpoints
