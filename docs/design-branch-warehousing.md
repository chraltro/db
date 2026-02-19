# Branch-Aware Warehousing — Design Document

## Status

**Design only.** This document describes the architecture for branch-aware warehousing in dp. Implementation is deferred until teams need concurrent branch development on the same warehouse.

## Problem

When multiple team members work on different branches, they may modify the same SQL models. Currently, running `dp transform` on one branch overwrites the shared warehouse, making it impossible to:

- Develop two features that touch the same gold table simultaneously
- Compare the output of a model on branch A vs branch B
- Keep main's data intact while experimenting on a feature branch

## Approach: Schema Prefixing

When on a non-main branch, dp prefixes all schema names with a sanitized branch name.

### Schema naming

```
main branch:       gold.earthquake_summary
feature/add-risk:  feature_add_risk__gold.earthquake_summary
fix/typo:          fix_typo__gold.earthquake_summary
```

Sanitization rules:
- Replace `/`, `-`, `.` with `_`
- Lowercase
- Prefix with branch name + `__` (double underscore separator)

### Which schemas get branched

Not all schemas should be branched. Landing and bronze data is typically shared — it comes from external sources and doesn't change between branches.

| Schema   | Branched? | Rationale |
|----------|-----------|-----------|
| landing  | No        | Raw data from external sources, same across branches |
| bronze   | No        | Light cleanup, rarely changes between feature branches |
| silver   | Yes       | Business logic often changes per feature |
| gold     | Yes       | Consumption models are the primary diff target |

Configuration in `project.yml`:

```yaml
branching:
  enabled: true
  branch_schemas: [silver, gold]       # Only these get prefixed
  shared_schemas: [landing, bronze]    # Always use the real schema
```

### Read fallback

When a branched model depends on a schema that hasn't been built on the current branch, dp falls back to reading from main's schema.

Example: `feature_add_risk__gold.earthquake_summary` depends on `silver.earthquake_events`. If `feature_add_risk__silver.earthquake_events` doesn't exist, dp reads from `silver.earthquake_events` (main's version).

This means you only need to build the models you've changed on your branch.

### SQL rewriting

When executing a model on a branch, dp rewrites the SQL to:
1. **Target** the branched schema: `CREATE TABLE feature_x__gold.model AS ...`
2. **Read from** branched schemas if they exist, falling back to main: use a view or CTE wrapper

Implementation approach:
```python
def _rewrite_sql_for_branch(query, branch_prefix, branched_schemas):
    """Rewrite schema references in SQL for branch isolation."""
    for schema in branched_schemas:
        # Replace schema.table references with branch-prefixed versions
        # Use DuckDB's schema search path or explicit rewriting
        pass
```

The rewriting must handle:
- `FROM schema.table` references
- `JOIN schema.table` references
- Subqueries referencing branched schemas
- `depends_on` references in comments (for DAG resolution)

### `dp serve` on a branch

When running the web UI on a branch:
- Show the branch's tables (prefixed schemas) as if they were the real schemas
- For schemas not yet built on the branch, show main's tables as fallback
- Display a branch indicator in the UI (already implemented in Task 3.5)

The API layer strips the branch prefix for display purposes.

## Commands

### `dp branch status`

Show what branch you're on and which models have been built:

```
Branch: feature/add-risk
Branched schemas: silver, gold

Built on this branch:
  feature_add_risk__gold.earthquake_summary (100 rows)
  feature_add_risk__gold.region_risk (50 rows)

Using main's version:
  silver.earthquake_events (2,450 rows)
  silver.earthquake_daily (365 rows)
  gold.top_earthquakes (100 rows)
```

### `dp branch clean`

Drop all tables prefixed with the current branch name:

```bash
dp branch clean                    # clean current branch
dp branch clean feature/old-work   # clean a specific branch
dp branch clean --all              # clean all branch tables (keep main)
```

### `dp branch merge`

Promote branch tables to the real schemas. This is a destructive operation (overwrites main's tables):

```bash
dp branch merge                    # merge current branch into main schemas
dp branch merge --dry-run          # show what would change
```

Implementation: rename/copy tables from `branch_prefix__schema.table` to `schema.table`.

### `dp branch list`

Show all branches that have tables in the warehouse:

```
Branch                    Tables   Last Modified
feature/add-risk          2        2024-01-15 14:30
fix/data-cleanup          5        2024-01-14 09:15
```

## Storage Implications

Each branch creates copies of modified tables. For a project with 20 gold tables averaging 100MB each:

- Main: 2GB
- Each branch (touching 3 models): ~300MB additional
- 5 active branches: ~3.5GB total

Mitigation strategies:
- Only branch schemas that change (silver/gold, not landing/bronze)
- `dp branch clean` to remove stale branch data
- Auto-clean branches that haven't been touched in N days (configurable)

## Conflict Resolution

When two branches modify the same model:
1. Each branch has its own copy — no conflict during development
2. On merge, the second branch to merge overwrites the first
3. `dp branch merge --dry-run` shows which main tables will be replaced
4. Use `dp diff` to compare branch output vs current main before merging

There is no automatic conflict resolution. The last merge wins. This matches how SQL schema changes work in practice — the final DDL statement defines the table.

## Implementation Order

1. **Schema prefixing logic** — the core rewriting engine
2. **`dp transform` branch awareness** — detect branch, rewrite SQL
3. **Read fallback** — check branched schema first, fall back to main
4. **CLI commands** — branch status, clean, merge, list
5. **Web UI** — branch-aware table display
6. **Auto-cleanup** — scheduled cleanup of stale branch tables

## Open Questions

1. **Views vs tables on branches**: Should branched views point to branched tables? This creates a cascade — changing one silver table means rebuilding all downstream views on the branch.

2. **Cross-branch queries**: Should `dp query` on a branch automatically use branched schemas? Probably yes for consistency, but this could surprise users.

3. **DuckDB schema search path**: DuckDB supports `SET search_path`. Could we use this instead of SQL rewriting? This would be simpler but might not handle all cases (explicit schema references in SQL).

4. **Notebook isolation**: Should notebooks running on a branch see branched data? If so, the pre-injected `db` connection needs branch-aware schema resolution.

5. **Landing schema branching**: Some teams might want to branch landing data (e.g., testing a new API endpoint that produces different raw data). Should this be configurable per-branch?
