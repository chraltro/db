# Notebooks

dp includes a custom notebook format (`.dpnb`) for interactive data exploration, pipeline development, and debugging. Notebooks combine code, SQL, and markdown cells in a single file that can be executed both interactively in the web UI and as part of data pipelines.

## Notebook Format

`.dpnb` files are JSON documents with a list of cells:

```json
{
  "cells": [
    {
      "type": "markdown",
      "source": "# Earthquake Data Analysis\nExploring USGS earthquake data."
    },
    {
      "type": "code",
      "source": "import requests\nresponse = requests.get('https://earthquake.usgs.gov/...')\ndata = response.json()"
    },
    {
      "type": "sql",
      "source": "SELECT COUNT(*) FROM landing.earthquakes"
    },
    {
      "type": "ingest",
      "source": "db.execute('CREATE OR REPLACE TABLE landing.raw AS SELECT * FROM ...')"
    }
  ]
}
```

## Cell Types

### markdown

Rendered as formatted text in the web UI. Use for documentation, headers, and explanations:

```json
{
  "type": "markdown",
  "source": "## Data Quality Check\nVerify that all required columns have data."
}
```

### code

Python code cells. The `db` DuckDB connection is available, just like in ingest scripts:

```json
{
  "type": "code",
  "source": "import pandas as pd\nresult = db.execute('SELECT * FROM gold.summary').fetchdf()\nprint(result.describe())"
}
```

### sql

SQL cells that execute against the DuckDB warehouse:

```json
{
  "type": "sql",
  "source": "SELECT region, COUNT(*) AS event_count\nFROM silver.earthquake_events\nGROUP BY region\nORDER BY event_count DESC\nLIMIT 10"
}
```

SQL results are displayed as formatted tables in the web UI.

### ingest

Special code cells that write data to the warehouse. Used for data loading operations:

```json
{
  "type": "ingest",
  "source": "db.execute('CREATE SCHEMA IF NOT EXISTS landing')\ndb.execute('CREATE OR REPLACE TABLE landing.data AS ...')"
}
```

## Using Notebooks in Pipelines

Notebooks can be used as ingest or export steps in your pipeline:

```yaml
streams:
  full-refresh:
    steps:
      - ingest: [earthquakes]      # Runs ingest/earthquakes.dpnb
      - transform: [all]
      - export: [all]
```

Place notebooks in `ingest/` or `export/` directories. dp executes all code, SQL, and ingest cells in order, providing the `db` connection.

### Pipeline Execution Rules

- Markdown cells are skipped during pipeline execution
- Code and ingest cells execute with `db` pre-injected
- SQL cells execute against the DuckDB connection
- Cell execution is sequential (top to bottom)
- Errors in any cell stop execution (for ingest notebooks)

## Interactive Execution

### Web UI

Start the web server and navigate to a notebook:

```bash
dp serve
```

The notebook UI provides:
- Cell-by-cell execution
- SQL result display as tables
- Markdown rendering
- Output capture for code cells

### Running from CLI

Execute a notebook as a script:

```bash
dp run ingest/earthquakes.dpnb
```

This runs all cells sequentially, logging output to the console.

## Debug Notebooks

dp can automatically generate debug notebooks for failed models:

```bash
dp debug silver.customers
```

This creates `notebooks/debug_silver_customers.dpnb` pre-populated with:

1. **Error description** -- The error message from the run log
2. **Upstream dependency cells** -- SQL cells querying each upstream table
3. **Failing model SQL** -- The model's SQL for interactive editing
4. **Assertion diagnostics** -- If assertions failed, cells to investigate each failure

### Debug Workflow

1. A transform fails: `dp transform` shows `silver.customers: ERROR`
2. Generate a debug notebook: `dp debug silver.customers`
3. Open in the web UI: `dp serve`, navigate to notebooks
4. Execute cells interactively to identify the issue
5. Fix the SQL model and re-run: `dp transform`

## Promoting Notebook SQL to Models

Convert a SQL query from a notebook into a proper transform model:

```bash
dp promote notebooks/explore.dpnb --name my_model --schema silver
```

This:
1. Extracts the last SQL cell from the notebook
2. Auto-detects table dependencies
3. Creates `transform/silver/my_model.sql` with proper config comments
4. Validates the new model fits in the DAG

You can also promote from a literal SQL string or file:

```bash
dp promote "SELECT * FROM bronze.data WHERE active = true" --name active_data --schema silver
dp promote query.sql --name my_model --schema gold
```

## Notebook API

### List Notebooks

```bash
curl http://localhost:3000/api/notebooks
```

### Read a Notebook

```bash
curl http://localhost:3000/api/notebooks/explore.dpnb
```

### Execute a Cell

```bash
curl -X POST http://localhost:3000/api/notebooks/explore.dpnb/execute \
  -H "Content-Type: application/json" \
  -d '{"cell_index": 2}'
```

### Save a Notebook

```bash
curl -X PUT http://localhost:3000/api/notebooks/explore.dpnb \
  -H "Content-Type: application/json" \
  -d '{"cells": [...]}'
```

## Conversion

Notebooks support conversion between formats:

- `.dpnb` to Python script -- Extracts code cells into a `.py` file
- Python script to `.dpnb` -- Wraps a script in a single code cell

## Best Practices

1. **Use notebooks for exploration** -- Notebooks are ideal for ad-hoc analysis and debugging. For production transforms, promote queries to SQL model files.

2. **Keep pipeline notebooks focused** -- Notebooks in `ingest/` should do one thing: load data. Keep exploration in `notebooks/`.

3. **Use markdown cells** -- Document your analysis with markdown cells. Future you will thank present you.

4. **Debug with dp debug** -- When transforms fail, use `dp debug` instead of manually creating debug notebooks.

5. **Version control notebooks** -- `.dpnb` files are JSON and work well with git. They are part of your project.

## Related Pages

- [Transforms](transforms) -- SQL models (the production target for promoted queries)
- [Pipelines](pipelines) -- Using notebooks in pipeline steps
- [Getting Started](getting-started) -- Project structure overview
- [CLI Reference](cli-reference) -- Notebook-related commands
