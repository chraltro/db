from __future__ import annotations

import json

from dp.engine.notebook import extract_notebook_outputs


def test_extract_notebook_outputs_explicit():
    """Extract explicitly declared outputs."""
    nb = {
        "title": "Test",
        "outputs": ["landing.earthquakes", "landing.weather"],
        "cells": [],
    }
    outputs = extract_notebook_outputs(nb)
    assert outputs == ["landing.earthquakes", "landing.weather"]


def test_extract_notebook_outputs_from_sql_cells():
    """Extract outputs inferred from SQL cells."""
    nb = {
        "title": "Test",
        "cells": [
            {"type": "sql", "source": "CREATE TABLE landing.data AS SELECT 1 AS id"},
            {"type": "sql", "source": "SELECT * FROM landing.data"},
        ],
    }
    outputs = extract_notebook_outputs(nb)
    assert "landing.data" in outputs


def test_extract_notebook_outputs_from_ingest_cells():
    """Extract outputs inferred from ingest cells."""
    nb = {
        "title": "Test",
        "cells": [
            {
                "type": "ingest",
                "source": json.dumps({
                    "source_type": "csv",
                    "source_path": "/data/test.csv",
                    "target_schema": "landing",
                    "target_table": "raw_data",
                }),
            },
        ],
    }
    outputs = extract_notebook_outputs(nb)
    assert "landing.raw_data" in outputs


def test_extract_notebook_outputs_from_code_cells():
    """Extract outputs inferred from code cell patterns."""
    nb = {
        "title": "Test",
        "cells": [
            {
                "type": "code",
                "source": "db.execute('CREATE OR REPLACE TABLE landing.events AS SELECT 1 AS id')",
            },
        ],
    }
    outputs = extract_notebook_outputs(nb)
    assert "landing.events" in outputs


def test_extract_notebook_outputs_mixed_cells():
    """Extract outputs from a notebook with multiple output-producing cells."""
    nb = {
        "title": "Multi-output",
        "cells": [
            {"type": "sql", "source": "CREATE TABLE landing.t1 AS SELECT 1 AS id"},
            {"type": "sql", "source": "CREATE OR REPLACE TABLE bronze.t2 AS SELECT 1"},
            {
                "type": "ingest",
                "source": json.dumps({
                    "source_type": "csv",
                    "source_path": "/data.csv",
                    "target_schema": "landing",
                    "target_table": "t3",
                }),
            },
            {"type": "code", "source": "db.execute('CREATE TABLE landing.t4 AS SELECT 1')"},
            {"type": "sql", "source": "SELECT * FROM landing.t1"},  # Read-only, no output
        ],
    }
    outputs = extract_notebook_outputs(nb)
    assert "landing.t1" in outputs
    assert "bronze.t2" in outputs
    assert "landing.t3" in outputs
    assert "landing.t4" in outputs
    # SELECT-only queries should not appear as outputs
    assert len(outputs) == 4
