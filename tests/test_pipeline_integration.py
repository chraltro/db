"""Integration tests for the full pipeline path.

Tests the complete data flow: seed → ingest → transform → export,
exercising the stream orchestration, error handling, retry logic,
and cross-layer data integrity.
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import duckdb
import pytest

from dp.engine.database import connect, ensure_meta_table
from dp.engine.runner import run_script, run_scripts_in_dir
from dp.engine.seeds import run_seeds
from dp.engine.transform import run_transform


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def project(tmp_path):
    """Create a complete test project with all layers."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()

    # Create directory structure
    for d in ["ingest", "transform/bronze", "transform/silver",
              "transform/gold", "export", "seeds"]:
        (project_dir / d).mkdir(parents=True, exist_ok=True)

    # project.yml
    (project_dir / "project.yml").write_text(textwrap.dedent("""\
        name: test-pipeline
        database:
          path: warehouse.duckdb
        streams:
          full-refresh:
            description: "Full pipeline"
            steps:
              - seed: [all]
              - ingest: [all]
              - transform: [all]
              - export: [all]
    """))

    # Seed data
    (project_dir / "seeds" / "categories.csv").write_text(
        "id,name,priority\n"
        "1,critical,1\n"
        "2,high,2\n"
        "3,medium,3\n"
        "4,low,4\n"
    )

    # Ingest script
    (project_dir / "ingest" / "load_events.py").write_text(textwrap.dedent("""\
        db.execute("CREATE SCHEMA IF NOT EXISTS landing")
        db.execute(\"\"\"
            CREATE OR REPLACE TABLE landing.events AS
            SELECT * FROM (VALUES
                (1, 'login', 1, '2024-01-01'),
                (2, 'purchase', 1, '2024-01-01'),
                (3, 'login', 2, '2024-01-02'),
                (4, 'logout', 1, '2024-01-02'),
                (5, 'purchase', 3, '2024-01-03')
            ) AS t(id, event_type, category_id, event_date)
        \"\"\")
        print("Loaded 5 events")
    """))

    # Bronze transform
    (project_dir / "transform" / "bronze" / "events.sql").write_text(textwrap.dedent("""\
        -- config: materialized=table, schema=bronze
        -- depends_on: landing.events
        -- assert: row_count > 0
        -- assert: no_nulls(id)

        SELECT
            id,
            event_type,
            category_id,
            CAST(event_date AS DATE) AS event_date
        FROM landing.events
    """))

    # Silver transform with join
    (project_dir / "transform" / "silver" / "enriched_events.sql").write_text(textwrap.dedent("""\
        -- config: materialized=table, schema=silver
        -- depends_on: bronze.events
        -- description: Events enriched with category info from seeds
        -- assert: row_count > 0
        -- assert: unique(id)

        SELECT
            e.id,
            e.event_type,
            e.event_date,
            e.category_id,
            c.name AS category_name,
            c.priority AS category_priority
        FROM bronze.events e
        LEFT JOIN seeds.categories c ON e.category_id = c.id
    """))

    # Gold aggregation
    (project_dir / "transform" / "gold" / "event_summary.sql").write_text(textwrap.dedent("""\
        -- config: materialized=table, schema=gold
        -- depends_on: silver.enriched_events
        -- assert: row_count > 0
        -- assert: unique(event_type)

        SELECT
            event_type,
            COUNT(*) AS event_count,
            COUNT(DISTINCT event_date) AS active_days,
            MIN(category_priority) AS highest_priority
        FROM silver.enriched_events
        GROUP BY event_type
    """))

    # Export script
    (project_dir / "export" / "summary_export.py").write_text(textwrap.dedent("""\
        from pathlib import Path
        output_dir = Path(__file__).parent.parent / "output"
        output_dir.mkdir(exist_ok=True)
        dest = str(output_dir / "event_summary.csv").replace("'", "''")
        db.execute(f"COPY gold.event_summary TO '{dest}' (HEADER, DELIMITER ',')")
        rows = db.execute("SELECT COUNT(*) FROM gold.event_summary").fetchone()[0]
        print(f"Exported {rows} rows")
    """))

    return project_dir


@pytest.fixture
def db(project):
    """Open a DuckDB connection to the test project's warehouse."""
    db_path = project / "warehouse.duckdb"
    conn = connect(db_path)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Full pipeline tests
# ---------------------------------------------------------------------------


class TestFullPipeline:
    """Test the complete seed → ingest → transform → export pipeline."""

    def test_full_pipeline_end_to_end(self, project, db):
        """The full pipeline should produce correct data across all layers."""
        # Step 1: Seeds
        seed_results = run_seeds(db, project / "seeds", force=True)
        assert seed_results["seeds.categories"] == "built"

        # Verify seed data
        rows = db.execute("SELECT COUNT(*) FROM seeds.categories").fetchone()[0]
        assert rows == 4

        # Step 2: Ingest
        ingest_results = run_scripts_in_dir(db, project / "ingest", "ingest")
        assert len(ingest_results) == 1
        assert ingest_results[0]["status"] == "success"

        # Verify landing data
        rows = db.execute("SELECT COUNT(*) FROM landing.events").fetchone()[0]
        assert rows == 5

        # Step 3: Transform
        transform_results = run_transform(db, project / "transform", force=True)
        assert transform_results["bronze.events"] == "built"
        assert transform_results["silver.enriched_events"] == "built"
        assert transform_results["gold.event_summary"] == "built"

        # Verify data flows through layers correctly
        assert db.execute("SELECT COUNT(*) FROM bronze.events").fetchone()[0] == 5
        assert db.execute("SELECT COUNT(*) FROM silver.enriched_events").fetchone()[0] == 5
        # 3 event types: login, purchase, logout
        assert db.execute("SELECT COUNT(*) FROM gold.event_summary").fetchone()[0] == 3

        # Verify join worked (category names populated)
        nulls = db.execute(
            "SELECT COUNT(*) FROM silver.enriched_events WHERE category_name IS NULL"
        ).fetchone()[0]
        assert nulls == 0  # all events have valid category_ids

        # Step 4: Export
        export_results = run_scripts_in_dir(db, project / "export", "export")
        assert len(export_results) == 1
        assert export_results[0]["status"] == "success"

        # Verify exported file
        csv_path = project / "output" / "event_summary.csv"
        assert csv_path.exists()
        lines = csv_path.read_text().strip().split("\n")
        assert len(lines) == 4  # header + 3 event types

    def test_pipeline_data_integrity(self, project, db):
        """Gold layer should reflect correct aggregations."""
        # Run full pipeline
        run_seeds(db, project / "seeds", force=True)
        run_scripts_in_dir(db, project / "ingest", "ingest")
        run_transform(db, project / "transform", force=True)

        # Verify gold data correctness
        summary = db.execute(
            "SELECT event_type, event_count, active_days "
            "FROM gold.event_summary ORDER BY event_type"
        ).fetchall()

        assert ("login", 2, 2) in summary
        assert ("logout", 1, 1) in summary
        assert ("purchase", 2, 2) in summary

    def test_pipeline_metadata_logged(self, project, db):
        """Pipeline runs should create metadata entries."""
        run_seeds(db, project / "seeds", force=True)
        run_scripts_in_dir(db, project / "ingest", "ingest")
        run_transform(db, project / "transform", force=True)

        # Check run_log has entries for all steps
        run_types = db.execute(
            "SELECT DISTINCT run_type FROM _dp_internal.run_log"
        ).fetchall()
        run_types = {r[0] for r in run_types}
        assert "seed" in run_types
        assert "ingest" in run_types
        assert "transform" in run_types

        # Check assertion results were recorded
        assertion_count = db.execute(
            "SELECT COUNT(*) FROM _dp_internal.assertion_results"
        ).fetchone()[0]
        assert assertion_count > 0

        # All assertions should have passed
        failed = db.execute(
            "SELECT COUNT(*) FROM _dp_internal.assertion_results WHERE passed = false"
        ).fetchone()[0]
        assert failed == 0


# ---------------------------------------------------------------------------
# Seed tests
# ---------------------------------------------------------------------------


class TestSeedPipeline:
    """Test seed loading and change detection."""

    def test_seed_change_detection(self, project, db):
        """Seeds should skip loading when content hasn't changed."""
        # First load
        results1 = run_seeds(db, project / "seeds", force=False)
        assert results1["seeds.categories"] == "built"

        # Second load — should skip
        results2 = run_seeds(db, project / "seeds", force=False)
        assert results2["seeds.categories"] == "skipped"

        # Force reload
        results3 = run_seeds(db, project / "seeds", force=True)
        assert results3["seeds.categories"] == "built"

    def test_seed_used_in_transform(self, project, db):
        """Seed data should be accessible to transform models via seeds.* schema."""
        run_seeds(db, project / "seeds", force=True)
        run_scripts_in_dir(db, project / "ingest", "ingest")
        run_transform(db, project / "transform", force=True)

        # Verify the silver layer joined with seeds correctly
        result = db.execute(
            "SELECT category_name FROM silver.enriched_events "
            "WHERE category_id = 1"
        ).fetchone()
        assert result[0] == "critical"


# ---------------------------------------------------------------------------
# Ingest error handling tests
# ---------------------------------------------------------------------------


class TestIngestErrors:
    """Test ingest failure scenarios."""

    def test_ingest_error_stops_pipeline(self, project, db):
        """A failing ingest script should stop further ingest scripts."""
        # Add a second (failing) ingest script that runs first alphabetically
        (project / "ingest" / "aaa_fail.py").write_text(
            'raise RuntimeError("Connection failed")\n'
        )

        results = run_scripts_in_dir(db, project / "ingest", "ingest")
        # Should stop after the first error
        assert any(r["status"] == "error" for r in results)
        # The second script should NOT have run (ingest stops on error)
        assert len(results) == 1  # only aaa_fail.py ran

    def test_ingest_error_logged(self, project, db):
        """Failed ingest scripts should be recorded in run_log."""
        (project / "ingest" / "bad_script.py").write_text(
            'raise ValueError("bad data")\n'
        )

        run_scripts_in_dir(db, project / "ingest", "ingest")

        errors = db.execute(
            "SELECT COUNT(*) FROM _dp_internal.run_log "
            "WHERE run_type = 'ingest' AND status = 'error'"
        ).fetchone()[0]
        assert errors >= 1

    def test_export_error_does_not_stop_other_exports(self, project, db):
        """Export errors should NOT stop remaining exports."""
        # Set up data first
        run_seeds(db, project / "seeds", force=True)
        run_scripts_in_dir(db, project / "ingest", "ingest")
        run_transform(db, project / "transform", force=True)

        # Add a failing export
        (project / "export" / "aaa_fail_export.py").write_text(
            'raise RuntimeError("Export target down")\n'
        )

        results = run_scripts_in_dir(db, project / "export", "export")
        statuses = [r["status"] for r in results]
        # Both scripts ran (export doesn't stop on error, unlike ingest)
        assert len(results) == 2
        assert "error" in statuses
        assert "success" in statuses


# ---------------------------------------------------------------------------
# Transform assertion failure tests
# ---------------------------------------------------------------------------


class TestTransformAssertions:
    """Test transform assertion behavior in the pipeline."""

    def test_assertion_failure_recorded(self, project, db):
        """Failed assertions should be recorded but model still built."""
        run_seeds(db, project / "seeds", force=True)
        run_scripts_in_dir(db, project / "ingest", "ingest")

        # Override gold model with an assertion that will fail
        (project / "transform" / "gold" / "event_summary.sql").write_text(textwrap.dedent("""\
            -- config: materialized=table, schema=gold
            -- depends_on: silver.enriched_events
            -- assert: row_count > 100

            SELECT event_type, COUNT(*) AS event_count
            FROM silver.enriched_events
            GROUP BY event_type
        """))

        results = run_transform(db, project / "transform", force=True)

        # Model should be built but marked as assertion_failed
        assert results["gold.event_summary"] == "assertion_failed"

        # Data should still be there
        rows = db.execute("SELECT COUNT(*) FROM gold.event_summary").fetchone()[0]
        assert rows == 3

    def test_all_assertions_pass(self, project, db):
        """All assertions should pass in the default pipeline."""
        run_seeds(db, project / "seeds", force=True)
        run_scripts_in_dir(db, project / "ingest", "ingest")
        results = run_transform(db, project / "transform", force=True)

        # No assertion failures
        assert all(v in ("built", "skipped") for v in results.values()), \
            f"Unexpected results: {results}"


# ---------------------------------------------------------------------------
# Selective target tests
# ---------------------------------------------------------------------------


class TestSelectiveTargets:
    """Test running specific targets within each pipeline step."""

    def test_ingest_specific_target(self, project, db):
        """Running specific targets should only execute those scripts."""
        # Add a second ingest script
        (project / "ingest" / "load_extra.py").write_text(
            'db.execute("CREATE SCHEMA IF NOT EXISTS landing")\n'
            'db.execute("CREATE OR REPLACE TABLE landing.extra AS SELECT 1 AS x")\n'
            'print("Loaded 1 row")\n'
        )

        results = run_scripts_in_dir(
            db, project / "ingest", "ingest", targets=["load_events"]
        )
        assert len(results) == 1
        assert results[0]["script"] == "load_events.py"

    def test_transform_specific_target(self, project, db):
        """Running specific transform targets should only build those models."""
        # Set up data
        run_seeds(db, project / "seeds", force=True)
        run_scripts_in_dir(db, project / "ingest", "ingest")

        results = run_transform(
            db, project / "transform",
            targets=["bronze.events"],
            force=True,
        )
        assert "bronze.events" in results
        assert results["bronze.events"] == "built"
        # Other models should not appear
        assert "gold.event_summary" not in results


# ---------------------------------------------------------------------------
# Change detection tests
# ---------------------------------------------------------------------------


class TestChangeDetection:
    """Test that change detection works across the pipeline."""

    def test_transform_skips_unchanged(self, project, db):
        """Transform should skip models that haven't changed."""
        run_seeds(db, project / "seeds", force=True)
        run_scripts_in_dir(db, project / "ingest", "ingest")

        # First run: all built
        results1 = run_transform(db, project / "transform", force=False)
        assert results1["bronze.events"] == "built"

        # Second run: all skipped (nothing changed)
        results2 = run_transform(db, project / "transform", force=False)
        assert results2["bronze.events"] == "skipped"
        assert results2["silver.enriched_events"] == "skipped"
        assert results2["gold.event_summary"] == "skipped"

    def test_downstream_rebuilds_on_upstream_change(self, project, db):
        """Changing an upstream model should rebuild its direct dependents."""
        run_seeds(db, project / "seeds", force=True)
        run_scripts_in_dir(db, project / "ingest", "ingest")

        # First run
        run_transform(db, project / "transform", force=False)

        # Modify bronze model
        (project / "transform" / "bronze" / "events.sql").write_text(textwrap.dedent("""\
            -- config: materialized=table, schema=bronze
            -- depends_on: landing.events
            -- assert: row_count > 0

            SELECT id, UPPER(event_type) AS event_type, category_id,
                   CAST(event_date AS DATE) AS event_date
            FROM landing.events
        """))

        # Second run: bronze changed → silver rebuilds (direct dependent)
        # Gold's direct upstream (silver SQL) didn't change, so gold correctly skips
        results = run_transform(db, project / "transform", force=False)
        assert results["bronze.events"] == "built"
        assert results["silver.enriched_events"] == "built"

        # Verify the change propagated through bronze and silver
        event_type = db.execute(
            "SELECT event_type FROM silver.enriched_events WHERE id = 1"
        ).fetchone()[0]
        assert event_type == "LOGIN"  # UPPER applied by bronze, propagated to silver


# ---------------------------------------------------------------------------
# Notebook-based pipeline tests
# ---------------------------------------------------------------------------


class TestNotebookPipeline:
    """Test .dpnb notebooks as pipeline steps."""

    def test_notebook_ingest(self, project, db):
        """A .dpnb notebook should work as an ingest step."""
        # Remove the .py ingest and add a notebook
        (project / "ingest" / "load_events.py").unlink()

        notebook = {
            "title": "Test Ingest",
            "cells": [
                {
                    "id": "c1",
                    "type": "code",
                    "source": (
                        'db.execute("CREATE SCHEMA IF NOT EXISTS landing")\n'
                        'db.execute("CREATE OR REPLACE TABLE landing.events AS '
                        "SELECT * FROM (VALUES "
                        "(1, 'login', 1, '2024-01-01'), "
                        "(2, 'purchase', 2, '2024-01-02')"
                        ") AS t(id, event_type, category_id, event_date)"
                        '")\n'
                        'print("Loaded 2 events")'
                    ),
                    "outputs": [],
                },
            ],
        }
        (project / "ingest" / "load_events.dpnb").write_text(
            json.dumps(notebook, indent=2)
        )

        results = run_scripts_in_dir(db, project / "ingest", "ingest")
        assert len(results) == 1
        assert results[0]["status"] == "success"

        rows = db.execute("SELECT COUNT(*) FROM landing.events").fetchone()[0]
        assert rows == 2


# ---------------------------------------------------------------------------
# Stream execution via CLI
# ---------------------------------------------------------------------------


class TestStreamCLI:
    """Test stream execution through the Typer CLI runner."""

    def test_stream_full_refresh(self, project):
        """The full-refresh stream should complete without errors."""
        from typer.testing import CliRunner
        from dp.cli import app

        import os
        original_cwd = os.getcwd()
        try:
            os.chdir(project)
            runner = CliRunner()
            result = runner.invoke(app, ["stream", "full-refresh"])
            assert result.exit_code == 0, f"Stream failed: {result.output}"
            assert "Stream completed successfully" in result.output
        finally:
            os.chdir(original_cwd)

    def test_stream_nonexistent(self, project):
        """Running a nonexistent stream should exit with error."""
        from typer.testing import CliRunner
        from dp.cli import app

        import os
        original_cwd = os.getcwd()
        try:
            os.chdir(project)
            runner = CliRunner()
            result = runner.invoke(app, ["stream", "nonexistent"])
            assert result.exit_code != 0
            assert "not found" in result.output
        finally:
            os.chdir(original_cwd)

    def test_stream_with_force(self, project):
        """--force flag should rebuild all models."""
        from typer.testing import CliRunner
        from dp.cli import app

        import os
        original_cwd = os.getcwd()
        try:
            os.chdir(project)
            runner = CliRunner()
            # First run
            result1 = runner.invoke(app, ["stream", "full-refresh"])
            assert result1.exit_code == 0

            # Second run with force
            result2 = runner.invoke(app, ["stream", "full-refresh", "--force"])
            assert result2.exit_code == 0
            assert "Stream completed successfully" in result2.output
        finally:
            os.chdir(original_cwd)
