"""DuckDB connection management."""

from __future__ import annotations

from pathlib import Path

import duckdb


def connect(db_path: str | Path, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection to the given path."""
    db_path = str(db_path)
    conn = duckdb.connect(db_path, read_only=read_only)
    # Enable progress bar for long-running queries
    conn.execute("SET enable_progress_bar = true")
    return conn


def ensure_schemas(conn: duckdb.DuckDBPyConnection, schemas: list[str]) -> None:
    """Create schemas if they don't exist."""
    for schema in schemas:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def ensure_meta_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the internal metadata tables for change tracking, profiling, and assertions."""
    conn.execute("""
        CREATE SCHEMA IF NOT EXISTS _dp_internal
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _dp_internal.model_state (
            model_path   VARCHAR PRIMARY KEY,
            content_hash VARCHAR NOT NULL,
            upstream_hash VARCHAR NOT NULL,
            materialized_as VARCHAR NOT NULL,
            last_run_at  TIMESTAMP DEFAULT current_timestamp,
            run_duration_ms BIGINT DEFAULT 0,
            row_count    BIGINT DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _dp_internal.run_log (
            run_id       VARCHAR DEFAULT gen_random_uuid()::VARCHAR,
            run_type     VARCHAR NOT NULL,
            target       VARCHAR NOT NULL,
            status       VARCHAR NOT NULL,
            started_at   TIMESTAMP DEFAULT current_timestamp,
            finished_at  TIMESTAMP,
            duration_ms  BIGINT,
            rows_affected BIGINT DEFAULT 0,
            error        VARCHAR,
            log_output   VARCHAR
        )
    """)
    # Model profiling stats (auto-computed after each build)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _dp_internal.model_profiles (
            model_path       VARCHAR PRIMARY KEY,
            row_count        BIGINT DEFAULT 0,
            column_count     INTEGER DEFAULT 0,
            null_percentages JSON,
            distinct_counts  JSON,
            profiled_at      TIMESTAMP DEFAULT current_timestamp
        )
    """)
    # Data quality assertion results
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _dp_internal.assertion_results (
            id          VARCHAR DEFAULT gen_random_uuid()::VARCHAR,
            model_path  VARCHAR NOT NULL,
            expression  VARCHAR NOT NULL,
            passed      BOOLEAN NOT NULL,
            detail      VARCHAR,
            checked_at  TIMESTAMP DEFAULT current_timestamp
        )
    """)
    # Alert/notification log
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _dp_internal.alert_log (
            id          VARCHAR DEFAULT gen_random_uuid()::VARCHAR,
            alert_type  VARCHAR NOT NULL,
            channel     VARCHAR NOT NULL,
            target      VARCHAR,
            message     VARCHAR NOT NULL,
            status      VARCHAR NOT NULL,
            sent_at     TIMESTAMP DEFAULT current_timestamp,
            error       VARCHAR
        )
    """)


def log_run(
    conn: duckdb.DuckDBPyConnection,
    run_type: str,
    target: str,
    status: str,
    duration_ms: int = 0,
    rows_affected: int = 0,
    error: str | None = None,
    log_output: str | None = None,
) -> None:
    """Insert a run log entry."""
    conn.execute(
        """
        INSERT INTO _dp_internal.run_log
            (run_type, target, status, duration_ms, rows_affected, error, log_output)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [run_type, target, status, duration_ms, rows_affected, error, log_output],
    )
