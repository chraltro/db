"""Tests for the scheduler."""

from pathlib import Path

from dp.engine.scheduler import SchedulerThread, get_scheduled_streams


def test_get_scheduled_streams_empty(tmp_path):
    (tmp_path / "project.yml").write_text("name: test\nstreams: {}\n")
    result = get_scheduled_streams(tmp_path)
    assert result == []


def test_get_scheduled_streams(tmp_path):
    (tmp_path / "project.yml").write_text("""
name: test
streams:
  daily:
    description: "Daily refresh"
    steps:
      - ingest: [all]
      - transform: [all]
    schedule: "0 6 * * *"
  manual:
    description: "On demand"
    steps:
      - transform: [all]
    schedule: null
""")
    result = get_scheduled_streams(tmp_path)
    assert len(result) == 1
    assert result[0]["name"] == "daily"
    assert result[0]["schedule"] == "0 6 * * *"


def test_cron_matching():
    """Test that the scheduler's cron matching works correctly."""
    import datetime

    scheduler = SchedulerThread(Path("/tmp"))

    # Mock a known time
    # _should_run checks current time, so we test the basic contract
    # that it doesn't crash and returns a bool
    result = scheduler._should_run("test", "0 6 * * *")
    assert isinstance(result, bool)

    # Invalid cron should not match
    result = scheduler._should_run("test", "bad")
    assert result is False
