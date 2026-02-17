"""Scheduler: Huey + SqliteHuey for cron-based stream execution.

Runs streams on cron schedules defined in project.yml.
Also supports file-watching for auto-rebuild on SQL changes.
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path

from huey import SqliteHuey, crontab
from rich.console import Console

console = Console()
logger = logging.getLogger("dp.scheduler")

# Global huey instance — initialized by start_scheduler()
_huey: SqliteHuey | None = None
_project_dir: Path | None = None


def _get_huey(project_dir: Path) -> SqliteHuey:
    """Get or create the Huey instance backed by SQLite in the project dir."""
    global _huey
    if _huey is None:
        db_path = project_dir / ".dp_scheduler.db"
        _huey = SqliteHuey(filename=str(db_path), immediate=False)
    return _huey


def _run_stream_task(project_dir_str: str, stream_name: str) -> dict:
    """Execute a stream. Called by Huey as a task."""
    from dp.config import load_project
    from dp.engine.database import connect
    from dp.engine.runner import run_scripts_in_dir
    from dp.engine.transform import run_transform

    project_dir = Path(project_dir_str)
    config = load_project(project_dir)
    stream_config = config.streams.get(stream_name)
    if not stream_config:
        return {"error": f"Stream '{stream_name}' not found"}

    db_path = project_dir / config.database.path
    conn = connect(db_path)
    step_results = []

    try:
        for step in stream_config.steps:
            if step.action == "ingest":
                results = run_scripts_in_dir(conn, project_dir / "ingest", "ingest", step.targets)
                step_results.append({"action": "ingest", "results": [r["status"] for r in results]})
            elif step.action == "transform":
                targets = step.targets if step.targets != ["all"] else None
                results = run_transform(conn, project_dir / "transform", targets=targets)
                step_results.append({"action": "transform", "results": results})
            elif step.action == "export":
                results = run_scripts_in_dir(conn, project_dir / "export", "export", step.targets)
                step_results.append({"action": "export", "results": [r["status"] for r in results]})

        logger.info("Stream '%s' completed: %s", stream_name, step_results)
        return {"stream": stream_name, "status": "success", "steps": step_results}
    except Exception as e:
        logger.error("Stream '%s' failed: %s", stream_name, e)
        return {"stream": stream_name, "status": "error", "error": str(e)}
    finally:
        conn.close()


def _parse_cron(cron_expr: str) -> dict:
    """Parse a cron expression '* * * * *' into Huey crontab kwargs.

    Format: minute hour day month day_of_week
    """
    parts = cron_expr.strip().split()
    if len(parts) != 5:
        raise ValueError(f"Invalid cron expression: '{cron_expr}' (expected 5 fields)")

    kwargs = {}
    fields = ["minute", "hour", "day", "month", "day_of_week"]
    for field_name, value in zip(fields, parts):
        if value != "*":
            kwargs[field_name] = value
    return kwargs


def get_scheduled_streams(project_dir: Path) -> list[dict]:
    """Return info about all streams that have cron schedules."""
    from dp.config import load_project

    config = load_project(project_dir)
    scheduled = []
    for name, stream in config.streams.items():
        if stream.schedule:
            scheduled.append({
                "name": name,
                "description": stream.description,
                "schedule": stream.schedule,
                "steps": [{"action": s.action, "targets": s.targets} for s in stream.steps],
            })
    return scheduled


class SchedulerThread(threading.Thread):
    """Background thread that runs scheduled streams via a simple cron loop."""

    def __init__(self, project_dir: Path):
        super().__init__(daemon=True, name="dp-scheduler")
        self.project_dir = project_dir
        self._stop_event = threading.Event()
        self._schedules: list[dict] = []
        self._last_run: dict[str, float] = {}

    def stop(self) -> None:
        self._stop_event.set()

    def _should_run(self, name: str, cron_expr: str) -> bool:
        """Check if a cron expression matches the current minute."""
        import datetime

        now = datetime.datetime.now()
        parts = cron_expr.strip().split()
        if len(parts) != 5:
            return False

        checks = [
            (parts[0], now.minute),
            (parts[1], now.hour),
            (parts[2], now.day),
            (parts[3], now.month),
            (parts[4], now.weekday()),  # 0=Monday in Python
        ]

        for pattern, current in checks:
            if pattern == "*":
                continue
            if "/" in pattern:
                # */5 = every 5 units
                _, step = pattern.split("/", 1)
                if current % int(step) != 0:
                    return False
            elif "," in pattern:
                if current not in [int(v) for v in pattern.split(",")]:
                    return False
            elif "-" in pattern:
                lo, hi = pattern.split("-", 1)
                if not (int(lo) <= current <= int(hi)):
                    return False
            elif current != int(pattern):
                return False

        # Don't run more than once per minute
        last = self._last_run.get(name, 0)
        minute_key = now.replace(second=0, microsecond=0).timestamp()
        if last >= minute_key:
            return False

        return True

    def run(self) -> None:
        from dp.config import load_project

        logger.info("Scheduler started for %s", self.project_dir)

        while not self._stop_event.is_set():
            try:
                config = load_project(self.project_dir)
                for name, stream in config.streams.items():
                    if not stream.schedule:
                        continue
                    if self._should_run(name, stream.schedule):
                        import datetime

                        minute_key = datetime.datetime.now().replace(
                            second=0, microsecond=0
                        ).timestamp()
                        self._last_run[name] = minute_key
                        logger.info("Scheduler triggering stream: %s", name)
                        console.print(f"[bold blue]Scheduler:[/bold blue] Running stream '{name}'")
                        try:
                            _run_stream_task(str(self.project_dir), name)
                            console.print(f"[bold green]Scheduler:[/bold green] Stream '{name}' completed")
                        except Exception as e:
                            console.print(f"[bold red]Scheduler:[/bold red] Stream '{name}' failed: {e}")
            except Exception as e:
                logger.error("Scheduler error: %s", e)

            # Sleep until next check (poll every 30 seconds)
            self._stop_event.wait(30)

        logger.info("Scheduler stopped")


class FileWatcher(threading.Thread):
    """Watches transform/ and ingest/ for changes, triggers rebuilds."""

    def __init__(self, project_dir: Path, on_change: callable | None = None):
        super().__init__(daemon=True, name="dp-watcher")
        self.project_dir = project_dir
        self.on_change = on_change
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        from watchdog.events import FileSystemEventHandler
        from watchdog.observers import Observer

        project_dir = self.project_dir

        class Handler(FileSystemEventHandler):
            def __init__(self):
                self._debounce: dict[str, float] = {}

            def on_modified(self, event):
                if event.is_directory:
                    return
                path = Path(event.src_path)
                if path.suffix not in (".sql", ".py"):
                    return

                # Debounce: ignore events within 2 seconds of the last
                now = time.time()
                last = self._debounce.get(event.src_path, 0)
                if now - last < 2:
                    return
                self._debounce[event.src_path] = now

                rel = path.relative_to(project_dir)
                logger.info("File changed: %s", rel)
                console.print(f"[bold yellow]Watcher:[/bold yellow] {rel} changed")

                if str(rel).startswith("transform"):
                    console.print("[bold yellow]Watcher:[/bold yellow] Running transform...")
                    try:
                        from dp.config import load_project
                        from dp.engine.database import connect
                        from dp.engine.transform import run_transform

                        config = load_project(project_dir)
                        conn = connect(project_dir / config.database.path)
                        try:
                            run_transform(conn, project_dir / "transform")
                        finally:
                            conn.close()
                        console.print("[bold green]Watcher:[/bold green] Transform completed")
                    except Exception as e:
                        console.print(f"[bold red]Watcher:[/bold red] Transform failed: {e}")

        observer = Observer()
        handler = Handler()

        watch_dirs = [project_dir / "transform", project_dir / "ingest"]
        for d in watch_dirs:
            if d.exists():
                observer.schedule(handler, str(d), recursive=True)
                logger.info("Watching: %s", d)

        observer.start()
        console.print("[bold]File watcher started[/bold]")

        try:
            while not self._stop_event.is_set():
                self._stop_event.wait(1)
        finally:
            observer.stop()
            observer.join()
