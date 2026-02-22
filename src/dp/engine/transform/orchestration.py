"""Pipeline orchestration: sequential and parallel transform runners."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
from rich.console import Console

from dp.engine.database import ensure_meta_table, log_run

from .discovery import (
    _compute_upstream_hash,
    _has_changed,
    _update_state,
    build_dag,
    build_dag_tiers,
    discover_models,
)
from .execution import _execute_single_model, execute_model
from .models import SQLModel
from .quality import (
    _save_assertions,
    _save_profile,
    profile_model,
    run_assertions,
)

console = Console()
logger = logging.getLogger("dp.transform")


def run_transform(
    conn: duckdb.DuckDBPyConnection,
    transform_dir: Path,
    targets: list[str] | None = None,
    force: bool = False,
    parallel: bool = False,
    max_workers: int = 4,
    db_path: str | None = None,
) -> dict[str, str]:
    """Run the full transformation pipeline.

    Args:
        conn: DuckDB connection
        transform_dir: Path to transform/ directory
        targets: Specific models to run (None = all)
        force: Force rebuild even if unchanged
        parallel: Enable parallel execution of independent models
        max_workers: Max number of parallel workers
        db_path: Explicit database path (required for parallel mode)

    Returns:
        Dict of model_name -> status ("built", "skipped", "error")
    """
    ensure_meta_table(conn)
    models = discover_models(transform_dir)

    if not models:
        console.print("[yellow]No SQL models found in transform/[/yellow]")
        return {}

    # Filter to targets if specified
    if targets and targets != ["all"]:
        target_set = set(targets)
        models = [m for m in models if m.full_name in target_set or m.name in target_set]
        if not models:
            all_names = [m.full_name for m in discover_models(transform_dir)]
            console.print(f"[yellow]No models matched targets: {', '.join(targets)}[/yellow]")
            if all_names:
                console.print(f"[dim]Available models: {', '.join(all_names)}[/dim]")
            return {}

    if parallel:
        return _run_transform_parallel(conn, models, force, max_workers, db_path=db_path)
    return _run_transform_sequential(conn, models, force)


def _run_transform_sequential(
    conn: duckdb.DuckDBPyConnection,
    models: list[SQLModel],
    force: bool,
) -> dict[str, str]:
    """Run models sequentially (original behavior + assertions + profiling)."""
    ordered = build_dag(models)
    model_map = {m.full_name: m for m in ordered}

    # Compute upstream hashes
    for model in ordered:
        model.upstream_hash = _compute_upstream_hash(model, model_map)

    results: dict[str, str] = {}

    for model in ordered:
        changed = force or _has_changed(conn, model)
        label = f"[bold]{model.full_name}[/bold] ({model.materialized})"

        if not changed:
            console.print(f"  [dim]skip[/dim]  {label}")
            results[model.full_name] = "skipped"
            continue

        try:
            duration_ms, row_count = execute_model(conn, model)
            _update_state(conn, model, duration_ms, row_count)
            log_run(conn, "transform", model.full_name, "success", duration_ms, row_count)

            suffix = f" ({row_count:,} rows, {duration_ms}ms)" if row_count else f" ({duration_ms}ms)"
            console.print(f"  [green]done[/green]  {label}{suffix}")

            # Run data quality assertions
            if model.assertions:
                assertion_results = run_assertions(conn, model)
                _save_assertions(conn, model, assertion_results)
                for ar in assertion_results:
                    if ar.passed:
                        console.print(f"         [green]pass[/green]  assert: {ar.expression}")
                    else:
                        console.print(f"         [red]FAIL[/red]  assert: {ar.expression} ({ar.detail})")

                failed = [ar for ar in assertion_results if not ar.passed]
                if failed:
                    results[model.full_name] = "assertion_failed"
                    continue

            # Auto-profile for tables
            if model.materialized in ("table", "incremental"):
                profile = profile_model(conn, model)
                _save_profile(conn, model, profile)
                null_alerts = [
                    col for col, pct in profile.null_percentages.items()
                    if pct > 50.0
                ]
                if null_alerts:
                    console.print(
                        f"         [yellow]warn[/yellow]  high nulls: "
                        f"{', '.join(f'{c}({profile.null_percentages[c]}%)' for c in null_alerts)}"
                    )

            results[model.full_name] = "built"

        except Exception as e:
            log_run(conn, "transform", model.full_name, "error", error=str(e))
            console.print(f"  [red]fail[/red]  {label}: {e}")
            results[model.full_name] = "error"

    return results


def _run_transform_parallel(
    conn: duckdb.DuckDBPyConnection,
    models: list[SQLModel],
    force: bool,
    max_workers: int,
    db_path: str | None = None,
) -> dict[str, str]:
    """Run models in parallel by DAG tiers.

    Models within the same tier are independent and can execute concurrently.
    Each tier must complete before the next one starts.
    Assertion failures in a tier block the next tier.
    """
    tiers = build_dag_tiers(models)
    model_map = {m.full_name: m for m in models}

    # Compute upstream hashes
    ordered = build_dag(models)
    for model in ordered:
        model.upstream_hash = _compute_upstream_hash(model, model_map)

    # Resolve database path explicitly
    db_path_str = db_path
    if not db_path_str:
        # Fall back to extracting from connection
        try:
            result = conn.execute("SELECT current_setting('duckdb_database_file')").fetchone()
            db_path_str = result[0] if result and result[0] else None
        except Exception as e:
            logger.debug("Could not extract db path from connection: %s", e)
    if not db_path_str:
        console.print("[yellow]Cannot determine database path, falling back to sequential[/yellow]")
        return _run_transform_sequential(conn, models, force)

    results: dict[str, str] = {}
    total_tiers = len(tiers)

    for tier_idx, tier in enumerate(tiers, 1):
        # Check if any previous tier had failures that should block this tier
        has_blocking_failure = any(
            s in ("error", "assertion_failed") for s in results.values()
        )
        if has_blocking_failure:
            for model in tier:
                console.print(f"  [dim]skip[/dim]  [bold]{model.full_name}[/bold] (upstream failure)")
                results[model.full_name] = "skipped"
            continue

        if len(tier) > 1:
            console.print(f"  [dim]tier {tier_idx}/{total_tiers}[/dim] ({len(tier)} models in parallel)")

        if len(tier) == 1:
            # Single model — run in the main connection
            model = tier[0]
            changed = force or _has_changed(conn, model)
            label = f"[bold]{model.full_name}[/bold] ({model.materialized})"

            if not changed:
                console.print(f"  [dim]skip[/dim]  {label}")
                results[model.full_name] = "skipped"
                continue

            try:
                duration_ms, row_count = execute_model(conn, model)
                _update_state(conn, model, duration_ms, row_count)
                log_run(conn, "transform", model.full_name, "success", duration_ms, row_count)

                # Assertions
                if model.assertions:
                    ar_results = run_assertions(conn, model)
                    _save_assertions(conn, model, ar_results)
                    failed = [ar for ar in ar_results if not ar.passed]
                    if failed:
                        for ar in failed:
                            console.print(f"         [red]FAIL[/red]  assert: {ar.expression} ({ar.detail})")
                        results[model.full_name] = "assertion_failed"
                        continue

                # Profile
                if model.materialized in ("table", "incremental"):
                    profile = profile_model(conn, model)
                    _save_profile(conn, model, profile)

                suffix = f" ({row_count:,} rows, {duration_ms}ms)" if row_count else f" ({duration_ms}ms)"
                console.print(f"  [green]done[/green]  {label}{suffix}")
                results[model.full_name] = "built"

            except Exception as e:
                log_run(conn, "transform", model.full_name, "error", error=str(e))
                console.print(f"  [red]fail[/red]  {label}: {e}")
                results[model.full_name] = "error"
        else:
            # Multiple models — run in parallel with separate connections
            # Collect ALL results from all futures before reporting
            tier_results: list[tuple[str, ModelResult]] = []
            with ThreadPoolExecutor(max_workers=min(max_workers, len(tier))) as executor:
                futures = {
                    executor.submit(
                        _execute_single_model, db_path_str, model, force, model_map
                    ): model
                    for model in tier
                }
                for future in as_completed(futures):
                    tier_results.append(future.result())

            # Report all results from this tier
            for model_name, model_result in tier_results:
                label = f"[bold]{model_name}[/bold]"
                if model_result.status == "skipped":
                    console.print(f"  [dim]skip[/dim]  {label}")
                elif model_result.status == "built":
                    suffix = ""
                    if model_result.row_count:
                        suffix = f" ({model_result.row_count:,} rows, {model_result.duration_ms}ms)"
                    else:
                        suffix = f" ({model_result.duration_ms}ms)"
                    console.print(f"  [green]done[/green]  {label}{suffix}")
                elif model_result.status == "assertion_failed":
                    console.print(f"  [red]FAIL[/red]  {label}: assertion(s) failed")
                else:
                    console.print(f"  [red]fail[/red]  {label}: {model_result.error}")

                results[model_name] = model_result.status

    return results
