"""Schema Sentinel: upstream schema change detection, impact analysis, and fix suggestions."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb

logger = logging.getLogger("havn.sentinel")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class SentinelConfig:
    """Sentinel settings from project.yml."""

    enabled: bool = True
    on_change: str = "pause"  # 'pause', 'warn', 'continue'
    track_ordering: bool = False
    rename_inference: bool = True
    auto_fix: bool = False
    select_star_warning: bool = True


@dataclass
class ColumnInfo:
    """Schema information for a single column."""

    name: str
    type: str
    nullable: bool = True
    position: int = 0

    def to_dict(self) -> dict:
        return {"name": self.name, "type": self.type, "nullable": self.nullable, "position": self.position}


@dataclass
class SchemaChange:
    """A single schema change detected between two snapshots."""

    change_type: str  # column_added, column_removed, column_renamed, type_changed, etc.
    severity: str  # breaking, warning, info
    column_name: str
    old_value: str = ""
    new_value: str = ""
    rename_candidate: str | None = None

    def to_dict(self) -> dict:
        d = {
            "change_type": self.change_type,
            "severity": self.severity,
            "column_name": self.column_name,
            "old_value": self.old_value,
            "new_value": self.new_value,
        }
        if self.rename_candidate:
            d["rename_candidate"] = self.rename_candidate
        return d


@dataclass
class ImpactRecord:
    """Impact of a schema change on a downstream model."""

    model_name: str
    impact_type: str  # 'direct', 'transitive', 'safe'
    columns_affected: list[str] = field(default_factory=list)
    fix_suggestion: str = ""
    lines: list[int] = field(default_factory=list)


@dataclass
class SchemaDiff:
    """Full diff between two schema snapshots for a source."""

    diff_id: str
    run_id: str
    source_name: str
    prev_snapshot_id: str | None
    curr_snapshot_id: str
    changes: list[SchemaChange]
    has_breaking: bool = False


# ---------------------------------------------------------------------------
# Metadata database
# ---------------------------------------------------------------------------

_METADATA_DIR = ".dp/metadata"


def _sentinel_db_path(project_dir: Path) -> Path:
    return project_dir / _METADATA_DIR / "sentinel.duckdb"


def _ensure_sentinel_db(project_dir: Path) -> duckdb.DuckDBPyConnection:
    """Open (and create if needed) the sentinel metadata database."""
    meta_dir = project_dir / _METADATA_DIR
    meta_dir.mkdir(parents=True, exist_ok=True)

    db_path = _sentinel_db_path(project_dir)
    conn = duckdb.connect(str(db_path))

    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_snapshots (
            snapshot_id VARCHAR PRIMARY KEY,
            run_id      VARCHAR,
            source_name VARCHAR NOT NULL,
            columns     JSON NOT NULL,
            schema_hash VARCHAR NOT NULL,
            captured_at TIMESTAMP DEFAULT current_timestamp
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_diffs (
            diff_id          VARCHAR PRIMARY KEY,
            run_id           VARCHAR,
            source_name      VARCHAR NOT NULL,
            prev_snapshot_id VARCHAR,
            curr_snapshot_id VARCHAR NOT NULL,
            changes          JSON NOT NULL,
            created_at       TIMESTAMP DEFAULT current_timestamp
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS impact_records (
            diff_id          VARCHAR NOT NULL,
            model_name       VARCHAR NOT NULL,
            impact_type      VARCHAR NOT NULL,
            columns_affected VARCHAR[] DEFAULT [],
            fix_suggestion   TEXT DEFAULT '',
            fix_applied      BOOLEAN DEFAULT false,
            resolved_at      TIMESTAMP,
            PRIMARY KEY (diff_id, model_name)
        )
    """)
    return conn


def _close_db(conn: duckdb.DuckDBPyConnection) -> None:
    try:
        conn.close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Schema snapshot capture
# ---------------------------------------------------------------------------


def _hash_columns(columns: list[ColumnInfo]) -> str:
    """Hash column definitions for fast comparison."""
    data = "|".join(f"{c.name}:{c.type}:{c.nullable}" for c in columns)
    return hashlib.sha256(data.encode()).hexdigest()[:16]


def capture_source_schema(
    warehouse_conn: duckdb.DuckDBPyConnection,
    source_name: str,
) -> list[ColumnInfo]:
    """Capture the current schema of a source table from the warehouse.

    Args:
        warehouse_conn: DuckDB connection to the warehouse.
        source_name: Fully qualified name (schema.table).

    Returns:
        List of ColumnInfo for the source's columns.
    """
    parts = source_name.split(".")
    if len(parts) == 2:
        schema, name = parts
    else:
        schema, name = "main", source_name

    try:
        rows = warehouse_conn.execute(
            """
            SELECT column_name, data_type, is_nullable, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            [schema, name],
        ).fetchall()
    except Exception:
        return []

    return [
        ColumnInfo(
            name=r[0],
            type=r[1],
            nullable=r[2] == "YES",
            position=r[3],
        )
        for r in rows
    ]


def snapshot_source(
    project_dir: Path,
    warehouse_conn: duckdb.DuckDBPyConnection,
    source_name: str,
    run_id: str | None = None,
) -> str:
    """Capture and store a schema snapshot for a source.

    Returns the snapshot_id.
    """
    columns = capture_source_schema(warehouse_conn, source_name)
    if not columns:
        return ""

    schema_hash = _hash_columns(columns)
    snapshot_id = str(uuid.uuid4())
    columns_json = json.dumps([c.to_dict() for c in columns])

    meta_conn = _ensure_sentinel_db(project_dir)
    try:
        meta_conn.execute(
            """
            INSERT INTO schema_snapshots (snapshot_id, run_id, source_name, columns, schema_hash)
            VALUES (?, ?, ?, ?, ?)
            """,
            [snapshot_id, run_id, source_name, columns_json, schema_hash],
        )
    finally:
        _close_db(meta_conn)

    return snapshot_id


# ---------------------------------------------------------------------------
# Schema diff computation
# ---------------------------------------------------------------------------

# Type compatibility groups for rename inference and type widening/narrowing
_TYPE_GROUPS = {
    "INTEGER": "numeric", "BIGINT": "numeric", "SMALLINT": "numeric", "TINYINT": "numeric",
    "HUGEINT": "numeric", "INT": "numeric",
    "FLOAT": "float", "DOUBLE": "float", "REAL": "float", "DECIMAL": "float",
    "VARCHAR": "string", "TEXT": "string", "STRING": "string", "CHAR": "string",
    "BOOLEAN": "boolean", "BOOL": "boolean",
    "DATE": "temporal", "TIMESTAMP": "temporal", "TIMESTAMPTZ": "temporal",
    "TIMESTAMP WITH TIME ZONE": "temporal",
    "BLOB": "binary", "BYTEA": "binary",
}

_TYPE_WIDTH = {
    "TINYINT": 1, "SMALLINT": 2, "INTEGER": 3, "INT": 3, "BIGINT": 4, "HUGEINT": 5,
    "FLOAT": 10, "REAL": 10, "DOUBLE": 11, "DECIMAL": 12,
    "CHAR": 20, "VARCHAR": 21, "TEXT": 22, "STRING": 22,
}


def _type_compatible(t1: str, t2: str) -> bool:
    """Check if two types are in the same compatibility group."""
    g1 = _TYPE_GROUPS.get(t1.upper(), t1.upper())
    g2 = _TYPE_GROUPS.get(t2.upper(), t2.upper())
    return g1 == g2


def _classify_type_change(old_type: str, new_type: str) -> str:
    """Classify a type change as widened, narrowed, or just changed."""
    old_w = _TYPE_WIDTH.get(old_type.upper())
    new_w = _TYPE_WIDTH.get(new_type.upper())
    if old_w is not None and new_w is not None:
        if _type_compatible(old_type, new_type):
            if new_w > old_w:
                return "type_widened"
            elif new_w < old_w:
                return "type_narrowed"
    return "type_changed"


def compute_diff(
    prev_columns: list[ColumnInfo],
    curr_columns: list[ColumnInfo],
    config: SentinelConfig | None = None,
) -> list[SchemaChange]:
    """Compute the schema diff between two column lists.

    Handles: column_added, column_removed, column_renamed (inferred),
    type_changed, type_widened, type_narrowed, nullable_changed, order_changed.
    """
    if config is None:
        config = SentinelConfig()

    prev_by_name = {c.name.lower(): c for c in prev_columns}
    curr_by_name = {c.name.lower(): c for c in curr_columns}

    prev_names = set(prev_by_name.keys())
    curr_names = set(curr_by_name.keys())

    added = curr_names - prev_names
    removed = prev_names - curr_names
    common = prev_names & curr_names

    changes: list[SchemaChange] = []

    # Rename inference: match removed → added by type + position
    rename_pairs: dict[str, str] = {}  # old_name → new_name
    if config.rename_inference and added and removed:
        for old_name in list(removed):
            old_col = prev_by_name[old_name]
            best_match = None
            best_score = -1

            for new_name in list(added):
                new_col = curr_by_name[new_name]
                if not _type_compatible(old_col.type, new_col.type):
                    continue
                # Position proximity score (within ±2)
                pos_diff = abs(old_col.position - new_col.position)
                if pos_diff > 2:
                    continue
                score = 10 - pos_diff
                if old_col.type.upper() == new_col.type.upper():
                    score += 5  # Exact type match bonus
                if score > best_score:
                    best_score = score
                    best_match = new_name

            if best_match is not None:
                rename_pairs[old_name] = best_match
                changes.append(SchemaChange(
                    change_type="column_renamed",
                    severity="breaking",
                    column_name=old_name,
                    old_value=old_name,
                    new_value=best_match,
                    rename_candidate=best_match,
                ))
                removed.discard(old_name)
                added.discard(best_match)

    # Remaining removals
    for name in sorted(removed):
        changes.append(SchemaChange(
            change_type="column_removed",
            severity="breaking",
            column_name=name,
            old_value=prev_by_name[name].type,
        ))

    # Remaining additions
    for name in sorted(added):
        changes.append(SchemaChange(
            change_type="column_added",
            severity="info",
            column_name=name,
            new_value=curr_by_name[name].type,
        ))

    # Type changes in common columns
    for name in sorted(common):
        old_col = prev_by_name[name]
        new_col = curr_by_name[name]

        if old_col.type.upper() != new_col.type.upper():
            change_type = _classify_type_change(old_col.type, new_col.type)
            severity = {
                "type_widened": "info",
                "type_narrowed": "breaking",
                "type_changed": "warning",
            }[change_type]
            changes.append(SchemaChange(
                change_type=change_type,
                severity=severity,
                column_name=name,
                old_value=old_col.type,
                new_value=new_col.type,
            ))

        if old_col.nullable != new_col.nullable:
            changes.append(SchemaChange(
                change_type="nullable_changed",
                severity="warning",
                column_name=name,
                old_value="NULLABLE" if old_col.nullable else "NOT NULL",
                new_value="NULLABLE" if new_col.nullable else "NOT NULL",
            ))

    # Order changes
    if config.track_ordering:
        for name in sorted(common):
            old_col = prev_by_name[name]
            new_col = curr_by_name[name]
            if old_col.position != new_col.position:
                changes.append(SchemaChange(
                    change_type="order_changed",
                    severity="info",
                    column_name=name,
                    old_value=str(old_col.position),
                    new_value=str(new_col.position),
                ))

    return changes


# ---------------------------------------------------------------------------
# Impact analysis (Level 2: sqlglot AST)
# ---------------------------------------------------------------------------


def _extract_column_refs_from_sql(sql: str, source_name: str) -> tuple[set[str], bool, list[int]]:
    """Extract column references from SQL that come from a specific source.

    Returns:
        (set of column names referenced, uses_select_star, line numbers with refs)
    """
    from havn.engine.sql_analysis import strip_config_comments

    query = strip_config_comments(sql)

    # Check for SELECT *
    uses_star = bool(re.search(r'\bSELECT\s+\*', query, re.IGNORECASE))

    # Try sqlglot AST parsing
    try:
        import sqlglot
        from sqlglot import exp

        parsed = sqlglot.parse_one(query, read="duckdb")

        # Build alias map
        alias_map: dict[str, str] = {}
        for table in parsed.find_all(exp.Table):
            db = (table.db or "").lower()
            name = (table.name or "").lower()
            alias = (table.alias or "").lower()
            if db and name:
                fqn = f"{db}.{name}"
                if alias:
                    alias_map[alias] = fqn
                alias_map[name] = fqn

        # Find source aliases
        source_lower = source_name.lower()
        source_aliases = set()
        for alias, fqn in alias_map.items():
            if fqn == source_lower:
                source_aliases.add(alias)
        parts = source_lower.split(".")
        if len(parts) == 2:
            source_aliases.add(parts[1])

        # Extract column references from source
        col_refs: set[str] = set()
        for col in parsed.find_all(exp.Column):
            col_name = (col.name or "").lower()
            table_ref = (col.table or "").lower()

            if table_ref and table_ref in source_aliases:
                col_refs.add(col_name)
            elif not table_ref and col_name:
                # Unqualified column - could be from any source
                col_refs.add(col_name)

        # Find line numbers (approximate via regex)
        lines = set()
        for col_name in col_refs:
            for i, line in enumerate(sql.split("\n"), 1):
                if re.search(r'\b' + re.escape(col_name) + r'\b', line, re.IGNORECASE):
                    lines.add(i)

        return col_refs, uses_star, sorted(lines)

    except Exception:
        # Fallback to regex
        return _regex_column_refs(sql, source_name)


def _regex_column_refs(sql: str, source_name: str) -> tuple[set[str], bool, list[int]]:
    """Regex fallback for column reference extraction."""
    uses_star = bool(re.search(r'\bSELECT\s+\*', sql, re.IGNORECASE))
    # Simple word extraction - find all identifiers used in the SQL
    words = set(re.findall(r'\b([a-zA-Z_]\w*)\b', sql.lower()))
    # Filter out SQL keywords
    sql_kw = {
        "select", "from", "where", "and", "or", "not", "in", "is", "null",
        "as", "on", "join", "left", "right", "inner", "outer", "full", "cross",
        "group", "by", "order", "having", "limit", "offset", "union", "all",
        "insert", "update", "delete", "create", "drop", "alter", "table",
        "view", "schema", "index", "into", "values", "set", "case", "when",
        "then", "else", "end", "between", "like", "exists", "distinct",
        "count", "sum", "avg", "min", "max", "cast", "coalesce", "true",
        "false", "asc", "desc", "with", "recursive", "over", "partition",
        "row_number", "rank", "dense_rank", "int", "integer", "varchar",
        "text", "boolean", "float", "double", "date", "timestamp", "bigint",
        "config", "materialized", "depends_on", "assert",
    }
    col_refs = words - sql_kw
    lines = []
    for col in col_refs:
        for i, line in enumerate(sql.split("\n"), 1):
            if re.search(r'\b' + re.escape(col) + r'\b', line, re.IGNORECASE):
                lines.append(i)
                break
    return col_refs, uses_star, sorted(set(lines))


def analyze_impact(
    project_dir: Path,
    source_name: str,
    changes: list[SchemaChange],
    warehouse_conn: duckdb.DuckDBPyConnection | None = None,
    config: SentinelConfig | None = None,
) -> list[ImpactRecord]:
    """Analyze the impact of schema changes on downstream models.

    Uses sqlglot AST parsing (Level 2) to resolve column references.
    """
    if config is None:
        config = SentinelConfig()

    from havn.engine.transform import build_dag, discover_models

    transform_dir = project_dir / "transform"
    if not transform_dir.exists():
        return []

    models = discover_models(transform_dir)
    if not models:
        return []

    # Changed column names
    changed_cols = set()
    for ch in changes:
        changed_cols.add(ch.column_name.lower())
        if ch.rename_candidate:
            changed_cols.add(ch.rename_candidate.lower())

    # Find all models that depend on this source (directly or transitively)
    # Build dependency graph
    dep_graph: dict[str, set[str]] = {}  # model -> set of dependencies
    for m in models:
        dep_graph[m.full_name] = set(d.lower() for d in m.depends_on)

    # Direct dependents
    direct_deps = set()
    for m in models:
        if source_name.lower() in dep_graph.get(m.full_name, set()):
            direct_deps.add(m.full_name)

    # Transitive dependents (BFS)
    transitive_deps = set()
    queue = list(direct_deps)
    visited = set(direct_deps)
    while queue:
        current = queue.pop(0)
        for m in models:
            if current in dep_graph.get(m.full_name, set()) and m.full_name not in visited:
                visited.add(m.full_name)
                transitive_deps.add(m.full_name)
                queue.append(m.full_name)

    impacts: list[ImpactRecord] = []
    model_map = {m.full_name: m for m in models}

    for model_name in sorted(direct_deps | transitive_deps):
        model = model_map.get(model_name)
        if not model:
            continue

        if model_name in direct_deps:
            # Analyze which columns are referenced
            col_refs, uses_star, lines = _extract_column_refs_from_sql(model.sql, source_name)
            affected_cols = sorted(col_refs & changed_cols)

            if affected_cols or uses_star:
                impact_type = "direct"
                fix = _generate_fix(model_name, str(model.path), changes, affected_cols, uses_star, source_name, lines)
            else:
                impact_type = "safe"
                fix = ""

            impacts.append(ImpactRecord(
                model_name=model_name,
                impact_type=impact_type,
                columns_affected=affected_cols if affected_cols else (["*"] if uses_star else []),
                fix_suggestion=fix,
                lines=lines,
            ))
        else:
            # Transitive: depends on a directly impacted model
            impacts.append(ImpactRecord(
                model_name=model_name,
                impact_type="transitive",
                columns_affected=[],
                fix_suggestion="Re-run after upstream models are fixed.",
            ))

    return impacts


# ---------------------------------------------------------------------------
# Fix suggestion generation
# ---------------------------------------------------------------------------


def _generate_fix(
    model_name: str,
    model_path: str,
    changes: list[SchemaChange],
    affected_cols: list[str],
    uses_star: bool,
    source_name: str,
    lines: list[int],
) -> str:
    """Generate a fix suggestion for an impacted model."""
    suggestions = []

    for ch in changes:
        col_lower = ch.column_name.lower()
        if col_lower not in affected_cols and not uses_star:
            continue

        line_ref = f" (lines {', '.join(str(l) for l in lines)})" if lines else ""

        if ch.change_type == "column_removed":
            if ch.rename_candidate:
                suggestions.append(
                    f"Column `{ch.column_name}` may have been renamed to `{ch.rename_candidate}`. "
                    f"Update references in `{model_name}`{line_ref}."
                )
            else:
                suggestions.append(
                    f"Remove reference to `{ch.column_name}` in `{model_name}`{line_ref}."
                )

        elif ch.change_type == "column_renamed":
            suggestions.append(
                f"Update `{ch.old_value}` to `{ch.new_value}` in `{model_name}`{line_ref}."
            )

        elif ch.change_type == "type_changed":
            suggestions.append(
                f"Column `{ch.column_name}` changed from `{ch.old_value}` to `{ch.new_value}`. "
                f"Add explicit `CAST({ch.column_name} AS {ch.old_value})` in `{model_name}`{line_ref} "
                f"if downstream logic depends on the original type."
            )

        elif ch.change_type == "type_narrowed":
            suggestions.append(
                f"Column `{ch.column_name}` narrowed from `{ch.old_value}` to `{ch.new_value}`. "
                f"Add `TRY_CAST({ch.column_name} AS {ch.new_value})` with fallback in `{model_name}`{line_ref}."
            )

        elif ch.change_type == "nullable_changed" and ch.new_value == "NULLABLE":
            suggestions.append(
                f"Column `{ch.column_name}` is now nullable. "
                f"Add `COALESCE({ch.column_name}, <default>)` in `{model_name}`{line_ref}."
            )

    if uses_star:
        suggestions.append(
            f"Model `{model_name}` uses `SELECT *` from `{source_name}`. "
            f"Pin to explicit columns to avoid breakage from future schema changes."
        )

    return " ".join(suggestions)


# ---------------------------------------------------------------------------
# Run sentinel check (main entry point)
# ---------------------------------------------------------------------------


def run_sentinel_check(
    project_dir: Path,
    warehouse_conn: duckdb.DuckDBPyConnection,
    source_names: list[str],
    run_id: str | None = None,
    config: SentinelConfig | None = None,
) -> list[SchemaDiff]:
    """Run schema sentinel check on all specified sources.

    For each source:
    1. Capture current schema
    2. Compare against previous snapshot
    3. If changed, compute diff and impact analysis

    Returns list of SchemaDiffs (one per source with changes).
    """
    if config is None:
        config = SentinelConfig()

    if not config.enabled:
        return []

    diffs: list[SchemaDiff] = []
    meta_conn = _ensure_sentinel_db(project_dir)

    try:
        for source_name in source_names:
            # 1. Capture current schema
            curr_columns = capture_source_schema(warehouse_conn, source_name)
            if not curr_columns:
                continue

            curr_hash = _hash_columns(curr_columns)

            # 2. Get previous snapshot
            prev_row = meta_conn.execute(
                """
                SELECT snapshot_id, columns, schema_hash
                FROM schema_snapshots
                WHERE source_name = ?
                ORDER BY captured_at DESC LIMIT 1
                """,
                [source_name],
            ).fetchone()

            # Store current snapshot
            curr_snapshot_id = str(uuid.uuid4())
            curr_columns_json = json.dumps([c.to_dict() for c in curr_columns])
            meta_conn.execute(
                "INSERT INTO schema_snapshots (snapshot_id, run_id, source_name, columns, schema_hash) "
                "VALUES (?, ?, ?, ?, ?)",
                [curr_snapshot_id, run_id, source_name, curr_columns_json, curr_hash],
            )

            if not prev_row:
                # First snapshot - no diff to compute
                continue

            prev_snapshot_id, prev_columns_json, prev_hash = prev_row

            # 3. Fast comparison
            if prev_hash == curr_hash:
                continue

            # 4. Compute detailed diff
            prev_columns = [
                ColumnInfo(**c) for c in json.loads(prev_columns_json)
            ]
            changes = compute_diff(prev_columns, curr_columns, config)

            if not changes:
                continue

            # 5. Store diff
            diff_id = str(uuid.uuid4())
            changes_json = json.dumps([c.to_dict() for c in changes])
            meta_conn.execute(
                "INSERT INTO schema_diffs (diff_id, run_id, source_name, prev_snapshot_id, curr_snapshot_id, changes) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [diff_id, run_id, source_name, prev_snapshot_id, curr_snapshot_id, changes_json],
            )

            has_breaking = any(c.severity == "breaking" for c in changes)

            # 6. Impact analysis
            impacts = analyze_impact(project_dir, source_name, changes, warehouse_conn, config)

            # Store impact records
            for impact in impacts:
                meta_conn.execute(
                    """
                    INSERT INTO impact_records (diff_id, model_name, impact_type, columns_affected, fix_suggestion)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [diff_id, impact.model_name, impact.impact_type,
                     impact.columns_affected, impact.fix_suggestion],
                )

            diffs.append(SchemaDiff(
                diff_id=diff_id,
                run_id=run_id or "",
                source_name=source_name,
                prev_snapshot_id=prev_snapshot_id,
                curr_snapshot_id=curr_snapshot_id,
                changes=changes,
                has_breaking=has_breaking,
            ))

    finally:
        _close_db(meta_conn)

    return diffs


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


def get_source_names_from_models(project_dir: Path) -> list[str]:
    """Extract all source table names referenced by models in the DAG."""
    from havn.engine.transform import discover_models

    transform_dir = project_dir / "transform"
    if not transform_dir.exists():
        return []

    models = discover_models(transform_dir)
    model_names = {m.full_name for m in models}

    sources = set()
    for m in models:
        for dep in m.depends_on:
            if dep not in model_names:
                sources.add(dep)

    return sorted(sources)


def get_recent_diffs(
    project_dir: Path,
    limit: int = 50,
) -> list[dict]:
    """Get recent schema diffs with their changes."""
    meta_conn = _ensure_sentinel_db(project_dir)
    try:
        rows = meta_conn.execute(
            """
            SELECT diff_id, run_id, source_name, prev_snapshot_id, curr_snapshot_id,
                   changes, created_at
            FROM schema_diffs
            ORDER BY created_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        return [
            {
                "diff_id": r[0],
                "run_id": r[1],
                "source_name": r[2],
                "prev_snapshot_id": r[3],
                "curr_snapshot_id": r[4],
                "changes": json.loads(r[5]) if isinstance(r[5], str) else r[5],
                "created_at": str(r[6]) if r[6] else "",
            }
            for r in rows
        ]
    finally:
        _close_db(meta_conn)


def get_impacts_for_diff(
    project_dir: Path,
    diff_id: str,
) -> list[dict]:
    """Get impact records for a specific diff."""
    meta_conn = _ensure_sentinel_db(project_dir)
    try:
        rows = meta_conn.execute(
            """
            SELECT diff_id, model_name, impact_type, columns_affected,
                   fix_suggestion, fix_applied, resolved_at
            FROM impact_records
            WHERE diff_id = ?
            ORDER BY impact_type, model_name
            """,
            [diff_id],
        ).fetchall()
        return [
            {
                "diff_id": r[0],
                "model_name": r[1],
                "impact_type": r[2],
                "columns_affected": r[3] if r[3] else [],
                "fix_suggestion": r[4] or "",
                "fix_applied": r[5],
                "resolved_at": str(r[6]) if r[6] else None,
            }
            for r in rows
        ]
    finally:
        _close_db(meta_conn)


def get_schema_history(
    project_dir: Path,
    source_name: str,
    limit: int = 20,
) -> list[dict]:
    """Get schema snapshot history for a source."""
    meta_conn = _ensure_sentinel_db(project_dir)
    try:
        rows = meta_conn.execute(
            """
            SELECT snapshot_id, run_id, columns, schema_hash, captured_at
            FROM schema_snapshots
            WHERE source_name = ?
            ORDER BY captured_at DESC
            LIMIT ?
            """,
            [source_name, limit],
        ).fetchall()
        return [
            {
                "snapshot_id": r[0],
                "run_id": r[1],
                "columns": json.loads(r[2]) if isinstance(r[2], str) else r[2],
                "schema_hash": r[3],
                "captured_at": str(r[4]) if r[4] else "",
            }
            for r in rows
        ]
    finally:
        _close_db(meta_conn)


def resolve_impact(
    project_dir: Path,
    diff_id: str,
    model_name: str,
) -> bool:
    """Mark an impact record as resolved."""
    meta_conn = _ensure_sentinel_db(project_dir)
    try:
        meta_conn.execute(
            "UPDATE impact_records SET resolved_at = current_timestamp WHERE diff_id = ? AND model_name = ?",
            [diff_id, model_name],
        )
        return True
    except Exception:
        return False
    finally:
        _close_db(meta_conn)


def apply_rename_fix(
    project_dir: Path,
    model_path: str,
    old_name: str,
    new_name: str,
) -> dict:
    """Apply a rename fix to a model SQL file.

    Returns {"status": "success"|"error", "message": str}.
    """
    full_path = project_dir / model_path
    if not full_path.exists():
        return {"status": "error", "message": f"File not found: {model_path}"}

    content = full_path.read_text()
    # Word-boundary replacement to avoid partial matches
    pattern = re.compile(r'\b' + re.escape(old_name) + r'\b', re.IGNORECASE)
    new_content = pattern.sub(new_name, content)

    if new_content == content:
        return {"status": "error", "message": f"No occurrences of `{old_name}` found in {model_path}"}

    count = len(pattern.findall(content))
    full_path.write_text(new_content)

    return {
        "status": "success",
        "message": f"Replaced {count} occurrence(s) of `{old_name}` with `{new_name}` in {model_path}",
    }
