import React, { useState, useEffect, useRef } from "react";
import { api } from "./api";
import SortableTable from "./SortableTable";
import { useHintTriggerFn } from "./HintSystem";

export default function TablesPanel({ selectedTable, onQueryTable }) {
  const [columns, setColumns] = useState([]);
  const [preview, setPreview] = useState(null);
  const [loading, setLoading] = useState(false);
  const [rowCount, setRowCount] = useState(null);
  const [sortCol, setSortCol] = useState(null);
  const [sortDir, setSortDir] = useState("ASC");
  const [stats, setStats] = useState(null);
  const [statsLoading, setStatsLoading] = useState(false);
  const setHintTrigger = useHintTriggerFn();
  const hasTriggeredRef = useRef(false);

  useEffect(() => {
    if (selectedTable && !hasTriggeredRef.current) {
      hasTriggeredRef.current = true;
      setHintTrigger("firstTableSelected", true);
    }
  }, [selectedTable]);

  useEffect(() => {
    if (!selectedTable) {
      setColumns([]);
      setPreview(null);
      setRowCount(null);
      setSortCol(null);
      setStats(null);
      return;
    }
    const [schema, name] = selectedTable.split(".");
    setLoading(true);
    setRowCount(null);
    setSortCol(null);
    setStats(null);
    Promise.all([
      api.describeTable(schema, name),
      api.runQuery(`SELECT * FROM ${schema}.${name} LIMIT 100`),
      api.runQuery(`SELECT COUNT(*) AS cnt FROM ${schema}.${name}`),
    ])
      .then(([info, data, countData]) => {
        setColumns(info.columns);
        setPreview(data);
        if (countData.rows && countData.rows[0]) {
          setRowCount(countData.rows[0][0]);
        }
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [selectedTable]);

  function handleColumnClick(colName) {
    if (!selectedTable) return;
    const [schema, name] = selectedTable.split(".");
    const newDir = sortCol === colName && sortDir === "ASC" ? "DESC" : "ASC";
    setSortCol(colName);
    setSortDir(newDir);
    setLoading(true);
    api.runQuery(`SELECT * FROM ${schema}.${name} ORDER BY "${colName}" ${newDir} LIMIT 100`)
      .then((data) => setPreview(data))
      .catch(() => {})
      .finally(() => setLoading(false));
  }

  async function loadStats() {
    if (!selectedTable || statsLoading) return;
    setStatsLoading(true);
    const [schema, name] = selectedTable.split(".");
    const numericCols = columns.filter((c) =>
      /int|float|double|decimal|numeric|bigint|smallint|tinyint|real/i.test(c.type),
    );
    if (numericCols.length === 0) {
      setStats([]);
      setStatsLoading(false);
      return;
    }
    const selects = numericCols.map((c) =>
      `MIN("${c.name}") AS "${c.name}_min", MAX("${c.name}") AS "${c.name}_max", SUM(CASE WHEN "${c.name}" IS NULL THEN 1 ELSE 0 END) AS "${c.name}_nulls"`,
    ).join(", ");
    try {
      const data = await api.runQuery(`SELECT ${selects} FROM ${schema}.${name}`);
      if (data.rows && data.rows[0]) {
        const row = data.rows[0];
        const result = numericCols.map((c, i) => ({
          name: c.name,
          type: c.type,
          min: row[i * 3],
          max: row[i * 3 + 1],
          nulls: row[i * 3 + 2],
        }));
        setStats(result);
      }
    } catch {
      setStats([]);
    } finally {
      setStatsLoading(false);
    }
  }

  function handleQueryTable() {
    if (!selectedTable || !onQueryTable) return;
    const [schema, name] = selectedTable.split(".");
    onQueryTable(schema, name);
  }

  if (!selectedTable) {
    return <div style={st.placeholder}>Select a table from the sidebar to view its schema and data.</div>;
  }

  if (loading && !preview) {
    return <div style={st.placeholder}>Loading...</div>;
  }

  function formatCount(n) {
    if (n == null) return "";
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M rows`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K rows`;
    return `${n} row${n !== 1 ? "s" : ""}`;
  }

  return (
    <div style={st.container}>
      <div style={st.tableHeader}>
        <strong style={st.selectedName}>{selectedTable}</strong>
        <span style={st.colCount}>{columns.length} columns</span>
        {rowCount != null && <span style={st.rowCount}>{formatCount(rowCount)}</span>}
        <div style={st.headerActions}>
          {onQueryTable && (
            <button onClick={handleQueryTable} style={st.actionBtn}>Query this table</button>
          )}
          <button onClick={loadStats} disabled={statsLoading} style={st.actionBtn}>
            {statsLoading ? "Loading..." : stats ? "Refresh Stats" : "Show Stats"}
          </button>
        </div>
      </div>
      <div style={st.columnsBar} data-dp-hint="columns-bar">
        {columns.map((c) => (
          <button
            key={c.name}
            onClick={() => handleColumnClick(c.name)}
            style={{
              ...st.colChip,
              ...(sortCol === c.name ? st.colChipActive : {}),
            }}
            title={`Sort by ${c.name}`}
          >
            {c.name} <span style={st.colType}>{c.type}</span>
            {sortCol === c.name && <span style={st.sortArrow}>{sortDir === "ASC" ? " \u2191" : " \u2193"}</span>}
          </button>
        ))}
      </div>
      {stats && stats.length > 0 && (
        <div style={st.statsBar}>
          <span style={st.statsLabel}>Stats (numeric columns)</span>
          <div style={st.statsList}>
            {stats.map((s) => (
              <div key={s.name} style={st.statItem}>
                <span style={st.statName}>{s.name}</span>
                <span style={st.statVal}>min: {s.min ?? "NULL"}</span>
                <span style={st.statVal}>max: {s.max ?? "NULL"}</span>
                <span style={st.statVal}>nulls: {s.nulls}</span>
              </div>
            ))}
          </div>
        </div>
      )}
      {stats && stats.length === 0 && (
        <div style={st.statsBar}>
          <span style={st.statsLabel}>No numeric columns for stats</span>
        </div>
      )}
      {preview && (
        <div style={st.previewWrap}>
          <SortableTable
            columns={preview.columns}
            rows={preview.rows}
            columnTypes={columns.map((c) => c.type)}
          />
        </div>
      )}
    </div>
  );
}

const st = {
  container: { display: "flex", flexDirection: "column", height: "100%", overflow: "auto", padding: "12px" },
  placeholder: { color: "var(--dp-text-dim)", padding: "24px", textAlign: "center" },
  tableHeader: { display: "flex", alignItems: "center", gap: "12px", padding: "4px 8px 8px", fontSize: "14px", flexWrap: "wrap" },
  selectedName: { fontFamily: "var(--dp-font-mono)" },
  colCount: { color: "var(--dp-text-secondary)", fontSize: "12px" },
  rowCount: { color: "var(--dp-text-dim)", fontSize: "12px", fontFamily: "var(--dp-font-mono)" },
  headerActions: { marginLeft: "auto", display: "flex", gap: "6px" },
  actionBtn: { padding: "4px 12px", background: "var(--dp-btn-bg)", border: "1px solid var(--dp-btn-border)", borderRadius: "var(--dp-radius-lg)", color: "var(--dp-accent)", cursor: "pointer", fontSize: "11px", fontWeight: 500 },
  columnsBar: { display: "flex", flexWrap: "wrap", gap: "4px", padding: "4px 8px 12px" },
  colChip: { background: "var(--dp-btn-bg)", border: "1px solid var(--dp-border)", padding: "3px 8px", borderRadius: "var(--dp-radius)", fontSize: "11px", fontFamily: "var(--dp-font-mono)", cursor: "pointer", color: "var(--dp-text)" },
  colChipActive: { borderColor: "var(--dp-accent)", color: "var(--dp-accent)", fontWeight: 600 },
  colType: { color: "var(--dp-text-secondary)", marginLeft: "2px" },
  sortArrow: { color: "var(--dp-accent)", fontWeight: 700 },
  statsBar: { padding: "8px 8px 12px", borderTop: "1px solid var(--dp-border)", marginBottom: "4px" },
  statsLabel: { fontSize: "11px", color: "var(--dp-text-secondary)", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.3px" },
  statsList: { display: "flex", flexWrap: "wrap", gap: "8px", marginTop: "6px" },
  statItem: { display: "flex", gap: "8px", padding: "4px 10px", background: "var(--dp-bg-tertiary)", borderRadius: "var(--dp-radius)", fontSize: "11px", fontFamily: "var(--dp-font-mono)" },
  statName: { fontWeight: 600, color: "var(--dp-text)" },
  statVal: { color: "var(--dp-text-secondary)" },
  previewWrap: { overflow: "auto", flex: 1 },
};
