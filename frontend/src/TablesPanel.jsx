import React, { useState, useEffect } from "react";
import { api } from "./api";

export default function TablesPanel() {
  const [tables, setTables] = useState([]);
  const [selected, setSelected] = useState(null);
  const [columns, setColumns] = useState([]);
  const [preview, setPreview] = useState(null);

  useEffect(() => {
    api.listTables().then(setTables).catch(() => {});
  }, []);

  async function selectTable(schema, name) {
    setSelected(`${schema}.${name}`);
    try {
      const info = await api.describeTable(schema, name);
      setColumns(info.columns);
      const data = await api.runQuery(`SELECT * FROM ${schema}.${name} LIMIT 100`);
      setPreview(data);
    } catch {}
  }

  return (
    <div style={styles.container}>
      <div style={styles.sidebar}>
        <div style={styles.sidebarHeader}>Tables & Views</div>
        {tables.length === 0 && (
          <div style={styles.empty}>No tables yet. Run a pipeline first.</div>
        )}
        {tables.map((t) => (
          <div
            key={`${t.schema}.${t.name}`}
            style={{
              ...styles.tableItem,
              background: selected === `${t.schema}.${t.name}` ? "var(--dp-bg-secondary)" : "transparent",
            }}
            onClick={() => selectTable(t.schema, t.name)}
          >
            <span style={styles.schema}>{t.schema}.</span>
            <span>{t.name}</span>
            <span style={styles.type}>{t.type === "VIEW" ? "V" : "T"}</span>
          </div>
        ))}
      </div>
      <div style={styles.main}>
        {!selected && <div style={styles.placeholder}>Select a table to view its schema and data.</div>}
        {selected && (
          <>
            <div style={styles.tableHeader}>
              <strong>{selected}</strong>
              <span style={styles.colCount}>{columns.length} columns</span>
            </div>
            <div style={styles.columnsBar}>
              {columns.map((c) => (
                <span key={c.name} style={styles.colChip}>
                  {c.name} <span style={styles.colType}>{c.type}</span>
                </span>
              ))}
            </div>
            {preview && (
              <div style={styles.previewWrap}>
                <table style={styles.table}>
                  <thead>
                    <tr>
                      {preview.columns.map((col) => (
                        <th key={col} style={styles.th}>{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {preview.rows.map((row, i) => (
                      <tr key={i}>
                        {row.map((val, j) => (
                          <td key={j} style={styles.td}>
                            {val === null ? <span style={styles.null}>NULL</span> : String(val)}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}

const styles = {
  container: { display: "flex", height: "100%", overflow: "hidden" },
  sidebar: { width: "240px", borderRight: "1px solid var(--dp-border)", overflow: "auto", background: "var(--dp-bg-tertiary)" },
  sidebarHeader: { padding: "8px 12px", fontSize: "11px", fontWeight: "600", color: "var(--dp-text-secondary)", letterSpacing: "0.5px" },
  empty: { padding: "12px", color: "var(--dp-text-dim)", fontSize: "13px" },
  tableItem: { display: "flex", alignItems: "center", gap: "2px", padding: "4px 12px", cursor: "pointer", fontSize: "13px", fontFamily: "var(--dp-font-mono)" },
  schema: { color: "var(--dp-text-secondary)" },
  type: { marginLeft: "auto", fontSize: "10px", color: "var(--dp-text-dim)" },
  main: { flex: 1, overflow: "auto", padding: "8px" },
  placeholder: { color: "var(--dp-text-dim)", padding: "24px", textAlign: "center" },
  tableHeader: { display: "flex", alignItems: "center", gap: "12px", padding: "8px", fontSize: "14px" },
  colCount: { color: "var(--dp-text-secondary)", fontSize: "12px" },
  columnsBar: { display: "flex", flexWrap: "wrap", gap: "4px", padding: "4px 8px 12px" },
  colChip: { background: "var(--dp-btn-bg)", padding: "2px 8px", borderRadius: "var(--dp-radius)", fontSize: "11px", fontFamily: "var(--dp-font-mono)" },
  colType: { color: "var(--dp-text-secondary)" },
  previewWrap: { overflow: "auto" },
  table: { width: "100%", borderCollapse: "collapse", fontSize: "12px", fontFamily: "var(--dp-font-mono)" },
  th: { textAlign: "left", padding: "6px 12px", borderBottom: "1px solid var(--dp-border-light)", color: "var(--dp-text-secondary)", fontWeight: 600, position: "sticky", top: 0, background: "var(--dp-bg)" },
  td: { padding: "4px 12px", borderBottom: "1px solid var(--dp-border)", color: "var(--dp-text)" },
  null: { color: "var(--dp-text-dim)", fontStyle: "italic" },
};
