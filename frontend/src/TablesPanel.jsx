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
        <div style={styles.sidebarHeader}>TABLES & VIEWS</div>
        {tables.length === 0 && (
          <div style={styles.empty}>No tables yet. Run a pipeline first.</div>
        )}
        {tables.map((t) => (
          <div
            key={`${t.schema}.${t.name}`}
            data-dp-table-item=""
            style={{
              ...styles.tableItem,
              background: selected === `${t.schema}.${t.name}` ? "var(--dp-bg-secondary)" : "transparent",
              borderLeft: selected === `${t.schema}.${t.name}` ? "2px solid var(--dp-accent)" : "2px solid transparent",
            }}
            onClick={() => selectTable(t.schema, t.name)}
          >
            <span style={styles.schema}>{t.schema}.</span>
            <span style={styles.tableName}>{t.name}</span>
            <span style={{
              ...styles.type,
              color: t.type === "VIEW" ? "var(--dp-purple)" : "var(--dp-accent)",
            }}>{t.type === "VIEW" ? "V" : "T"}</span>
          </div>
        ))}
      </div>
      <div style={styles.main}>
        {!selected && <div style={styles.placeholder}>Select a table to view its schema and data.</div>}
        {selected && (
          <>
            <div style={styles.tableHeader}>
              <strong style={styles.selectedName}>{selected}</strong>
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
  sidebar: { width: "260px", borderRight: "1px solid var(--dp-border)", overflow: "auto", background: "var(--dp-bg-tertiary)" },
  sidebarHeader: { padding: "8px 12px", fontSize: "10px", fontWeight: "600", color: "var(--dp-text-dim)", letterSpacing: "1px" },
  empty: { padding: "12px", color: "var(--dp-text-dim)", fontSize: "13px" },
  tableItem: { display: "flex", alignItems: "center", gap: "2px", padding: "5px 12px", cursor: "pointer", fontSize: "13px", fontFamily: "var(--dp-font-mono)" },
  schema: { color: "var(--dp-text-secondary)" },
  tableName: { color: "var(--dp-text)" },
  type: { marginLeft: "auto", fontSize: "10px", fontWeight: 600 },
  main: { flex: 1, overflow: "auto", padding: "12px" },
  placeholder: { color: "var(--dp-text-dim)", padding: "24px", textAlign: "center" },
  tableHeader: { display: "flex", alignItems: "center", gap: "12px", padding: "4px 8px 8px", fontSize: "14px" },
  selectedName: { fontFamily: "var(--dp-font-mono)" },
  colCount: { color: "var(--dp-text-secondary)", fontSize: "12px" },
  columnsBar: { display: "flex", flexWrap: "wrap", gap: "4px", padding: "4px 8px 12px" },
  colChip: { background: "var(--dp-btn-bg)", border: "1px solid var(--dp-border)", padding: "3px 8px", borderRadius: "var(--dp-radius)", fontSize: "11px", fontFamily: "var(--dp-font-mono)" },
  colType: { color: "var(--dp-text-secondary)", marginLeft: "2px" },
  previewWrap: { overflow: "auto" },
  table: { width: "100%", borderCollapse: "collapse", fontSize: "12px", fontFamily: "var(--dp-font-mono)" },
  th: { textAlign: "left", padding: "6px 12px", borderBottom: "2px solid var(--dp-border-light)", color: "var(--dp-text-secondary)", fontWeight: 600, position: "sticky", top: 0, background: "var(--dp-bg)" },
  td: { padding: "4px 12px", borderBottom: "1px solid var(--dp-border)", color: "var(--dp-text)" },
  null: { color: "var(--dp-text-dim)", fontStyle: "italic", fontSize: "11px" },
};
