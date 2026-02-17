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
              background: selected === `${t.schema}.${t.name}` ? "#1f2937" : "transparent",
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
  sidebar: {
    width: "240px",
    borderRight: "1px solid #21262d",
    overflow: "auto",
    background: "#0d1117",
  },
  sidebarHeader: {
    padding: "8px 12px",
    fontSize: "11px",
    fontWeight: "600",
    color: "#8b949e",
    letterSpacing: "0.5px",
  },
  empty: { padding: "12px", color: "#484f58", fontSize: "13px" },
  tableItem: {
    display: "flex",
    alignItems: "center",
    gap: "2px",
    padding: "4px 12px",
    cursor: "pointer",
    fontSize: "13px",
    fontFamily: "monospace",
  },
  schema: { color: "#8b949e" },
  type: { marginLeft: "auto", fontSize: "10px", color: "#484f58" },
  main: { flex: 1, overflow: "auto", padding: "8px" },
  placeholder: { color: "#484f58", padding: "24px", textAlign: "center" },
  tableHeader: {
    display: "flex",
    alignItems: "center",
    gap: "12px",
    padding: "8px",
    fontSize: "14px",
  },
  colCount: { color: "#8b949e", fontSize: "12px" },
  columnsBar: {
    display: "flex",
    flexWrap: "wrap",
    gap: "4px",
    padding: "4px 8px 12px",
  },
  colChip: {
    background: "#21262d",
    padding: "2px 8px",
    borderRadius: "4px",
    fontSize: "11px",
    fontFamily: "monospace",
  },
  colType: { color: "#8b949e" },
  previewWrap: { overflow: "auto" },
  table: { width: "100%", borderCollapse: "collapse", fontSize: "12px", fontFamily: "monospace" },
  th: {
    textAlign: "left",
    padding: "6px 12px",
    borderBottom: "1px solid #30363d",
    color: "#8b949e",
    fontWeight: 600,
    position: "sticky",
    top: 0,
    background: "#0f1117",
  },
  td: { padding: "4px 12px", borderBottom: "1px solid #21262d", color: "#c9d1d9" },
  null: { color: "#484f58", fontStyle: "italic" },
};
