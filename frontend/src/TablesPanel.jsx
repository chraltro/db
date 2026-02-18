import React, { useState, useEffect } from "react";
import { api } from "./api";
import SortableTable from "./SortableTable";

export default function TablesPanel({ selectedTable }) {
  const [columns, setColumns] = useState([]);
  const [preview, setPreview] = useState(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!selectedTable) {
      setColumns([]);
      setPreview(null);
      return;
    }
    const [schema, name] = selectedTable.split(".");
    setLoading(true);
    Promise.all([
      api.describeTable(schema, name),
      api.runQuery(`SELECT * FROM ${schema}.${name} LIMIT 100`),
    ])
      .then(([info, data]) => {
        setColumns(info.columns);
        setPreview(data);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [selectedTable]);

  if (!selectedTable) {
    return <div style={styles.placeholder}>Select a table from the sidebar to view its schema and data.</div>;
  }

  if (loading) {
    return <div style={styles.placeholder}>Loading...</div>;
  }

  return (
    <div style={styles.container}>
      <div style={styles.tableHeader}>
        <strong style={styles.selectedName}>{selectedTable}</strong>
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

const styles = {
  container: { display: "flex", flexDirection: "column", height: "100%", overflow: "auto", padding: "12px" },
  placeholder: { color: "var(--dp-text-dim)", padding: "24px", textAlign: "center" },
  tableHeader: { display: "flex", alignItems: "center", gap: "12px", padding: "4px 8px 8px", fontSize: "14px" },
  selectedName: { fontFamily: "var(--dp-font-mono)" },
  colCount: { color: "var(--dp-text-secondary)", fontSize: "12px" },
  columnsBar: { display: "flex", flexWrap: "wrap", gap: "4px", padding: "4px 8px 12px" },
  colChip: { background: "var(--dp-btn-bg)", border: "1px solid var(--dp-border)", padding: "3px 8px", borderRadius: "var(--dp-radius)", fontSize: "11px", fontFamily: "var(--dp-font-mono)" },
  colType: { color: "var(--dp-text-secondary)", marginLeft: "2px" },
  previewWrap: { overflow: "auto", flex: 1 },
};
