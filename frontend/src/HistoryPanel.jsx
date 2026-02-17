import React, { useState, useEffect } from "react";
import { api } from "./api";

export default function HistoryPanel() {
  const [history, setHistory] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadHistory();
  }, []);

  async function loadHistory() {
    setLoading(true);
    try {
      const data = await api.getHistory(100);
      setHistory(data);
    } catch {}
    setLoading(false);
  }

  return (
    <div style={styles.container}>
      <div style={styles.header}>
        <span>Run History</span>
        <button onClick={loadHistory} style={styles.refreshBtn}>
          Refresh
        </button>
      </div>
      {loading && <div style={styles.loading}>Loading...</div>}
      {!loading && history.length === 0 && (
        <div style={styles.empty}>No runs yet. Execute a pipeline to see history here.</div>
      )}
      {!loading && history.length > 0 && (
        <div style={styles.tableWrap}>
          <table style={styles.table}>
            <thead>
              <tr>
                <th style={styles.th}>Type</th>
                <th style={styles.th}>Target</th>
                <th style={styles.th}>Status</th>
                <th style={styles.th}>Time</th>
                <th style={styles.th}>Duration</th>
                <th style={styles.th}>Rows</th>
                <th style={styles.th}>Error</th>
              </tr>
            </thead>
            <tbody>
              {history.map((row) => (
                <tr key={row.run_id}>
                  <td style={styles.td}>
                    <span style={styles.typeBadge}>{row.run_type}</span>
                  </td>
                  <td style={{ ...styles.td, fontFamily: "monospace" }}>{row.target}</td>
                  <td style={styles.td}>
                    <span
                      style={{
                        color: row.status === "success" ? "#3fb950" : "#f85149",
                      }}
                    >
                      {row.status}
                    </span>
                  </td>
                  <td style={{ ...styles.td, color: "#8b949e" }}>
                    {row.started_at ? row.started_at.slice(0, 19) : ""}
                  </td>
                  <td style={{ ...styles.td, textAlign: "right" }}>
                    {row.duration_ms != null ? `${row.duration_ms}ms` : ""}
                  </td>
                  <td style={{ ...styles.td, textAlign: "right" }}>
                    {row.rows_affected || ""}
                  </td>
                  <td style={{ ...styles.td, color: "#f85149", maxWidth: "300px", overflow: "hidden", textOverflow: "ellipsis" }}>
                    {row.error || ""}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

const styles = {
  container: { display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" },
  header: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "8px 12px",
    borderBottom: "1px solid #21262d",
  },
  refreshBtn: {
    background: "#21262d",
    border: "1px solid #30363d",
    borderRadius: "6px",
    color: "#e1e4e8",
    padding: "4px 12px",
    cursor: "pointer",
    fontSize: "12px",
  },
  loading: { padding: "24px", color: "#8b949e", textAlign: "center" },
  empty: { padding: "24px", color: "#484f58", textAlign: "center" },
  tableWrap: { flex: 1, overflow: "auto" },
  table: { width: "100%", borderCollapse: "collapse", fontSize: "12px" },
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
  td: { padding: "4px 12px", borderBottom: "1px solid #21262d", color: "#c9d1d9", fontSize: "12px" },
  typeBadge: {
    background: "#21262d",
    padding: "1px 6px",
    borderRadius: "4px",
    fontSize: "11px",
  },
};
