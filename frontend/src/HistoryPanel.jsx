import React, { useState, useEffect } from "react";
import { api } from "./api";

export default function HistoryPanel({ onOpenFile }) {
  const [history, setHistory] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    loadHistory();
  }, []);

  async function loadHistory() {
    setLoading(true);
    setError(null);
    try {
      const data = await api.getHistory(100);
      setHistory(data);
    } catch (e) {
      setError(e.message || "Failed to load history");
    }
    setLoading(false);
  }

  return (
    <div style={styles.container}>
      <div style={styles.header}>
        <span style={styles.headerTitle}>Run History</span>
        <button onClick={loadHistory} style={styles.refreshBtn}>
          Refresh
        </button>
      </div>
      {loading && <div style={styles.loading}>Loading...</div>}
      {!loading && error && (
        <div style={styles.error}>{error}</div>
      )}
      {!loading && !error && history.length === 0 && (
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
                  <td style={{ ...styles.td, fontFamily: "var(--dp-font-mono)", fontWeight: 500 }}>
                    {onOpenFile && row.target ? (
                      <span
                        style={styles.fileLink}
                        onClick={() => onOpenFile(row.target, 1, 1)}
                        onMouseEnter={(e) => { e.currentTarget.style.color = "var(--dp-accent)"; e.currentTarget.style.textDecoration = "underline"; }}
                        onMouseLeave={(e) => { e.currentTarget.style.color = ""; e.currentTarget.style.textDecoration = ""; }}
                        title={`Open ${row.target}`}
                      >
                        {row.target}
                      </span>
                    ) : row.target}
                  </td>
                  <td style={styles.td}>
                    <span style={{
                      ...styles.statusBadge,
                      background: row.status === "success"
                        ? "color-mix(in srgb, var(--dp-green) 12%, transparent)"
                        : "color-mix(in srgb, var(--dp-red) 12%, transparent)",
                      color: row.status === "success" ? "var(--dp-green)" : "var(--dp-red)",
                    }}>{row.status}</span>
                  </td>
                  <td style={{ ...styles.td, color: "var(--dp-text-secondary)" }}>
                    {row.started_at ? row.started_at.slice(0, 19).replace("T", " ") : ""}
                  </td>
                  <td style={{ ...styles.td, textAlign: "right", fontFamily: "var(--dp-font-mono)" }}>
                    {row.duration_ms != null ? `${row.duration_ms}ms` : ""}
                  </td>
                  <td style={{ ...styles.td, textAlign: "right", fontFamily: "var(--dp-font-mono)" }}>
                    {row.rows_affected || ""}
                  </td>
                  <td style={{ ...styles.td, color: "var(--dp-red)", maxWidth: "300px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
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
  header: { display: "flex", alignItems: "center", justifyContent: "space-between", padding: "8px 12px", borderBottom: "1px solid var(--dp-border)" },
  headerTitle: { fontWeight: 600, fontSize: "13px" },
  refreshBtn: { background: "var(--dp-btn-bg)", border: "1px solid var(--dp-btn-border)", borderRadius: "var(--dp-radius-lg)", color: "var(--dp-text)", padding: "4px 12px", cursor: "pointer", fontSize: "12px", fontWeight: 500 },
  loading: { padding: "24px", color: "var(--dp-text-secondary)", textAlign: "center" },
  empty: { padding: "24px", color: "var(--dp-text-dim)", textAlign: "center" },
  error: { padding: "24px", color: "var(--dp-red)", textAlign: "center" },
  tableWrap: { flex: 1, overflow: "auto" },
  table: { width: "100%", borderCollapse: "collapse", fontSize: "12px" },
  th: { textAlign: "left", padding: "6px 12px", borderBottom: "2px solid var(--dp-border-light)", color: "var(--dp-text-secondary)", fontWeight: 600, position: "sticky", top: 0, background: "var(--dp-bg)" },
  td: { padding: "5px 12px", borderBottom: "1px solid var(--dp-border)", color: "var(--dp-text)", fontSize: "12px" },
  typeBadge: { background: "var(--dp-btn-bg)", padding: "2px 8px", borderRadius: "var(--dp-radius)", fontSize: "11px", fontWeight: 500, textTransform: "capitalize" },
  statusBadge: { padding: "2px 8px", borderRadius: "var(--dp-radius)", fontSize: "11px", fontWeight: 600 },
  fileLink: { cursor: "pointer", transition: "color 0.15s" },
};
