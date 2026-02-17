import React, { useState } from "react";
import { api } from "./api";

export default function QueryPanel({ addOutput }) {
  const [sql, setSql] = useState("SELECT 1 AS hello");
  const [results, setResults] = useState(null);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState(null);

  async function runQuery() {
    setRunning(true);
    setError(null);
    try {
      const data = await api.runQuery(sql);
      setResults(data);
      addOutput("info", `Query returned ${data.rows.length} rows`);
    } catch (e) {
      setError(e.message);
      addOutput("error", `Query error: ${e.message}`);
    } finally {
      setRunning(false);
    }
  }

  return (
    <div style={styles.container}>
      <div style={styles.inputArea}>
        <textarea
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          style={styles.textarea}
          placeholder="Enter SQL query..."
          onKeyDown={(e) => {
            if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
              e.preventDefault();
              runQuery();
            }
          }}
        />
        <button onClick={runQuery} disabled={running} style={styles.runBtn}>
          {running ? "Running..." : "Run (Ctrl+Enter)"}
        </button>
      </div>
      {error && <div style={styles.error}>{error}</div>}
      {results && (
        <div style={styles.tableWrap}>
          <table style={styles.table}>
            <thead>
              <tr>
                {results.columns.map((col) => (
                  <th key={col} style={styles.th}>{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {results.rows.map((row, i) => (
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
          {results.truncated && (
            <div style={styles.truncated}>Results truncated to 1000 rows</div>
          )}
        </div>
      )}
    </div>
  );
}

const styles = {
  container: { display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" },
  inputArea: { display: "flex", gap: "8px", padding: "8px", borderBottom: "1px solid #21262d" },
  textarea: {
    flex: 1,
    minHeight: "60px",
    maxHeight: "200px",
    background: "#0d1117",
    color: "#e1e4e8",
    border: "1px solid #30363d",
    borderRadius: "6px",
    padding: "8px",
    fontFamily: "monospace",
    fontSize: "13px",
    resize: "vertical",
  },
  runBtn: {
    padding: "8px 16px",
    background: "#238636",
    border: "1px solid #2ea043",
    borderRadius: "6px",
    color: "#fff",
    cursor: "pointer",
    fontSize: "12px",
    alignSelf: "flex-end",
  },
  error: { padding: "8px", color: "#f85149", fontSize: "13px", fontFamily: "monospace" },
  tableWrap: { flex: 1, overflow: "auto", padding: "0 8px 8px" },
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
  truncated: { padding: "8px", color: "#d29922", fontSize: "12px" },
};
