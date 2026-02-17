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
        <div style={styles.runCol}>
          <button onClick={runQuery} disabled={running} style={styles.runBtn}>
            {running ? "Running..." : "Run"}
          </button>
          <span style={styles.hint}>Ctrl+Enter</span>
        </div>
      </div>
      {error && <div style={styles.error}>{error}</div>}
      {results && (
        <div style={styles.tableWrap}>
          <div style={styles.resultsMeta}>
            {results.rows.length} row{results.rows.length !== 1 ? "s" : ""} returned
            {results.truncated && <span style={styles.truncatedMeta}> (truncated to 1000)</span>}
          </div>
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
        </div>
      )}
    </div>
  );
}

const styles = {
  container: { display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" },
  inputArea: { display: "flex", gap: "8px", padding: "8px", borderBottom: "1px solid var(--dp-border)" },
  textarea: { flex: 1, minHeight: "60px", maxHeight: "200px", background: "var(--dp-bg-tertiary)", color: "var(--dp-text)", border: "1px solid var(--dp-border-light)", borderRadius: "var(--dp-radius-lg)", padding: "8px", fontFamily: "var(--dp-font-mono)", fontSize: "13px", resize: "vertical" },
  runCol: { display: "flex", flexDirection: "column", alignItems: "center", gap: "4px", alignSelf: "flex-end" },
  runBtn: { padding: "8px 20px", background: "var(--dp-green)", border: "1px solid var(--dp-green-border)", borderRadius: "var(--dp-radius-lg)", color: "#fff", cursor: "pointer", fontSize: "12px", fontWeight: 500 },
  hint: { fontSize: "10px", color: "var(--dp-text-dim)" },
  error: { padding: "8px 12px", color: "var(--dp-red)", fontSize: "13px", fontFamily: "var(--dp-font-mono)", background: "color-mix(in srgb, var(--dp-red) 8%, transparent)", margin: "0 8px", borderRadius: "var(--dp-radius)" },
  tableWrap: { flex: 1, overflow: "auto", padding: "0 8px 8px" },
  resultsMeta: { padding: "6px 12px", fontSize: "11px", color: "var(--dp-text-secondary)" },
  truncatedMeta: { color: "var(--dp-yellow)" },
  table: { width: "100%", borderCollapse: "collapse", fontSize: "12px", fontFamily: "var(--dp-font-mono)" },
  th: { textAlign: "left", padding: "6px 12px", borderBottom: "2px solid var(--dp-border-light)", color: "var(--dp-text-secondary)", fontWeight: 600, position: "sticky", top: 0, background: "var(--dp-bg)" },
  td: { padding: "4px 12px", borderBottom: "1px solid var(--dp-border)", color: "var(--dp-text)" },
  null: { color: "var(--dp-text-dim)", fontStyle: "italic", fontSize: "11px" },
};
