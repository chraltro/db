import React, { useEffect, useRef } from "react";

export default function OutputPanel({ output, onClear }) {
  const endRef = useRef(null);

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [output]);

  return (
    <div style={styles.container}>
      <div style={styles.header}>
        <span style={styles.headerTitle}>Output</span>
        <span style={styles.count}>{output.length > 0 ? `${output.length} entries` : ""}</span>
        <button onClick={onClear} style={styles.clearBtn}>
          Clear
        </button>
      </div>
      <div style={styles.log}>
        {output.length === 0 && (
          <div style={styles.placeholder}>Run a script or transform to see output here.</div>
        )}
        {output.map((entry, i) => (
          <div key={i} style={styles.entry}>
            <span style={styles.ts}>{entry.ts}</span>
            <span style={{
              ...styles.indicator,
              background: entry.type === "error" ? "var(--dp-red)"
                : entry.type === "warn" ? "var(--dp-yellow)"
                : "var(--dp-accent)",
            }} />
            <span style={typeStyles[entry.type] || typeStyles.info}>{entry.message}</span>
          </div>
        ))}
        <div ref={endRef} />
      </div>
    </div>
  );
}

const typeStyles = {
  info: { color: "var(--dp-text)" },
  error: { color: "var(--dp-red)" },
  warn: { color: "var(--dp-yellow)" },
  log: { color: "var(--dp-text-secondary)" },
};

const styles = {
  container: { height: "180px", borderTop: "1px solid var(--dp-border)", display: "flex", flexDirection: "column", background: "var(--dp-bg-tertiary)" },
  header: { display: "flex", alignItems: "center", gap: "8px", padding: "4px 12px", fontSize: "12px", fontWeight: "600", color: "var(--dp-text-secondary)", borderBottom: "1px solid var(--dp-border)" },
  headerTitle: { fontSize: "11px", textTransform: "uppercase", letterSpacing: "0.5px" },
  count: { fontSize: "10px", color: "var(--dp-text-dim)", flex: 1 },
  clearBtn: { background: "none", border: "none", color: "var(--dp-text-secondary)", cursor: "pointer", fontSize: "11px" },
  log: { flex: 1, overflow: "auto", padding: "4px 12px", fontFamily: "var(--dp-font-mono)", fontSize: "12px", lineHeight: "1.7" },
  placeholder: { color: "var(--dp-text-dim)", fontStyle: "italic", padding: "8px 0", fontSize: "12px" },
  entry: { display: "flex", gap: "8px", alignItems: "baseline" },
  ts: { color: "var(--dp-text-dim)", flexShrink: 0, fontSize: "11px" },
  indicator: { width: "4px", height: "4px", borderRadius: "50%", flexShrink: 0, marginTop: "2px" },
};
