import React, { useEffect, useRef } from "react";

export default function OutputPanel({ output, onClear }) {
  const endRef = useRef(null);

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [output]);

  return (
    <div style={styles.container}>
      <div style={styles.header}>
        <span>Output</span>
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
            <span style={typeStyles[entry.type] || typeStyles.info}>{entry.message}</span>
          </div>
        ))}
        <div ref={endRef} />
      </div>
    </div>
  );
}

const typeStyles = {
  info: { color: "#c9d1d9" },
  error: { color: "#f85149" },
  warn: { color: "#d29922" },
  log: { color: "#8b949e" },
};

const styles = {
  container: {
    height: "180px",
    borderTop: "1px solid #21262d",
    display: "flex",
    flexDirection: "column",
    background: "#0d1117",
  },
  header: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "4px 12px",
    fontSize: "12px",
    fontWeight: "600",
    color: "#8b949e",
    borderBottom: "1px solid #21262d",
  },
  clearBtn: {
    background: "none",
    border: "none",
    color: "#8b949e",
    cursor: "pointer",
    fontSize: "11px",
  },
  log: {
    flex: 1,
    overflow: "auto",
    padding: "4px 12px",
    fontFamily: "monospace",
    fontSize: "12px",
    lineHeight: "1.6",
  },
  placeholder: { color: "#484f58", fontStyle: "italic", padding: "8px 0" },
  entry: { display: "flex", gap: "8px" },
  ts: { color: "#484f58", flexShrink: 0 },
};
