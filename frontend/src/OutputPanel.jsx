import React, { useEffect, useRef } from "react";

// Match file:line:col at start of message (lint output, e.g. "transform\silver\foo.sql:19:20 ...")
const FILE_LINE_RE = /^([\w./\\-]+\.\w+):(\d+):(\d+)/;
// Match identifier with a dot followed by ": " (model or script reference, e.g. "silver.earthquake_daily: skipped")
const REF_RE = /^([\w/\\-]+\.[\w./\\-]+): /;

function renderMessage(message, style, onOpenFile) {
  if (!onOpenFile) return <span style={style}>{message}</span>;

  // Try file:line:col first (lint output)
  const lineMatch = message.match(FILE_LINE_RE);
  if (lineMatch) {
    const [fullMatch, filePath, lineStr, colStr] = lineMatch;
    const line = parseInt(lineStr, 10);
    const col = parseInt(colStr, 10);
    const rest = message.slice(fullMatch.length);
    return (
      <span style={style}>
        <span
          style={styles.fileLink}
          onClick={() => onOpenFile(filePath, line, col)}
          onMouseEnter={(e) => { e.currentTarget.style.color = "var(--dp-accent)"; e.currentTarget.style.textDecoration = "underline"; }}
          onMouseLeave={(e) => { e.currentTarget.style.color = ""; e.currentTarget.style.textDecoration = ""; }}
          title={`Open ${filePath} at line ${line}`}
        >
          {fullMatch}
        </span>
        {rest}
      </span>
    );
  }

  // Try identifier: status (model or script reference)
  const refMatch = message.match(REF_RE);
  if (refMatch) {
    const ref = refMatch[1];
    const rest = message.slice(ref.length);
    return (
      <span style={style}>
        <span
          style={styles.fileLink}
          onClick={() => onOpenFile(ref, 1, 1)}
          onMouseEnter={(e) => { e.currentTarget.style.color = "var(--dp-accent)"; e.currentTarget.style.textDecoration = "underline"; }}
          onMouseLeave={(e) => { e.currentTarget.style.color = ""; e.currentTarget.style.textDecoration = ""; }}
          title={`Open ${ref}`}
        >
          {ref}
        </span>
        {rest}
      </span>
    );
  }

  // Try inline file paths anywhere in the message (e.g. "Exported 31 rows to output/file.csv")
  const INLINE_PATH_RE = /([\w-]+[/\\][\w./\\-]+\.\w+)/g;
  const parts = [];
  let lastIndex = 0;
  let m;
  while ((m = INLINE_PATH_RE.exec(message)) !== null) {
    if (m.index > lastIndex) parts.push({ text: message.slice(lastIndex, m.index) });
    parts.push({ text: m[1], isLink: true });
    lastIndex = m.index + m[0].length;
  }
  if (parts.length > 0) {
    if (lastIndex < message.length) parts.push({ text: message.slice(lastIndex) });
    return (
      <span style={style}>
        {parts.map((p, i) => p.isLink ? (
          <span
            key={i}
            style={styles.fileLink}
            onClick={() => onOpenFile(p.text, 1, 1)}
            onMouseEnter={(e) => { e.currentTarget.style.color = "var(--dp-accent)"; e.currentTarget.style.textDecoration = "underline"; }}
            onMouseLeave={(e) => { e.currentTarget.style.color = ""; e.currentTarget.style.textDecoration = ""; }}
            title={`Open ${p.text}`}
          >
            {p.text}
          </span>
        ) : p.text)}
      </span>
    );
  }

  return <span style={style}>{message}</span>;
}

export default function OutputPanel({ output, onClear, height = 180, onOpenFile }) {
  const endRef = useRef(null);

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [output]);

  return (
    <div style={{ ...styles.container, height }}>
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
            {renderMessage(entry.message, typeStyles[entry.type] || typeStyles.info, onOpenFile)}
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
  container: { borderTop: "1px solid var(--dp-border)", display: "flex", flexDirection: "column", background: "var(--dp-bg-tertiary)" },
  header: { display: "flex", alignItems: "center", gap: "8px", padding: "4px 12px", fontSize: "12px", fontWeight: "600", color: "var(--dp-text-secondary)", borderBottom: "1px solid var(--dp-border)" },
  headerTitle: { fontSize: "11px", textTransform: "uppercase", letterSpacing: "0.5px" },
  count: { fontSize: "10px", color: "var(--dp-text-dim)", flex: 1 },
  clearBtn: { background: "none", border: "none", color: "var(--dp-text-secondary)", cursor: "pointer", fontSize: "11px" },
  log: { flex: 1, overflow: "auto", padding: "4px 12px", fontFamily: "var(--dp-font-mono)", fontSize: "12px", lineHeight: "1.7" },
  placeholder: { color: "var(--dp-text-dim)", fontStyle: "italic", padding: "8px 0", fontSize: "12px" },
  entry: { display: "flex", gap: "8px", alignItems: "baseline" },
  ts: { color: "var(--dp-text-dim)", flexShrink: 0, fontSize: "11px" },
  indicator: { width: "4px", height: "4px", borderRadius: "50%", flexShrink: 0, marginTop: "2px" },
  fileLink: { cursor: "pointer", transition: "color 0.15s" },
};
