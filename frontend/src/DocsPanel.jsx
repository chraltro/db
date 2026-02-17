import React, { useState, useEffect } from "react";
import { api } from "./api";

// Minimal markdown renderer — handles headers, tables, code blocks, bold, links, lists
function renderMarkdown(md) {
  const lines = md.split("\n");
  const elements = [];
  let i = 0;
  let key = 0;

  while (i < lines.length) {
    const line = lines[i];

    // Code blocks
    if (line.startsWith("```")) {
      const lang = line.slice(3).trim();
      const codeLines = [];
      i++;
      while (i < lines.length && !lines[i].startsWith("```")) {
        codeLines.push(lines[i]);
        i++;
      }
      i++; // skip closing ```
      elements.push(
        <pre key={key++} style={mdStyles.codeBlock}>
          <code>{codeLines.join("\n")}</code>
        </pre>,
      );
      continue;
    }

    // Tables
    if (line.includes("|") && line.trim().startsWith("|")) {
      const tableRows = [];
      while (i < lines.length && lines[i].includes("|") && lines[i].trim().startsWith("|")) {
        const cells = lines[i]
          .split("|")
          .slice(1, -1)
          .map((c) => c.trim());
        tableRows.push(cells);
        i++;
      }
      if (tableRows.length >= 2) {
        const header = tableRows[0];
        // Skip separator row (row 1)
        const body = tableRows.slice(2);
        elements.push(
          <table key={key++} style={mdStyles.table}>
            <thead>
              <tr>
                {header.map((h, j) => (
                  <th key={j} style={mdStyles.th}>
                    {renderInline(h)}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {body.map((row, ri) => (
                <tr key={ri}>
                  {row.map((cell, ci) => (
                    <td key={ci} style={mdStyles.td}>
                      {renderInline(cell)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>,
        );
      }
      continue;
    }

    // Details/summary (pass through as-is)
    if (line.trim().startsWith("<details>") || line.trim().startsWith("</details>")) {
      i++;
      continue;
    }
    if (line.trim().startsWith("<summary>")) {
      const text = line.replace(/<\/?summary>/g, "").trim();
      elements.push(
        <div key={key++} style={mdStyles.summary}>
          {text}
        </div>,
      );
      i++;
      continue;
    }

    // Headings
    if (line.startsWith("### ")) {
      elements.push(
        <h3 key={key++} style={mdStyles.h3}>
          {renderInline(line.slice(4).replace(/<[^>]*>/g, ""))}
        </h3>,
      );
      i++;
      continue;
    }
    if (line.startsWith("## ")) {
      elements.push(
        <h2 key={key++} style={mdStyles.h2}>
          {renderInline(line.slice(3))}
        </h2>,
      );
      i++;
      continue;
    }
    if (line.startsWith("# ")) {
      elements.push(
        <h1 key={key++} style={mdStyles.h1}>
          {renderInline(line.slice(2))}
        </h1>,
      );
      i++;
      continue;
    }

    // Horizontal rule
    if (line.trim() === "---") {
      elements.push(<hr key={key++} style={mdStyles.hr} />);
      i++;
      continue;
    }

    // List items
    if (line.trimStart().startsWith("- ")) {
      const indent = line.length - line.trimStart().length;
      elements.push(
        <div key={key++} style={{ ...mdStyles.listItem, paddingLeft: 12 + indent * 8 }}>
          {renderInline(line.trimStart().slice(2))}
        </div>,
      );
      i++;
      continue;
    }

    // Empty line
    if (line.trim() === "") {
      i++;
      continue;
    }

    // Paragraph
    elements.push(
      <p key={key++} style={mdStyles.p}>
        {renderInline(line)}
      </p>,
    );
    i++;
  }

  return elements;
}

function renderInline(text) {
  // Bold: **text**
  // Code: `text`
  // Links: [text](url)
  const parts = [];
  let remaining = text;
  let k = 0;

  while (remaining.length > 0) {
    // Code
    const codeMatch = remaining.match(/`([^`]+)`/);
    // Bold
    const boldMatch = remaining.match(/\*\*([^*]+)\*\*/);

    const matches = [codeMatch, boldMatch].filter(Boolean);
    if (matches.length === 0) {
      parts.push(remaining);
      break;
    }

    // Pick earliest match
    const earliest = matches.reduce((a, b) => (a.index < b.index ? a : b));

    if (earliest.index > 0) {
      parts.push(remaining.slice(0, earliest.index));
    }

    if (earliest === codeMatch) {
      parts.push(
        <code key={k++} style={mdStyles.inlineCode}>
          {codeMatch[1]}
        </code>,
      );
    } else {
      parts.push(<strong key={k++}>{boldMatch[1]}</strong>);
    }

    remaining = remaining.slice(earliest.index + earliest[0].length);
  }

  return parts;
}

export default function DocsPanel() {
  const [markdown, setMarkdown] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadDocs();
  }, []);

  async function loadDocs() {
    setLoading(true);
    try {
      const data = await api.getDocs();
      setMarkdown(data.markdown);
    } catch {}
    setLoading(false);
  }

  return (
    <div style={styles.container}>
      <div style={styles.header}>
        <span>Documentation</span>
        <button onClick={loadDocs} style={styles.refreshBtn}>
          Refresh
        </button>
      </div>
      <div style={styles.content}>
        {loading && <div style={styles.loading}>Generating docs...</div>}
        {!loading && markdown && renderMarkdown(markdown)}
        {!loading && !markdown && (
          <div style={styles.empty}>No documentation available. Run a pipeline first.</div>
        )}
      </div>
    </div>
  );
}

const styles = {
  container: { display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" },
  header: { display: "flex", alignItems: "center", justifyContent: "space-between", padding: "8px 12px", borderBottom: "1px solid var(--dp-border)", fontWeight: 600, fontSize: "13px" },
  refreshBtn: { background: "var(--dp-btn-bg)", border: "1px solid var(--dp-btn-border)", borderRadius: "var(--dp-radius-lg)", color: "var(--dp-text)", padding: "4px 12px", cursor: "pointer", fontSize: "12px" },
  content: { flex: 1, overflow: "auto", padding: "16px 24px", maxWidth: "900px" },
  loading: { color: "var(--dp-text-secondary)", textAlign: "center", padding: "24px" },
  empty: { color: "var(--dp-text-dim)", textAlign: "center", padding: "24px" },
};

const mdStyles = {
  h1: { fontSize: "24px", fontWeight: 700, margin: "24px 0 12px", borderBottom: "1px solid var(--dp-border)", paddingBottom: "8px" },
  h2: { fontSize: "20px", fontWeight: 600, margin: "20px 0 10px", borderBottom: "1px solid var(--dp-border)", paddingBottom: "6px" },
  h3: { fontSize: "16px", fontWeight: 600, margin: "16px 0 8px" },
  p: { margin: "4px 0", lineHeight: 1.5, fontSize: "13px" },
  hr: { border: "none", borderTop: "1px solid var(--dp-border)", margin: "16px 0" },
  listItem: { fontSize: "13px", lineHeight: 1.6, paddingLeft: "12px" },
  table: { width: "100%", borderCollapse: "collapse", margin: "8px 0", fontSize: "12px", fontFamily: "var(--dp-font-mono)" },
  th: { textAlign: "left", padding: "6px 12px", borderBottom: "1px solid var(--dp-border-light)", color: "var(--dp-text-secondary)", fontWeight: 600 },
  td: { padding: "4px 12px", borderBottom: "1px solid var(--dp-border)", color: "var(--dp-text)" },
  codeBlock: { background: "var(--dp-bg-secondary)", border: "1px solid var(--dp-border)", borderRadius: "var(--dp-radius-lg)", padding: "12px", margin: "8px 0", fontSize: "12px", fontFamily: "var(--dp-font-mono)", overflow: "auto", color: "var(--dp-text)" },
  inlineCode: { background: "var(--dp-btn-bg)", padding: "1px 5px", borderRadius: "3px", fontSize: "12px", fontFamily: "var(--dp-font-mono)" },
  summary: { color: "var(--dp-accent)", cursor: "pointer", fontSize: "13px", margin: "4px 0" },
};
