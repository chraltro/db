import React from "react";
import MonacoEditor from "@monaco-editor/react";
import { useTheme } from "./ThemeProvider";
import { getTheme } from "./themes";

export default function Editor({ content, language, onChange, activeFile }) {
  const { themeId } = useTheme();
  const currentTheme = getTheme(themeId);
  const monacoTheme = currentTheme.dark ? "vs-dark" : "vs";

  if (!activeFile) {
    return (
      <div style={styles.empty}>
        <div style={styles.emptyIcon}>
          <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="var(--dp-text-dim)" strokeWidth="1" strokeLinecap="round" strokeLinejoin="round">
            <path d="M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z" />
            <polyline points="14,2 14,8 20,8" />
            <line x1="16" y1="13" x2="8" y2="13" />
            <line x1="16" y1="17" x2="8" y2="17" />
            <polyline points="10,9 9,9 8,9" />
          </svg>
        </div>
        <p style={styles.emptyText}>Select a file to edit</p>
        <p style={styles.emptyHint}>
          SQL files in <code style={styles.code}>transform/</code> are transformation models.
          <br />
          Python files in <code style={styles.code}>ingest/</code> and <code style={styles.code}>export/</code> are data scripts.
        </p>
      </div>
    );
  }

  return (
    <MonacoEditor
      height="100%"
      language={language}
      value={content}
      onChange={(val) => onChange(val || "")}
      theme={monacoTheme}
      options={{
        minimap: { enabled: false },
        fontSize: 13,
        lineNumbers: "on",
        renderLineHighlight: "all",
        scrollBeyondLastLine: false,
        wordWrap: "on",
        padding: { top: 8 },
        tabSize: 4,
        insertSpaces: true,
        smoothScrolling: true,
        cursorBlinking: "smooth",
        cursorSmoothCaretAnimation: "on",
        fontFamily: "var(--dp-font-mono)",
      }}
    />
  );
}

const styles = {
  empty: { display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", height: "100%", color: "var(--dp-text-secondary)", gap: "8px" },
  emptyIcon: { marginBottom: "4px", opacity: 0.6 },
  emptyText: { fontSize: "16px", marginBottom: "4px", fontWeight: 500 },
  emptyHint: { fontSize: "13px", textAlign: "center", lineHeight: "1.7", color: "var(--dp-text-dim)" },
  code: { background: "var(--dp-btn-bg)", padding: "1px 5px", borderRadius: "3px", fontSize: "12px", fontFamily: "var(--dp-font-mono)" },
};
