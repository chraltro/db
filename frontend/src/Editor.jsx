import React from "react";
import MonacoEditor from "@monaco-editor/react";

export default function Editor({ content, language, onChange, activeFile }) {
  if (!activeFile) {
    return (
      <div style={styles.empty}>
        <p style={styles.emptyText}>Select a file to edit</p>
        <p style={styles.emptyHint}>
          SQL files in transform/ are transformation models.
          <br />
          Python files in ingest/ and export/ are data scripts.
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
      theme="vs-dark"
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
      }}
    />
  );
}

const styles = {
  empty: { display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", height: "100%", color: "var(--dp-text-secondary)" },
  emptyText: { fontSize: "16px", marginBottom: "8px" },
  emptyHint: { fontSize: "13px", textAlign: "center", lineHeight: "1.6" },
};
