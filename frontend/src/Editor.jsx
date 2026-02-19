import React, { useEffect, useRef } from "react";
import MonacoEditor, { loader } from "@monaco-editor/react";
import { useTheme } from "./ThemeProvider";
import { getTheme } from "./themes";
import { api } from "./api";

// Cache for table schema lookups to avoid repeated API calls
const schemaCache = new Map();

// Register the SQL hover provider once when Monaco loads
let hoverRegistered = false;
loader.init().then((monaco) => {
  if (hoverRegistered) return;
  hoverRegistered = true;

  monaco.languages.registerHoverProvider("sql", {
    provideHover: async (model, position) => {
      const line = model.getLineContent(position.lineNumber);
      const word = model.getWordAtPosition(position);
      if (!word) return null;

      // Detect schema.table pattern around cursor
      let schema = null;
      let table = null;

      // Case 1: cursor is on the table part (after the dot)
      const before = line.substring(0, word.startColumn - 1);
      const dotBefore = before.match(/(\w+)\.\s*$/);
      if (dotBefore) {
        schema = dotBefore[1];
        table = word.word;
      }

      // Case 2: cursor is on the schema part (before the dot)
      if (!schema) {
        const after = line.substring(word.endColumn - 1);
        const dotAfter = after.match(/^\s*\.(\w+)/);
        if (dotAfter) {
          schema = word.word;
          table = dotAfter[1];
        }
      }

      if (!schema || !table) return null;

      const cacheKey = `${schema}.${table}`;
      let info = schemaCache.get(cacheKey);

      if (info === undefined) {
        try {
          info = await api.describeTable(schema, table);
          schemaCache.set(cacheKey, info);
        } catch {
          schemaCache.set(cacheKey, null);
          info = null;
        }
      }

      if (!info || !info.columns || info.columns.length === 0) {
        return {
          range: new monaco.Range(position.lineNumber, word.startColumn, position.lineNumber, word.endColumn),
          contents: [{ value: `*${schema}.${table}* — table not found in warehouse` }],
        };
      }

      const lines = [`**${schema}.${table}** — ${info.columns.length} columns`, ""];
      for (const col of info.columns) {
        lines.push(`- \`${col.name}\` *${col.type}*`);
      }

      return {
        range: new monaco.Range(position.lineNumber, word.startColumn, position.lineNumber, word.endColumn),
        contents: [{ value: lines.join("\n") }],
      };
    },
  });
});

export default function Editor({ content, language, onChange, activeFile, onMount, goToLine, onFormat, onPreview }) {
  const { themeId } = useTheme();
  const currentTheme = getTheme(themeId);
  const monacoTheme = currentTheme.dark ? "vs-dark" : "vs";
  const editorRef = useRef(null);
  const onFormatRef = useRef(onFormat);
  onFormatRef.current = onFormat;
  const onPreviewRef = useRef(onPreview);
  onPreviewRef.current = onPreview;

  function handleEditorMount(editor, monaco) {
    editorRef.current = editor;
    if (onMount) onMount(editor);

    editor.addAction({
      id: "dp-format-sql",
      label: "Format SQL (dp lint --fix)",
      keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyMod.Shift | monaco.KeyCode.KeyF],
      precondition: null,
      keybindingContext: null,
      run: () => { if (onFormatRef.current) onFormatRef.current(); },
    });

    editor.addAction({
      id: "dp-preview-sql",
      label: "Preview SQL results",
      keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter],
      precondition: null,
      keybindingContext: null,
      run: () => { if (onPreviewRef.current) onPreviewRef.current(); },
    });
  }

  useEffect(() => {
    if (!goToLine || !editorRef.current) return;
    const { line, col } = goToLine;
    editorRef.current.revealLineInCenter(line);
    editorRef.current.setPosition({ lineNumber: line, column: col || 1 });
    editorRef.current.focus();
  }, [goToLine]);

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
      onMount={(editor, monaco) => handleEditorMount(editor, monaco)}
      options={{
        minimap: { enabled: false },
        hover: { above: false },
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
