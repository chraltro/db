import React, { useEffect, useRef } from "react";
import MonacoEditor, { loader } from "@monaco-editor/react";
import { useTheme } from "./ThemeProvider";
import { getTheme } from "./themes";
import { api } from "./api";

// Cache for table schema lookups to avoid repeated API calls
const schemaCache = new Map();
// Cached table list for completions (populated on first completion request)
let tablesCache = null;
let tablesCacheTime = 0;

async function getTablesCache() {
  const now = Date.now();
  if (tablesCache && now - tablesCacheTime < 30_000) return tablesCache;
  try {
    tablesCache = await api.listTables();
    tablesCacheTime = now;
  } catch {
    tablesCache = tablesCache || [];
  }
  return tablesCache;
}

async function getColumnsCache(schema, table) {
  const key = `${schema}.${table}`;
  let info = schemaCache.get(key);
  if (info !== undefined) return info;
  try {
    info = await api.describeTable(schema, table);
    schemaCache.set(key, info);
  } catch {
    schemaCache.set(key, null);
    info = null;
  }
  return info;
}

// Register SQL hover + completion providers once when Monaco loads
let providersRegistered = false;
loader.init().then((monaco) => {
  if (providersRegistered) return;
  providersRegistered = true;

  // --- Completion provider ---

  // SQL keywords after which a table reference (schema.table) is expected
  const TABLE_CONTEXTS = /\b(?:FROM|JOIN|INNER\s+JOIN|LEFT\s+(?:OUTER\s+)?JOIN|RIGHT\s+(?:OUTER\s+)?JOIN|FULL\s+(?:OUTER\s+)?JOIN|CROSS\s+JOIN|INTO|UPDATE|TABLE)\s+(\w*)$/i;

  // SQL contexts where column names make sense
  const COLUMN_CONTEXTS = /\b(?:SELECT|WHERE|AND|OR|ON|USING|GROUP\s+BY|ORDER\s+BY|HAVING|SET|WHEN|THEN|ELSE|CASE|BETWEEN|AS|DISTINCT|NOT|IN|IS|LIKE|ILIKE|LIMIT|OFFSET)\s+(\w*)$/i;

  // Also columns after a comma (continuing a SELECT list, GROUP BY list, etc.)
  const AFTER_COMMA = /,\s*(\w*)$/;

  // Alias.column — "a." where "a" is an alias for a table
  const ALIAS_DOT = /\b(\w+)\.\s*(\w*)$/;

  // Extract all schema.table references and their aliases from the full SQL text
  function extractTableRefs(text) {
    const refs = [];
    const pattern = /\b(?:FROM|JOIN)\s+(\w+)\.(\w+)(?:\s+(?:AS\s+)?(\w+))?/gi;
    let m;
    while ((m = pattern.exec(text)) !== null) {
      const schema = m[1], table = m[2], alias = m[3] || null;
      refs.push({ schema, table, alias });
    }
    return refs;
  }

  monaco.languages.registerCompletionItemProvider("sql", {
    triggerCharacters: [".", " ", ","],
    provideCompletionItems: async (model, position) => {
      const fullText = model.getValue();
      // All text up to the cursor position
      const offset = model.getOffsetAt(position);
      const textBefore = fullText.substring(0, offset);

      const tables = await getTablesCache();

      // 1) After "schema." — always suggest tables in that schema
      const dotMatch = textBefore.match(ALIAS_DOT);
      if (dotMatch) {
        const prefix = dotMatch[1].toLowerCase();
        const partial = (dotMatch[2] || "").toLowerCase();

        // Check if prefix is a known schema — suggest schema.table completions
        const schemaMatch = tables.some((t) => t.schema.toLowerCase() === prefix);
        if (schemaMatch) {
          const suggestions = [];
          for (const t of tables) {
            if (t.schema.toLowerCase() === prefix && t.name.toLowerCase().startsWith(partial)) {
              suggestions.push({
                label: `${t.schema}.${t.name}`,
                kind: monaco.languages.CompletionItemKind.Struct,
                insertText: t.name,
                detail: t.type || "table",
                range: {
                  startLineNumber: position.lineNumber,
                  startColumn: position.column - partial.length,
                  endLineNumber: position.lineNumber,
                  endColumn: position.column,
                },
              });
            }
          }
          return { suggestions };
        }

        // Check if prefix is a table alias — suggest columns
        const tableRefs = extractTableRefs(fullText);
        const aliasRef = tableRefs.find(
          (r) => (r.alias && r.alias.toLowerCase() === prefix) || (!r.alias && r.table.toLowerCase() === prefix)
        );
        if (aliasRef) {
          const info = await getColumnsCache(aliasRef.schema, aliasRef.table);
          if (info && info.columns) {
            return {
              suggestions: info.columns
                .filter((c) => c.name.toLowerCase().startsWith(partial))
                .map((c) => ({
                  label: c.name,
                  kind: monaco.languages.CompletionItemKind.Field,
                  insertText: c.name,
                  detail: `${aliasRef.schema}.${aliasRef.table} — ${c.type}`,
                  range: {
                    startLineNumber: position.lineNumber,
                    startColumn: position.column - partial.length,
                    endLineNumber: position.lineNumber,
                    endColumn: position.column,
                  },
                })),
            };
          }
        }

        return { suggestions: [] };
      }

      // 2) Table context — FROM, JOIN, INTO, etc.
      const tableCtx = textBefore.match(TABLE_CONTEXTS);
      if (tableCtx) {
        const partial = (tableCtx[1] || "").toLowerCase();
        const suggestions = [];
        for (const t of tables) {
          const full = `${t.schema}.${t.name}`;
          if (
            t.schema.toLowerCase().startsWith(partial) ||
            t.name.toLowerCase().startsWith(partial) ||
            full.toLowerCase().startsWith(partial)
          ) {
            suggestions.push({
              label: full,
              kind: monaco.languages.CompletionItemKind.Struct,
              insertText: full,
              detail: t.type || "table",
              range: {
                startLineNumber: position.lineNumber,
                startColumn: position.column - partial.length,
                endLineNumber: position.lineNumber,
                endColumn: position.column,
              },
            });
          }
        }
        return { suggestions };
      }

      // 3) Column context — SELECT, WHERE, ON, GROUP BY, etc. or after comma
      const colCtx = textBefore.match(COLUMN_CONTEXTS) || textBefore.match(AFTER_COMMA);
      if (colCtx) {
        const partial = (colCtx[1] || "").toLowerCase();
        const tableRefs = extractTableRefs(fullText);
        if (tableRefs.length === 0) return { suggestions: [] };

        // Fetch columns for all referenced tables
        const allCols = [];
        for (const ref of tableRefs) {
          const info = await getColumnsCache(ref.schema, ref.table);
          if (info && info.columns) {
            for (const col of info.columns) {
              allCols.push({ ...col, source: `${ref.schema}.${ref.table}` });
            }
          }
        }

        // Deduplicate by name (if same column in multiple tables, show source)
        const seen = new Map();
        for (const col of allCols) {
          const key = col.name.toLowerCase();
          if (seen.has(key)) { seen.get(key).ambiguous = true; }
          else { seen.set(key, { ...col, ambiguous: false }); }
        }

        const suggestions = [];
        for (const [, col] of seen) {
          if (col.name.toLowerCase().startsWith(partial)) {
            suggestions.push({
              label: col.name,
              kind: monaco.languages.CompletionItemKind.Field,
              insertText: col.name,
              detail: `${col.type} — ${col.source}`,
              range: {
                startLineNumber: position.lineNumber,
                startColumn: position.column - partial.length,
                endLineNumber: position.lineNumber,
                endColumn: position.column,
              },
            });
          }
        }
        return { suggestions };
      }

      return { suggestions: [] };
    },
  });

  // --- Hover provider ---
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
        quickSuggestions: true,
        suggestOnTriggerCharacters: true,
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
