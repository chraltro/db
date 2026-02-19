import React, { useState, useEffect, useRef } from "react";
import { format as formatSQL } from "sql-formatter";
import { api } from "./api";
import SortableTable from "./SortableTable";
import { useHintTriggerFn } from "./HintSystem";
import ResizeHandle from "./ResizeHandle";
import useResizable from "./useResizable";

const MAX_HISTORY = 50;

function getHistory() {
  try {
    return JSON.parse(localStorage.getItem("dp_query_history") || "[]");
  } catch { return []; }
}

function saveHistory(h) {
  localStorage.setItem("dp_query_history", JSON.stringify(h.slice(0, MAX_HISTORY)));
}

/**
 * Schema sidebar — lists schemas/tables. Click table name to insert
 * `schema.table` at cursor. Expand to see columns.
 */
function SchemaSidebar({ tables, onInsert }) {
  const [expanded, setExpanded] = useState({});

  const schemas = {};
  for (const t of tables) {
    if (!schemas[t.schema]) schemas[t.schema] = [];
    schemas[t.schema].push(t);
  }
  const SCHEMA_ORDER = ["landing", "bronze", "silver", "gold"];
  const schemaNames = Object.keys(schemas).sort((a, b) => {
    const ai = SCHEMA_ORDER.indexOf(a);
    const bi = SCHEMA_ORDER.indexOf(b);
    if (ai !== -1 && bi !== -1) return ai - bi;
    if (ai !== -1) return -1;
    if (bi !== -1) return 1;
    return a.localeCompare(b);
  });

  async function toggleExpand(key) {
    if (expanded[key]) {
      setExpanded((prev) => ({ ...prev, [key]: null }));
      return;
    }
    const [schema, table] = key.split(".");
    try {
      const info = await api.describeTable(schema, table);
      setExpanded((prev) => ({ ...prev, [key]: info.columns || [] }));
    } catch {
      setExpanded((prev) => ({ ...prev, [key]: [] }));
    }
  }

  if (tables.length === 0) {
    return <div style={sbSt.empty}>No tables in warehouse</div>;
  }

  return (
    <div style={sbSt.container}>
      <div style={sbSt.header}>Tables</div>
      <div style={sbSt.list}>
        {schemaNames.map((schema) => (
          <div key={schema}>
            <div style={sbSt.schemaRow}>
              <span style={sbSt.schemaName}>{schema}</span>
              <span style={sbSt.schemaCount}>{schemas[schema].length}</span>
            </div>
            {schemas[schema].map((t) => {
              const key = `${t.schema}.${t.name}`;
              const cols = expanded[key];
              return (
                <div key={key}>
                  <div style={sbSt.tableRow}>
                    <button onClick={() => toggleExpand(key)} style={sbSt.expandBtn}>
                      {cols ? "\u25BE" : "\u25B8"}
                    </button>
                    <button onClick={() => onInsert(key)} style={sbSt.tableName} title={`Insert ${key}`}>
                      {t.name}
                    </button>
                  </div>
                  {cols && cols.length > 0 && (
                    <div style={sbSt.colList}>
                      {cols.map((c) => (
                        <div key={c.name} style={sbSt.colItem} onClick={() => onInsert(c.name)} title={`Insert ${c.name}`}>
                          <span style={sbSt.colName}>{c.name}</span>
                          <span style={sbSt.colType}>{c.type}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        ))}
      </div>
    </div>
  );
}

const sbSt = {
  container: { width: "100%", height: "100%", overflow: "auto", background: "var(--dp-bg-tertiary)", fontSize: "12px" },
  header: { padding: "8px 10px", fontWeight: 600, fontSize: "11px", color: "var(--dp-text-secondary)", textTransform: "uppercase", letterSpacing: "0.3px", borderBottom: "1px solid var(--dp-border)" },
  list: { padding: "4px 0" },
  empty: { padding: "16px 10px", color: "var(--dp-text-dim)", fontSize: "12px", textAlign: "center" },
  schemaRow: { display: "flex", alignItems: "center", justifyContent: "space-between", padding: "5px 10px 2px", marginTop: "4px" },
  schemaName: { fontWeight: 600, color: "var(--dp-accent)", fontSize: "11px", textTransform: "uppercase", letterSpacing: "0.3px" },
  schemaCount: { fontSize: "10px", color: "var(--dp-text-dim)" },
  tableRow: { display: "flex", alignItems: "center", padding: "2px 6px 2px 8px" },
  expandBtn: { background: "none", border: "none", color: "var(--dp-text-dim)", cursor: "pointer", fontSize: "10px", padding: "2px 4px", width: "18px" },
  tableName: { background: "none", border: "none", color: "var(--dp-text)", cursor: "pointer", fontSize: "12px", fontFamily: "var(--dp-font-mono)", padding: "2px 4px", textAlign: "left", flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" },
  colList: { paddingLeft: "30px", paddingBottom: "2px" },
  colItem: { display: "flex", justifyContent: "space-between", gap: "6px", padding: "1px 8px", cursor: "pointer", borderRadius: "var(--dp-radius)" },
  colName: { fontFamily: "var(--dp-font-mono)", color: "var(--dp-text-secondary)", fontSize: "11px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" },
  colType: { color: "var(--dp-text-dim)", fontSize: "10px", flexShrink: 0 },
};

export default function QueryPanel({ addOutput }) {
  const [sql, setSql] = useState("");
  const [results, setResults] = useState(null);
  const [queryRunning, setQueryRunning] = useState(false);
  const [error, setError] = useState(null);
  const [history, setHistory] = useState(getHistory);
  const [tables, setTables] = useState([]);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [viewMode, setViewMode] = useState("table");
  const textareaRef = useRef(null);
  const historyRef = useRef(null);
  const setHintTrigger = useHintTriggerFn();
  const [sidebarWidth, onSidebarResize, onSidebarResizeStart] = useResizable("dp_query_sidebar_width", 200, 120, 400);
  const [editorHeight, onEditorResize, onEditorResizeStart] = useResizable("dp_query_editor_height", 120, 60, 500);

  useEffect(() => {
    api.listTables().then(setTables).catch(() => {});
    setHintTrigger("queryPanelOpened", true);
  }, []);

  // Listen for prefill events from TablesPanel "Query this table"
  useEffect(() => {
    function handlePrefill() {
      if (window.__dp_prefill_query) {
        setSql(window.__dp_prefill_query);
        delete window.__dp_prefill_query;
      }
    }
    window.addEventListener("dp_prefill_query", handlePrefill);
    return () => window.removeEventListener("dp_prefill_query", handlePrefill);
  }, []);

  // Close history dropdown on outside click
  useEffect(() => {
    if (!historyOpen) return;
    const handler = (e) => {
      if (historyRef.current && !historyRef.current.contains(e.target)) setHistoryOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [historyOpen]);

  async function runQuery() {
    if (!sql.trim()) return;
    setQueryRunning(true);
    setError(null);
    try {
      const data = await api.runQuery(sql);
      setResults(data);
      const newHistory = [{ sql: sql.trim(), ts: new Date().toISOString() }, ...history.filter((h) => h.sql !== sql.trim())];
      setHistory(newHistory);
      saveHistory(newHistory);
      addOutput("info", `Query: ${data.rows.length} rows (${data.columns.length} cols)`);
    } catch (e) {
      setError(e.message);
      addOutput("error", `Query error: ${e.message}`);
    } finally {
      setQueryRunning(false);
    }
  }

  function formatQuery() {
    if (!sql.trim()) return;
    try {
      const formatted = formatSQL(sql, { language: "sql", keywordCase: "upper", indentStyle: "standard" });
      setSql(formatted);
    } catch {
      // leave as-is if formatter fails
    }
  }

  function handleKeyDown(e) {
    if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
      e.preventDefault();
      runQuery();
    }
    if ((e.ctrlKey || e.metaKey) && e.shiftKey && e.key === "F") {
      e.preventDefault();
      formatQuery();
    }
  }

  function insertAtCursor(text) {
    const ta = textareaRef.current;
    if (!ta) {
      setSql((prev) => prev + (prev ? " " : "") + text);
      return;
    }
    const start = ta.selectionStart;
    const end = ta.selectionEnd;
    const before = sql.substring(0, start);
    const after = sql.substring(end);
    const newSql = before + text + after;
    setSql(newSql);
    setTimeout(() => {
      ta.focus();
      ta.selectionStart = ta.selectionEnd = start + text.length;
    }, 0);
  }

  // Generate starter query suggestions from available tables
  function getSuggestions() {
    if (tables.length === 0) return [];
    const suggestions = [];
    const TIER = ["gold", "silver", "bronze", "landing"];
    for (const tier of TIER) {
      const t = tables.find((t) => t.schema === tier);
      if (t) {
        suggestions.push({ sql: `SELECT * FROM ${t.schema}.${t.name} LIMIT 10`, label: `Preview ${t.schema}.${t.name}` });
        if (suggestions.length === 1) {
          suggestions.push({ sql: `SELECT COUNT(*) AS total FROM ${t.schema}.${t.name}`, label: `Count ${t.schema}.${t.name}` });
        }
      }
      if (suggestions.length >= 3) break;
    }
    if (suggestions.length < 4) {
      suggestions.push({
        sql: "SELECT table_schema, table_name FROM information_schema.tables\nWHERE table_schema NOT IN ('information_schema', '_dp_internal')\nORDER BY table_schema, table_name",
        label: "List all tables",
      });
    }
    return suggestions;
  }

  const suggestions = getSuggestions();

  return (
    <div style={st.container}>
      <div style={st.main}>
        {/* Schema sidebar */}
        <div data-dp-hint="query-sidebar" style={{ display: "flex", flexDirection: "column", width: sidebarWidth, flexShrink: 0 }}>
          <SchemaSidebar tables={tables} onInsert={insertAtCursor} />
        </div>
        <ResizeHandle direction="horizontal" onResize={onSidebarResize} onResizeStart={onSidebarResizeStart} />

        {/* Query area */}
        <div style={st.queryArea}>
          {/* Toolbar */}
          <div style={st.toolbar}>
            <button onClick={runQuery} disabled={queryRunning || !sql.trim()} style={st.runBtn}>
              {queryRunning ? "Running..." : "Run"} <span style={st.shortcut}>Ctrl+Enter</span>
            </button>
            <button onClick={formatQuery} disabled={!sql.trim()} style={st.fmtBtn} title="Format SQL (Ctrl+Shift+F)">
              Format <span style={st.shortcut}>Ctrl+Shift+F</span>
            </button>

            {/* History dropdown */}
            <div ref={historyRef} style={st.historyWrapper}>
              <button onClick={() => setHistoryOpen(!historyOpen)} style={st.historyBtn} title="Query history">
                {"\u23F0"} {history.length > 0 && <span style={st.historyCount}>{history.length}</span>}
              </button>
              {historyOpen && (
                <div style={st.historyDropdown}>
                  <div style={st.historyHeader}>Recent Queries</div>
                  {history.length === 0 ? (
                    <div style={st.historyEmpty}>No history yet</div>
                  ) : (
                    history.slice(0, 20).map((h, i) => (
                      <button
                        key={i}
                        onClick={() => { setSql(h.sql); setHistoryOpen(false); }}
                        style={st.historyItem}
                        onMouseEnter={(e) => e.currentTarget.style.background = "var(--dp-btn-bg)"}
                        onMouseLeave={(e) => e.currentTarget.style.background = "none"}
                      >
                        <span style={st.historySQL}>{h.sql.substring(0, 80)}{h.sql.length > 80 ? "..." : ""}</span>
                      </button>
                    ))
                  )}
                </div>
              )}
            </div>

            {results && (
              <div style={st.viewToggle}>
                <button onClick={() => setViewMode("table")} style={viewMode === "table" ? st.viewBtnActive : st.viewBtn}>Table</button>
                <button onClick={() => setViewMode("chart")} style={viewMode === "chart" ? st.viewBtnActive : st.viewBtn}>Chart</button>
              </div>
            )}
          </div>

          {/* SQL textarea */}
          <div style={{ ...st.editorWrapper, height: editorHeight }}>
            <textarea
              ref={textareaRef}
              value={sql}
              onChange={(e) => setSql(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Write SQL here... (Ctrl+Enter to run)"
              style={{ ...st.textarea, height: "100%", resize: "none" }}
              spellCheck={false}
            />
          </div>
          <ResizeHandle direction="vertical" onResize={onEditorResize} onResizeStart={onEditorResizeStart} />

          {/* Starter suggestions when textarea is empty */}
          {!sql.trim() && suggestions.length > 0 && (
            <div style={st.suggestions}>
              <span style={st.suggestLabel}>Try:</span>
              {suggestions.map((s, i) => (
                <button key={i} onClick={() => setSql(s.sql)} style={st.suggestBtn}>
                  {s.label}
                </button>
              ))}
            </div>
          )}

          {/* Error */}
          {error && (
            <div style={st.error}>
              <span style={st.errorLabel}>Error</span>
              {error}
            </div>
          )}

          {/* Results */}
          {results && (
            <div style={st.results}>
              <div style={st.resultsHeader}>
                <span>{results.rows.length} row{results.rows.length !== 1 ? "s" : ""}, {results.columns.length} column{results.columns.length !== 1 ? "s" : ""}</span>
              </div>
              <div style={st.resultsBody}>
                {viewMode === "table" ? (
                  <SortableTable columns={results.columns} rows={results.rows} />
                ) : (
                  <div style={st.chartPlaceholder}>Chart view available for numeric results.</div>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

const st = {
  container: { display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" },
  main: { display: "flex", flex: 1, overflow: "hidden" },

  queryArea: { flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" },
  toolbar: { display: "flex", alignItems: "center", gap: "8px", padding: "6px 12px", borderBottom: "1px solid var(--dp-border)", flexShrink: 0 },
  runBtn: {
    padding: "5px 14px",
    background: "var(--dp-green)",
    border: "1px solid var(--dp-green-border)",
    borderRadius: "var(--dp-radius-lg)",
    color: "#fff",
    cursor: "pointer",
    fontSize: "12px",
    fontWeight: 600,
    display: "flex",
    alignItems: "center",
    gap: "6px",
  },
  shortcut: { fontSize: "10px", opacity: 0.7 },
  fmtBtn: { padding: "5px 10px", background: "var(--dp-btn-bg)", border: "1px solid var(--dp-btn-border)", borderRadius: "var(--dp-radius-lg)", color: "var(--dp-text-secondary)", cursor: "pointer", fontSize: "12px", fontWeight: 500, display: "flex", alignItems: "center", gap: "6px" },

  historyWrapper: { position: "relative" },
  historyBtn: { background: "var(--dp-btn-bg)", border: "1px solid var(--dp-btn-border)", borderRadius: "var(--dp-radius-lg)", color: "var(--dp-text-secondary)", cursor: "pointer", padding: "4px 8px", fontSize: "14px", display: "flex", alignItems: "center", gap: "4px" },
  historyCount: { fontSize: "10px", background: "var(--dp-bg-tertiary)", padding: "0 5px", borderRadius: "8px", color: "var(--dp-text-dim)", fontWeight: 500 },
  historyDropdown: { position: "absolute", top: "100%", left: 0, marginTop: "4px", background: "var(--dp-bg-secondary)", border: "1px solid var(--dp-border)", borderRadius: "var(--dp-radius)", zIndex: 100, width: "400px", maxHeight: "300px", overflow: "auto", boxShadow: "0 4px 12px rgba(0,0,0,0.3)" },
  historyHeader: { padding: "6px 12px", fontSize: "11px", fontWeight: 600, color: "var(--dp-text-secondary)", borderBottom: "1px solid var(--dp-border)" },
  historyEmpty: { padding: "16px", color: "var(--dp-text-dim)", fontSize: "12px", textAlign: "center" },
  historyItem: { display: "block", width: "100%", padding: "6px 12px", background: "none", border: "none", borderBottom: "1px solid var(--dp-border)", color: "var(--dp-text)", cursor: "pointer", textAlign: "left", fontSize: "12px" },
  historySQL: { fontFamily: "var(--dp-font-mono)", fontSize: "11px", color: "var(--dp-text)", display: "block", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" },

  viewToggle: { display: "flex", marginLeft: "auto", gap: "1px", background: "var(--dp-border)", borderRadius: "var(--dp-radius-lg)", overflow: "hidden" },
  viewBtn: { padding: "3px 10px", background: "var(--dp-btn-bg)", border: "none", color: "var(--dp-text-secondary)", cursor: "pointer", fontSize: "11px", fontWeight: 500 },
  viewBtnActive: { padding: "3px 10px", background: "var(--dp-bg-secondary)", border: "none", color: "var(--dp-text)", cursor: "pointer", fontSize: "11px", fontWeight: 600 },

  editorWrapper: { flexShrink: 0 },
  textarea: { width: "100%", height: "100%", padding: "10px 12px", background: "var(--dp-bg)", border: "none", color: "var(--dp-text)", fontFamily: "var(--dp-font-mono)", fontSize: "13px", resize: "none", outline: "none", boxSizing: "border-box", lineHeight: 1.5 },

  suggestions: { display: "flex", alignItems: "center", gap: "6px", padding: "8px 12px", flexWrap: "wrap", borderBottom: "1px solid var(--dp-border)" },
  suggestLabel: { fontSize: "11px", color: "var(--dp-text-dim)", fontWeight: 500 },
  suggestBtn: { padding: "3px 10px", background: "var(--dp-bg-secondary)", border: "1px solid var(--dp-border)", borderRadius: "var(--dp-radius-lg)", color: "var(--dp-accent)", cursor: "pointer", fontSize: "11px", fontWeight: 500 },

  error: { padding: "8px 12px", background: "rgba(255,0,0,0.05)", borderBottom: "1px solid var(--dp-red)", color: "var(--dp-red)", fontSize: "12px", fontFamily: "var(--dp-font-mono)", whiteSpace: "pre-wrap" },
  errorLabel: { fontWeight: 600, marginRight: "8px" },

  results: { flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" },
  resultsHeader: { padding: "6px 12px", fontSize: "11px", color: "var(--dp-text-secondary)", borderBottom: "1px solid var(--dp-border)", flexShrink: 0 },
  resultsBody: { flex: 1, overflow: "auto" },
  chartPlaceholder: { padding: "32px", color: "var(--dp-text-dim)", textAlign: "center", fontSize: "13px" },
};
