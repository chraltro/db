import React, { useState, useEffect, useCallback } from "react";
import { api } from "./api";

const SOURCE_TYPES = [
  { value: "csv", label: "CSV File" },
  { value: "parquet", label: "Parquet File" },
  { value: "json", label: "JSON File" },
  { value: "url", label: "URL" },
  { value: "database", label: "Database Connection" },
];

const DEFAULT_INGEST = {
  source_type: "csv",
  source_path: "",
  target_schema: "landing",
  target_table: "",
  connection: "",
  options: {},
};

function CellOutput({ outputs }) {
  if (!outputs || outputs.length === 0) return null;
  return (
    <div style={cs.outputArea}>
      {outputs.map((out, i) => {
        if (out.type === "table") {
          return (
            <div key={i} style={cs.tableWrap}>
              <table style={cs.table}>
                <thead>
                  <tr>
                    {out.columns.map((col, j) => (
                      <th key={j} style={cs.th}>{col}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {out.rows.slice(0, 200).map((row, ri) => (
                    <tr key={ri}>
                      {row.map((v, ci) => (
                        <td key={ci} style={cs.td}>
                          {v === null ? <span style={cs.null}>NULL</span> : String(v)}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
              {out.total_rows > 200 && (
                <div style={cs.truncated}>Showing 200 of {out.total_rows} rows</div>
              )}
            </div>
          );
        }
        if (out.type === "error") {
          return <pre key={i} style={cs.error}>{out.text}</pre>;
        }
        return <pre key={i} style={cs.text}>{out.text}</pre>;
      })}
    </div>
  );
}

// --- Ingest cell form editor ---

function IngestCellEditor({ cell, notebookName, onUpdate, onDelete, externalRunning }) {
  // Parse existing source JSON or use defaults
  const parseSource = (src) => {
    if (!src) return { ...DEFAULT_INGEST };
    try {
      return { ...DEFAULT_INGEST, ...JSON.parse(src) };
    } catch {
      return { ...DEFAULT_INGEST };
    }
  };

  const [spec, setSpec] = useState(() => parseSource(cell.source));
  const [showSource, setShowSource] = useState(false);
  const [rawSource, setRawSource] = useState(cell.source || JSON.stringify(DEFAULT_INGEST, null, 2));
  const [outputs, setOutputs] = useState(cell.outputs || []);
  const [running, setRunning] = useState(false);
  const [duration, setDuration] = useState(cell.duration_ms);
  const [connections, setConnections] = useState([]);
  const [showOptions, setShowOptions] = useState(false);
  const isRunning = running || externalRunning;

  useEffect(() => {
    setOutputs(cell.outputs || []);
    setDuration(cell.duration_ms);
  }, [cell.outputs, cell.duration_ms]);

  useEffect(() => {
    const parsed = parseSource(cell.source);
    setSpec(parsed);
    setRawSource(cell.source || JSON.stringify(DEFAULT_INGEST, null, 2));
  }, [cell.id]);

  // Fetch connections when database type is selected
  useEffect(() => {
    if (spec.source_type === "database") {
      api.listConfiguredConnectors().then(setConnections).catch(() => {});
    }
  }, [spec.source_type]);

  const updateSpec = useCallback((updates) => {
    setSpec((prev) => {
      const next = { ...prev, ...updates };
      const json = JSON.stringify(next);
      setRawSource(JSON.stringify(next, null, 2));
      onUpdate({ ...cell, source: json });
      return next;
    });
  }, [cell, onUpdate]);

  // Auto-populate table name from file path
  function handlePathChange(path) {
    const updates = { source_path: path };
    if (!spec.target_table || spec.target_table === fileNameToTable(spec.source_path)) {
      updates.target_table = fileNameToTable(path);
    }
    updateSpec(updates);
  }

  function fileNameToTable(path) {
    if (!path) return "";
    const name = path.split("/").pop().split("\\").pop();
    return name.replace(/\.[^.]+$/, "").replace(/[^a-zA-Z0-9_]/g, "_").toLowerCase();
  }

  async function runCell() {
    setRunning(true);
    try {
      const json = showSource ? rawSource : JSON.stringify(spec);
      const result = await api.runCell(notebookName, json, { cell_type: "ingest" });
      setOutputs(result.outputs);
      setDuration(result.duration_ms);
      onUpdate({ ...cell, source: json, outputs: result.outputs, duration_ms: result.duration_ms });
    } catch (e) {
      setOutputs([{ type: "error", text: e.message }]);
    }
    setRunning(false);
  }

  function handleRawSourceChange(val) {
    setRawSource(val);
    try {
      const parsed = JSON.parse(val);
      setSpec({ ...DEFAULT_INGEST, ...parsed });
      onUpdate({ ...cell, source: val });
    } catch {
      // Invalid JSON — just update raw, don't sync spec
    }
  }

  const isFile = ["csv", "parquet", "json"].includes(spec.source_type);
  const isUrl = spec.source_type === "url";
  const isDb = spec.source_type === "database";

  return (
    <div style={cs.codeCell}>
      <div style={cs.cellHeader}>
        <button onClick={runCell} disabled={isRunning} style={cs.runBtn}>
          {isRunning ? "..." : "\u25B6"}
        </button>
        <span style={{ ...cs.cellType, color: "var(--dp-blue, var(--dp-text-dim))" }}>INGEST</span>
        <span style={{ flex: 1 }} />
        <button
          onClick={() => setShowSource(!showSource)}
          style={cs.toggleBtn}
          title={showSource ? "Show form" : "Show source"}
        >
          {showSource ? "Form" : "{ }"}
        </button>
        {duration != null && <span style={cs.duration}>{duration}ms</span>}
        <button data-dp-danger="" onClick={onDelete} style={cs.deleteBtn} title="Delete cell">&times;</button>
      </div>

      {showSource ? (
        <textarea
          value={rawSource}
          onChange={(e) => handleRawSourceChange(e.target.value)}
          onKeyDown={(e) => {
            if ((e.ctrlKey || e.metaKey) && e.key === "Enter") { e.preventDefault(); runCell(); }
          }}
          style={cs.codeInput}
          rows={Math.max(4, rawSource.split("\n").length + 1)}
          spellCheck={false}
        />
      ) : (
        <div style={ig.form}>
          {/* Source Type */}
          <div style={ig.row}>
            <label style={ig.label}>Source</label>
            <select
              value={spec.source_type}
              onChange={(e) => updateSpec({ source_type: e.target.value, connection: "", source_path: "" })}
              style={ig.select}
            >
              {SOURCE_TYPES.map((t) => (
                <option key={t.value} value={t.value}>{t.label}</option>
              ))}
            </select>
          </div>

          {/* File path for CSV/Parquet/JSON */}
          {isFile && (
            <div style={ig.row}>
              <label style={ig.label}>File Path</label>
              <input
                value={spec.source_path}
                onChange={(e) => handlePathChange(e.target.value)}
                placeholder="data/earthquakes.csv"
                style={ig.input}
              />
            </div>
          )}

          {/* URL input */}
          {isUrl && (
            <div style={ig.row}>
              <label style={ig.label}>URL</label>
              <input
                value={spec.source_path}
                onChange={(e) => handlePathChange(e.target.value)}
                placeholder="https://example.com/data.csv"
                style={ig.input}
              />
            </div>
          )}

          {/* Database connection picker */}
          {isDb && (
            <div style={ig.row}>
              <label style={ig.label}>Connection</label>
              <select
                value={spec.connection}
                onChange={(e) => updateSpec({ connection: e.target.value })}
                style={ig.select}
              >
                <option value="">Select connection...</option>
                {connections.map((c) => (
                  <option key={c.name} value={c.name}>{c.name} ({c.type})</option>
                ))}
              </select>
            </div>
          )}
          {isDb && (
            <div style={ig.row}>
              <label style={ig.label}>Source Table</label>
              <input
                value={spec.source_path}
                onChange={(e) => handlePathChange(e.target.value)}
                placeholder="public.users"
                style={ig.input}
              />
            </div>
          )}

          {/* Target schema + table */}
          <div style={ig.row}>
            <label style={ig.label}>Target</label>
            <div style={ig.targetRow}>
              <input
                value={spec.target_schema}
                onChange={(e) => updateSpec({ target_schema: e.target.value })}
                placeholder="landing"
                style={{ ...ig.input, flex: "0 0 120px" }}
              />
              <span style={ig.dot}>.</span>
              <input
                value={spec.target_table}
                onChange={(e) => updateSpec({ target_table: e.target.value })}
                placeholder="table_name"
                style={ig.input}
              />
            </div>
          </div>

          {/* Advanced options (collapsible) */}
          <div style={ig.row}>
            <button onClick={() => setShowOptions(!showOptions)} style={ig.optionsToggle}>
              {showOptions ? "\u25BC" : "\u25B6"} Options
            </button>
          </div>
          {showOptions && (
            <div style={ig.optionsArea}>
              <textarea
                value={JSON.stringify(spec.options || {}, null, 2)}
                onChange={(e) => {
                  try {
                    updateSpec({ options: JSON.parse(e.target.value) });
                  } catch { /* ignore invalid JSON while typing */ }
                }}
                placeholder='{"delimiter": ",", "header": true}'
                style={{ ...cs.codeInput, minHeight: "48px" }}
                rows={3}
                spellCheck={false}
              />
            </div>
          )}
        </div>
      )}

      <CellOutput outputs={outputs} />
    </div>
  );
}

// --- SQL cell ---

function SqlCell({ cell, notebookName, onUpdate, onDelete, externalRunning }) {
  const [source, setSource] = useState(cell.source || "");
  const [outputs, setOutputs] = useState(cell.outputs || []);
  const [running, setRunning] = useState(false);
  const [duration, setDuration] = useState(cell.duration_ms);
  const isRunning = running || externalRunning;

  useEffect(() => {
    setOutputs(cell.outputs || []);
    setDuration(cell.duration_ms);
  }, [cell.outputs, cell.duration_ms]);

  useEffect(() => { setSource(cell.source || ""); }, [cell.id]);

  async function runCell() {
    setRunning(true);
    try {
      const result = await api.runCell(notebookName, source, { cell_type: "sql" });
      setOutputs(result.outputs);
      setDuration(result.duration_ms);
      onUpdate({ ...cell, source, outputs: result.outputs, duration_ms: result.duration_ms });
    } catch (e) {
      setOutputs([{ type: "error", text: e.message }]);
    }
    setRunning(false);
  }

  return (
    <div style={cs.codeCell}>
      <div style={cs.cellHeader}>
        <button onClick={runCell} disabled={isRunning} style={cs.runBtn}>
          {isRunning ? "..." : "\u25B6"}
        </button>
        <span style={cs.cellType}>SQL</span>
        <span style={{ flex: 1 }} />
        {duration != null && <span style={cs.duration}>{duration}ms</span>}
        <button data-dp-danger="" onClick={onDelete} style={cs.deleteBtn} title="Delete cell">&times;</button>
      </div>
      <textarea
        value={source}
        onChange={(e) => {
          setSource(e.target.value);
          onUpdate({ ...cell, source: e.target.value });
        }}
        onKeyDown={(e) => {
          if ((e.ctrlKey || e.metaKey) && e.key === "Enter") { e.preventDefault(); runCell(); }
        }}
        style={cs.codeInput}
        rows={Math.max(3, source.split("\n").length + 1)}
        spellCheck={false}
      />
      <CellOutput outputs={outputs} />
    </div>
  );
}

// --- Code cell ---

function CodeCell({ cell, notebookName, onUpdate, onDelete, externalRunning }) {
  const [source, setSource] = useState(cell.source || "");
  const [outputs, setOutputs] = useState(cell.outputs || []);
  const [running, setRunning] = useState(false);
  const [duration, setDuration] = useState(cell.duration_ms);
  const isRunning = running || externalRunning;

  useEffect(() => {
    setOutputs(cell.outputs || []);
    setDuration(cell.duration_ms);
  }, [cell.outputs, cell.duration_ms]);

  useEffect(() => { setSource(cell.source || ""); }, [cell.id]);

  async function runCell() {
    setRunning(true);
    try {
      const result = await api.runCell(notebookName, source, { cell_type: "code" });
      setOutputs(result.outputs);
      setDuration(result.duration_ms);
      onUpdate({ ...cell, source, outputs: result.outputs, duration_ms: result.duration_ms });
    } catch (e) {
      setOutputs([{ type: "error", text: e.message }]);
    }
    setRunning(false);
  }

  return (
    <div style={cs.codeCell}>
      <div style={cs.cellHeader}>
        <button onClick={runCell} disabled={isRunning} style={cs.runBtn}>
          {isRunning ? "..." : "\u25B6"}
        </button>
        <span style={cs.cellType}>PY</span>
        <span style={{ flex: 1 }} />
        {duration != null && <span style={cs.duration}>{duration}ms</span>}
        <button data-dp-danger="" onClick={onDelete} style={cs.deleteBtn} title="Delete cell">&times;</button>
      </div>
      <textarea
        value={source}
        onChange={(e) => {
          setSource(e.target.value);
          onUpdate({ ...cell, source: e.target.value });
        }}
        onKeyDown={(e) => {
          if ((e.ctrlKey || e.metaKey) && e.key === "Enter") { e.preventDefault(); runCell(); }
        }}
        style={cs.codeInput}
        rows={Math.max(3, source.split("\n").length + 1)}
        spellCheck={false}
      />
      <CellOutput outputs={outputs} />
    </div>
  );
}

// --- Dispatcher ---

function NotebookCell({ cell, notebookName, onUpdate, onDelete, externalRunning }) {
  if (cell.type === "markdown") {
    return (
      <MarkdownCell cell={cell} onUpdate={onUpdate} onDelete={onDelete} />
    );
  }
  if (cell.type === "sql") {
    return (
      <SqlCell cell={cell} notebookName={notebookName} onUpdate={onUpdate} onDelete={onDelete} externalRunning={externalRunning} />
    );
  }
  if (cell.type === "ingest") {
    return (
      <IngestCellEditor cell={cell} notebookName={notebookName} onUpdate={onUpdate} onDelete={onDelete} externalRunning={externalRunning} />
    );
  }
  return (
    <CodeCell cell={cell} notebookName={notebookName} onUpdate={onUpdate} onDelete={onDelete} externalRunning={externalRunning} />
  );
}

function MarkdownCell({ cell, onUpdate, onDelete }) {
  const [source, setSource] = useState(cell.source || "");
  useEffect(() => { setSource(cell.source || ""); }, [cell.id]);

  return (
    <div style={cs.mdCell}>
      <div style={cs.cellHeader}>
        <span style={cs.cellType}>MD</span>
        <span style={{ flex: 1 }} />
        <button data-dp-danger="" onClick={onDelete} style={cs.deleteBtn} title="Delete cell">&times;</button>
      </div>
      <textarea
        value={source}
        onChange={(e) => {
          setSource(e.target.value);
          onUpdate({ ...cell, source: e.target.value });
        }}
        style={cs.mdInput}
        rows={Math.max(2, source.split("\n").length)}
      />
    </div>
  );
}

export default function NotebookPanel({ openPath }) {
  const [notebooks, setNotebooks] = useState([]);
  const [active, setActive] = useState(null);
  const [notebook, setNotebook] = useState(null);
  const [newName, setNewName] = useState("");
  const [runningAll, setRunningAll] = useState(false);
  const [runningCellId, setRunningCellId] = useState(null);
  const [nbError, setNbError] = useState(null);

  useEffect(() => { loadList(); }, []);

  // Open a notebook when openPath changes (e.g. clicked from file tree)
  useEffect(() => {
    if (openPath && openPath !== active) {
      openNotebook(openPath);
    }
  }, [openPath]);

  async function loadList() {
    try {
      const data = await api.listNotebooks();
      setNotebooks(data);
    } catch (e) { setNbError(e.message || "Failed to load notebooks"); }
  }

  async function openNotebook(name) {
    setNbError(null);
    try {
      const nb = await api.getNotebook(name);
      setActive(name);
      setNotebook(nb);
    } catch (e) {
      setNbError(e.message || "Failed to open notebook");
    }
  }

  async function createNotebook() {
    const name = newName.trim().replace(/\s+/g, "_") || "untitled";
    setNbError(null);
    try {
      const nb = await api.createNotebook(name, name);
      setNewName("");
      await loadList();
      setActive(name);
      setNotebook(nb);
    } catch (e) {
      setNbError(e.message || "Failed to create notebook");
    }
  }

  async function saveNotebook() {
    if (!active || !notebook) return;
    try {
      await api.saveNotebook(active, notebook);
    } catch (e) {
      setNbError(e.message || "Failed to save notebook");
    }
  }

  async function runAll() {
    if (!active || !notebook || runningAll) return;
    setRunningAll(true);
    setNbError(null);
    try {
      await saveNotebook();
      const cells = [...notebook.cells];
      let firstCode = true;
      for (let i = 0; i < cells.length; i++) {
        const cell = cells[i];
        if (cell.type === "markdown") continue;
        setRunningCellId(cell.id);
        const cellType = cell.type || "code";
        try {
          const result = await api.runCell(active, cell.source, { reset: firstCode, cell_type: cellType });
          firstCode = false;
          cells[i] = { ...cell, outputs: result.outputs, duration_ms: result.duration_ms };
          setNotebook((prev) => ({ ...prev, cells: [...cells] }));
        } catch (e) {
          cells[i] = { ...cell, outputs: [{ type: "error", text: e.message }] };
          setNotebook((prev) => ({ ...prev, cells: [...cells] }));
          break;
        }
      }
    } catch (e) {
      setNbError(e.message || "Failed to run notebook");
    }
    setRunningCellId(null);
    setRunningAll(false);
  }

  function addCell(type) {
    if (!notebook) return;
    const id = "cell_" + Math.random().toString(36).slice(2, 8);
    let source = "";
    if (type === "markdown") source = "## Heading";
    else if (type === "ingest") source = JSON.stringify(DEFAULT_INGEST, null, 2);
    else if (type === "sql") source = "SELECT 1";
    const cell = { id, type, source, outputs: [] };
    setNotebook({ ...notebook, cells: [...notebook.cells, cell] });
  }

  function updateCell(idx, updated) {
    const cells = [...notebook.cells];
    cells[idx] = updated;
    setNotebook({ ...notebook, cells });
  }

  function deleteCell(idx) {
    const cells = notebook.cells.filter((_, i) => i !== idx);
    setNotebook({ ...notebook, cells });
  }

  if (!notebook) {
    return (
      <div style={s.container}>
        <div style={s.listHeader}>
          <span style={s.title}>Notebooks</span>
          <div style={s.newRow}>
            <input
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
              placeholder="notebook name..."
              style={s.input}
              onKeyDown={(e) => e.key === "Enter" && createNotebook()}
            />
            <button onClick={createNotebook} style={s.btn}>New</button>
          </div>
        </div>
        {nbError && <div style={{ color: "var(--dp-red)", fontSize: "12px", padding: "6px 12px" }}>{nbError}</div>}
        <div style={s.list}>
          {notebooks.length === 0 && (
            <div style={s.empty}>No notebooks yet. Create one above.</div>
          )}
          {notebooks.map((nb) => (
            <div key={nb.path} data-dp-notebook="" onClick={() => openNotebook(nb.path)} style={s.nbItem}>
              <span style={s.nbName}>{nb.title || nb.name}</span>
              <span style={s.nbMeta}><span style={s.nbPath}>{nb.path}</span> · {nb.cells} cells</span>
            </div>
          ))}
        </div>
      </div>
    );
  }

  return (
    <div style={s.container}>
      <div style={s.nbHeader}>
        <button onClick={() => { setNotebook(null); setActive(null); }} style={s.backBtn}>&larr; Back</button>
        <span style={s.nbTitle}>{notebook.title || active}</span>
        <div style={s.nbActions}>
          <button onClick={() => addCell("sql")} style={s.btn}>+ SQL</button>
          <button onClick={() => addCell("code")} style={s.btn}>+ Code</button>
          <button onClick={() => addCell("ingest")} style={s.btn}>+ Ingest</button>
          <button onClick={() => addCell("markdown")} style={s.btn}>+ Markdown</button>
          <button onClick={saveNotebook} style={s.btn}>Save</button>
          <button onClick={runAll} disabled={runningAll} style={s.runAllBtn}>
            {runningAll ? "Running..." : "Run All"}
          </button>
        </div>
      </div>
      <div style={s.cells}>
        {notebook.cells.length === 0 && (
          <div style={s.empty}>No cells yet. Add a SQL, code, ingest, or markdown cell above.</div>
        )}
        {notebook.cells.map((cell, i) => (
          <div key={cell.id || i} style={cs.cellWrap}>
            <NotebookCell
              cell={cell}
              notebookName={active}
              onUpdate={(updated) => updateCell(i, updated)}
              onDelete={() => deleteCell(i)}
              externalRunning={runningCellId === cell.id}
            />
          </div>
        ))}
      </div>
    </div>
  );
}

const s = {
  container: { display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" },
  listHeader: { padding: "12px 16px", borderBottom: "1px solid var(--dp-border)" },
  title: { fontSize: "16px", fontWeight: 600 },
  newRow: { display: "flex", gap: "8px", marginTop: "8px" },
  input: { flex: 1, padding: "6px 10px", background: "var(--dp-bg-tertiary)", border: "1px solid var(--dp-border-light)", borderRadius: "var(--dp-radius-lg)", color: "var(--dp-text)", fontSize: "13px" },
  btn: { padding: "5px 12px", background: "var(--dp-btn-bg)", border: "1px solid var(--dp-btn-border)", borderRadius: "var(--dp-radius-lg)", color: "var(--dp-text)", cursor: "pointer", fontSize: "12px", fontWeight: 500 },
  list: { flex: 1, overflow: "auto", padding: "8px" },
  empty: { color: "var(--dp-text-dim)", textAlign: "center", padding: "24px" },
  nbItem: { padding: "10px 12px", borderRadius: "var(--dp-radius-lg)", cursor: "pointer", display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "4px", border: "1px solid var(--dp-border)" },
  nbName: { fontWeight: 500, fontSize: "13px" },
  nbMeta: { color: "var(--dp-text-secondary)", fontSize: "12px" },
  nbPath: { fontFamily: "var(--dp-font-mono)", fontSize: "11px", color: "var(--dp-text-dim)" },
  nbHeader: { display: "flex", alignItems: "center", gap: "12px", padding: "8px 12px", borderBottom: "1px solid var(--dp-border)" },
  backBtn: { padding: "4px 8px", background: "none", border: "1px solid var(--dp-border-light)", borderRadius: "var(--dp-radius-lg)", color: "var(--dp-text-secondary)", cursor: "pointer", fontSize: "12px" },
  nbTitle: { fontWeight: 600, fontSize: "14px", flex: 1 },
  nbActions: { display: "flex", gap: "6px" },
  runAllBtn: { padding: "5px 12px", background: "var(--dp-green)", border: "1px solid var(--dp-green-border)", borderRadius: "var(--dp-radius-lg)", color: "#fff", cursor: "pointer", fontSize: "12px", fontWeight: 500 },
  cells: { flex: 1, overflow: "auto", padding: "12px 16px" },
};

const cs = {
  cellWrap: { position: "relative", marginBottom: "10px" },
  codeCell: { border: "1px solid var(--dp-border)", borderRadius: "var(--dp-radius-lg)", background: "var(--dp-bg-tertiary)", overflow: "hidden" },
  mdCell: { border: "1px solid var(--dp-border)", borderRadius: "var(--dp-radius-lg)", background: "var(--dp-bg-tertiary)", overflow: "hidden" },
  cellHeader: { display: "flex", alignItems: "center", gap: "8px", padding: "4px 8px", minHeight: "32px", borderBottom: "1px solid var(--dp-border)", background: "var(--dp-bg-secondary)" },
  cellType: { fontSize: "9px", fontWeight: 700, color: "var(--dp-text-dim)", letterSpacing: "0.5px", textTransform: "uppercase" },
  runBtn: { width: "28px", height: "24px", background: "var(--dp-green)", border: "none", borderRadius: "var(--dp-radius)", color: "#fff", cursor: "pointer", fontSize: "11px", fontWeight: 600 },
  toggleBtn: { padding: "2px 8px", background: "none", border: "1px solid var(--dp-border-light)", borderRadius: "var(--dp-radius)", color: "var(--dp-text-secondary)", cursor: "pointer", fontSize: "10px", fontWeight: 600 },
  duration: { color: "var(--dp-text-secondary)", fontSize: "11px" },
  codeInput: { width: "100%", padding: "8px 12px", background: "transparent", border: "none", color: "var(--dp-text)", fontFamily: "var(--dp-font-mono)", fontSize: "13px", resize: "vertical", outline: "none", boxSizing: "border-box", lineHeight: 1.5 },
  mdInput: { width: "100%", padding: "8px 12px", background: "transparent", border: "none", color: "var(--dp-text)", fontSize: "13px", resize: "vertical", outline: "none", boxSizing: "border-box", lineHeight: 1.5 },
  outputArea: { borderTop: "1px solid var(--dp-border)", padding: "8px 12px", maxHeight: "300px", overflow: "auto", background: "color-mix(in srgb, var(--dp-bg) 50%, var(--dp-bg-tertiary))" },
  tableWrap: { overflow: "auto" },
  table: { width: "100%", borderCollapse: "collapse", fontSize: "12px", fontFamily: "var(--dp-font-mono)" },
  th: { textAlign: "left", padding: "4px 8px", borderBottom: "1px solid var(--dp-border-light)", color: "var(--dp-text-secondary)", fontWeight: 600 },
  td: { padding: "3px 8px", borderBottom: "1px solid var(--dp-border)", color: "var(--dp-text)" },
  null: { color: "var(--dp-text-dim)", fontStyle: "italic" },
  truncated: { padding: "4px", color: "var(--dp-yellow)", fontSize: "11px" },
  error: { color: "var(--dp-red)", fontSize: "12px", fontFamily: "var(--dp-font-mono)", margin: 0, whiteSpace: "pre-wrap" },
  text: { color: "var(--dp-text)", fontSize: "12px", fontFamily: "var(--dp-font-mono)", margin: 0, whiteSpace: "pre-wrap" },
  deleteBtn: { width: "22px", height: "22px", background: "none", border: "none", color: "var(--dp-text-dim)", cursor: "pointer", fontSize: "14px", lineHeight: "22px", textAlign: "center", borderRadius: "var(--dp-radius)", flexShrink: 0 },
};

// Ingest form styles
const ig = {
  form: { padding: "8px 12px" },
  row: { display: "flex", alignItems: "center", gap: "8px", marginBottom: "6px" },
  label: { width: "70px", flexShrink: 0, fontSize: "11px", fontWeight: 600, color: "var(--dp-text-secondary)", textTransform: "uppercase", letterSpacing: "0.3px" },
  input: { flex: 1, padding: "5px 8px", background: "var(--dp-bg)", border: "1px solid var(--dp-border-light)", borderRadius: "var(--dp-radius)", color: "var(--dp-text)", fontFamily: "var(--dp-font-mono)", fontSize: "12px", outline: "none", boxSizing: "border-box" },
  select: { flex: 1, padding: "5px 8px", background: "var(--dp-bg)", border: "1px solid var(--dp-border-light)", borderRadius: "var(--dp-radius)", color: "var(--dp-text)", fontSize: "12px", outline: "none" },
  targetRow: { display: "flex", alignItems: "center", gap: "4px", flex: 1 },
  dot: { color: "var(--dp-text-dim)", fontSize: "14px", fontWeight: 700 },
  optionsToggle: { background: "none", border: "none", color: "var(--dp-text-secondary)", cursor: "pointer", fontSize: "11px", padding: "2px 0", fontWeight: 500 },
  optionsArea: { marginLeft: "78px", marginBottom: "4px" },
};
