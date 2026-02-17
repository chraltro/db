import React, { useState, useEffect } from "react";
import { api } from "./api";

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

function NotebookCell({ cell, notebookName, onUpdate }) {
  const [source, setSource] = useState(cell.source || "");
  const [outputs, setOutputs] = useState(cell.outputs || []);
  const [running, setRunning] = useState(false);
  const [duration, setDuration] = useState(cell.duration_ms);

  async function runCell() {
    setRunning(true);
    try {
      const result = await api.runCell(notebookName, source);
      setOutputs(result.outputs);
      setDuration(result.duration_ms);
      onUpdate({ ...cell, source, outputs: result.outputs, duration_ms: result.duration_ms });
    } catch (e) {
      setOutputs([{ type: "error", text: e.message }]);
    }
    setRunning(false);
  }

  if (cell.type === "markdown") {
    return (
      <div style={cs.mdCell}>
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

  return (
    <div style={cs.codeCell}>
      <div style={cs.cellHeader}>
        <button onClick={runCell} disabled={running} style={cs.runBtn}>
          {running ? "..." : "\u25B6"}
        </button>
        {duration != null && <span style={cs.duration}>{duration}ms</span>}
      </div>
      <textarea
        value={source}
        onChange={(e) => {
          setSource(e.target.value);
          onUpdate({ ...cell, source: e.target.value });
        }}
        onKeyDown={(e) => {
          if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
            e.preventDefault();
            runCell();
          }
        }}
        style={cs.codeInput}
        rows={Math.max(3, source.split("\n").length + 1)}
        spellCheck={false}
      />
      <CellOutput outputs={outputs} />
    </div>
  );
}

export default function NotebookPanel() {
  const [notebooks, setNotebooks] = useState([]);
  const [active, setActive] = useState(null);
  const [notebook, setNotebook] = useState(null);
  const [newName, setNewName] = useState("");

  useEffect(() => { loadList(); }, []);

  async function loadList() {
    try {
      const data = await api.listNotebooks();
      setNotebooks(data);
    } catch {}
  }

  async function openNotebook(name) {
    try {
      const nb = await api.getNotebook(name);
      setActive(name);
      setNotebook(nb);
    } catch (e) {
      alert(e.message);
    }
  }

  async function createNotebook() {
    const name = newName.trim().replace(/\s+/g, "_") || "untitled";
    try {
      const nb = await api.createNotebook(name, name);
      setNewName("");
      await loadList();
      setActive(name);
      setNotebook(nb);
    } catch (e) {
      alert(e.message);
    }
  }

  async function saveNotebook() {
    if (!active || !notebook) return;
    try {
      await api.saveNotebook(active, notebook);
    } catch (e) {
      alert(e.message);
    }
  }

  async function runAll() {
    if (!active) return;
    try {
      await saveNotebook();
      const result = await api.runNotebook(active);
      setNotebook(result);
    } catch (e) {
      alert(e.message);
    }
  }

  function addCell(type) {
    if (!notebook) return;
    const id = "cell_" + Math.random().toString(36).slice(2, 8);
    const cell = { id, type, source: type === "code" ? "" : "## Heading", outputs: [] };
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
        <div style={s.list}>
          {notebooks.length === 0 && (
            <div style={s.empty}>No notebooks yet. Create one above.</div>
          )}
          {notebooks.map((nb) => (
            <div key={nb.name} onClick={() => openNotebook(nb.name)} style={s.nbItem}>
              <span style={s.nbName}>{nb.title || nb.name}</span>
              <span style={s.nbMeta}>{nb.cells} cells</span>
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
          <button onClick={() => addCell("code")} style={s.btn}>+ Code</button>
          <button onClick={() => addCell("markdown")} style={s.btn}>+ Markdown</button>
          <button onClick={saveNotebook} style={s.btn}>Save</button>
          <button onClick={runAll} style={s.runAllBtn}>Run All</button>
        </div>
      </div>
      <div style={s.cells}>
        {notebook.cells.map((cell, i) => (
          <div key={cell.id || i} style={cs.cellWrap}>
            <NotebookCell
              cell={cell}
              notebookName={active}
              onUpdate={(updated) => updateCell(i, updated)}
            />
            <button onClick={() => deleteCell(i)} style={cs.deleteBtn} title="Delete cell">&times;</button>
          </div>
        ))}
      </div>
    </div>
  );
}

const s = {
  container: { display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" },
  listHeader: { padding: "12px 16px", borderBottom: "1px solid #21262d" },
  title: { fontSize: "16px", fontWeight: 600 },
  newRow: { display: "flex", gap: "8px", marginTop: "8px" },
  input: {
    flex: 1, padding: "6px 10px", background: "#0d1117", border: "1px solid #30363d",
    borderRadius: "6px", color: "#e1e4e8", fontSize: "13px",
  },
  btn: {
    padding: "4px 12px", background: "#21262d", border: "1px solid #30363d",
    borderRadius: "6px", color: "#e1e4e8", cursor: "pointer", fontSize: "12px",
  },
  list: { flex: 1, overflow: "auto", padding: "8px" },
  empty: { color: "#484f58", textAlign: "center", padding: "24px" },
  nbItem: {
    padding: "10px 12px", borderRadius: "6px", cursor: "pointer", display: "flex",
    justifyContent: "space-between", alignItems: "center", marginBottom: "4px",
    border: "1px solid #21262d",
  },
  nbName: { fontWeight: 500, fontSize: "13px" },
  nbMeta: { color: "#8b949e", fontSize: "12px" },
  nbHeader: {
    display: "flex", alignItems: "center", gap: "12px", padding: "8px 12px",
    borderBottom: "1px solid #21262d",
  },
  backBtn: {
    padding: "4px 8px", background: "none", border: "1px solid #30363d",
    borderRadius: "6px", color: "#8b949e", cursor: "pointer", fontSize: "12px",
  },
  nbTitle: { fontWeight: 600, fontSize: "14px", flex: 1 },
  nbActions: { display: "flex", gap: "6px" },
  runAllBtn: {
    padding: "4px 12px", background: "#238636", border: "1px solid #2ea043",
    borderRadius: "6px", color: "#fff", cursor: "pointer", fontSize: "12px",
  },
  cells: { flex: 1, overflow: "auto", padding: "12px 16px" },
};

const cs = {
  cellWrap: { position: "relative", marginBottom: "8px" },
  codeCell: {
    border: "1px solid #21262d", borderRadius: "6px", background: "#0d1117", overflow: "hidden",
  },
  mdCell: {
    border: "1px solid #1c2128", borderRadius: "6px", background: "#0d1117", overflow: "hidden",
  },
  cellHeader: {
    display: "flex", alignItems: "center", gap: "8px", padding: "4px 8px",
    borderBottom: "1px solid #21262d", background: "#161b22",
  },
  runBtn: {
    width: "28px", height: "24px", background: "#238636", border: "none",
    borderRadius: "4px", color: "#fff", cursor: "pointer", fontSize: "11px",
  },
  duration: { color: "#8b949e", fontSize: "11px" },
  codeInput: {
    width: "100%", padding: "8px 12px", background: "transparent", border: "none",
    color: "#e1e4e8", fontFamily: "monospace", fontSize: "13px", resize: "vertical",
    outline: "none", boxSizing: "border-box",
  },
  mdInput: {
    width: "100%", padding: "8px 12px", background: "transparent", border: "none",
    color: "#c9d1d9", fontSize: "13px", resize: "vertical", outline: "none",
    boxSizing: "border-box",
  },
  outputArea: { borderTop: "1px solid #21262d", padding: "8px 12px", maxHeight: "300px", overflow: "auto" },
  tableWrap: { overflow: "auto" },
  table: { width: "100%", borderCollapse: "collapse", fontSize: "12px", fontFamily: "monospace" },
  th: { textAlign: "left", padding: "4px 8px", borderBottom: "1px solid #30363d", color: "#8b949e", fontWeight: 600 },
  td: { padding: "3px 8px", borderBottom: "1px solid #21262d", color: "#c9d1d9" },
  null: { color: "#484f58", fontStyle: "italic" },
  truncated: { padding: "4px", color: "#d29922", fontSize: "11px" },
  error: { color: "#f85149", fontSize: "12px", fontFamily: "monospace", margin: 0, whiteSpace: "pre-wrap" },
  text: { color: "#c9d1d9", fontSize: "12px", fontFamily: "monospace", margin: 0, whiteSpace: "pre-wrap" },
  deleteBtn: {
    position: "absolute", top: "4px", right: "4px", width: "20px", height: "20px",
    background: "none", border: "none", color: "#484f58", cursor: "pointer",
    fontSize: "14px", lineHeight: "20px", textAlign: "center",
  },
};
