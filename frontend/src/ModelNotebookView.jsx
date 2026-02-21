import React, { useState, useEffect, useCallback } from "react";
import { api } from "./api";

/**
 * Notebook-style view for SQL models.
 * Cell 1: editable SQL source (saved back to .sql file)
 * Cell 2: sample output (SELECT * FROM model LIMIT 50)
 * Sidebar: upstream/downstream lineage with column-level details
 */
export default function ModelNotebookView({ modelName, onClose, onSaved }) {
  const [loading, setLoading] = useState(true);
  const [data, setData] = useState(null);
  const [sql, setSql] = useState("");
  const [dirty, setDirty] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState(null);
  const [showLineage, setShowLineage] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await api.getModelNotebookView(modelName);
      setData(result);
      setSql(result.sql_source);
      setDirty(false);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [modelName]);

  useEffect(() => { load(); }, [load]);

  const handleSave = async () => {
    if (!data) return;
    setSaving(true);
    try {
      await api.saveFile(data.path, sql);
      setDirty(false);
      if (onSaved) onSaved();
    } catch (e) {
      setError(e.message);
    } finally {
      setSaving(false);
    }
  };

  const handleRunTransform = async () => {
    try {
      setError(null);
      await api.runTransform([modelName], false);
      await load(); // Reload to get fresh sample data
    } catch (e) {
      setError(e.message);
    }
  };

  if (loading) return <div className="model-notebook loading">Loading model...</div>;
  if (error && !data) return <div className="model-notebook error">Error: {error}</div>;
  if (!data) return null;

  return (
    <div className="model-notebook">
      <div className="model-notebook-header">
        <div className="model-notebook-title">
          <span className="model-name">{data.model}</span>
          <span className="model-type">{data.materialized}</span>
          {dirty && <span className="model-dirty">modified</span>}
        </div>
        <div className="model-notebook-actions">
          <button onClick={() => setShowLineage(!showLineage)} className="btn-sm">
            {showLineage ? "Hide" : "Show"} Lineage
          </button>
          <button onClick={handleSave} disabled={!dirty || saving} className="btn-sm btn-primary">
            {saving ? "Saving..." : "Save"}
          </button>
          <button onClick={handleRunTransform} className="btn-sm">
            Run
          </button>
          {onClose && (
            <button onClick={onClose} className="btn-sm">Close</button>
          )}
        </div>
      </div>

      {error && <div className="model-notebook-error">{error}</div>}

      <div className="model-notebook-body" style={{ display: "flex" }}>
        <div className="model-notebook-cells" style={{ flex: 1 }}>
          {/* Cell 1: SQL Editor */}
          <div className="notebook-cell">
            <div className="cell-header">
              <span className="cell-label">SQL</span>
              <span className="cell-path">{data.path}</span>
            </div>
            <textarea
              className="cell-editor"
              value={sql}
              onChange={(e) => { setSql(e.target.value); setDirty(true); }}
              spellCheck={false}
              style={{
                fontFamily: "monospace",
                fontSize: "13px",
                width: "100%",
                minHeight: "200px",
                resize: "vertical",
                padding: "8px",
                border: "1px solid var(--border, #333)",
                borderRadius: "4px",
                backgroundColor: "var(--bg-secondary, #1e1e1e)",
                color: "var(--text, #ccc)",
              }}
              onKeyDown={(e) => {
                if ((e.ctrlKey || e.metaKey) && e.key === "s") {
                  e.preventDefault();
                  if (dirty) handleSave();
                }
              }}
            />
          </div>

          {/* Cell 2: Sample Output */}
          <div className="notebook-cell">
            <div className="cell-header">
              <span className="cell-label">Output</span>
              <span className="cell-info">SELECT * FROM {data.model} LIMIT 50</span>
            </div>
            {data.sample_data ? (
              <div className="cell-output" style={{ overflowX: "auto", maxHeight: "400px" }}>
                <table className="sample-table" style={{ fontSize: "12px", borderCollapse: "collapse", width: "100%" }}>
                  <thead>
                    <tr>
                      {data.sample_data.columns.map((col) => (
                        <th key={col} style={{ padding: "4px 8px", borderBottom: "1px solid var(--border, #333)", textAlign: "left", whiteSpace: "nowrap" }}>
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {data.sample_data.rows.map((row, i) => (
                      <tr key={i}>
                        {row.map((val, j) => (
                          <td key={j} style={{ padding: "3px 8px", borderBottom: "1px solid var(--border, #222)", maxWidth: "200px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                            {val === null ? <span style={{ color: "#666" }}>null</span> : String(val)}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="cell-output-empty" style={{ padding: "12px", color: "#666" }}>
                No data. Run the model first.
              </div>
            )}
          </div>
        </div>

        {/* Lineage sidebar */}
        {showLineage && (
          <div className="model-lineage-sidebar" style={{ width: "250px", padding: "8px 12px", borderLeft: "1px solid var(--border, #333)", fontSize: "12px" }}>
            <h4 style={{ margin: "0 0 8px" }}>Upstream</h4>
            {data.upstream.length > 0 ? (
              <ul style={{ listStyle: "none", padding: 0, margin: "0 0 16px" }}>
                {data.upstream.map((u) => (
                  <li key={u} style={{ padding: "2px 0" }}>{u}</li>
                ))}
              </ul>
            ) : <p style={{ color: "#666" }}>None</p>}

            <h4 style={{ margin: "0 0 8px" }}>Downstream</h4>
            {data.downstream.length > 0 ? (
              <ul style={{ listStyle: "none", padding: 0, margin: "0 0 16px" }}>
                {data.downstream.map((d) => (
                  <li key={d} style={{ padding: "2px 0" }}>{d}</li>
                ))}
              </ul>
            ) : <p style={{ color: "#666" }}>None</p>}

            {data.lineage && Object.keys(data.lineage).length > 0 && (
              <>
                <h4 style={{ margin: "0 0 8px" }}>Column Lineage</h4>
                <ul style={{ listStyle: "none", padding: 0 }}>
                  {Object.entries(data.lineage).map(([col, sources]) => (
                    <li key={col} style={{ padding: "2px 0" }}>
                      <strong>{col}</strong>
                      {Array.isArray(sources) && sources.length > 0 && (
                        <span style={{ color: "#888" }}> ← {sources.map(s => typeof s === "string" ? s : `${s.table}.${s.column}`).join(", ")}</span>
                      )}
                    </li>
                  ))}
                </ul>
              </>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
