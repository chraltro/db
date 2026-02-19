import React, { useState } from "react";
import SortableTable from "./SortableTable";
import { useHintTriggerFn } from "./HintSystem";

export default function DiffPanel({ api, addOutput }) {
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [expandedModel, setExpandedModel] = useState(null);
  const setHintTrigger = useHintTriggerFn();

  const runDiff = async () => {
    setLoading(true);
    setResults(null);
    try {
      const data = await api.runDiff();
      setResults(data);
      const changes = data.filter(
        (r) => r.added || r.removed || r.modified || r.is_new || r.error || (r.schema_changes && r.schema_changes.length)
      );
      if (changes.length === 0) {
        addOutput("Diff complete: no changes detected.");
      } else {
        addOutput(`Diff complete: ${changes.length} model(s) with changes.`);
        setHintTrigger("hasDiffChanges", true);
      }
    } catch (err) {
      addOutput(`Diff failed: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  const schemaLabel = (sc) => {
    if (!sc || !sc.length) return "\u2014";
    const adds = sc.filter((s) => s.change_type === "added").length;
    const removes = sc.filter((s) => s.change_type === "removed").length;
    const changes = sc.filter((s) => s.change_type === "type_changed").length;
    const parts = [];
    if (adds) parts.push(`+${adds} col`);
    if (removes) parts.push(`-${removes} col`);
    if (changes) parts.push(`~${changes} col`);
    return parts.join(", ");
  };

  return (
    <div style={{ padding: "16px" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 16 }}>
        <h2 style={{ margin: 0, fontSize: 18 }}>Data Diff</h2>
        <button
          onClick={runDiff}
          disabled={loading}
          style={{
            padding: "6px 16px",
            borderRadius: 4,
            border: "1px solid var(--border)",
            background: "var(--accent)",
            color: "#fff",
            cursor: loading ? "wait" : "pointer",
          }}
        >
          {loading ? "Running..." : "Run Diff"}
        </button>
      </div>

      <p style={{ color: "var(--text-secondary)", fontSize: 13, marginBottom: 16 }}>
        Compare model SQL output against currently materialized tables. Shows what would change if you run transforms now.
      </p>

      {results && (
        <div data-dp-hint="diff-results">
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
            <thead>
              <tr style={{ borderBottom: "2px solid var(--border)", textAlign: "left" }}>
                <th style={thStyle}>Model</th>
                <th style={{ ...thStyle, textAlign: "right" }}>Before</th>
                <th style={{ ...thStyle, textAlign: "right" }}>After</th>
                <th style={{ ...thStyle, textAlign: "right" }}>Added</th>
                <th style={{ ...thStyle, textAlign: "right" }}>Removed</th>
                <th style={{ ...thStyle, textAlign: "right" }}>Modified</th>
                <th style={thStyle}>Schema</th>
              </tr>
            </thead>
            <tbody>
              {results.map((r) => (
                <React.Fragment key={r.model}>
                  <tr
                    onClick={() =>
                      setExpandedModel(expandedModel === r.model ? null : r.model)
                    }
                    style={{
                      borderBottom: "1px solid var(--border)",
                      cursor: "pointer",
                      background:
                        expandedModel === r.model
                          ? "var(--bg-secondary)"
                          : "transparent",
                    }}
                  >
                    <td style={tdStyle}>
                      <strong>{r.model}</strong>
                    </td>
                    <td style={{ ...tdStyle, textAlign: "right" }}>
                      {r.error ? "" : r.is_new ? "NEW" : r.total_before.toLocaleString()}
                    </td>
                    <td style={{ ...tdStyle, textAlign: "right" }}>
                      {r.error ? "" : r.total_after.toLocaleString()}
                    </td>
                    <td
                      style={{
                        ...tdStyle,
                        textAlign: "right",
                        color: r.added ? "#22c55e" : undefined,
                      }}
                    >
                      {r.error ? "" : r.added ? `+${r.added}` : "0"}
                    </td>
                    <td
                      style={{
                        ...tdStyle,
                        textAlign: "right",
                        color: r.removed ? "#ef4444" : undefined,
                      }}
                    >
                      {r.error ? "" : r.removed || "0"}
                    </td>
                    <td
                      style={{
                        ...tdStyle,
                        textAlign: "right",
                        color: r.modified ? "#eab308" : undefined,
                      }}
                    >
                      {r.error ? "" : r.modified || "0"}
                    </td>
                    <td style={tdStyle}>
                      {r.error ? (
                        <span style={{ color: "#ef4444" }}>ERROR</span>
                      ) : (
                        <span
                          style={{
                            color:
                              r.schema_changes && r.schema_changes.length
                                ? "#3b82f6"
                                : undefined,
                          }}
                        >
                          {schemaLabel(r.schema_changes)}
                        </span>
                      )}
                    </td>
                  </tr>
                  {expandedModel === r.model && (
                    <tr>
                      <td colSpan={7} style={{ padding: "8px 12px" }}>
                        <ExpandedDiff result={r} />
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function ExpandedDiff({ result }) {
  if (result.error) {
    return (
      <div style={{ color: "#ef4444", padding: 8 }}>
        Error: {result.error}
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      {result.schema_changes && result.schema_changes.length > 0 && (
        <div>
          <h4 style={{ margin: "0 0 8px 0", fontSize: 13 }}>Schema Changes</h4>
          {result.schema_changes.map((sc, i) => (
            <div key={i} style={{ fontSize: 12, padding: "2px 0" }}>
              {sc.change_type === "added" && (
                <span style={{ color: "#22c55e" }}>
                  + {sc.column} ({sc.new_type})
                </span>
              )}
              {sc.change_type === "removed" && (
                <span style={{ color: "#ef4444" }}>
                  - {sc.column} ({sc.old_type})
                </span>
              )}
              {sc.change_type === "type_changed" && (
                <span style={{ color: "#eab308" }}>
                  ~ {sc.column}: {sc.old_type} → {sc.new_type}
                </span>
              )}
            </div>
          ))}
        </div>
      )}

      {result.sample_added && result.sample_added.length > 0 && (
        <div>
          <h4 style={{ margin: "0 0 8px 0", fontSize: 13, color: "#22c55e" }}>
            Added Rows ({result.added})
          </h4>
          <SortableTable
            columns={Object.keys(result.sample_added[0])}
            rows={result.sample_added.map((r) => Object.values(r))}
          />
        </div>
      )}

      {result.sample_removed && result.sample_removed.length > 0 && (
        <div>
          <h4 style={{ margin: "0 0 8px 0", fontSize: 13, color: "#ef4444" }}>
            Removed Rows ({result.removed})
          </h4>
          <SortableTable
            columns={Object.keys(result.sample_removed[0])}
            rows={result.sample_removed.map((r) => Object.values(r))}
          />
        </div>
      )}

      {result.sample_modified && result.sample_modified.length > 0 && (
        <div>
          <h4 style={{ margin: "0 0 8px 0", fontSize: 13, color: "#eab308" }}>
            Modified Rows ({result.modified})
          </h4>
          <SortableTable
            columns={Object.keys(result.sample_modified[0])}
            rows={result.sample_modified.map((r) => Object.values(r))}
          />
        </div>
      )}

      {!result.sample_added?.length &&
        !result.sample_removed?.length &&
        !result.sample_modified?.length &&
        !result.schema_changes?.length && (
          <div style={{ color: "var(--text-secondary)", fontSize: 13 }}>
            No changes detected for this model.
          </div>
        )}
    </div>
  );
}

const thStyle = { padding: "8px 12px", fontWeight: 600 };
const tdStyle = { padding: "8px 12px" };
