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
        addOutput("info", "Diff complete: no changes detected.");
      } else {
        addOutput("info", `Diff complete: ${changes.length} model(s) with changes.`);
        setHintTrigger("hasDiffChanges", true);
      }
    } catch (err) {
      addOutput("error", `Diff failed: ${err.message}`);
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
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "8px 12px", borderBottom: "1px solid var(--havn-border)" }}>
        <button
          onClick={runDiff}
          disabled={loading}
          style={{
            padding: "4px 12px",
            borderRadius: "var(--havn-radius-lg)",
            border: "1px solid var(--havn-green-border)",
            background: "var(--havn-green)",
            color: "#fff",
            cursor: loading ? "wait" : "pointer",
            fontSize: "11px",
            fontWeight: 500,
          }}
        >
          {loading ? "Running..." : "Run Diff"}
        </button>
      </div>

      <p style={{ color: "var(--havn-text-secondary)", fontSize: 13, marginBottom: 16, padding: "0 12px" }}>
        Compare model SQL output against currently materialized tables. Shows what would change if you run transforms now.
      </p>

      {results && (
        <div data-havn-hint="diff-results">
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
            <thead>
              <tr style={{ borderBottom: "2px solid var(--havn-border)", textAlign: "left" }}>
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
                      borderBottom: "1px solid var(--havn-border)",
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
                        color: r.added ? "var(--havn-green)" : undefined,
                      }}
                    >
                      {r.error ? "" : r.added ? `+${r.added}` : "0"}
                    </td>
                    <td
                      style={{
                        ...tdStyle,
                        textAlign: "right",
                        color: r.removed ? "var(--havn-red)" : undefined,
                      }}
                    >
                      {r.error ? "" : r.removed || "0"}
                    </td>
                    <td
                      style={{
                        ...tdStyle,
                        textAlign: "right",
                        color: r.modified ? "var(--havn-yellow)" : undefined,
                      }}
                    >
                      {r.error ? "" : r.modified || "0"}
                    </td>
                    <td style={tdStyle}>
                      {r.error ? (
                        <span style={{ color: "var(--havn-red)" }}>ERROR</span>
                      ) : (
                        <span
                          style={{
                            color:
                              r.schema_changes && r.schema_changes.length
                                ? "var(--havn-accent)"
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
      <div style={{ color: "var(--havn-red)", padding: 8 }}>
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
                <span style={{ color: "var(--havn-green)" }}>
                  + {sc.column} ({sc.new_type})
                </span>
              )}
              {sc.change_type === "removed" && (
                <span style={{ color: "var(--havn-red)" }}>
                  - {sc.column} ({sc.old_type})
                </span>
              )}
              {sc.change_type === "type_changed" && (
                <span style={{ color: "var(--havn-yellow)" }}>
                  ~ {sc.column}: {sc.old_type} → {sc.new_type}
                </span>
              )}
            </div>
          ))}
        </div>
      )}

      {result.sample_added && result.sample_added.length > 0 && (
        <div>
          <h4 style={{ margin: "0 0 8px 0", fontSize: 13, color: "var(--havn-green)" }}>
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
          <h4 style={{ margin: "0 0 8px 0", fontSize: 13, color: "var(--havn-red)" }}>
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
          <h4 style={{ margin: "0 0 8px 0", fontSize: 13, color: "var(--havn-yellow)" }}>
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
          <div style={{ color: "var(--havn-text-secondary)", fontSize: 13 }}>
            No changes detected for this model.
          </div>
        )}
    </div>
  );
}

const thStyle = { padding: "8px 12px", fontWeight: 600 };
const tdStyle = { padding: "8px 12px" };
