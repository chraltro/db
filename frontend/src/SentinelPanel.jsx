import React, { useState, useEffect, useCallback } from "react";
import { api } from "./api";

const SEVERITY_COLORS = {
  breaking: "#f85149",
  warning: "#d29922",
  info: "#8b949e",
};

const IMPACT_COLORS = {
  direct: "#f85149",
  transitive: "#d29922",
  safe: "#3fb950",
};

export default function SentinelPanel() {
  const [view, setView] = useState("check"); // check, diffs, history
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // Check view state
  const [checkResult, setCheckResult] = useState(null);

  // Diffs view state
  const [diffs, setDiffs] = useState([]);
  const [selectedDiff, setSelectedDiff] = useState(null);
  const [impacts, setImpacts] = useState([]);

  // History view state
  const [sources, setSources] = useState([]);
  const [selectedSource, setSelectedSource] = useState(null);
  const [history, setHistory] = useState([]);

  // Run schema check
  const runCheck = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await api.runSentinelCheck();
      setCheckResult(result);
    } catch (e) {
      setError(e.message || "Check failed");
    } finally {
      setLoading(false);
    }
  }, []);

  // Load diffs
  const loadDiffs = useCallback(async () => {
    setLoading(true);
    try {
      const d = await api.getSentinelDiffs();
      setDiffs(d);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, []);

  // Load impacts for a diff
  const loadImpacts = useCallback(async (diffId) => {
    try {
      const i = await api.getSentinelImpacts(diffId);
      setImpacts(i);
      setSelectedDiff(diffId);
    } catch (e) {
      setError(e.message);
    }
  }, []);

  // Load sources and history
  const loadSources = useCallback(async () => {
    try {
      const s = await api.getSentinelSources();
      setSources(s);
    } catch (e) {
      setError(e.message);
    }
  }, []);

  const loadHistory = useCallback(async (sourceName) => {
    setSelectedSource(sourceName);
    try {
      const h = await api.getSentinelHistory(sourceName);
      setHistory(h);
    } catch (e) {
      setError(e.message);
    }
  }, []);

  useEffect(() => {
    if (view === "diffs") loadDiffs();
    if (view === "history") loadSources();
  }, [view]);

  async function handleResolve(diffId, modelName) {
    try {
      await api.resolveSentinelImpact(diffId, modelName);
      if (selectedDiff) loadImpacts(selectedDiff);
    } catch (e) {
      setError(e.message);
    }
  }

  return (
    <div style={st.container}>
      {/* Header */}
      <div style={st.header}>
        <span style={st.title}>Schema Sentinel</span>
        <div style={st.tabs}>
          {["check", "diffs", "history"].map(t => (
            <button
              key={t}
              onClick={() => setView(t)}
              style={{
                ...st.tab,
                borderBottom: view === t ? "2px solid var(--havn-accent, #58a6ff)" : "2px solid transparent",
                color: view === t ? "var(--havn-text)" : "var(--havn-text-secondary)",
              }}
            >
              {t.charAt(0).toUpperCase() + t.slice(1)}
            </button>
          ))}
        </div>
      </div>

      {error && <div style={st.error}>{error}</div>}

      {/* Check view */}
      {view === "check" && (
        <div style={st.body}>
          <div style={st.actionBar}>
            <button onClick={runCheck} disabled={loading} style={st.btn}>
              {loading ? "Checking..." : "Run Schema Check"}
            </button>
            {checkResult && (
              <span style={st.dim}>
                {checkResult.sources_checked} source(s) checked, {checkResult.diffs?.length || 0} change(s)
              </span>
            )}
          </div>

          {checkResult && checkResult.diffs && checkResult.diffs.length === 0 && (
            <div style={st.success}>No schema changes detected.</div>
          )}

          {checkResult && checkResult.diffs && checkResult.diffs.map((diff, i) => (
            <div key={i} style={st.diffCard}>
              <div style={st.diffHeader}>
                <span style={st.diffSource}>{diff.source_name}</span>
                {diff.has_breaking && <span style={st.breakingBadge}>BREAKING</span>}
                <span style={st.dim}>{diff.changes.length} change(s)</span>
              </div>

              {/* Changes table */}
              <table style={st.table}>
                <thead>
                  <tr>
                    <th style={st.th}>Change</th>
                    <th style={st.th}>Severity</th>
                    <th style={st.th}>Column</th>
                    <th style={st.th}>Details</th>
                  </tr>
                </thead>
                <tbody>
                  {diff.changes.map((ch, j) => (
                    <tr key={j}>
                      <td style={st.td}>{ch.change_type}</td>
                      <td style={st.td}>
                        <span style={{ ...st.sevBadge, background: SEVERITY_COLORS[ch.severity] || "#8b949e" }}>
                          {ch.severity}
                        </span>
                      </td>
                      <td style={{ ...st.td, fontWeight: 600 }}>{ch.column_name}</td>
                      <td style={st.td}>
                        {ch.old_value && ch.new_value
                          ? `${ch.old_value} \u2192 ${ch.new_value}`
                          : ch.old_value
                            ? `was: ${ch.old_value}`
                            : ch.new_value || ""}
                        {ch.rename_candidate && (
                          <span style={{ color: "#d29922", marginLeft: 6, fontSize: 11 }}>
                            (rename? {ch.rename_candidate})
                          </span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>

              {/* Impacts */}
              {diff.impacts && diff.impacts.length > 0 && (
                <div style={st.impactSection}>
                  <div style={st.impactTitle}>
                    Impact Analysis ({diff.impacts.length} model{diff.impacts.length !== 1 ? "s" : ""})
                  </div>
                  {diff.impacts.map((imp, k) => (
                    <div key={k} style={st.impactRow}>
                      <span style={{ ...st.impactBadge, background: IMPACT_COLORS[imp.impact_type] || "#8b949e" }}>
                        {imp.impact_type}
                      </span>
                      <span style={st.impactModel}>{imp.model_name}</span>
                      {imp.columns_affected && imp.columns_affected.length > 0 && (
                        <span style={st.dim}>cols: {imp.columns_affected.join(", ")}</span>
                      )}
                      {imp.fix_suggestion && (
                        <div style={st.fixSuggestion}>{imp.fix_suggestion}</div>
                      )}
                      {!imp.resolved_at && (
                        <button
                          onClick={() => handleResolve(diff.diff_id, imp.model_name)}
                          style={st.dismissBtn}
                        >
                          Dismiss
                        </button>
                      )}
                      {imp.resolved_at && (
                        <span style={{ ...st.dim, fontSize: 10 }}>Resolved</span>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Diffs view */}
      {view === "diffs" && (
        <div style={st.body}>
          {loading && <div style={st.dim}>Loading...</div>}

          <div style={st.splitView}>
            {/* Diff list */}
            <div style={st.leftPane}>
              {diffs.length === 0 && !loading && (
                <div style={st.dim}>No schema diffs recorded. Run a check first.</div>
              )}
              {diffs.map((d, i) => (
                <div
                  key={i}
                  onClick={() => loadImpacts(d.diff_id)}
                  style={{
                    ...st.diffListItem,
                    background: selectedDiff === d.diff_id ? "var(--havn-bg-tertiary)" : "transparent",
                  }}
                >
                  <div style={{ fontWeight: 600, fontSize: 12 }}>{d.source_name}</div>
                  <div style={{ fontSize: 11, color: "var(--havn-text-dim)" }}>
                    {d.created_at?.slice(0, 19)} &middot; {d.changes?.length || 0} change(s)
                    {d.changes?.some(c => c.severity === "breaking") && (
                      <span style={{ color: "#f85149", marginLeft: 6 }}>BREAKING</span>
                    )}
                  </div>
                </div>
              ))}
            </div>

            {/* Impact detail */}
            <div style={st.rightPane}>
              {selectedDiff && impacts.length > 0 ? (
                <>
                  <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 8 }}>
                    Impact Analysis
                  </div>
                  {impacts.map((imp, k) => (
                    <div key={k} style={st.impactRow}>
                      <span style={{ ...st.impactBadge, background: IMPACT_COLORS[imp.impact_type] || "#8b949e" }}>
                        {imp.impact_type}
                      </span>
                      <span style={st.impactModel}>{imp.model_name}</span>
                      {imp.columns_affected?.length > 0 && (
                        <span style={st.dim}>cols: {imp.columns_affected.join(", ")}</span>
                      )}
                      {imp.fix_suggestion && (
                        <div style={st.fixSuggestion}>{imp.fix_suggestion}</div>
                      )}
                    </div>
                  ))}
                </>
              ) : selectedDiff ? (
                <div style={st.dim}>No impacts for this diff.</div>
              ) : (
                <div style={st.dim}>Select a diff to see impact analysis.</div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* History view */}
      {view === "history" && (
        <div style={st.body}>
          <div style={st.splitView}>
            {/* Source list */}
            <div style={st.leftPane}>
              <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 6 }}>Sources</div>
              {sources.map((s, i) => (
                <div
                  key={i}
                  onClick={() => loadHistory(s.name)}
                  style={{
                    ...st.diffListItem,
                    background: selectedSource === s.name ? "var(--havn-bg-tertiary)" : "transparent",
                  }}
                >
                  <span style={{ fontWeight: 500, fontSize: 12 }}>{s.name}</span>
                  <span style={{
                    fontSize: 10, marginLeft: 6,
                    color: s.exists ? "#3fb950" : "#8b949e",
                  }}>
                    {s.exists ? "exists" : "missing"}
                  </span>
                </div>
              ))}
              {sources.length === 0 && <div style={st.dim}>No sources found.</div>}
            </div>

            {/* Schema history */}
            <div style={st.rightPane}>
              {selectedSource && history.length > 0 ? (
                <>
                  <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 8 }}>
                    Schema History: {selectedSource}
                  </div>
                  {history.map((snap, i) => (
                    <div key={i} style={st.historyItem}>
                      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                        <span style={{ fontSize: 11, color: "var(--havn-text-dim)" }}>
                          {snap.captured_at?.slice(0, 19)}
                        </span>
                        <span style={{ fontSize: 10, color: "var(--havn-text-dim)" }}>
                          {snap.columns?.length} col(s) &middot; {snap.schema_hash?.slice(0, 8)}
                        </span>
                      </div>
                      <div style={{ marginTop: 4, fontSize: 11 }}>
                        {snap.columns?.slice(0, 10).map((c, j) => (
                          <span key={j} style={st.colTag}>
                            {c.name}: <span style={{ color: "var(--havn-text-dim)" }}>{c.type}</span>
                          </span>
                        ))}
                        {snap.columns?.length > 10 && (
                          <span style={{ fontSize: 10, color: "var(--havn-text-dim)" }}>
                            +{snap.columns.length - 10} more
                          </span>
                        )}
                      </div>
                    </div>
                  ))}
                </>
              ) : selectedSource ? (
                <div style={st.dim}>No schema history for {selectedSource}.</div>
              ) : (
                <div style={st.dim}>Select a source to see schema history.</div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

const st = {
  container: { display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" },
  header: { display: "flex", alignItems: "center", justifyContent: "space-between", padding: "8px 12px", borderBottom: "1px solid var(--havn-border)", fontSize: 13 },
  title: { fontWeight: 600 },
  tabs: { display: "flex", gap: 4 },
  tab: { background: "none", border: "none", cursor: "pointer", padding: "4px 10px", fontSize: 12, fontWeight: 500 },
  body: { flex: 1, overflow: "auto", padding: 12 },
  error: { padding: "8px 12px", background: "#f8514920", color: "#f85149", fontSize: 12, borderBottom: "1px solid var(--havn-border)" },
  actionBar: { display: "flex", alignItems: "center", gap: 12, marginBottom: 12 },
  btn: { padding: "6px 14px", background: "var(--havn-accent, #58a6ff)", color: "#fff", border: "none", borderRadius: 4, cursor: "pointer", fontSize: 12, fontWeight: 600 },
  dim: { color: "var(--havn-text-dim)", fontSize: 12 },
  success: { padding: 12, background: "#3fb95015", color: "#3fb950", borderRadius: 4, fontSize: 12, fontWeight: 500 },
  diffCard: { border: "1px solid var(--havn-border)", borderRadius: 6, marginBottom: 12, overflow: "hidden" },
  diffHeader: { display: "flex", alignItems: "center", gap: 10, padding: "8px 12px", background: "var(--havn-bg-secondary)", borderBottom: "1px solid var(--havn-border)" },
  diffSource: { fontWeight: 600, fontSize: 13 },
  breakingBadge: { background: "#f85149", color: "#fff", padding: "1px 6px", borderRadius: 3, fontSize: 10, fontWeight: 700 },
  table: { width: "100%", borderCollapse: "collapse", fontSize: 12 },
  th: { textAlign: "left", padding: "6px 12px", background: "var(--havn-bg-tertiary)", borderBottom: "1px solid var(--havn-border)", fontWeight: 600, fontSize: 11 },
  td: { padding: "5px 12px", borderBottom: "1px solid var(--havn-border-light)", fontSize: 12 },
  sevBadge: { color: "#fff", padding: "1px 5px", borderRadius: 3, fontSize: 10, fontWeight: 600 },
  impactSection: { padding: "8px 12px" },
  impactTitle: { fontWeight: 600, fontSize: 12, marginBottom: 6, color: "var(--havn-text-secondary)" },
  impactRow: { padding: "6px 0", borderBottom: "1px solid var(--havn-border-light)", display: "flex", flexWrap: "wrap", alignItems: "center", gap: 8 },
  impactBadge: { color: "#fff", padding: "1px 6px", borderRadius: 3, fontSize: 10, fontWeight: 600 },
  impactModel: { fontWeight: 600, fontSize: 12 },
  fixSuggestion: { width: "100%", fontSize: 11, color: "var(--havn-text-secondary)", marginTop: 2, paddingLeft: 4, borderLeft: "2px solid var(--havn-border-light)" },
  dismissBtn: { background: "none", border: "1px solid var(--havn-border)", borderRadius: 3, padding: "1px 8px", fontSize: 10, cursor: "pointer", color: "var(--havn-text-secondary)" },
  splitView: { display: "flex", gap: 0, height: "calc(100% - 8px)" },
  leftPane: { width: 280, borderRight: "1px solid var(--havn-border)", overflow: "auto", padding: "8px 0" },
  rightPane: { flex: 1, overflow: "auto", padding: "8px 12px" },
  diffListItem: { padding: "8px 12px", cursor: "pointer", borderBottom: "1px solid var(--havn-border-light)" },
  historyItem: { padding: "8px 0", borderBottom: "1px solid var(--havn-border-light)" },
  colTag: { display: "inline-block", marginRight: 8, fontSize: 11 },
};
