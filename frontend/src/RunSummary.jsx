import React from "react";

/**
 * Post-pipeline-run summary card.
 * Shown in the output area after a stream/transform completes,
 * bridging the user from "it ran" to "here's what to do next."
 */
export default function RunSummary({ summary, onNavigate, onDismiss }) {
  if (!summary) return null;

  const { type, status, models, totalRows, duration, errors } = summary;
  const isSuccess = status === "success";
  const builtCount = models ? models.filter((m) => m.result === "built").length : 0;
  const skippedCount = models ? models.filter((m) => m.result === "skipped").length : 0;
  const errorCount = errors || (models ? models.filter((m) => m.result === "error").length : 0);

  return (
    <div style={{
      ...st.container,
      borderColor: isSuccess ? "var(--dp-green)" : "var(--dp-red)",
    }}>
      <div style={st.header}>
        <span style={{
          ...st.statusBadge,
          background: isSuccess ? "var(--dp-green)" : "var(--dp-red)",
        }}>
          {isSuccess ? "Pipeline Complete" : "Pipeline Failed"}
        </span>
        <button onClick={onDismiss} style={st.dismiss}>Dismiss</button>
      </div>

      <div style={st.stats}>
        {builtCount > 0 && (
          <div style={st.stat}>
            <span style={st.statValue}>{builtCount}</span>
            <span style={st.statLabel}>model{builtCount !== 1 ? "s" : ""} built</span>
          </div>
        )}
        {skippedCount > 0 && (
          <div style={st.stat}>
            <span style={{ ...st.statValue, color: "var(--dp-text-secondary)" }}>{skippedCount}</span>
            <span style={st.statLabel}>skipped</span>
          </div>
        )}
        {errorCount > 0 && (
          <div style={st.stat}>
            <span style={{ ...st.statValue, color: "var(--dp-red)" }}>{errorCount}</span>
            <span style={st.statLabel}>error{errorCount !== 1 ? "s" : ""}</span>
          </div>
        )}
        {totalRows > 0 && (
          <div style={st.stat}>
            <span style={st.statValue}>{totalRows.toLocaleString()}</span>
            <span style={st.statLabel}>rows</span>
          </div>
        )}
        {duration > 0 && (
          <div style={st.stat}>
            <span style={st.statValue}>{duration < 1000 ? `${duration}ms` : `${(duration / 1000).toFixed(1)}s`}</span>
            <span style={st.statLabel}>duration</span>
          </div>
        )}
      </div>

      {/* Built models list (top 5) */}
      {models && models.filter((m) => m.result === "built").length > 0 && (
        <div style={st.modelList}>
          {models.filter((m) => m.result === "built").slice(0, 5).map((m) => (
            <span key={m.name} style={st.modelChip}>{m.name}</span>
          ))}
          {models.filter((m) => m.result === "built").length > 5 && (
            <span style={st.moreChip}>+{models.filter((m) => m.result === "built").length - 5} more</span>
          )}
        </div>
      )}

      <div style={st.actions}>
        <button onClick={() => onNavigate("Tables")} style={st.actionBtn}>
          View Tables
        </button>
        <button onClick={() => onNavigate("Query")} style={st.actionBtn}>
          Query Data
        </button>
        <button onClick={() => onNavigate("DAG")} style={st.actionBtn}>
          See DAG
        </button>
        <button onClick={() => onNavigate("History")} style={st.actionBtn}>
          History
        </button>
      </div>
    </div>
  );
}

const st = {
  container: {
    margin: "8px 12px",
    padding: "12px 16px",
    background: "var(--dp-bg-secondary)",
    border: "1px solid",
    borderRadius: "var(--dp-radius-lg)",
  },
  header: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    marginBottom: "10px",
  },
  statusBadge: {
    padding: "3px 10px",
    borderRadius: "var(--dp-radius)",
    color: "#fff",
    fontSize: "12px",
    fontWeight: 600,
  },
  dismiss: {
    background: "none",
    border: "none",
    color: "var(--dp-text-dim)",
    cursor: "pointer",
    fontSize: "11px",
  },
  stats: {
    display: "flex",
    gap: "20px",
    marginBottom: "10px",
  },
  stat: {
    display: "flex",
    alignItems: "baseline",
    gap: "4px",
  },
  statValue: {
    fontSize: "16px",
    fontWeight: 700,
    color: "var(--dp-text)",
    fontFamily: "var(--dp-font-mono)",
  },
  statLabel: {
    fontSize: "11px",
    color: "var(--dp-text-secondary)",
  },
  modelList: {
    display: "flex",
    flexWrap: "wrap",
    gap: "4px",
    marginBottom: "10px",
  },
  modelChip: {
    fontSize: "11px",
    fontFamily: "var(--dp-font-mono)",
    padding: "2px 8px",
    background: "var(--dp-bg-tertiary)",
    borderRadius: "var(--dp-radius)",
    color: "var(--dp-text)",
  },
  moreChip: {
    fontSize: "11px",
    padding: "2px 8px",
    color: "var(--dp-text-dim)",
  },
  actions: {
    display: "flex",
    gap: "6px",
  },
  actionBtn: {
    padding: "4px 12px",
    background: "var(--dp-btn-bg)",
    border: "1px solid var(--dp-btn-border)",
    borderRadius: "var(--dp-radius-lg)",
    color: "var(--dp-accent)",
    cursor: "pointer",
    fontSize: "12px",
    fontWeight: 500,
  },
};
