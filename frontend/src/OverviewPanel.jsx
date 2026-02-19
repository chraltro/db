import React, { useState, useEffect } from "react";
import { api } from "./api";

function timeAgo(dateStr) {
  if (!dateStr) return "";
  const now = new Date();
  const d = new Date(dateStr);
  const seconds = Math.floor((now - d) / 1000);
  if (seconds < 60) return "just now";
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function formatRows(n) {
  if (n == null) return "-";
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

export default function OverviewPanel({ onNavigate }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    load();
  }, []);

  async function load() {
    setLoading(true);
    try {
      const overview = await api.getOverview();
      setData(overview);
    } catch (e) {
      console.error("Failed to load overview:", e);
    } finally {
      setLoading(false);
    }
  }

  if (loading) {
    return <div style={st.container}><div style={st.center}>Loading overview...</div></div>;
  }

  if (!data) {
    return <div style={st.container}><div style={st.center}>Failed to load overview.</div></div>;
  }

  const recentRuns = data.recent_runs || [];
  const successCount = recentRuns.filter((r) => r.status === "success").length;
  const errorCount = recentRuns.filter((r) => r.status === "error").length;
  const schemas = data.schemas || [];
  const lastRun = recentRuns[0];

  return (
    <div style={st.container}>
      <div style={st.scrollArea}>
        {/* Hero / CTA section */}
        {!data.has_data && (
          <div style={st.hero}>
            <div style={st.heroTitle}>Get started with dp</div>
            <div style={st.heroDesc}>
              Connect a data source to start building your warehouse. Import a CSV, connect a database, or set up a recurring connector.
            </div>
            <button onClick={() => onNavigate("Data Sources")} style={st.heroCta}>
              Connect Your Data
            </button>
          </div>
        )}

        {/* Stats row */}
        <div style={st.statsRow}>
          <div style={st.statCard}>
            <div style={st.statValue}>{data.total_tables}</div>
            <div style={st.statLabel}>Tables</div>
          </div>
          <div style={st.statCard}>
            <div style={st.statValue}>{formatRows(data.total_rows)}</div>
            <div style={st.statLabel}>Total Rows</div>
          </div>
          <div style={st.statCard}>
            <div style={st.statValue}>{data.connectors}</div>
            <div style={st.statLabel}>Connectors</div>
          </div>
          <div style={st.statCard}>
            <div style={{ ...st.statValue, color: errorCount > 0 ? "var(--dp-red)" : "var(--dp-green)" }}>
              {recentRuns.length > 0 ? `${successCount}/${recentRuns.length}` : "-"}
            </div>
            <div style={st.statLabel}>Runs OK (recent)</div>
          </div>
        </div>

        <div style={st.columns}>
          {/* Left column */}
          <div style={st.column}>
            {/* Pipeline health */}
            <div style={st.card}>
              <div style={st.cardHeader}>
                <span style={st.cardTitle}>Pipeline Health</span>
                {lastRun && (
                  <span style={st.cardSubtitle}>Last run {timeAgo(lastRun.started_at)}</span>
                )}
              </div>
              {recentRuns.length === 0 ? (
                <div style={st.emptyState}>
                  No pipeline runs yet. Run a stream or transform to see activity here.
                </div>
              ) : (
                <div style={st.runList}>
                  {recentRuns.slice(0, 10).map((run) => (
                    <div key={run.run_id} style={st.runItem}>
                      <span style={{
                        ...st.statusDot,
                        background: run.status === "success" ? "var(--dp-green)" : "var(--dp-red)",
                      }} />
                      <span style={st.runType}>{run.run_type}</span>
                      <span style={st.runTarget}>{run.target}</span>
                      <span style={st.runMeta}>
                        {run.rows_affected > 0 && <span>{formatRows(run.rows_affected)} rows</span>}
                        {run.duration_ms > 0 && <span>{run.duration_ms}ms</span>}
                      </span>
                      <span style={st.runTime}>{timeAgo(run.started_at)}</span>
                    </div>
                  ))}
                </div>
              )}
              {recentRuns.length > 0 && (
                <button onClick={() => onNavigate("History")} style={st.cardLink}>
                  View all history
                </button>
              )}
            </div>
          </div>

          {/* Right column */}
          <div style={st.column}>
            {/* Warehouse summary */}
            <div style={st.card}>
              <div style={st.cardHeader}>
                <span style={st.cardTitle}>Warehouse</span>
              </div>
              {schemas.length === 0 ? (
                <div style={st.emptyState}>
                  No data in the warehouse yet.
                </div>
              ) : (
                <div style={st.schemaList}>
                  {schemas.map((s) => (
                    <div key={s.name} style={st.schemaItem}>
                      <span style={st.schemaName}>{s.name}</span>
                      <span style={st.schemaStat}>{s.tables} table{s.tables !== 1 ? "s" : ""}</span>
                      {s.views > 0 && <span style={st.schemaStat}>{s.views} view{s.views !== 1 ? "s" : ""}</span>}
                      <span style={st.schemaRows}>{formatRows(s.total_rows)} rows</span>
                    </div>
                  ))}
                </div>
              )}
              {schemas.length > 0 && (
                <button onClick={() => onNavigate("Tables")} style={st.cardLink}>
                  Browse tables
                </button>
              )}
            </div>

            {/* Quick actions */}
            <div style={st.card}>
              <div style={st.cardHeader}>
                <span style={st.cardTitle}>Quick Actions</span>
              </div>
              <div style={st.quickActions}>
                <button onClick={() => onNavigate("Data Sources")} style={st.quickAction}>
                  <span style={st.qaIcon}>+</span>
                  <span>Add Data Source</span>
                </button>
                <button onClick={() => onNavigate("Query")} style={st.quickAction}>
                  <span style={st.qaIcon}>&gt;</span>
                  <span>Run a Query</span>
                </button>
                <button onClick={() => onNavigate("Editor")} style={st.quickAction}>
                  <span style={st.qaIcon}>#</span>
                  <span>Edit Transforms</span>
                </button>
                <button onClick={() => onNavigate("DAG")} style={st.quickAction}>
                  <span style={st.qaIcon}>&bull;</span>
                  <span>View DAG</span>
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

const st = {
  container: { display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" },
  center: { display: "flex", alignItems: "center", justifyContent: "center", height: "100%", color: "var(--dp-text-secondary)", fontSize: "13px" },
  scrollArea: { flex: 1, overflow: "auto", padding: "20px 24px" },

  // Hero
  hero: {
    background: "linear-gradient(135deg, var(--dp-bg-secondary), var(--dp-bg-tertiary))",
    border: "1px solid var(--dp-border)",
    borderRadius: "var(--dp-radius-lg)",
    padding: "32px",
    textAlign: "center",
    marginBottom: "20px",
  },
  heroTitle: { fontSize: "20px", fontWeight: 700, color: "var(--dp-text)", marginBottom: "8px" },
  heroDesc: { fontSize: "14px", color: "var(--dp-text-secondary)", lineHeight: 1.6, maxWidth: "480px", margin: "0 auto 20px" },
  heroCta: {
    padding: "10px 28px",
    background: "var(--dp-green)",
    border: "1px solid var(--dp-green-border)",
    borderRadius: "var(--dp-radius-lg)",
    color: "#fff",
    cursor: "pointer",
    fontSize: "14px",
    fontWeight: 600,
  },

  // Stats
  statsRow: {
    display: "grid",
    gridTemplateColumns: "repeat(4, 1fr)",
    gap: "12px",
    marginBottom: "20px",
  },
  statCard: {
    background: "var(--dp-bg-secondary)",
    border: "1px solid var(--dp-border)",
    borderRadius: "var(--dp-radius-lg)",
    padding: "16px",
    textAlign: "center",
  },
  statValue: { fontSize: "24px", fontWeight: 700, color: "var(--dp-text)", fontFamily: "var(--dp-font-mono)" },
  statLabel: { fontSize: "11px", color: "var(--dp-text-secondary)", marginTop: "4px", textTransform: "uppercase", letterSpacing: "0.5px", fontWeight: 500 },

  // Layout
  columns: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: "16px" },
  column: { display: "flex", flexDirection: "column", gap: "16px" },

  // Cards
  card: {
    background: "var(--dp-bg-secondary)",
    border: "1px solid var(--dp-border)",
    borderRadius: "var(--dp-radius-lg)",
    overflow: "hidden",
  },
  cardHeader: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "12px 16px",
    borderBottom: "1px solid var(--dp-border)",
  },
  cardTitle: { fontSize: "13px", fontWeight: 600, color: "var(--dp-text)" },
  cardSubtitle: { fontSize: "11px", color: "var(--dp-text-dim)" },
  cardLink: {
    display: "block",
    width: "100%",
    padding: "8px 16px",
    background: "none",
    border: "none",
    borderTop: "1px solid var(--dp-border)",
    color: "var(--dp-accent)",
    cursor: "pointer",
    fontSize: "12px",
    fontWeight: 500,
    textAlign: "left",
  },

  emptyState: {
    padding: "20px 16px",
    color: "var(--dp-text-dim)",
    fontSize: "13px",
    textAlign: "center",
    lineHeight: 1.5,
  },

  // Runs
  runList: { padding: "4px 0" },
  runItem: {
    display: "flex",
    alignItems: "center",
    gap: "8px",
    padding: "6px 16px",
    fontSize: "12px",
    borderBottom: "1px solid var(--dp-border)",
  },
  statusDot: { width: "6px", height: "6px", borderRadius: "50%", flexShrink: 0 },
  runType: {
    fontSize: "10px",
    fontWeight: 600,
    color: "var(--dp-text-secondary)",
    background: "var(--dp-bg-tertiary)",
    padding: "1px 6px",
    borderRadius: "var(--dp-radius)",
    textTransform: "uppercase",
    flexShrink: 0,
  },
  runTarget: {
    fontFamily: "var(--dp-font-mono)",
    color: "var(--dp-text)",
    flex: 1,
    overflow: "hidden",
    textOverflow: "ellipsis",
    whiteSpace: "nowrap",
  },
  runMeta: {
    display: "flex",
    gap: "8px",
    color: "var(--dp-text-dim)",
    fontSize: "11px",
    fontFamily: "var(--dp-font-mono)",
    flexShrink: 0,
  },
  runTime: { color: "var(--dp-text-dim)", fontSize: "11px", flexShrink: 0 },

  // Schemas
  schemaList: { padding: "4px 0" },
  schemaItem: {
    display: "flex",
    alignItems: "center",
    gap: "10px",
    padding: "8px 16px",
    fontSize: "13px",
    borderBottom: "1px solid var(--dp-border)",
  },
  schemaName: {
    fontWeight: 600,
    fontFamily: "var(--dp-font-mono)",
    color: "var(--dp-accent)",
    minWidth: "70px",
  },
  schemaStat: { color: "var(--dp-text-secondary)", fontSize: "12px" },
  schemaRows: {
    marginLeft: "auto",
    fontFamily: "var(--dp-font-mono)",
    color: "var(--dp-text-dim)",
    fontSize: "12px",
  },

  // Quick actions
  quickActions: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1px", background: "var(--dp-border)" },
  quickAction: {
    display: "flex",
    alignItems: "center",
    gap: "8px",
    padding: "14px 16px",
    background: "var(--dp-bg-secondary)",
    border: "none",
    color: "var(--dp-text)",
    cursor: "pointer",
    fontSize: "13px",
    fontWeight: 500,
    textAlign: "left",
  },
  qaIcon: {
    width: "24px",
    height: "24px",
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    background: "var(--dp-bg-tertiary)",
    borderRadius: "var(--dp-radius)",
    fontFamily: "var(--dp-font-mono)",
    fontSize: "14px",
    fontWeight: 700,
    color: "var(--dp-accent)",
    flexShrink: 0,
  },
};
