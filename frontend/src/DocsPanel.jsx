import React, { useState, useEffect } from "react";
import { api } from "./api";

function NavItem({ table, isActive, onClick }) {
  return (
    <div
      style={{
        ...s.navItem,
        background: isActive ? "var(--dp-bg-secondary)" : "transparent",
        borderLeft: isActive ? "2px solid var(--dp-accent)" : "2px solid transparent",
      }}
      onClick={onClick}
    >
      <span style={s.navLabel}>{table.name}</span>
      <span style={s.navBadge}>{table.type}</span>
    </div>
  );
}

function TableDetail({ table }) {
  const [showSql, setShowSql] = useState(false);

  if (!table) {
    return <div style={s.detailEmpty}>Select a table from the sidebar to view details.</div>;
  }

  const hasColDocs = table.columns.some((c) => c.description);

  return (
    <div style={s.detail}>
      <div style={s.detailHeader}>
        <code style={s.detailTitle}>{table.full_name}</code>
        <span style={s.detailType}>{table.type.toUpperCase()}</span>
      </div>

      {table.description && <p style={s.detailDesc}>{table.description}</p>}

      <div style={s.metaRow}>
        {table.materialized && (
          <span style={s.metaTag}>Materialized: {table.materialized}</span>
        )}
        {table.row_count != null && (
          <span style={s.metaTag}>Rows: {table.row_count.toLocaleString()}</span>
        )}
        {table.depends_on && table.depends_on.length > 0 && (
          <span style={s.metaTag}>
            Depends on: {table.depends_on.map((d) => (
              <code key={d} style={s.depCode}>{d}</code>
            ))}
          </span>
        )}
      </div>

      <h4 style={s.sectionTitle}>Columns ({table.columns.length})</h4>
      <table style={s.colTable}>
        <thead>
          <tr>
            <th style={s.colTh}>Column</th>
            <th style={s.colTh}>Type</th>
            <th style={s.colTh}>Nullable</th>
            {hasColDocs && <th style={s.colTh}>Description</th>}
          </tr>
        </thead>
        <tbody>
          {table.columns.map((col) => (
            <tr key={col.name}>
              <td style={s.colTdName}><code>{col.name}</code></td>
              <td style={s.colTd}>{col.type}</td>
              <td style={s.colTd}>{col.nullable ? "yes" : "no"}</td>
              {hasColDocs && <td style={s.colTd}>{col.description}</td>}
            </tr>
          ))}
        </tbody>
      </table>

      {table.sql && (
        <div style={s.sqlSection}>
          <button onClick={() => setShowSql(!showSql)} style={s.sqlToggle}>
            {showSql ? "\u25BE" : "\u25B8"} SQL Source
          </button>
          {showSql && (
            <pre style={s.sqlBlock}>
              <code>{table.sql}</code>
            </pre>
          )}
        </div>
      )}
    </div>
  );
}

export default function DocsPanel() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState(null);
  const [expandedSchemas, setExpandedSchemas] = useState({});

  useEffect(() => {
    loadDocs();
  }, []);

  async function loadDocs() {
    setLoading(true);
    try {
      const result = await api.getStructuredDocs();
      setData(result);
      // Auto-expand all schemas and select first table
      const expanded = {};
      for (const schema of result.schemas || []) {
        expanded[schema.name] = true;
      }
      setExpandedSchemas(expanded);
      if (!selected && result.schemas?.length > 0 && result.schemas[0].tables.length > 0) {
        setSelected(result.schemas[0].tables[0].full_name);
      }
    } catch {}
    setLoading(false);
  }

  function toggleSchema(name) {
    setExpandedSchemas((prev) => ({ ...prev, [name]: !prev[name] }));
  }

  // Find selected table object
  let selectedTable = null;
  if (data && selected) {
    for (const schema of data.schemas) {
      for (const t of schema.tables) {
        if (t.full_name === selected) {
          selectedTable = t;
          break;
        }
      }
      if (selectedTable) break;
    }
  }

  const totalTables = data ? data.schemas.reduce((sum, sc) => sum + sc.tables.length, 0) : 0;

  return (
    <div style={s.container}>
      <div style={s.header}>
        <span>Documentation</span>
        <span style={s.headerCount}>{totalTables > 0 ? `${totalTables} tables` : ""}</span>
        <button onClick={loadDocs} style={s.refreshBtn}>Refresh</button>
      </div>
      <div style={s.body}>
        {loading && <div style={s.loading}>Loading docs...</div>}
        {!loading && (!data || data.schemas.length === 0) && (
          <div style={s.loading}>No documentation available. Run a pipeline first.</div>
        )}
        {!loading && data && data.schemas.length > 0 && (
          <>
            <div style={s.nav}>
              {data.schemas.map((schema) => (
                <div key={schema.name}>
                  <div style={s.schemaRow} onClick={() => toggleSchema(schema.name)}>
                    <span style={{ ...s.schemaArrow, transform: expandedSchemas[schema.name] ? "rotate(0deg)" : "rotate(-90deg)" }}>
                      {"\u25BE"}
                    </span>
                    <span style={s.schemaName}>{schema.name}</span>
                    <span style={s.schemaCount}>{schema.tables.length}</span>
                  </div>
                  {expandedSchemas[schema.name] && schema.tables.map((t) => (
                    <NavItem
                      key={t.full_name}
                      table={t}
                      isActive={selected === t.full_name}
                      onClick={() => setSelected(t.full_name)}
                    />
                  ))}
                </div>
              ))}
            </div>
            <div style={s.content}>
              <TableDetail table={selectedTable} />
            </div>
          </>
        )}
      </div>
    </div>
  );
}

const s = {
  container: { display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" },
  header: { display: "flex", alignItems: "center", gap: "8px", padding: "8px 12px", borderBottom: "1px solid var(--dp-border)", fontWeight: 600, fontSize: "13px" },
  headerCount: { color: "var(--dp-text-dim)", fontWeight: 400, fontSize: "12px", flex: 1 },
  refreshBtn: { background: "var(--dp-btn-bg)", border: "1px solid var(--dp-btn-border)", borderRadius: "var(--dp-radius-lg)", color: "var(--dp-text)", padding: "4px 12px", cursor: "pointer", fontSize: "12px", fontWeight: 500 },
  body: { display: "flex", flex: 1, overflow: "hidden" },
  loading: { color: "var(--dp-text-secondary)", textAlign: "center", padding: "24px", width: "100%" },
  // Left nav
  nav: { width: "220px", minWidth: "220px", borderRight: "1px solid var(--dp-border)", overflow: "auto", padding: "4px 0" },
  schemaRow: { display: "flex", alignItems: "center", gap: "6px", padding: "6px 12px", cursor: "pointer", fontSize: "11px", fontWeight: 600, color: "var(--dp-text-dim)", letterSpacing: "0.5px", textTransform: "uppercase" },
  schemaArrow: { fontSize: "10px", width: "10px", display: "inline-block", transition: "transform 0.12s ease" },
  schemaName: { flex: 1 },
  schemaCount: { fontSize: "10px", color: "var(--dp-text-dim)", fontWeight: 400 },
  navItem: { display: "flex", alignItems: "center", gap: "6px", padding: "5px 12px 5px 28px", cursor: "pointer", fontSize: "12px", whiteSpace: "nowrap" },
  navLabel: { flex: 1, overflow: "hidden", textOverflow: "ellipsis", color: "var(--dp-text)" },
  navBadge: { fontSize: "10px", color: "var(--dp-text-dim)", flexShrink: 0 },
  // Right content
  content: { flex: 1, overflow: "auto", padding: "20px 28px" },
  detailEmpty: { color: "var(--dp-text-dim)", fontSize: "13px", padding: "24px", textAlign: "center" },
  detail: {},
  detailHeader: { display: "flex", alignItems: "center", gap: "10px", marginBottom: "12px" },
  detailTitle: { fontSize: "18px", fontWeight: 600, fontFamily: "var(--dp-font-mono)", color: "var(--dp-text)" },
  detailType: { fontSize: "11px", fontWeight: 500, color: "var(--dp-text-dim)", background: "var(--dp-bg-secondary)", padding: "2px 8px", borderRadius: "var(--dp-radius)" },
  detailDesc: { margin: "0 0 12px", fontSize: "13px", lineHeight: 1.5, color: "var(--dp-text-secondary)" },
  metaRow: { display: "flex", flexWrap: "wrap", gap: "8px", marginBottom: "16px" },
  metaTag: { fontSize: "12px", color: "var(--dp-text-secondary)", background: "var(--dp-bg-secondary)", padding: "3px 10px", borderRadius: "var(--dp-radius)", display: "inline-flex", alignItems: "center", gap: "4px" },
  depCode: { fontFamily: "var(--dp-font-mono)", fontSize: "11px", color: "var(--dp-accent)" },
  sectionTitle: { fontSize: "13px", fontWeight: 600, margin: "16px 0 8px", color: "var(--dp-text)" },
  colTable: { width: "100%", borderCollapse: "collapse", fontSize: "12px", fontFamily: "var(--dp-font-mono)" },
  colTh: { textAlign: "left", padding: "6px 12px", borderBottom: "1px solid var(--dp-border-light)", color: "var(--dp-text-secondary)", fontWeight: 600, fontFamily: "var(--dp-font)", fontSize: "11px" },
  colTd: { padding: "4px 12px", borderBottom: "1px solid var(--dp-border)", color: "var(--dp-text)", fontSize: "12px" },
  colTdName: { padding: "4px 12px", borderBottom: "1px solid var(--dp-border)", color: "var(--dp-accent)", fontSize: "12px" },
  sqlSection: { marginTop: "16px" },
  sqlToggle: { background: "none", border: "none", color: "var(--dp-accent)", cursor: "pointer", fontSize: "12px", fontWeight: 500, padding: "4px 0" },
  sqlBlock: { background: "var(--dp-bg-secondary)", border: "1px solid var(--dp-border)", borderRadius: "var(--dp-radius-lg)", padding: "12px", margin: "8px 0", fontSize: "12px", fontFamily: "var(--dp-font-mono)", overflow: "auto", color: "var(--dp-text)" },
};
