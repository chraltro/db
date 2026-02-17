import React, { useState } from "react";
import { api } from "./api";

export default function ImportPanel({ addOutput }) {
  const [mode, setMode] = useState("file"); // "file" or "connection"
  const [filePath, setFilePath] = useState("");
  const [targetSchema, setTargetSchema] = useState("landing");
  const [targetTable, setTargetTable] = useState("");
  const [preview, setPreview] = useState(null);
  const [importing, setImporting] = useState(false);
  const [uploadedFile, setUploadedFile] = useState(null);

  // Connection state
  const [connType, setConnType] = useState("postgres");
  const [connParams, setConnParams] = useState({ host: "localhost", port: "5432", database: "", user: "", password: "" });
  const [connTables, setConnTables] = useState([]);
  const [connTested, setConnTested] = useState(false);
  const [sourceTable, setSourceTable] = useState("");

  async function handleUpload(e) {
    const file = e.target.files[0];
    if (!file) return;
    try {
      const result = await api.uploadFile(file);
      setUploadedFile(result);
      setFilePath(result.path);
      setTargetTable(file.name.split(".")[0].replace(/[^a-zA-Z0-9_]/g, "_"));
      addOutput("info", `Uploaded ${result.name} (${(result.size / 1024).toFixed(1)}KB)`);
    } catch (e) {
      addOutput("error", `Upload failed: ${e.message}`);
    }
  }

  async function doPreview() {
    try {
      const data = await api.previewFile(filePath, targetSchema, targetTable);
      setPreview(data);
      addOutput("info", `Preview: ${data.total_preview} rows, ${data.columns.length} columns`);
    } catch (e) {
      addOutput("error", `Preview failed: ${e.message}`);
    }
  }

  async function doImport() {
    setImporting(true);
    try {
      if (mode === "file") {
        const result = await api.importFile(filePath, targetSchema, targetTable || undefined);
        if (result.status === "success") {
          addOutput("info", `Imported ${result.rows} rows into ${result.table} (${result.duration_ms}ms)`);
        } else {
          addOutput("error", `Import failed: ${result.error}`);
        }
      } else {
        const params = { ...connParams, port: parseInt(connParams.port) || 5432 };
        const result = await api.importFromConnection(connType, params, sourceTable, targetSchema, targetTable || undefined);
        if (result.status === "success") {
          addOutput("info", `Imported ${result.rows} rows into ${result.table} (${result.duration_ms}ms)`);
        } else {
          addOutput("error", `Import failed: ${result.error}`);
        }
      }
    } catch (e) {
      addOutput("error", `Import error: ${e.message}`);
    }
    setImporting(false);
  }

  async function testConnection() {
    const params = { ...connParams, port: parseInt(connParams.port) || 5432 };
    try {
      const result = await api.testConnection(connType, params);
      if (result.success) {
        setConnTested(true);
        setConnTables(result.tables || []);
        addOutput("info", `Connected! Found ${result.tables.length} tables.`);
      } else {
        addOutput("error", `Connection failed: ${result.error}`);
      }
    } catch (e) {
      addOutput("error", `Connection error: ${e.message}`);
    }
  }

  return (
    <div style={st.container}>
      <div style={st.header}>
        <span style={st.title}>Import Data</span>
        <div style={st.modeToggle}>
          <button onClick={() => setMode("file")} style={mode === "file" ? st.modeActive : st.modeBtn}>
            File Upload
          </button>
          <button onClick={() => setMode("connection")} style={mode === "connection" ? st.modeActive : st.modeBtn}>
            Database
          </button>
        </div>
      </div>

      <div style={st.content}>
        {mode === "file" ? (
          <div style={st.section}>
            <div style={st.formGroup}>
              <label style={st.label}>Upload a CSV, Parquet, or JSON file</label>
              <input type="file" onChange={handleUpload} accept=".csv,.parquet,.pq,.json,.jsonl,.ndjson" style={st.fileInput} />
              {uploadedFile && <div style={st.uploadInfo}>Uploaded: {uploadedFile.name}</div>}
            </div>
            <div style={st.formGroup}>
              <label style={st.label}>Or enter file path</label>
              <input value={filePath} onChange={(e) => setFilePath(e.target.value)} style={st.input} placeholder="/path/to/data.csv" />
            </div>
            <div style={st.formRow}>
              <div style={st.formGroup}>
                <label style={st.label}>Target Schema</label>
                <input value={targetSchema} onChange={(e) => setTargetSchema(e.target.value)} style={st.input} />
              </div>
              <div style={st.formGroup}>
                <label style={st.label}>Table Name (auto-detect if empty)</label>
                <input value={targetTable} onChange={(e) => setTargetTable(e.target.value)} style={st.input} placeholder="auto" />
              </div>
            </div>
            <div style={st.actions}>
              <button onClick={doPreview} disabled={!filePath} style={st.btn}>Preview</button>
              <button onClick={doImport} disabled={!filePath || importing} style={st.btnPrimary}>
                {importing ? "Importing..." : "Import"}
              </button>
            </div>
          </div>
        ) : (
          <div style={st.section}>
            <div style={st.formGroup}>
              <label style={st.label}>Connection Type</label>
              <select value={connType} onChange={(e) => { setConnType(e.target.value); setConnTested(false); }} style={st.select}>
                <option value="postgres">PostgreSQL</option>
                <option value="mysql">MySQL</option>
                <option value="sqlite">SQLite</option>
              </select>
            </div>
            {connType !== "sqlite" ? (
              <>
                <div style={st.formRow}>
                  <div style={st.formGroup}>
                    <label style={st.label}>Host</label>
                    <input value={connParams.host} onChange={(e) => setConnParams({ ...connParams, host: e.target.value })} style={st.input} />
                  </div>
                  <div style={{ ...st.formGroup, maxWidth: "100px" }}>
                    <label style={st.label}>Port</label>
                    <input value={connParams.port} onChange={(e) => setConnParams({ ...connParams, port: e.target.value })} style={st.input} />
                  </div>
                </div>
                <div style={st.formRow}>
                  <div style={st.formGroup}>
                    <label style={st.label}>Database</label>
                    <input value={connParams.database} onChange={(e) => setConnParams({ ...connParams, database: e.target.value })} style={st.input} />
                  </div>
                  <div style={st.formGroup}>
                    <label style={st.label}>User</label>
                    <input value={connParams.user} onChange={(e) => setConnParams({ ...connParams, user: e.target.value })} style={st.input} />
                  </div>
                </div>
                <div style={st.formGroup}>
                  <label style={st.label}>Password</label>
                  <input type="password" value={connParams.password} onChange={(e) => setConnParams({ ...connParams, password: e.target.value })} style={st.input} />
                </div>
              </>
            ) : (
              <div style={st.formGroup}>
                <label style={st.label}>Database Path</label>
                <input value={connParams.database} onChange={(e) => setConnParams({ ...connParams, database: e.target.value })} style={st.input} placeholder="/path/to/db.sqlite" />
              </div>
            )}
            <div style={st.actions}>
              <button onClick={testConnection} style={st.btn}>Test Connection</button>
            </div>
            {connTested && connTables.length > 0 && (
              <div style={st.formGroup}>
                <label style={st.label}>Source Table</label>
                <select value={sourceTable} onChange={(e) => setSourceTable(e.target.value)} style={st.select}>
                  <option value="">Select a table...</option>
                  {connTables.map((t) => (
                    <option key={`${t.schema}.${t.name}`} value={`${t.schema}.${t.name}`}>
                      {t.schema}.{t.name}
                    </option>
                  ))}
                </select>
              </div>
            )}
            {connTested && sourceTable && (
              <div style={st.formRow}>
                <div style={st.formGroup}>
                  <label style={st.label}>Target Schema</label>
                  <input value={targetSchema} onChange={(e) => setTargetSchema(e.target.value)} style={st.input} />
                </div>
                <div style={st.formGroup}>
                  <label style={st.label}>Table Name</label>
                  <input value={targetTable} onChange={(e) => setTargetTable(e.target.value)} style={st.input} placeholder="auto" />
                </div>
              </div>
            )}
            {connTested && sourceTable && (
              <div style={st.actions}>
                <button onClick={doImport} disabled={importing} style={st.btnPrimary}>
                  {importing ? "Importing..." : "Import"}
                </button>
              </div>
            )}
          </div>
        )}

        {preview && (
          <div style={st.previewSection}>
            <div style={st.previewHeader}>
              Preview: {preview.total_preview} rows, {preview.columns.length} columns
            </div>
            <div style={st.tableWrap}>
              <table style={st.table}>
                <thead>
                  <tr>
                    {preview.columns.map((col, i) => (
                      <th key={i} style={st.th}>
                        {col}
                        {preview.column_types?.[i] && (
                          <span style={st.colType}> {preview.column_types[i].type}</span>
                        )}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {preview.rows.slice(0, 50).map((row, ri) => (
                    <tr key={ri}>
                      {row.map((v, ci) => (
                        <td key={ci} style={st.td}>
                          {v === null ? <span style={st.null}>NULL</span> : String(v)}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

const st = {
  container: { display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" },
  header: {
    display: "flex", alignItems: "center", justifyContent: "space-between",
    padding: "8px 12px", borderBottom: "1px solid #21262d",
  },
  title: { fontWeight: 600, fontSize: "14px" },
  modeToggle: { display: "flex", gap: "2px", background: "#0d1117", borderRadius: "6px", padding: "2px" },
  modeBtn: {
    padding: "4px 12px", background: "none", border: "none", borderRadius: "4px",
    color: "#8b949e", cursor: "pointer", fontSize: "12px",
  },
  modeActive: {
    padding: "4px 12px", background: "#21262d", border: "none", borderRadius: "4px",
    color: "#e1e4e8", cursor: "pointer", fontSize: "12px",
  },
  content: { flex: 1, overflow: "auto", padding: "16px" },
  section: { maxWidth: "600px" },
  formGroup: { marginBottom: "12px", flex: 1 },
  formRow: { display: "flex", gap: "12px" },
  label: { display: "block", fontSize: "12px", color: "#8b949e", marginBottom: "4px" },
  input: {
    width: "100%", padding: "6px 10px", background: "#0d1117", border: "1px solid #30363d",
    borderRadius: "6px", color: "#e1e4e8", fontSize: "13px", boxSizing: "border-box",
  },
  select: {
    width: "100%", padding: "6px 10px", background: "#0d1117", border: "1px solid #30363d",
    borderRadius: "6px", color: "#e1e4e8", fontSize: "13px",
  },
  fileInput: { fontSize: "12px", color: "#8b949e" },
  uploadInfo: { fontSize: "12px", color: "#58a6ff", marginTop: "4px" },
  actions: { display: "flex", gap: "8px", marginTop: "12px" },
  btn: {
    padding: "6px 16px", background: "#21262d", border: "1px solid #30363d",
    borderRadius: "6px", color: "#e1e4e8", cursor: "pointer", fontSize: "12px",
  },
  btnPrimary: {
    padding: "6px 16px", background: "#238636", border: "1px solid #2ea043",
    borderRadius: "6px", color: "#fff", cursor: "pointer", fontSize: "12px",
  },
  previewSection: { marginTop: "24px", border: "1px solid #21262d", borderRadius: "6px", overflow: "hidden" },
  previewHeader: {
    padding: "8px 12px", background: "#161b22", borderBottom: "1px solid #21262d",
    fontSize: "12px", color: "#8b949e",
  },
  tableWrap: { maxHeight: "300px", overflow: "auto" },
  table: { width: "100%", borderCollapse: "collapse", fontSize: "12px", fontFamily: "monospace" },
  th: { textAlign: "left", padding: "4px 8px", borderBottom: "1px solid #30363d", color: "#8b949e", fontWeight: 600, position: "sticky", top: 0, background: "#0d1117" },
  td: { padding: "3px 8px", borderBottom: "1px solid #21262d", color: "#c9d1d9" },
  colType: { color: "#484f58", fontWeight: 400, fontSize: "10px" },
  null: { color: "#484f58", fontStyle: "italic" },
};
