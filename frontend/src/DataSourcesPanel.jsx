import React, { useState, useEffect } from "react";
import { api } from "./api";

function _timeAgo(dateStr) {
  if (!dateStr) return "";
  const seconds = Math.floor((new Date() - new Date(dateStr)) / 1000);
  if (seconds < 60) return "just now";
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.floor(hours / 24)}d ago`;
}

/**
 * Unified Data Sources panel — merges the old Import and Connectors panels
 * into a single flow. Users pick a method (file, database, connector) and
 * can optionally make it recurring at the end.
 */
export default function DataSourcesPanel({ addOutput }) {
  const [view, setView] = useState("home"); // "home", "file", "database", "connector-catalog", "connector-setup", "manage"
  const [configured, setConfigured] = useState([]);
  const [available, setAvailable] = useState([]);
  const [loading, setLoading] = useState(true);

  // File import state
  const [filePath, setFilePath] = useState("");
  const [targetSchema, setTargetSchema] = useState("landing");
  const [targetTable, setTargetTable] = useState("");
  const [preview, setPreview] = useState(null);
  const [importing, setImporting] = useState(false);
  const [uploadedFile, setUploadedFile] = useState(null);

  // Database connection state
  const [connType, setConnType] = useState("postgres");
  const [connParams, setConnParams] = useState({ host: "localhost", port: "5432", database: "", user: "", password: "" });
  const [connTables, setConnTables] = useState([]);
  const [connTested, setConnTested] = useState(false);
  const [sourceTable, setSourceTable] = useState("");

  // Connector setup state
  const [selectedType, setSelectedType] = useState(null);
  const [step, setStep] = useState(1);
  const [configValues, setConfigValues] = useState({});
  const [connectionName, setConnectionName] = useState("");
  const [connTargetSchema, setConnTargetSchema] = useState("landing");
  const [schedule, setSchedule] = useState("");
  const [testResult, setTestResult] = useState(null);
  const [testing, setTesting] = useState(false);
  const [resources, setResources] = useState([]);
  const [selectedResources, setSelectedResources] = useState([]);
  const [discovering, setDiscovering] = useState(false);
  const [setting, setSetting] = useState(false);
  const [syncing, setSyncing] = useState(null);
  const [health, setHealth] = useState({});
  const [successBanner, setSuccessBanner] = useState(null);

  useEffect(() => { loadData(); }, []);

  // Auto-dismiss success banner after 8 seconds
  useEffect(() => {
    if (!successBanner) return;
    const timer = setTimeout(() => setSuccessBanner(null), 8000);
    return () => clearTimeout(timer);
  }, [successBanner]);

  async function loadData() {
    setLoading(true);
    try {
      const [avail, conf, healthData] = await Promise.all([
        api.listAvailableConnectors().catch(() => []),
        api.listConfiguredConnectors().catch(() => []),
        api.getConnectorHealth().catch(() => []),
      ]);
      setAvailable(avail);
      setConfigured(conf);
      // Index health by target
      const hMap = {};
      for (const h of healthData) {
        hMap[h.target] = h;
      }
      setHealth(hMap);
    } finally {
      setLoading(false);
    }
  }

  // --- File import ---
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

  async function doFileImport() {
    setImporting(true);
    try {
      const result = await api.importFile(filePath, targetSchema, targetTable || undefined);
      if (result.status === "success") {
        addOutput("info", `Imported ${result.rows} rows into ${result.table} (${result.duration_ms}ms)`);
        setSuccessBanner({ rows: result.rows, table: result.table });
        goHome();
      } else {
        addOutput("error", `Import failed: ${result.error}`);
      }
    } catch (e) {
      addOutput("error", `Import error: ${e.message}`);
    }
    setImporting(false);
  }

  // --- Database import ---
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

  async function doDbImport() {
    setImporting(true);
    try {
      const params = { ...connParams, port: parseInt(connParams.port) || 5432 };
      const result = await api.importFromConnection(connType, params, sourceTable, targetSchema, targetTable || undefined);
      if (result.status === "success") {
        addOutput("info", `Imported ${result.rows} rows into ${result.table} (${result.duration_ms}ms)`);
        setSuccessBanner({ rows: result.rows, table: result.table });
        goHome();
      } else {
        addOutput("error", `Import failed: ${result.error}`);
      }
    } catch (e) {
      addOutput("error", `Import error: ${e.message}`);
    }
    setImporting(false);
  }

  // --- Connector setup ---
  function startSetup(connector) {
    setSelectedType(connector);
    setStep(1);
    setTestResult(null);
    setResources([]);
    setSelectedResources([]);
    const defaults = {};
    for (const p of connector.params) {
      defaults[p.name] = p.default != null ? String(p.default) : "";
    }
    setConfigValues(defaults);
    setConnectionName(connector.name + "_1");
    setConnTargetSchema("landing");
    setSchedule(connector.default_schedule || "");
    setView("connector-setup");
  }

  async function doTest() {
    setTesting(true);
    setTestResult(null);
    try {
      const result = await api.testConnector(selectedType.name, configValues);
      setTestResult(result);
      if (result.success) {
        addOutput("info", `Connection test passed for ${selectedType.display_name}`);
        setStep(3);
        doDiscover();
      } else {
        addOutput("error", `Connection test failed: ${result.error}`);
      }
    } catch (e) {
      setTestResult({ success: false, error: e.message });
      addOutput("error", `Connection test error: ${e.message}`);
    } finally {
      setTesting(false);
    }
  }

  async function doDiscover() {
    setDiscovering(true);
    try {
      const res = await api.discoverConnector(selectedType.name, configValues);
      setResources(res);
      setSelectedResources(res.map((r) => r.name));
      addOutput("info", `Discovered ${res.length} resources`);
    } catch (e) {
      addOutput("error", `Discovery failed: ${e.message}`);
    } finally {
      setDiscovering(false);
    }
  }

  async function doSetup() {
    setSetting(true);
    try {
      const result = await api.setupConnector(
        selectedType.name, connectionName, configValues,
        selectedResources.length > 0 ? selectedResources : null,
        connTargetSchema, schedule || null,
      );
      addOutput("info", `Connector "${result.connection_name}" set up — ${result.tables.length} tables, script: ${result.script_path}`);
      await loadData();
      setView("home");
    } catch (e) {
      addOutput("error", `Setup failed: ${e.message}`);
    } finally {
      setSetting(false);
    }
  }

  async function doSync(name) {
    setSyncing(name);
    try {
      const result = await api.syncConnector(name);
      if (result.status === "success") {
        addOutput("info", `Synced "${name}" (${result.duration_ms}ms)`);
      } else {
        addOutput("error", `Sync failed for "${name}": ${result.error}`);
      }
    } catch (e) {
      addOutput("error", `Sync error: ${e.message}`);
    } finally {
      setSyncing(null);
    }
  }

  async function doRemove(name) {
    if (!confirm(`Remove connector "${name}"? This deletes the ingest script and config.`)) return;
    try {
      await api.removeConnector(name);
      addOutput("info", `Removed connector "${name}"`);
      await loadData();
    } catch (e) {
      addOutput("error", `Remove failed: ${e.message}`);
    }
  }

  async function doRegenerate(name) {
    try {
      const result = await api.regenerateConnector(name);
      if (result.status === "success") {
        addOutput("info", `Regenerated script for "${name}" — ${result.script_path}`);
      } else {
        addOutput("error", `Regenerate failed: ${result.error}`);
      }
    } catch (e) {
      addOutput("error", `Regenerate error: ${e.message}`);
    }
  }

  function toggleResource(name) {
    setSelectedResources((prev) =>
      prev.includes(name) ? prev.filter((r) => r !== name) : [...prev, name],
    );
  }

  function goHome() {
    setView("home");
    setPreview(null);
    setUploadedFile(null);
    setFilePath("");
    setTargetTable("");
    setConnTested(false);
    setSourceTable("");
  }

  if (loading) {
    return <div style={st.container}><div style={st.center}>Loading...</div></div>;
  }

  // --- Connector setup wizard ---
  if (view === "connector-setup" && selectedType) {
    return (
      <div style={st.container}>
        <div style={st.header}>
          <span style={st.title}>Set up {selectedType.display_name}</span>
          <button onClick={goHome} style={st.btnText}>Cancel</button>
        </div>
        <div style={st.content}>
          <div style={st.wizard}>
            <div style={st.steps}>
              {["Configure", "Test", "Select Resources", "Confirm"].map((label, i) => (
                <div key={i} style={{ ...st.step, ...(step >= i + 1 ? st.stepActive : {}) }}>
                  <span style={st.stepNum}>{i + 1}</span>
                  <span style={st.stepLabel}>{label}</span>
                </div>
              ))}
            </div>

            {step >= 1 && (
              <div style={st.section}>
                <h3 style={st.sectionTitle}>Connection Parameters</h3>
                <div style={st.formGroup}>
                  <label style={st.label}>Connection Name</label>
                  <input value={connectionName} onChange={(e) => setConnectionName(e.target.value)} style={st.input} placeholder="my_connection" />
                </div>
                {selectedType.params.map((p) => (
                  <div key={p.name} style={st.formGroup}>
                    <label style={st.label}>
                      {p.name.replace(/_/g, " ")}
                      {p.required && <span style={st.required}>*</span>}
                      {p.secret && <span style={st.secretBadge}>secret</span>}
                    </label>
                    <input
                      type={p.secret ? "password" : "text"}
                      value={configValues[p.name] || ""}
                      onChange={(e) => setConfigValues({ ...configValues, [p.name]: e.target.value })}
                      style={st.input}
                      placeholder={p.description}
                    />
                    {p.description && <span style={st.hint}>{p.description}</span>}
                  </div>
                ))}
                <div style={st.formRow}>
                  <div style={st.formGroup}>
                    <label style={st.label}>Target Schema</label>
                    <input value={connTargetSchema} onChange={(e) => setConnTargetSchema(e.target.value)} style={st.input} />
                  </div>
                  <div style={st.formGroup}>
                    <label style={st.label}>Schedule (cron)</label>
                    <input value={schedule} onChange={(e) => setSchedule(e.target.value)} style={st.input} placeholder={selectedType.default_schedule || "e.g. 0 6 * * *"} />
                  </div>
                </div>
                <div style={st.actions}>
                  <button onClick={() => { setStep(2); doTest(); }} disabled={testing || !connectionName} style={st.btnPrimary}>
                    {testing ? "Testing..." : "Test Connection"}
                  </button>
                  <button onClick={() => { setStep(3); doDiscover(); }} style={st.btn}>Skip Test</button>
                </div>
              </div>
            )}

            {step >= 2 && testResult && (
              <div style={st.section}>
                <h3 style={st.sectionTitle}>Connection Test</h3>
                <div style={{ ...st.testResult, borderColor: testResult.success ? "var(--dp-green)" : "var(--dp-red)" }}>
                  <span style={{ color: testResult.success ? "var(--dp-green)" : "var(--dp-red)", fontWeight: 600 }}>
                    {testResult.success ? "Connected successfully" : "Connection failed"}
                  </span>
                  {testResult.error && <div style={st.testError}>{testResult.error}</div>}
                </div>
                {!testResult.success && (
                  <div style={st.actions}>
                    <button onClick={() => { setStep(1); setTestResult(null); }} style={st.btn}>Back to Configure</button>
                  </div>
                )}
              </div>
            )}

            {step >= 3 && (
              <div style={st.section}>
                <h3 style={st.sectionTitle}>
                  Resources to Sync
                  {discovering && <span style={st.discovering}> (discovering...)</span>}
                </h3>
                {resources.length > 0 ? (
                  <>
                    <div style={st.resourceList}>
                      {resources.map((r) => (
                        <label key={r.name} style={st.resourceItem}>
                          <input type="checkbox" checked={selectedResources.includes(r.name)} onChange={() => toggleResource(r.name)} style={st.checkbox} />
                          <span style={st.resourceName}>{r.name}</span>
                          {r.description && <span style={st.resourceDesc}>{r.description}</span>}
                        </label>
                      ))}
                    </div>
                    <div style={st.resourceActions}>
                      <button onClick={() => setSelectedResources(resources.map((r) => r.name))} style={st.btnSmall}>Select All</button>
                      <button onClick={() => setSelectedResources([])} style={st.btnSmall}>Select None</button>
                      <span style={st.resourceCount}>{selectedResources.length}/{resources.length} selected</span>
                    </div>
                  </>
                ) : (
                  !discovering && <div style={st.dimText}>No resources discovered (tables will be auto-detected)</div>
                )}
                <div style={st.actions}>
                  <button onClick={() => setStep(4)} style={st.btnPrimary}>Continue</button>
                </div>
              </div>
            )}

            {step >= 4 && (
              <div style={st.section}>
                <h3 style={st.sectionTitle}>Review & Confirm</h3>
                <div style={st.summary}>
                  <div style={st.summaryRow}><span style={st.summaryLabel}>Connector</span><span>{selectedType.display_name}</span></div>
                  <div style={st.summaryRow}><span style={st.summaryLabel}>Name</span><span style={st.mono}>{connectionName}</span></div>
                  <div style={st.summaryRow}><span style={st.summaryLabel}>Schema</span><span style={st.mono}>{connTargetSchema}</span></div>
                  <div style={st.summaryRow}><span style={st.summaryLabel}>Resources</span><span>{selectedResources.length || "auto-detect"}</span></div>
                  {schedule && <div style={st.summaryRow}><span style={st.summaryLabel}>Schedule</span><span style={st.mono}>{schedule}</span></div>}
                </div>
                <div style={st.actions}>
                  <button onClick={doSetup} disabled={setting} style={st.btnPrimary}>
                    {setting ? "Setting up..." : "Create Connector"}
                  </button>
                  <button onClick={() => setStep(1)} style={st.btn}>Back</button>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    );
  }

  // --- Connector catalog ---
  if (view === "connector-catalog") {
    return (
      <div style={st.container}>
        <div style={st.header}>
          <span style={st.title}>Choose a Connector</span>
          <button onClick={goHome} style={st.btnText}>Back</button>
        </div>
        <div style={st.content}>
          <div style={st.catalog}>
            {available.map((c) => (
              <div key={c.name} style={st.card} onClick={() => startSetup(c)}>
                <div style={st.cardName}>{c.display_name}</div>
                <div style={st.cardDesc}>{c.description}</div>
                <div style={st.cardMeta}>
                  <span style={st.cardType}>{c.name}</span>
                  {c.default_schedule && <span style={st.cardSchedule}>{c.default_schedule}</span>}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    );
  }

  // --- File import ---
  if (view === "file") {
    return (
      <div style={st.container}>
        <div style={st.header}>
          <span style={st.title}>Import File</span>
          <button onClick={goHome} style={st.btnText}>Back</button>
        </div>
        <div style={st.content}>
          <div style={st.wizard}>
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
              <button onClick={doFileImport} disabled={!filePath || importing} style={st.btnPrimary}>
                {importing ? "Importing..." : "Import"}
              </button>
            </div>
          </div>

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

  // --- Database import ---
  if (view === "database") {
    return (
      <div style={st.container}>
        <div style={st.header}>
          <span style={st.title}>Import from Database</span>
          <button onClick={goHome} style={st.btnText}>Back</button>
        </div>
        <div style={st.content}>
          <div style={st.wizard}>
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
              <>
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
                <div style={st.actions}>
                  <button onClick={doDbImport} disabled={importing} style={st.btnPrimary}>
                    {importing ? "Importing..." : "Import"}
                  </button>
                </div>
              </>
            )}
          </div>
        </div>
      </div>
    );
  }

  // --- Manage existing connectors ---
  if (view === "manage") {
    return (
      <div style={st.container}>
        <div style={st.header}>
          <span style={st.title}>Manage Connectors</span>
          <button onClick={goHome} style={st.btnText}>Back</button>
        </div>
        <div style={st.content}>
          {configured.length === 0 ? (
            <div style={st.emptyState}>No connectors configured yet.</div>
          ) : (
            <div style={st.connectorList}>
              {configured.map((c) => {
                const scriptKey = `ingest/${c.name}.py`;
                const h = health[scriptKey];
                return (
                <div key={c.name} style={st.connectorItem}>
                  <div style={st.connectorMain}>
                    <div style={st.connectorNameRow}>
                      <span style={st.connectorName}>{c.name}</span>
                      <span style={st.connectorType}>{c.type}</span>
                      <span style={{ ...st.statusDot, background: h ? (h.status === "success" ? "var(--dp-green)" : "var(--dp-red)") : (c.has_script ? "var(--dp-yellow)" : "var(--dp-text-dim)") }} />
                      {h && <span style={{ ...st.healthInfo, color: h.status === "success" ? "var(--dp-text-dim)" : "var(--dp-red)" }}>
                        {h.status === "success" ? `Last synced ${_timeAgo(h.started_at)}` : `Last sync failed ${_timeAgo(h.started_at)}`}
                      </span>}
                    </div>
                    <div style={st.connectorParams}>
                      {Object.entries(c.params || {}).filter(([k]) => k !== "type").map(([k, v]) => (
                        <span key={k} style={st.paramChip}>{k}: {v}</span>
                      ))}
                    </div>
                  </div>
                  <div style={st.connectorActions}>
                    <button onClick={() => doSync(c.name)} disabled={syncing === c.name || !c.has_script} style={st.btn}>
                      {syncing === c.name ? "Syncing..." : "Sync"}
                    </button>
                    <button onClick={() => doRegenerate(c.name)} style={st.btn}>Regen</button>
                    <button onClick={() => doRemove(c.name)} style={st.btnDanger}>Remove</button>
                  </div>
                </div>
              );
              })}
            </div>
          )}
        </div>
      </div>
    );
  }

  // --- Home: choose method ---
  return (
    <div style={st.container}>
      <div style={st.header}>
        <span style={st.title}>Data Sources</span>
        {configured.length > 0 && (
          <button onClick={() => setView("manage")} style={st.btn}>
            Manage ({configured.length})
          </button>
        )}
      </div>
      <div style={st.content}>
        {/* Success banner after import */}
        {successBanner && (
          <div style={st.successBanner} onClick={() => setSuccessBanner(null)}>
            Imported {successBanner.rows.toLocaleString()} rows into <strong>{successBanner.table}</strong>
          </div>
        )}
        <div style={st.homeDesc}>
          How would you like to get data into your warehouse?
        </div>
        <div style={st.methodGrid}>
          <div style={st.methodCard} onClick={() => setView("file")}>
            <div style={st.methodIcon}>^</div>
            <div style={st.methodName}>Upload File</div>
            <div style={st.methodDesc}>
              Import a CSV, Parquet, or JSON file as a one-time load.
            </div>
          </div>
          <div style={st.methodCard} onClick={() => setView("database")}>
            <div style={st.methodIcon}>=</div>
            <div style={st.methodName}>Database Import</div>
            <div style={st.methodDesc}>
              Connect to PostgreSQL, MySQL, or SQLite and import a table.
            </div>
          </div>
          <div style={st.methodCard} onClick={() => setView("connector-catalog")}>
            <div style={st.methodIcon}>~</div>
            <div style={st.methodName}>Recurring Connector</div>
            <div style={st.methodDesc}>
              Set up a scheduled sync from databases, APIs, or other sources.
            </div>
          </div>
        </div>
        {configured.length > 0 && (
          <div style={st.configuredSection}>
            <div style={st.configuredHeader}>Active Connectors</div>
            {configured.map((c) => {
              const scriptKey = `ingest/${c.name}.py`;
              const h = health[scriptKey];
              return (
                <div key={c.name} style={st.configuredRow}>
                  <span style={{ ...st.statusDot, background: h ? (h.status === "success" ? "var(--dp-green)" : "var(--dp-red)") : (c.has_script ? "var(--dp-yellow)" : "var(--dp-text-dim)") }} />
                  <span style={st.connectorName}>{c.name}</span>
                  <span style={st.connectorType}>{c.type}</span>
                  {h && <span style={{ ...st.healthInfo, color: h.status === "success" ? "var(--dp-text-dim)" : "var(--dp-red)" }}>
                    {h.status === "success" ? `synced ${_timeAgo(h.started_at)}` : `failed ${_timeAgo(h.started_at)}`}
                  </span>}
                  <button onClick={() => doSync(c.name)} disabled={syncing === c.name || !c.has_script} style={st.btnSmall}>
                    {syncing === c.name ? "..." : "Sync"}
                  </button>
                </div>
              );
            })}
            <button onClick={() => setView("manage")} style={st.manageLink}>
              Manage all connectors
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

const st = {
  container: { display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" },
  center: { display: "flex", alignItems: "center", justifyContent: "center", height: "100%", color: "var(--dp-text-secondary)", fontSize: "13px" },
  header: { display: "flex", alignItems: "center", justifyContent: "space-between", padding: "8px 12px", borderBottom: "1px solid var(--dp-border)" },
  title: { fontWeight: 600, fontSize: "14px" },
  content: { flex: 1, overflow: "auto", padding: "16px" },

  // Home view
  homeDesc: { fontSize: "14px", color: "var(--dp-text-secondary)", marginBottom: "20px" },
  methodGrid: { display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: "12px", maxWidth: "800px", marginBottom: "24px" },
  methodCard: {
    padding: "24px 20px",
    background: "var(--dp-bg-secondary)",
    border: "1px solid var(--dp-border)",
    borderRadius: "var(--dp-radius-lg)",
    cursor: "pointer",
    textAlign: "center",
    transition: "border-color 0.15s",
  },
  methodIcon: {
    width: "40px",
    height: "40px",
    margin: "0 auto 12px",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    background: "var(--dp-bg-tertiary)",
    borderRadius: "var(--dp-radius-lg)",
    fontSize: "18px",
    fontWeight: 700,
    color: "var(--dp-accent)",
    fontFamily: "var(--dp-font-mono)",
  },
  methodName: { fontSize: "14px", fontWeight: 600, color: "var(--dp-text)", marginBottom: "6px" },
  methodDesc: { fontSize: "12px", color: "var(--dp-text-secondary)", lineHeight: 1.5 },

  // Configured connectors on home
  configuredSection: {
    borderTop: "1px solid var(--dp-border)",
    paddingTop: "16px",
    maxWidth: "600px",
  },
  configuredHeader: { fontSize: "12px", fontWeight: 600, color: "var(--dp-text-secondary)", marginBottom: "8px", textTransform: "uppercase", letterSpacing: "0.3px" },
  configuredRow: { display: "flex", alignItems: "center", gap: "8px", padding: "6px 0", fontSize: "13px" },
  manageLink: {
    background: "none",
    border: "none",
    color: "var(--dp-accent)",
    cursor: "pointer",
    fontSize: "12px",
    fontWeight: 500,
    padding: "8px 0 0",
  },

  // Shared styles
  btn: { padding: "5px 14px", background: "var(--dp-btn-bg)", border: "1px solid var(--dp-btn-border)", borderRadius: "var(--dp-radius-lg)", color: "var(--dp-text)", cursor: "pointer", fontSize: "12px", fontWeight: 500 },
  btnPrimary: { padding: "5px 14px", background: "var(--dp-green)", border: "1px solid var(--dp-green-border)", borderRadius: "var(--dp-radius-lg)", color: "#fff", cursor: "pointer", fontSize: "12px", fontWeight: 500 },
  btnDanger: { padding: "5px 14px", background: "var(--dp-btn-bg)", border: "1px solid var(--dp-btn-border)", borderRadius: "var(--dp-radius-lg)", color: "var(--dp-red)", cursor: "pointer", fontSize: "12px", fontWeight: 500 },
  btnText: { background: "none", border: "none", color: "var(--dp-text-secondary)", cursor: "pointer", fontSize: "12px", fontWeight: 500 },
  btnSmall: { padding: "2px 8px", background: "var(--dp-btn-bg)", border: "1px solid var(--dp-btn-border)", borderRadius: "var(--dp-radius)", color: "var(--dp-text-secondary)", cursor: "pointer", fontSize: "11px" },

  // Forms
  wizard: { maxWidth: "600px" },
  formGroup: { marginBottom: "12px", flex: 1 },
  formRow: { display: "flex", gap: "12px" },
  label: { display: "block", fontSize: "11px", color: "var(--dp-text-secondary)", marginBottom: "4px", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.3px" },
  required: { color: "var(--dp-red)", marginLeft: "2px" },
  secretBadge: { marginLeft: "6px", fontSize: "9px", color: "var(--dp-yellow)", background: "var(--dp-bg-tertiary)", padding: "1px 4px", borderRadius: "var(--dp-radius)", fontWeight: 500, textTransform: "none", letterSpacing: 0 },
  input: { width: "100%", padding: "6px 10px", background: "var(--dp-bg-tertiary)", border: "1px solid var(--dp-border-light)", borderRadius: "var(--dp-radius-lg)", color: "var(--dp-text)", fontSize: "13px", boxSizing: "border-box" },
  select: { width: "100%", padding: "6px 10px", background: "var(--dp-bg-tertiary)", border: "1px solid var(--dp-border-light)", borderRadius: "var(--dp-radius-lg)", color: "var(--dp-text)", fontSize: "13px" },
  hint: { display: "block", fontSize: "11px", color: "var(--dp-text-dim)", marginTop: "2px" },
  actions: { display: "flex", gap: "8px", marginTop: "12px" },

  // File upload
  fileInput: { fontSize: "12px", color: "var(--dp-text-secondary)" },
  uploadInfo: { fontSize: "12px", color: "var(--dp-accent)", marginTop: "4px" },

  // Preview table
  previewSection: { marginTop: "24px", border: "1px solid var(--dp-border)", borderRadius: "var(--dp-radius-lg)", overflow: "hidden" },
  previewHeader: { padding: "8px 12px", background: "var(--dp-bg-secondary)", borderBottom: "1px solid var(--dp-border)", fontSize: "12px", color: "var(--dp-text-secondary)" },
  tableWrap: { maxHeight: "300px", overflow: "auto" },
  table: { width: "100%", borderCollapse: "collapse", fontSize: "12px", fontFamily: "var(--dp-font-mono)" },
  th: { textAlign: "left", padding: "4px 8px", borderBottom: "1px solid var(--dp-border-light)", color: "var(--dp-text-secondary)", fontWeight: 600, position: "sticky", top: 0, background: "var(--dp-bg-tertiary)" },
  td: { padding: "3px 8px", borderBottom: "1px solid var(--dp-border)", color: "var(--dp-text)" },
  colType: { color: "var(--dp-text-dim)", fontWeight: 400, fontSize: "10px" },
  null: { color: "var(--dp-text-dim)", fontStyle: "italic" },

  // Connector wizard
  section: { marginBottom: "24px", paddingBottom: "16px", borderBottom: "1px solid var(--dp-border)" },
  sectionTitle: { fontSize: "14px", fontWeight: 600, margin: "0 0 12px" },
  steps: { display: "flex", gap: "4px", marginBottom: "24px" },
  step: { display: "flex", alignItems: "center", gap: "6px", padding: "6px 12px", borderRadius: "var(--dp-radius-lg)", fontSize: "12px", color: "var(--dp-text-dim)", background: "var(--dp-bg-tertiary)" },
  stepActive: { color: "var(--dp-text)", background: "var(--dp-btn-bg)", fontWeight: 500 },
  stepNum: { width: "18px", height: "18px", borderRadius: "50%", display: "inline-flex", alignItems: "center", justifyContent: "center", fontSize: "10px", fontWeight: 700, background: "var(--dp-bg-tertiary)", color: "var(--dp-text-secondary)" },
  stepLabel: {},

  // Test
  testResult: { padding: "10px 14px", border: "1px solid", borderRadius: "var(--dp-radius-lg)", marginBottom: "8px" },
  testError: { fontSize: "12px", color: "var(--dp-text-secondary)", marginTop: "4px" },

  // Resources
  discovering: { fontSize: "12px", color: "var(--dp-text-secondary)", fontWeight: 400 },
  resourceList: { display: "flex", flexDirection: "column", gap: "2px", marginBottom: "8px" },
  resourceItem: { display: "flex", alignItems: "center", gap: "8px", padding: "4px 8px", borderRadius: "var(--dp-radius)", cursor: "pointer", fontSize: "13px" },
  checkbox: { accentColor: "var(--dp-green)" },
  resourceName: { fontFamily: "var(--dp-font-mono)", fontWeight: 500 },
  resourceDesc: { color: "var(--dp-text-secondary)", fontSize: "12px" },
  resourceActions: { display: "flex", gap: "6px", alignItems: "center", marginBottom: "8px" },
  resourceCount: { fontSize: "11px", color: "var(--dp-text-dim)" },

  // Summary
  summary: { background: "var(--dp-bg-tertiary)", borderRadius: "var(--dp-radius-lg)", padding: "12px 16px", marginBottom: "12px" },
  summaryRow: { display: "flex", justifyContent: "space-between", padding: "4px 0", fontSize: "13px", borderBottom: "1px solid var(--dp-border)" },
  summaryLabel: { color: "var(--dp-text-secondary)", fontWeight: 500 },
  mono: { fontFamily: "var(--dp-font-mono)" },
  dimText: { fontSize: "12px", color: "var(--dp-text-dim)", marginBottom: "8px" },

  // Connector catalog
  catalog: { display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(240px, 1fr))", gap: "12px", maxWidth: "900px" },
  card: { padding: "16px", background: "var(--dp-bg-secondary)", border: "1px solid var(--dp-border)", borderRadius: "var(--dp-radius-lg)", cursor: "pointer", transition: "border-color 0.15s" },
  cardName: { fontWeight: 600, fontSize: "14px", marginBottom: "8px" },
  cardDesc: { fontSize: "12px", color: "var(--dp-text-secondary)", lineHeight: 1.5, marginBottom: "12px" },
  cardMeta: { display: "flex", gap: "8px", alignItems: "center" },
  cardType: { fontSize: "11px", color: "var(--dp-text-dim)", fontFamily: "var(--dp-font-mono)", background: "var(--dp-bg-tertiary)", padding: "1px 6px", borderRadius: "var(--dp-radius)" },
  cardSchedule: { fontSize: "10px", color: "var(--dp-text-dim)" },

  // Manage connectors
  connectorList: { display: "flex", flexDirection: "column", gap: "8px", maxWidth: "800px" },
  connectorItem: { display: "flex", alignItems: "center", justifyContent: "space-between", padding: "12px 16px", background: "var(--dp-bg-secondary)", border: "1px solid var(--dp-border)", borderRadius: "var(--dp-radius-lg)", gap: "16px" },
  connectorMain: { flex: 1, minWidth: 0 },
  connectorNameRow: { display: "flex", alignItems: "center", gap: "8px", marginBottom: "4px" },
  connectorName: { fontWeight: 600, fontSize: "14px", fontFamily: "var(--dp-font-mono)" },
  connectorType: { fontSize: "11px", color: "var(--dp-text-secondary)", background: "var(--dp-bg-tertiary)", padding: "1px 6px", borderRadius: "var(--dp-radius)", fontWeight: 500 },
  statusDot: { width: "6px", height: "6px", borderRadius: "50%", flexShrink: 0 },
  connectorParams: { display: "flex", flexWrap: "wrap", gap: "4px" },
  paramChip: { fontSize: "11px", color: "var(--dp-text-dim)", fontFamily: "var(--dp-font-mono)" },
  connectorActions: { display: "flex", gap: "6px", flexShrink: 0 },

  emptyState: { textAlign: "center", padding: "32px", color: "var(--dp-text-dim)", fontSize: "13px" },

  // Success banner
  successBanner: {
    padding: "10px 16px",
    background: "rgba(46, 160, 67, 0.1)",
    border: "1px solid var(--dp-green)",
    borderRadius: "var(--dp-radius-lg)",
    color: "var(--dp-green)",
    fontSize: "13px",
    marginBottom: "16px",
    cursor: "pointer",
  },
  // Health info
  healthInfo: { fontSize: "11px", marginLeft: "auto" },
};
