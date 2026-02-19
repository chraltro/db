import React, { useState, useEffect } from "react";
import { api } from "./api";

export default function ConnectorsPanel({ addOutput }) {
  const [view, setView] = useState("list"); // "list", "add", "setup"
  const [available, setAvailable] = useState([]);
  const [configured, setConfigured] = useState([]);
  const [loading, setLoading] = useState(true);
  const [syncing, setSyncing] = useState(null);

  // Setup wizard state
  const [selectedType, setSelectedType] = useState(null);
  const [step, setStep] = useState(1); // 1=config, 2=test, 3=discover, 4=confirm
  const [configValues, setConfigValues] = useState({});
  const [connectionName, setConnectionName] = useState("");
  const [targetSchema, setTargetSchema] = useState("landing");
  const [schedule, setSchedule] = useState("");
  const [testResult, setTestResult] = useState(null);
  const [testing, setTesting] = useState(false);
  const [resources, setResources] = useState([]);
  const [selectedResources, setSelectedResources] = useState([]);
  const [discovering, setDiscovering] = useState(false);
  const [setting, setSetting] = useState(false);

  useEffect(() => {
    loadData();
  }, []);

  async function loadData() {
    setLoading(true);
    try {
      const [avail, conf] = await Promise.all([
        api.listAvailableConnectors(),
        api.listConfiguredConnectors(),
      ]);
      setAvailable(avail);
      setConfigured(conf);
    } catch (e) {
      addOutput("error", `Failed to load connectors: ${e.message}`);
    } finally {
      setLoading(false);
    }
  }

  function startSetup(connector) {
    setSelectedType(connector);
    setStep(1);
    setTestResult(null);
    setResources([]);
    setSelectedResources([]);
    const defaults = {};
    for (const p of connector.params) {
      if (p.default !== null && p.default !== undefined) {
        defaults[p.name] = String(p.default);
      } else {
        defaults[p.name] = "";
      }
    }
    setConfigValues(defaults);
    setConnectionName(connector.name + "_1");
    setTargetSchema("landing");
    setSchedule(connector.default_schedule || "");
    setView("setup");
  }

  function cancelSetup() {
    setView("list");
    setSelectedType(null);
    setStep(1);
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
        selectedType.name,
        connectionName,
        configValues,
        selectedResources.length > 0 ? selectedResources : null,
        targetSchema,
        schedule || null,
      );
      addOutput("info", `Connector "${result.connection_name}" set up — ${result.tables.length} tables, script: ${result.script_path}`);
      await loadData();
      setView("list");
      setSelectedType(null);
      setStep(1);
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

  if (loading) {
    return (
      <div style={st.container}>
        <div style={st.center}>Loading connectors...</div>
      </div>
    );
  }

  // -- Setup wizard --
  if (view === "setup" && selectedType) {
    return (
      <div style={st.container}>
        <div style={st.header}>
          <span style={st.title}>Set up {selectedType.display_name}</span>
          <button onClick={cancelSetup} style={st.btnText}>Cancel</button>
        </div>
        <div style={st.content}>
          <div style={st.wizard}>
            {/* Steps indicator */}
            <div style={st.steps}>
              {["Configure", "Test", "Select Resources", "Confirm"].map((label, i) => (
                <div key={i} style={{ ...st.step, ...(step >= i + 1 ? st.stepActive : {}) }}>
                  <span style={st.stepNum}>{i + 1}</span>
                  <span style={st.stepLabel}>{label}</span>
                </div>
              ))}
            </div>

            {/* Step 1: Configure */}
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
                    <input value={targetSchema} onChange={(e) => setTargetSchema(e.target.value)} style={st.input} />
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
                  <button onClick={() => { setStep(3); doDiscover(); }} style={st.btn}>
                    Skip Test
                  </button>
                </div>
              </div>
            )}

            {/* Step 2: Test result */}
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

            {/* Step 3: Discover resources */}
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
                          <input
                            type="checkbox"
                            checked={selectedResources.includes(r.name)}
                            onChange={() => toggleResource(r.name)}
                            style={st.checkbox}
                          />
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
                  <button onClick={() => setStep(4)} style={st.btnPrimary}>
                    Continue
                  </button>
                </div>
              </div>
            )}

            {/* Step 4: Confirm */}
            {step >= 4 && (
              <div style={st.section}>
                <h3 style={st.sectionTitle}>Review & Confirm</h3>
                <div style={st.summary}>
                  <div style={st.summaryRow}><span style={st.summaryLabel}>Connector</span><span>{selectedType.display_name}</span></div>
                  <div style={st.summaryRow}><span style={st.summaryLabel}>Name</span><span style={st.mono}>{connectionName}</span></div>
                  <div style={st.summaryRow}><span style={st.summaryLabel}>Schema</span><span style={st.mono}>{targetSchema}</span></div>
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

  // -- Connector catalog ("add" view) --
  if (view === "add") {
    return (
      <div style={st.container}>
        <div style={st.header}>
          <span style={st.title}>Add Connector</span>
          <button onClick={() => setView("list")} style={st.btnText}>Back</button>
        </div>
        <div style={st.content}>
          <div style={st.catalog}>
            {available.map((c) => (
              <div key={c.name} style={st.card} onClick={() => startSetup(c)}>
                <div style={st.cardHeader}>
                  <span style={st.cardName}>{c.display_name}</span>
                </div>
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

  // -- Default: configured connectors list --
  return (
    <div style={st.container}>
      <div style={st.header}>
        <span style={st.title}>Data Connectors</span>
        <button onClick={() => setView("add")} style={st.btnPrimary}>Add Connector</button>
      </div>
      <div style={st.content}>
        {configured.length === 0 ? (
          <div style={st.empty}>
            <div style={st.emptyTitle}>No connectors configured</div>
            <div style={st.emptyDesc}>
              Connect to databases, APIs, and files to automatically sync data into your warehouse.
            </div>
            <button onClick={() => setView("add")} style={st.btnPrimary}>Browse Connectors</button>
          </div>
        ) : (
          <div style={st.connectorList}>
            {configured.map((c) => (
              <div key={c.name} style={st.connectorItem}>
                <div style={st.connectorMain}>
                  <div style={st.connectorNameRow}>
                    <span style={st.connectorName}>{c.name}</span>
                    <span style={st.connectorType}>{c.type}</span>
                    <span style={{ ...st.statusDot, background: c.has_script ? "var(--dp-green)" : "var(--dp-yellow)" }} />
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
            ))}
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

  // Buttons
  btn: { padding: "5px 14px", background: "var(--dp-btn-bg)", border: "1px solid var(--dp-btn-border)", borderRadius: "var(--dp-radius-lg)", color: "var(--dp-text)", cursor: "pointer", fontSize: "12px", fontWeight: 500 },
  btnPrimary: { padding: "5px 14px", background: "var(--dp-green)", border: "1px solid var(--dp-green-border)", borderRadius: "var(--dp-radius-lg)", color: "#fff", cursor: "pointer", fontSize: "12px", fontWeight: 500 },
  btnDanger: { padding: "5px 14px", background: "var(--dp-btn-bg)", border: "1px solid var(--dp-btn-border)", borderRadius: "var(--dp-radius-lg)", color: "var(--dp-red)", cursor: "pointer", fontSize: "12px", fontWeight: 500 },
  btnText: { background: "none", border: "none", color: "var(--dp-text-secondary)", cursor: "pointer", fontSize: "12px", fontWeight: 500 },
  btnSmall: { padding: "2px 8px", background: "var(--dp-btn-bg)", border: "1px solid var(--dp-btn-border)", borderRadius: "var(--dp-radius)", color: "var(--dp-text-secondary)", cursor: "pointer", fontSize: "11px" },

  // Empty state
  empty: { display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", gap: "12px", padding: "48px 24px", textAlign: "center" },
  emptyTitle: { fontSize: "16px", fontWeight: 600, color: "var(--dp-text)" },
  emptyDesc: { fontSize: "13px", color: "var(--dp-text-secondary)", maxWidth: "400px", lineHeight: 1.6 },

  // Connector list
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

  // Catalog
  catalog: { display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(240px, 1fr))", gap: "12px", maxWidth: "900px" },
  card: { padding: "16px", background: "var(--dp-bg-secondary)", border: "1px solid var(--dp-border)", borderRadius: "var(--dp-radius-lg)", cursor: "pointer", transition: "border-color 0.15s" },
  cardHeader: { marginBottom: "8px" },
  cardName: { fontWeight: 600, fontSize: "14px" },
  cardDesc: { fontSize: "12px", color: "var(--dp-text-secondary)", lineHeight: 1.5, marginBottom: "12px" },
  cardMeta: { display: "flex", gap: "8px", alignItems: "center" },
  cardType: { fontSize: "11px", color: "var(--dp-text-dim)", fontFamily: "var(--dp-font-mono)", background: "var(--dp-bg-tertiary)", padding: "1px 6px", borderRadius: "var(--dp-radius)" },
  cardSchedule: { fontSize: "10px", color: "var(--dp-text-dim)" },

  // Setup wizard
  wizard: { maxWidth: "600px" },
  steps: { display: "flex", gap: "4px", marginBottom: "24px" },
  step: { display: "flex", alignItems: "center", gap: "6px", padding: "6px 12px", borderRadius: "var(--dp-radius-lg)", fontSize: "12px", color: "var(--dp-text-dim)", background: "var(--dp-bg-tertiary)" },
  stepActive: { color: "var(--dp-text)", background: "var(--dp-btn-bg)", fontWeight: 500 },
  stepNum: { width: "18px", height: "18px", borderRadius: "50%", display: "inline-flex", alignItems: "center", justifyContent: "center", fontSize: "10px", fontWeight: 700, background: "var(--dp-bg-tertiary)", color: "var(--dp-text-secondary)" },
  stepLabel: {},
  section: { marginBottom: "24px", paddingBottom: "16px", borderBottom: "1px solid var(--dp-border)" },
  sectionTitle: { fontSize: "14px", fontWeight: 600, margin: "0 0 12px" },

  // Forms
  formGroup: { marginBottom: "12px", flex: 1 },
  formRow: { display: "flex", gap: "12px" },
  label: { display: "block", fontSize: "11px", color: "var(--dp-text-secondary)", marginBottom: "4px", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.3px" },
  required: { color: "var(--dp-red)", marginLeft: "2px" },
  secretBadge: { marginLeft: "6px", fontSize: "9px", color: "var(--dp-yellow)", background: "var(--dp-bg-tertiary)", padding: "1px 4px", borderRadius: "var(--dp-radius)", fontWeight: 500, textTransform: "none", letterSpacing: 0 },
  input: { width: "100%", padding: "6px 10px", background: "var(--dp-bg-tertiary)", border: "1px solid var(--dp-border-light)", borderRadius: "var(--dp-radius-lg)", color: "var(--dp-text)", fontSize: "13px", boxSizing: "border-box" },
  hint: { display: "block", fontSize: "11px", color: "var(--dp-text-dim)", marginTop: "2px" },
  actions: { display: "flex", gap: "8px", marginTop: "12px" },

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
};
