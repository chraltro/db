import React, { useState, useEffect } from "react";
import { api } from "./api";
import { useTheme } from "./ThemeProvider";
import { COLOR_THEMES, FONT_THEMES, getColorThemeIds, getFontThemeIds } from "./themes";
import { useHintSettings } from "./HintSystem";

function ThemeSection() {
  const { colorThemeId, fontThemeId, setColorThemeId, setFontThemeId } = useTheme();

  return (
    <div style={sec.section}>
      <h3 style={sec.heading}>Colors</h3>
      <p style={sec.desc}>Choose a color palette. Combine with any font below.</p>
      <div style={sec.themeGrid}>
        {getColorThemeIds().map((id) => {
          const theme = COLOR_THEMES[id];
          const active = colorThemeId === id;
          return (
            <button
              key={id}
              data-havn-theme-card=""
              onClick={() => setColorThemeId(id)}
              style={{
                ...sec.themeCard,
                border: active ? "2px solid var(--havn-accent)" : "1px solid var(--havn-border)",
                background: theme.vars["--havn-bg"],
              }}
            >
              <div style={{ display: "flex", gap: "4px", marginBottom: "8px" }}>
                <span style={{ width: "14px", height: "14px", borderRadius: "50%", background: theme.vars["--havn-accent"], border: `1px solid ${theme.vars["--havn-border"]}` }} />
                <span style={{ width: "14px", height: "14px", borderRadius: "50%", background: theme.vars["--havn-green"], border: `1px solid ${theme.vars["--havn-border"]}` }} />
                <span style={{ width: "14px", height: "14px", borderRadius: "50%", background: theme.vars["--havn-red"], border: `1px solid ${theme.vars["--havn-border"]}` }} />
              </div>
              <div style={{ color: theme.vars["--havn-text"], fontSize: "12px", fontWeight: 600, marginBottom: "2px" }}>{theme.name}</div>
              <div style={{ color: theme.vars["--havn-text-secondary"], fontSize: "10px", lineHeight: "1.3" }}>{theme.description}</div>
              {active && <div style={{ marginTop: "6px", fontSize: "9px", color: theme.vars["--havn-accent"], fontWeight: 700, letterSpacing: "0.5px", textTransform: "uppercase" }}>Active</div>}
            </button>
          );
        })}
      </div>

      <h3 style={{ ...sec.heading, marginTop: "20px" }}>Fonts</h3>
      <p style={sec.desc}>Choose a font pairing. Works with any color theme above.</p>
      <div style={sec.fontGrid}>
        {getFontThemeIds().map((id) => {
          const font = FONT_THEMES[id];
          const active = fontThemeId === id;
          return (
            <button
              key={id}
              onClick={() => setFontThemeId(id)}
              style={{
                ...sec.fontCard,
                border: active ? "2px solid var(--havn-accent)" : "1px solid var(--havn-border)",
                background: active ? "var(--havn-bg-secondary)" : "var(--havn-bg-tertiary)",
              }}
            >
              <div style={{ fontSize: "14px", fontWeight: 600, fontFamily: font.vars["--havn-font"], marginBottom: "2px", color: "var(--havn-text)" }}>
                Aa
              </div>
              <div style={{ fontSize: "11px", fontFamily: font.vars["--havn-font-mono"], color: "var(--havn-accent)", marginBottom: "4px" }}>
                0x1F {}
              </div>
              <div style={{ fontSize: "11px", fontWeight: 500, color: "var(--havn-text)" }}>{font.name}</div>
              <div style={{ fontSize: "10px", color: "var(--havn-text-secondary)", lineHeight: "1.3" }}>{font.description}</div>
              {active && <div style={{ marginTop: "4px", fontSize: "9px", color: "var(--havn-accent)", fontWeight: 700, letterSpacing: "0.5px", textTransform: "uppercase" }}>Active</div>}
            </button>
          );
        })}
      </div>
    </div>
  );
}

function SchedulerSection() {
  const [streams, setStreams] = useState([]);
  const [history, setHistory] = useState([]);
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(null);

  useEffect(() => {
    loadData();
  }, []);

  const loadData = async () => {
    setLoading(true);
    try {
      const [sched, hist] = await Promise.all([
        api.getScheduler(),
        api.getHistory()
      ]);
      setStreams(sched.streams || []);
      setHistory(hist || []);
    } catch (e) {
      console.error('Failed to load scheduler data:', e);
    }
    setLoading(false);
  };

  const formatCron = (cron) => {
    if (!cron) return '—';
    const parts = cron.split(' ');
    if (parts.length !== 5) return cron;
    const [min, hour, dom, mon, dow] = parts;
    if (dom === '*' && mon === '*' && dow === '*') return `Daily at ${hour.padStart(2,'0')}:${min.padStart(2,'0')}`;
    if (dom === '*' && mon === '*') return `${cron} (weekly)`;
    return cron;
  };

  const getLastRun = (streamName) => {
    const runs = history.filter(h => h.stream === streamName || h.name === streamName);
    if (runs.length === 0) return { status: '—', time: '—' };
    const last = runs[0];
    return { status: last.status || last.result || '—', time: last.started_at || last.timestamp || '—' };
  };

  const handleRunNow = async (streamName) => {
    setRunning(streamName);
    try {
      await api.runStream(streamName);
      await loadData();
    } catch (e) {
      console.error('Failed to run stream:', e);
    }
    setRunning(null);
  };

  return (
    <div style={sec.section}>
      <h3 style={sec.heading}>Scheduler</h3>
      <p style={sec.desc}>Configured pipeline schedules and their status.</p>
      {loading ? <p style={{ color: 'var(--havn-text-secondary)' }}>Loading...</p> : streams.length === 0 ? (
        <p style={{ color: 'var(--havn-text-secondary)' }}>No streams configured in project.yml</p>
      ) : (
        <table style={sec.table}>
          <thead>
            <tr>
              <th style={sec.th}>Stream</th>
              <th style={sec.th}>Description</th>
              <th style={sec.th}>Schedule</th>
              <th style={sec.th}>Last Run</th>
              <th style={sec.th}>Status</th>
              <th style={sec.th}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {streams.map(s => {
              const last = getLastRun(s.name);
              return (
                <tr key={s.name}>
                  <td style={sec.td}>{s.name}</td>
                  <td style={sec.td}>{s.description || '—'}</td>
                  <td style={sec.td}><code style={sec.code}>{formatCron(s.schedule)}</code></td>
                  <td style={sec.td}>{last.time}</td>
                  <td style={sec.td}>
                    <span style={{
                      padding: '2px 8px', borderRadius: 'var(--havn-radius)', fontSize: 11,
                      background: last.status === 'success' ? 'var(--havn-green-bg)' : last.status === 'error' ? 'var(--havn-red-bg)' : 'var(--havn-btn-bg)',
                      color: last.status === 'success' ? 'var(--havn-green)' : last.status === 'error' ? 'var(--havn-red)' : 'var(--havn-text-secondary)'
                    }}>{last.status}</span>
                  </td>
                  <td style={sec.td}>
                    <button
                      onClick={() => handleRunNow(s.name)}
                      disabled={running === s.name}
                      style={{ ...sec.addBtn, opacity: running === s.name ? 0.5 : 1 }}
                    >{running === s.name ? 'Running...' : 'Run Now'}</button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </div>
  );
}

function SecretsSection({ showConfirm }) {
  const [secrets, setSecrets] = useState([]);
  const [newKey, setNewKey] = useState("");
  const [newVal, setNewVal] = useState("");
  const [error, setError] = useState(null);

  useEffect(() => { loadSecrets(); }, []);

  async function loadSecrets() {
    try { setSecrets(await api.listSecrets()); } catch (e) { setError(e.message || "Failed to load secrets"); }
  }

  async function addSecret() {
    if (!newKey.trim()) return;
    setError(null);
    try {
      await api.setSecret(newKey.trim(), newVal);
      setNewKey("");
      setNewVal("");
      loadSecrets();
    } catch (e) { setError(e.message || "Failed to add secret"); }
  }

  async function removeSecret(key) {
    if (showConfirm && !(await showConfirm("Delete Secret", `Delete secret "${key}"?`, "Delete", true))) return;
    if (!showConfirm && !confirm(`Delete secret "${key}"?`)) return;
    setError(null);
    try { await api.deleteSecret(key); loadSecrets(); } catch (e) { setError(e.message || "Failed to delete secret"); }
  }

  return (
    <div style={sec.section}>
      <h3 style={sec.heading}>Secrets (.env)</h3>
      <p style={sec.desc}>Secrets are stored in .env and referenced as {"${VAR}"} in project.yml. Values are never exposed.</p>
      {error && <p style={{ color: "var(--havn-red)", fontSize: "12px", margin: "4px 0" }}>{error}</p>}
      {secrets.length > 0 && (
        <table style={sec.table}>
          <thead>
            <tr>
              <th style={sec.th}>Key</th>
              <th style={sec.th}>Value</th>
              <th style={sec.th}></th>
            </tr>
          </thead>
          <tbody>
            {secrets.map((s) => (
              <tr key={s.key}>
                <td style={sec.td}><code style={sec.code}>{s.key}</code></td>
                <td style={sec.td}><span style={sec.masked}>{s.masked_value}</span></td>
                <td style={{ ...sec.td, textAlign: "right" }}>
                  <button data-havn-danger="" onClick={() => removeSecret(s.key)} style={sec.delBtn}>Delete</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <div style={sec.addRow}>
        <input value={newKey} onChange={(e) => setNewKey(e.target.value)} placeholder="KEY" style={sec.input} />
        <input value={newVal} onChange={(e) => setNewVal(e.target.value)} placeholder="value" style={sec.input} type="password" />
        <button onClick={addSecret} style={sec.addBtn}>Add</button>
      </div>
    </div>
  );
}

function UsersSection({ showConfirm }) {
  const [users, setUsers] = useState([]);
  const [newUser, setNewUser] = useState("");
  const [newPass, setNewPass] = useState("");
  const [newRole, setNewRole] = useState("viewer");
  const [error, setError] = useState(null);

  useEffect(() => { loadUsers(); }, []);

  async function loadUsers() {
    try { setUsers(await api.listUsers()); } catch (e) { setError(e.message || "Failed to load users"); }
  }

  async function addUser() {
    if (!newUser.trim() || !newPass) return;
    setError(null);
    try {
      await api.createUser(newUser.trim(), newPass, newRole);
      setNewUser("");
      setNewPass("");
      loadUsers();
    } catch (e) { setError(e.message || "Failed to create user"); }
  }

  async function removeUser(username) {
    if (showConfirm && !(await showConfirm("Delete User", `Delete user "${username}"?`, "Delete", true))) return;
    if (!showConfirm && !confirm(`Delete user "${username}"?`)) return;
    setError(null);
    try { await api.deleteUser(username); loadUsers(); } catch (e) { setError(e.message || "Failed to delete user"); }
  }

  async function changeRole(username, role) {
    setError(null);
    try { await api.updateUser(username, { role }); loadUsers(); } catch (e) { setError(e.message || "Failed to update role"); }
  }

  return (
    <div style={sec.section}>
      <h3 style={sec.heading}>Users</h3>
      <p style={sec.desc}>
        Roles: <strong>admin</strong> (full access), <strong>editor</strong> (run + query), <strong>viewer</strong> (read-only).
        Enable auth with <code style={sec.code}>havn serve --auth</code>.
      </p>
      {error && <p style={{ color: "var(--havn-red)", fontSize: "12px", margin: "4px 0" }}>{error}</p>}
      {users.length > 0 && (
        <table style={sec.table}>
          <thead>
            <tr>
              <th style={sec.th}>Username</th>
              <th style={sec.th}>Role</th>
              <th style={sec.th}>Display Name</th>
              <th style={sec.th}>Last Login</th>
              <th style={sec.th}></th>
            </tr>
          </thead>
          <tbody>
            {users.map((u) => (
              <tr key={u.username}>
                <td style={sec.td}><strong>{u.username}</strong></td>
                <td style={sec.td}>
                  <select value={u.role} onChange={(e) => changeRole(u.username, e.target.value)} style={sec.roleSelect}>
                    <option value="admin">admin</option>
                    <option value="editor">editor</option>
                    <option value="viewer">viewer</option>
                  </select>
                </td>
                <td style={sec.td}>{u.display_name}</td>
                <td style={{ ...sec.td, color: "var(--havn-text-secondary)" }}>{u.last_login || "never"}</td>
                <td style={{ ...sec.td, textAlign: "right" }}>
                  <button data-havn-danger="" onClick={() => removeUser(u.username)} style={sec.delBtn}>Delete</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <div style={sec.addRow}>
        <input value={newUser} onChange={(e) => setNewUser(e.target.value)} placeholder="username" style={sec.input} />
        <input value={newPass} onChange={(e) => setNewPass(e.target.value)} placeholder="password" style={sec.input} type="password" />
        <select value={newRole} onChange={(e) => setNewRole(e.target.value)} style={sec.roleSelect}>
          <option value="admin">admin</option>
          <option value="editor">editor</option>
          <option value="viewer">viewer</option>
        </select>
        <button onClick={addUser} style={sec.addBtn}>Add User</button>
      </div>
    </div>
  );
}

function AlertsSection() {
  const [history, setHistory] = useState([]);
  const [loading, setLoading] = useState(true);
  const [slackUrl, setSlackUrl] = useState('');
  const [webhookUrl, setWebhookUrl] = useState('');
  const [testing, setTesting] = useState(null);
  const [testResult, setTestResult] = useState(null);

  useEffect(() => { loadHistory(); }, []);

  const loadHistory = async () => {
    setLoading(true);
    try {
      const data = await api.getAlertHistory(50);
      setHistory(data || []);
    } catch (e) {
      console.error('Failed to load alert history:', e);
    }
    setLoading(false);
  };

  const handleTest = async (channel) => {
    setTesting(channel);
    setTestResult(null);
    try {
      const config = channel === 'slack' ? { slack_webhook_url: slackUrl } : { webhook_url: webhookUrl };
      await api.testAlert(channel, config);
      setTestResult({ channel, success: true, message: 'Test alert sent successfully!' });
    } catch (e) {
      setTestResult({ channel, success: false, message: e.message || 'Test failed' });
    }
    setTesting(null);
  };

  return (
    <div style={sec.section}>
      <h3 style={sec.heading}>Alerts</h3>
      <p style={sec.desc}>Configure and test alert channels. View alert history.</p>

      <div style={{ display: 'flex', gap: 16, marginBottom: 16, flexWrap: 'wrap' }}>
        <div style={{ flex: 1, minWidth: 280 }}>
          <label style={sec.label}>Slack Webhook URL</label>
          <div style={{ display: 'flex', gap: 8 }}>
            <input style={{ ...sec.input, flex: 1 }} placeholder="https://hooks.slack.com/services/..." value={slackUrl} onChange={e => setSlackUrl(e.target.value)} />
            <button style={sec.addBtn} disabled={!slackUrl || testing === 'slack'} onClick={() => handleTest('slack')}>
              {testing === 'slack' ? 'Testing...' : 'Test Slack'}
            </button>
          </div>
        </div>
        <div style={{ flex: 1, minWidth: 280 }}>
          <label style={sec.label}>Generic Webhook URL</label>
          <div style={{ display: 'flex', gap: 8 }}>
            <input style={{ ...sec.input, flex: 1 }} placeholder="https://example.com/webhook" value={webhookUrl} onChange={e => setWebhookUrl(e.target.value)} />
            <button style={sec.addBtn} disabled={!webhookUrl || testing === 'webhook'} onClick={() => handleTest('webhook')}>
              {testing === 'webhook' ? 'Testing...' : 'Test Webhook'}
            </button>
          </div>
        </div>
      </div>

      {testResult && (
        <div style={{
          padding: '8px 12px', borderRadius: 'var(--havn-radius-lg)', marginBottom: 12, fontSize: 13,
          background: testResult.success ? 'var(--havn-green-bg)' : 'var(--havn-red-bg)',
          color: testResult.success ? 'var(--havn-green)' : 'var(--havn-red)',
          border: `1px solid ${testResult.success ? 'var(--havn-green-border)' : 'var(--havn-red-border)'}`
        }}>{testResult.message}</div>
      )}

      <h4 style={{ ...sec.heading, fontSize: 14, marginTop: 16 }}>Alert History</h4>
      {loading ? <p style={{ color: 'var(--havn-text-secondary)' }}>Loading...</p> : history.length === 0 ? (
        <p style={{ color: 'var(--havn-text-secondary)' }}>No alerts sent yet.</p>
      ) : (
        <table style={sec.table}>
          <thead>
            <tr>
              <th style={sec.th}>Type</th>
              <th style={sec.th}>Channel</th>
              <th style={sec.th}>Target</th>
              <th style={sec.th}>Message</th>
              <th style={sec.th}>Status</th>
              <th style={sec.th}>Sent At</th>
            </tr>
          </thead>
          <tbody>
            {history.map((a, i) => (
              <tr key={i}>
                <td style={sec.td}>{a.type || a.alert_type || '—'}</td>
                <td style={sec.td}>{a.channel || '—'}</td>
                <td style={sec.td} title={a.target || a.webhook_url || ''}>{(a.target || a.webhook_url || '—').slice(0, 30)}</td>
                <td style={sec.td} title={a.message || ''}>{(a.message || '—').slice(0, 50)}</td>
                <td style={sec.td}>
                  <span style={{
                    padding: '2px 8px', borderRadius: 'var(--havn-radius)', fontSize: 11,
                    background: a.status === 'sent' || a.status === 'success' ? 'var(--havn-green-bg)' : 'var(--havn-red-bg)',
                    color: a.status === 'sent' || a.status === 'success' ? 'var(--havn-green)' : 'var(--havn-red)'
                  }}>{a.status || '—'}</span>
                </td>
                <td style={sec.td}>{a.sent_at || a.created_at || '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

const DEFAULT_SQLFLUFF = `[sqlfluff]
dialect = duckdb
max_line_length = 120

[sqlfluff:rules:capitalisation.keywords]
capitalisation_policy = upper

[sqlfluff:rules:capitalisation.identifiers]
capitalisation_policy = lower

[sqlfluff:rules:capitalisation.functions]
capitalisation_policy = upper
`;

function LintConfigSection({ showConfirm }) {
  const [content, setContent] = useState("");
  const [exists, setExists] = useState(false);
  const [saved, setSaved] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => { loadConfig(); }, []);

  async function loadConfig() {
    try {
      const data = await api.getLintConfig();
      setExists(data.exists);
      setContent(data.exists ? data.content : DEFAULT_SQLFLUFF);
    } catch (e) { setError(e.message || "Failed to load config"); }
    setLoading(false);
  }

  async function save() {
    setError(null);
    try {
      await api.saveLintConfig(content);
      setExists(true);
      setSaved(true);
      setTimeout(() => setSaved(false), 2000);
    } catch (e) { setError(e.message || "Failed to save config"); }
  }

  async function remove() {
    if (showConfirm && !(await showConfirm("Reset Lint Config", "Delete .sqlfluff and revert to default lint settings?", "Delete", true))) return;
    if (!showConfirm && !confirm("Delete .sqlfluff and revert to default lint settings?")) return;
    setError(null);
    try {
      await api.deleteLintConfig();
      setExists(false);
      setContent(DEFAULT_SQLFLUFF);
    } catch (e) { setError(e.message || "Failed to delete config"); }
  }

  if (loading) return null;

  return (
    <div style={sec.section}>
      <h3 style={sec.heading}>SQLFluff Config</h3>
      {error && <p style={{ color: "var(--havn-red)", fontSize: "12px", margin: "4px 0" }}>{error}</p>}
      <p style={sec.desc}>
        {exists
          ? <>Editing <code style={sec.code}>.sqlfluff</code> in your project root. The linter uses this file.</>
          : <>No <code style={sec.code}>.sqlfluff</code> file found. Save to create one with custom lint rules.</>}
      </p>
      <textarea
        value={content}
        onChange={(e) => { setContent(e.target.value); setSaved(false); }}
        style={sec.configTextarea}
        rows={14}
        spellCheck={false}
      />
      <div style={{ ...sec.addRow, marginTop: "8px" }}>
        <button onClick={save} style={sec.addBtn}>{saved ? "Saved" : "Save"}</button>
        {exists && (
          <button onClick={remove} style={sec.delBtn}>Delete .sqlfluff</button>
        )}
      </div>
    </div>
  );
}

function HintsSection() {
  const { resetHints, totalHints, dismissedCount } = useHintSettings();
  const [justReset, setJustReset] = useState(false);

  function handleReset() {
    resetHints();
    setJustReset(true);
    setTimeout(() => setJustReset(false), 2000);
  }

  return (
    <div style={sec.section}>
      <h3 style={sec.heading}>Contextual Hints</h3>
      <p style={sec.desc}>
        Hints appear as you use the interface to highlight features at the right moment.
        {" "}{dismissedCount} of {totalHints} hints dismissed.
      </p>
      <button onClick={handleReset} style={sec.addBtn}>
        {justReset ? "Reset!" : "Reset all hints"}
      </button>
    </div>
  );
}

function GuideSection({ onShowGuide }) {
  return (
    <div style={sec.section}>
      <h3 style={sec.heading}>Getting Started Guide</h3>
      <p style={sec.desc}>Re-watch the interactive tour that explains the main areas of the interface.</p>
      <button onClick={onShowGuide} style={sec.addBtn}>Show Guide</button>
    </div>
  );
}

export default function SettingsPanel({ onShowGuide, showConfirm }) {
  return (
    <div style={sec.container}>
      <div style={sec.content}>
        {onShowGuide && <GuideSection onShowGuide={onShowGuide} />}
        <HintsSection />
        <ThemeSection />
        <LintConfigSection showConfirm={showConfirm} />
        <SchedulerSection />
        <SecretsSection showConfirm={showConfirm} />
        <UsersSection showConfirm={showConfirm} />
        <AlertsSection />
      </div>
    </div>
  );
}

const sec = {
  container: { display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" },
  header: { padding: "8px 12px", borderBottom: "1px solid var(--havn-border)", fontWeight: 600, fontSize: "13px" },
  content: { flex: 1, overflow: "auto", padding: "16px 24px", maxWidth: "800px" },
  section: { marginBottom: "32px" },
  heading: { fontSize: "16px", fontWeight: 600, margin: "0 0 4px" },
  desc: { fontSize: "12px", color: "var(--havn-text-secondary)", margin: "0 0 12px", lineHeight: 1.6 },
  code: { background: "var(--havn-btn-bg)", padding: "1px 5px", borderRadius: "3px", fontSize: "12px", fontFamily: "var(--havn-font-mono)" },
  table: { width: "100%", borderCollapse: "collapse", marginBottom: "12px", fontSize: "12px" },
  th: { textAlign: "left", padding: "6px 10px", borderBottom: "2px solid var(--havn-border-light)", color: "var(--havn-text-secondary)", fontWeight: 600 },
  td: { padding: "6px 10px", borderBottom: "1px solid var(--havn-border)" },
  masked: { color: "var(--havn-text-secondary)", fontFamily: "var(--havn-font-mono)" },
  addRow: { display: "flex", gap: "8px", alignItems: "center" },
  input: { flex: 1, padding: "6px 10px", background: "var(--havn-bg-tertiary)", border: "1px solid var(--havn-border-light)", borderRadius: "var(--havn-radius-lg)", color: "var(--havn-text)", fontSize: "13px" },
  roleSelect: { padding: "4px 8px", background: "var(--havn-bg-tertiary)", border: "1px solid var(--havn-border-light)", borderRadius: "var(--havn-radius)", color: "var(--havn-text)", fontSize: "12px" },
  addBtn: { padding: "6px 14px", background: "var(--havn-green)", border: "1px solid var(--havn-green-border)", borderRadius: "var(--havn-radius-lg)", color: "#fff", cursor: "pointer", fontSize: "12px", fontWeight: 500, whiteSpace: "nowrap" },
  delBtn: { padding: "3px 8px", background: "var(--havn-btn-bg)", border: "1px solid var(--havn-btn-border)", borderRadius: "var(--havn-radius)", color: "var(--havn-red)", cursor: "pointer", fontSize: "11px" },
  label: { display: "block", fontSize: "12px", color: "var(--havn-text-secondary)", marginBottom: "4px" },
  configTextarea: { width: "100%", padding: "10px 12px", background: "var(--havn-bg-tertiary)", border: "1px solid var(--havn-border-light)", borderRadius: "var(--havn-radius-lg)", color: "var(--havn-text)", fontFamily: "var(--havn-font-mono)", fontSize: "12px", lineHeight: 1.6, resize: "vertical", boxSizing: "border-box", outline: "none" },
  themeGrid: { display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(180px, 1fr))", gap: "10px" },
  themeCard: { padding: "12px", borderRadius: "var(--havn-radius-lg)", cursor: "pointer", textAlign: "left", display: "block" },
  fontGrid: { display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))", gap: "10px" },
  fontCard: { padding: "12px", borderRadius: "var(--havn-radius-lg)", cursor: "pointer", textAlign: "left", display: "block" },
};
