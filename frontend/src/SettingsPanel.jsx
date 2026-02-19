import React, { useState, useEffect } from "react";
import { api } from "./api";
import { useTheme } from "./ThemeProvider";
import { THEMES, getThemeIds, getTheme } from "./themes";
import { useHintSettings } from "./HintSystem";

function ThemeSection() {
  const { themeId, setThemeId } = useTheme();

  return (
    <div style={sec.section}>
      <h3 style={sec.heading}>Theme</h3>
      <p style={sec.desc}>Choose a visual theme. Your preference is saved locally.</p>
      <div style={sec.themeGrid}>
        {getThemeIds().map((id) => {
          const theme = getTheme(id);
          const active = themeId === id;
          return (
            <button
              key={id}
              data-dp-theme-card=""
              onClick={() => setThemeId(id)}
              style={{
                ...sec.themeCard,
                border: active ? "2px solid var(--dp-accent)" : "1px solid var(--dp-border)",
                background: theme.vars["--dp-bg"],
              }}
            >
              <div style={{ display: "flex", gap: "4px", marginBottom: "8px" }}>
                <span style={{ width: "14px", height: "14px", borderRadius: "50%", background: theme.vars["--dp-accent"], border: `1px solid ${theme.vars["--dp-border"]}` }} />
                <span style={{ width: "14px", height: "14px", borderRadius: "50%", background: theme.vars["--dp-green"], border: `1px solid ${theme.vars["--dp-border"]}` }} />
                <span style={{ width: "14px", height: "14px", borderRadius: "50%", background: theme.vars["--dp-red"], border: `1px solid ${theme.vars["--dp-border"]}` }} />
              </div>
              <div style={{ color: theme.vars["--dp-text"], fontSize: "12px", fontWeight: 600, marginBottom: "2px" }}>{theme.name}</div>
              <div style={{ color: theme.vars["--dp-text-secondary"], fontSize: "10px", lineHeight: "1.3" }}>{theme.description}</div>
              {active && <div style={{ marginTop: "6px", fontSize: "9px", color: theme.vars["--dp-accent"], fontWeight: 700, letterSpacing: "0.5px", textTransform: "uppercase" }}>Active</div>}
            </button>
          );
        })}
      </div>
    </div>
  );
}

function SecretsSection() {
  const [secrets, setSecrets] = useState([]);
  const [newKey, setNewKey] = useState("");
  const [newVal, setNewVal] = useState("");

  useEffect(() => { loadSecrets(); }, []);

  async function loadSecrets() {
    try { setSecrets(await api.listSecrets()); } catch {}
  }

  async function addSecret() {
    if (!newKey.trim()) return;
    try {
      await api.setSecret(newKey.trim(), newVal);
      setNewKey("");
      setNewVal("");
      loadSecrets();
    } catch (e) { alert(e.message); }
  }

  async function removeSecret(key) {
    if (!confirm(`Delete secret "${key}"?`)) return;
    try { await api.deleteSecret(key); loadSecrets(); } catch (e) { alert(e.message); }
  }

  return (
    <div style={sec.section}>
      <h3 style={sec.heading}>Secrets (.env)</h3>
      <p style={sec.desc}>Secrets are stored in .env and referenced as {"${VAR}"} in project.yml. Values are never exposed.</p>
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
                  <button data-dp-danger="" onClick={() => removeSecret(s.key)} style={sec.delBtn}>Delete</button>
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

function UsersSection() {
  const [users, setUsers] = useState([]);
  const [newUser, setNewUser] = useState("");
  const [newPass, setNewPass] = useState("");
  const [newRole, setNewRole] = useState("viewer");

  useEffect(() => { loadUsers(); }, []);

  async function loadUsers() {
    try { setUsers(await api.listUsers()); } catch {}
  }

  async function addUser() {
    if (!newUser.trim() || !newPass) return;
    try {
      await api.createUser(newUser.trim(), newPass, newRole);
      setNewUser("");
      setNewPass("");
      loadUsers();
    } catch (e) { alert(e.message); }
  }

  async function removeUser(username) {
    if (!confirm(`Delete user "${username}"?`)) return;
    try { await api.deleteUser(username); loadUsers(); } catch (e) { alert(e.message); }
  }

  async function changeRole(username, role) {
    try { await api.updateUser(username, { role }); loadUsers(); } catch (e) { alert(e.message); }
  }

  return (
    <div style={sec.section}>
      <h3 style={sec.heading}>Users</h3>
      <p style={sec.desc}>
        Roles: <strong>admin</strong> (full access), <strong>editor</strong> (run + query), <strong>viewer</strong> (read-only).
        Enable auth with <code style={sec.code}>dp serve --auth</code>.
      </p>
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
                <td style={{ ...sec.td, color: "var(--dp-text-secondary)" }}>{u.last_login || "never"}</td>
                <td style={{ ...sec.td, textAlign: "right" }}>
                  <button data-dp-danger="" onClick={() => removeUser(u.username)} style={sec.delBtn}>Delete</button>
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

function LintConfigSection() {
  const [content, setContent] = useState("");
  const [exists, setExists] = useState(false);
  const [saved, setSaved] = useState(false);
  const [loading, setLoading] = useState(true);

  useEffect(() => { loadConfig(); }, []);

  async function loadConfig() {
    try {
      const data = await api.getLintConfig();
      setExists(data.exists);
      setContent(data.exists ? data.content : DEFAULT_SQLFLUFF);
    } catch {}
    setLoading(false);
  }

  async function save() {
    try {
      await api.saveLintConfig(content);
      setExists(true);
      setSaved(true);
      setTimeout(() => setSaved(false), 2000);
    } catch (e) { alert(e.message); }
  }

  async function remove() {
    if (!confirm("Delete .sqlfluff and revert to default lint settings?")) return;
    try {
      await api.deleteLintConfig();
      setExists(false);
      setContent(DEFAULT_SQLFLUFF);
    } catch (e) { alert(e.message); }
  }

  if (loading) return null;

  return (
    <div style={sec.section}>
      <h3 style={sec.heading}>SQLFluff Config</h3>
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

export default function SettingsPanel({ onShowGuide }) {
  return (
    <div style={sec.container}>
      <div style={sec.header}>Settings</div>
      <div style={sec.content}>
        {onShowGuide && <GuideSection onShowGuide={onShowGuide} />}
        <HintsSection />
        <ThemeSection />
        <LintConfigSection />
        <SecretsSection />
        <UsersSection />
      </div>
    </div>
  );
}

const sec = {
  container: { display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" },
  header: { padding: "8px 12px", borderBottom: "1px solid var(--dp-border)", fontWeight: 600, fontSize: "14px" },
  content: { flex: 1, overflow: "auto", padding: "16px 24px", maxWidth: "800px" },
  section: { marginBottom: "32px" },
  heading: { fontSize: "16px", fontWeight: 600, margin: "0 0 4px" },
  desc: { fontSize: "12px", color: "var(--dp-text-secondary)", margin: "0 0 12px", lineHeight: 1.6 },
  code: { background: "var(--dp-btn-bg)", padding: "1px 5px", borderRadius: "3px", fontSize: "12px", fontFamily: "var(--dp-font-mono)" },
  table: { width: "100%", borderCollapse: "collapse", marginBottom: "12px", fontSize: "12px" },
  th: { textAlign: "left", padding: "6px 10px", borderBottom: "2px solid var(--dp-border-light)", color: "var(--dp-text-secondary)", fontWeight: 600 },
  td: { padding: "6px 10px", borderBottom: "1px solid var(--dp-border)" },
  masked: { color: "var(--dp-text-secondary)", fontFamily: "var(--dp-font-mono)" },
  addRow: { display: "flex", gap: "8px", alignItems: "center" },
  input: { flex: 1, padding: "6px 10px", background: "var(--dp-bg-tertiary)", border: "1px solid var(--dp-border-light)", borderRadius: "var(--dp-radius-lg)", color: "var(--dp-text)", fontSize: "13px" },
  roleSelect: { padding: "4px 8px", background: "var(--dp-bg-tertiary)", border: "1px solid var(--dp-border-light)", borderRadius: "var(--dp-radius)", color: "var(--dp-text)", fontSize: "12px" },
  addBtn: { padding: "6px 14px", background: "var(--dp-green)", border: "1px solid var(--dp-green-border)", borderRadius: "var(--dp-radius-lg)", color: "#fff", cursor: "pointer", fontSize: "12px", fontWeight: 500, whiteSpace: "nowrap" },
  delBtn: { padding: "3px 8px", background: "var(--dp-btn-bg)", border: "1px solid var(--dp-btn-border)", borderRadius: "var(--dp-radius)", color: "var(--dp-red)", cursor: "pointer", fontSize: "11px" },
  configTextarea: { width: "100%", padding: "10px 12px", background: "var(--dp-bg-tertiary)", border: "1px solid var(--dp-border-light)", borderRadius: "var(--dp-radius-lg)", color: "var(--dp-text)", fontFamily: "var(--dp-font-mono)", fontSize: "12px", lineHeight: 1.6, resize: "vertical", boxSizing: "border-box", outline: "none" },
  themeGrid: { display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(180px, 1fr))", gap: "10px" },
  themeCard: { padding: "12px", borderRadius: "var(--dp-radius-lg)", cursor: "pointer", textAlign: "left", display: "block" },
};
