import React, { useState, useEffect } from "react";
import { api } from "./api";

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
                <td style={sec.td}><code>{s.key}</code></td>
                <td style={sec.td}><span style={sec.masked}>{s.masked_value}</span></td>
                <td style={sec.td}>
                  <button onClick={() => removeSecret(s.key)} style={sec.delBtn}>Delete</button>
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
        Enable auth with <code>dp serve --auth</code>.
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
                  <select
                    value={u.role}
                    onChange={(e) => changeRole(u.username, e.target.value)}
                    style={sec.roleSelect}
                  >
                    <option value="admin">admin</option>
                    <option value="editor">editor</option>
                    <option value="viewer">viewer</option>
                  </select>
                </td>
                <td style={sec.td}>{u.display_name}</td>
                <td style={sec.td}>{u.last_login || "never"}</td>
                <td style={sec.td}>
                  <button onClick={() => removeUser(u.username)} style={sec.delBtn}>Delete</button>
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

export default function SettingsPanel() {
  return (
    <div style={sec.container}>
      <div style={sec.header}>Settings</div>
      <div style={sec.content}>
        <SecretsSection />
        <UsersSection />
      </div>
    </div>
  );
}

const sec = {
  container: { display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" },
  header: { padding: "8px 12px", borderBottom: "1px solid #21262d", fontWeight: 600, fontSize: "14px" },
  content: { flex: 1, overflow: "auto", padding: "16px 24px", maxWidth: "800px" },
  section: { marginBottom: "32px" },
  heading: { fontSize: "16px", fontWeight: 600, margin: "0 0 4px" },
  desc: { fontSize: "12px", color: "#8b949e", margin: "0 0 12px", lineHeight: 1.5 },
  table: { width: "100%", borderCollapse: "collapse", marginBottom: "12px", fontSize: "12px" },
  th: { textAlign: "left", padding: "6px 10px", borderBottom: "1px solid #30363d", color: "#8b949e", fontWeight: 600 },
  td: { padding: "5px 10px", borderBottom: "1px solid #21262d" },
  masked: { color: "#8b949e", fontFamily: "monospace" },
  addRow: { display: "flex", gap: "8px", alignItems: "center" },
  input: {
    flex: 1, padding: "6px 10px", background: "#0d1117", border: "1px solid #30363d",
    borderRadius: "6px", color: "#e1e4e8", fontSize: "13px",
  },
  roleSelect: {
    padding: "4px 8px", background: "#0d1117", border: "1px solid #30363d",
    borderRadius: "4px", color: "#e1e4e8", fontSize: "12px",
  },
  addBtn: {
    padding: "6px 14px", background: "#238636", border: "1px solid #2ea043",
    borderRadius: "6px", color: "#fff", cursor: "pointer", fontSize: "12px", whiteSpace: "nowrap",
  },
  delBtn: {
    padding: "3px 8px", background: "#21262d", border: "1px solid #30363d",
    borderRadius: "4px", color: "#f85149", cursor: "pointer", fontSize: "11px",
  },
};
