import React, { useState } from "react";
import { api } from "./api";

export default function LoginPage({ onLogin, needsSetup }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e) {
    e.preventDefault();
    setLoading(true);
    setError(null);
    try {
      let result;
      if (needsSetup) {
        result = await api.setup(username, password, displayName || username);
      } else {
        result = await api.login(username, password);
      }
      api.setToken(result.token);
      onLogin(result);
    } catch (err) {
      setError(err.message);
    }
    setLoading(false);
  }

  return (
    <div style={st.backdrop}>
      <form onSubmit={handleSubmit} style={st.card}>
        <div style={st.logo}>dp</div>
        <div style={st.subtitle}>
          {needsSetup ? "Create your admin account" : "Sign in to your data platform"}
        </div>

        {error && <div style={st.error}>{error}</div>}

        {needsSetup && (
          <input
            value={displayName}
            onChange={(e) => setDisplayName(e.target.value)}
            placeholder="Display Name"
            style={st.input}
          />
        )}
        <input
          value={username}
          onChange={(e) => setUsername(e.target.value)}
          placeholder="Username"
          style={st.input}
          autoFocus
        />
        <input
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          placeholder="Password"
          type="password"
          style={st.input}
        />
        <button type="submit" disabled={loading || !username || !password} style={st.btn}>
          {loading ? "..." : needsSetup ? "Create Account" : "Sign In"}
        </button>
      </form>
    </div>
  );
}

const st = {
  backdrop: {
    display: "flex", alignItems: "center", justifyContent: "center",
    height: "100vh", background: "#0f1117",
  },
  card: {
    width: "340px", padding: "32px", background: "#161b22",
    border: "1px solid #21262d", borderRadius: "12px",
    display: "flex", flexDirection: "column", gap: "12px",
  },
  logo: {
    fontSize: "32px", fontWeight: "bold", fontFamily: "monospace",
    color: "#58a6ff", textAlign: "center",
  },
  subtitle: {
    fontSize: "13px", color: "#8b949e", textAlign: "center", marginBottom: "8px",
  },
  error: {
    padding: "8px 12px", background: "#f8514922", border: "1px solid #f85149",
    borderRadius: "6px", color: "#f85149", fontSize: "12px",
  },
  input: {
    padding: "10px 12px", background: "#0d1117", border: "1px solid #30363d",
    borderRadius: "6px", color: "#e1e4e8", fontSize: "14px", outline: "none",
  },
  btn: {
    padding: "10px", background: "#238636", border: "1px solid #2ea043",
    borderRadius: "6px", color: "#fff", cursor: "pointer", fontSize: "14px",
    fontWeight: 600, marginTop: "4px",
  },
};
