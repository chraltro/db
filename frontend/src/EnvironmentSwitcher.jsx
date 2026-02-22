import React, { useState, useEffect } from "react";
import { api } from "./api";

/**
 * Environment indicator and switcher.
 * Shows the current environment and lets users switch between configured environments.
 */
export default function EnvironmentSwitcher() {
  const [env, setEnv] = useState(null);
  const [loading, setLoading] = useState(true);
  const [switching, setSwitching] = useState(false);

  useEffect(() => {
    api.getEnvironment().then(setEnv).catch(() => {}).finally(() => setLoading(false));
  }, []);

  if (loading || !env || env.available.length === 0) return null;

  const handleSwitch = async (envName) => {
    if (envName === env.active) return;
    const confirmed = window.confirm(
      `Switch to environment "${envName}"? This will reload the page and any unsaved changes will be lost.`
    );
    if (!confirmed) return;
    setSwitching(true);
    try {
      const result = await api.switchEnvironment(envName);
      setEnv({ ...env, active: result.active, database_path: result.database_path });
      window.location.reload();
    } catch (e) {
      console.error("Failed to switch environment:", e);
    } finally {
      setSwitching(false);
    }
  };

  return (
    <div className="env-switcher" style={{ display: "inline-flex", alignItems: "center", gap: "6px", fontSize: "12px" }}>
      <span style={{ color: "#888" }}>env:</span>
      <select
        value={env.active || ""}
        onChange={(e) => handleSwitch(e.target.value)}
        disabled={switching}
        style={{
          background: "var(--bg-secondary, #1e1e1e)",
          color: "var(--text, #ccc)",
          border: "1px solid var(--border, #333)",
          borderRadius: "3px",
          padding: "2px 4px",
          fontSize: "12px",
        }}
      >
        {env.available.map((e) => (
          <option key={e} value={e}>{e}</option>
        ))}
      </select>
      <span style={{ color: "#666", fontSize: "11px" }}>({env.database_path})</span>
    </div>
  );
}
