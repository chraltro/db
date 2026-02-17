import React, { useState, useEffect, useCallback } from "react";
import { api } from "./api";
import FileTree from "./FileTree";
import Editor from "./Editor";
import OutputPanel from "./OutputPanel";
import QueryPanel from "./QueryPanel";
import TablesPanel from "./TablesPanel";
import HistoryPanel from "./HistoryPanel";
import DAGPanel from "./DAGPanel";
import DocsPanel from "./DocsPanel";
import NotebookPanel from "./NotebookPanel";
import ImportPanel from "./ImportPanel";
import ChartPanel from "./ChartPanel";
import SettingsPanel from "./SettingsPanel";
import LoginPage from "./LoginPage";

const TABS = ["Editor", "Query", "Charts", "Tables", "Notebooks", "Import", "DAG", "Docs", "History", "Settings"];

export default function App() {
  const [files, setFiles] = useState([]);
  const [activeFile, setActiveFile] = useState(null);
  const [fileContent, setFileContent] = useState("");
  const [fileLang, setFileLang] = useState("sql");
  const [dirty, setDirty] = useState(false);
  const [output, setOutput] = useState([]);
  const [activeTab, setActiveTab] = useState("Editor");
  const [streams, setStreams] = useState({});
  const [running, setRunning] = useState(false);

  // Auth state
  const [authChecked, setAuthChecked] = useState(false);
  const [authRequired, setAuthRequired] = useState(false);
  const [needsSetup, setNeedsSetup] = useState(false);
  const [currentUser, setCurrentUser] = useState(null);

  useEffect(() => {
    checkAuth();
    window.addEventListener("dp_auth_required", () => setAuthRequired(true));
  }, []);

  async function checkAuth() {
    try {
      const status = await api.getAuthStatus();
      if (!status.auth_enabled) {
        setAuthChecked(true);
        setCurrentUser({ username: "local", role: "admin", display_name: "Local User" });
        return;
      }
      if (status.needs_setup) {
        setNeedsSetup(true);
        setAuthRequired(true);
        setAuthChecked(true);
        return;
      }
      // Try existing token
      if (api.getToken()) {
        try {
          const me = await api.getMe();
          setCurrentUser(me);
          setAuthChecked(true);
          return;
        } catch {
          api.setToken(null);
        }
      }
      setAuthRequired(true);
      setAuthChecked(true);
    } catch {
      // Server not requiring auth
      setAuthChecked(true);
      setCurrentUser({ username: "local", role: "admin", display_name: "Local User" });
    }
  }

  function handleLogin(result) {
    setAuthRequired(false);
    setNeedsSetup(false);
    setCurrentUser({ username: result.username, role: result.role || "admin" });
  }

  const loadFiles = useCallback(async () => {
    try {
      const data = await api.listFiles();
      setFiles(data);
    } catch (e) {
      addOutput("error", `Failed to load files: ${e.message}`);
    }
  }, []);

  const loadStreams = useCallback(async () => {
    try {
      const data = await api.listStreams();
      setStreams(data);
    } catch {}
  }, []);

  useEffect(() => {
    if (!authRequired && authChecked) {
      loadFiles();
      loadStreams();
    }
  }, [authRequired, authChecked, loadFiles, loadStreams]);

  function addOutput(type, message) {
    const ts = new Date().toLocaleTimeString();
    setOutput((prev) => [...prev, { type, message, ts }]);
  }

  async function openFile(path) {
    if (dirty && activeFile) {
      if (!confirm("Unsaved changes. Discard?")) return;
    }
    try {
      const data = await api.readFile(path);
      setActiveFile(path);
      setFileContent(data.content);
      setFileLang(data.language);
      setDirty(false);
      setActiveTab("Editor");
    } catch (e) {
      addOutput("error", `Failed to open: ${e.message}`);
    }
  }

  async function saveFile() {
    if (!activeFile) return;
    try {
      await api.saveFile(activeFile, fileContent);
      setDirty(false);
      addOutput("info", `Saved ${activeFile}`);
    } catch (e) {
      addOutput("error", `Failed to save: ${e.message}`);
    }
  }

  async function runCurrentFile() {
    if (!activeFile) return;
    if (dirty) await saveFile();
    setRunning(true);
    try {
      if (activeFile.endsWith(".sql")) {
        addOutput("info", "Running transform...");
        const data = await api.runTransform(null, false);
        for (const [model, status] of Object.entries(data.results || {})) {
          addOutput(status === "error" ? "error" : "info", `${model}: ${status}`);
        }
      } else if (activeFile.endsWith(".py")) {
        addOutput("info", `Running ${activeFile}...`);
        const data = await api.runScript(activeFile);
        addOutput(data.status === "error" ? "error" : "info", `${activeFile}: ${data.status} (${data.duration_ms}ms)`);
        if (data.log_output) addOutput("log", data.log_output);
        if (data.error) addOutput("error", data.error);
      }
    } catch (e) {
      addOutput("error", e.message);
    } finally {
      setRunning(false);
    }
  }

  async function runTransformAll(force = false) {
    setRunning(true);
    addOutput("info", `Running transform (force=${force})...`);
    try {
      const data = await api.runTransform(null, force);
      for (const [model, status] of Object.entries(data.results || {})) {
        addOutput(status === "error" ? "error" : "info", `${model}: ${status}`);
      }
    } catch (e) {
      addOutput("error", e.message);
    } finally {
      setRunning(false);
    }
  }

  async function runStream(name) {
    setRunning(true);
    addOutput("info", `Running stream: ${name}...`);
    try {
      const data = await api.runStream(name);
      for (const step of data.steps || []) {
        addOutput("info", `--- ${step.action} ---`);
        if (step.action === "transform") {
          for (const [model, status] of Object.entries(step.results || {})) {
            addOutput(status === "error" ? "error" : "info", `${model}: ${status}`);
          }
        } else {
          for (const r of step.results || []) {
            addOutput(r.status === "error" ? "error" : "info", `${r.status} (${r.duration_ms}ms)`);
          }
        }
      }
      addOutput("info", `Stream ${name} completed.`);
    } catch (e) {
      addOutput("error", e.message);
    } finally {
      setRunning(false);
    }
  }

  async function runLint(fix = false) {
    setRunning(true);
    addOutput("info", fix ? "Fixing SQL..." : "Linting SQL...");
    try {
      const data = await api.runLint(fix);
      if (data.count === 0) {
        addOutput("info", "No lint violations found.");
      } else {
        for (const v of data.violations || []) {
          addOutput("warn", `${v.file}:${v.line}:${v.col} [${v.code}] ${v.description}`);
        }
        addOutput("info", `${data.count} violation(s) found.`);
      }
      if (fix && activeFile) {
        const d = await api.readFile(activeFile);
        setFileContent(d.content);
      }
    } catch (e) {
      addOutput("error", e.message);
    } finally {
      setRunning(false);
    }
  }

  function handleLogout() {
    api.setToken(null);
    setCurrentUser(null);
    setAuthRequired(true);
  }

  if (!authChecked) {
    return <div style={styles.loading}>Loading...</div>;
  }

  if (authRequired) {
    return <LoginPage onLogin={handleLogin} needsSetup={needsSetup} />;
  }

  return (
    <div style={styles.container}>
      {/* Header */}
      <header style={styles.header}>
        <span style={styles.logo}>dp</span>
        <div style={styles.actions}>
          {Object.keys(streams).map((name) => (
            <button
              key={name}
              onClick={() => runStream(name)}
              disabled={running}
              style={styles.btnPrimary}
            >
              Run: {name}
            </button>
          ))}
          <button onClick={() => runTransformAll(false)} disabled={running} style={styles.btn}>
            Transform
          </button>
          <button onClick={() => runTransformAll(true)} disabled={running} style={styles.btn}>
            Transform (force)
          </button>
          <button onClick={() => runLint(false)} disabled={running} style={styles.btn}>
            Lint
          </button>
          <button onClick={() => runLint(true)} disabled={running} style={styles.btn}>
            Lint (fix)
          </button>
        </div>
        {currentUser && (
          <div style={styles.userInfo}>
            <span style={styles.userName}>{currentUser.display_name || currentUser.username}</span>
            <span style={styles.userRole}>{currentUser.role}</span>
            {currentUser.username !== "local" && (
              <button onClick={handleLogout} style={styles.logoutBtn}>Logout</button>
            )}
          </div>
        )}
      </header>

      <div style={styles.main}>
        {/* Sidebar */}
        <aside style={styles.sidebar}>
          <FileTree files={files} onSelect={openFile} activeFile={activeFile} />
        </aside>

        {/* Content */}
        <div style={styles.content}>
          {/* Tabs */}
          <div style={styles.tabs}>
            {TABS.map((tab) => (
              <button
                key={tab}
                data-dp-tab=""
                data-dp-active={activeTab === tab ? "true" : "false"}
                onClick={() => setActiveTab(tab)}
                style={activeTab === tab ? styles.tabActive : styles.tab}
              >
                {tab}
              </button>
            ))}
            {activeFile && activeTab === "Editor" && (
              <div style={styles.fileActions}>
                <span style={styles.fileName}>
                  {activeFile}
                  {dirty && <span style={styles.modifiedDot}> *</span>}
                </span>
                <button onClick={saveFile} disabled={!dirty} style={styles.btn}>
                  Save
                </button>
                <button onClick={runCurrentFile} disabled={running} style={styles.btnPrimary}>
                  Run
                </button>
              </div>
            )}
          </div>

          {/* Panel */}
          <div style={styles.panel}>
            {activeTab === "Editor" && (
              <Editor
                content={fileContent}
                language={fileLang}
                onChange={(val) => {
                  setFileContent(val);
                  setDirty(true);
                }}
                activeFile={activeFile}
              />
            )}
            {activeTab === "Query" && <QueryPanel addOutput={addOutput} />}
            {activeTab === "Charts" && <ChartPanel />}
            {activeTab === "Tables" && <TablesPanel />}
            {activeTab === "Notebooks" && <NotebookPanel />}
            {activeTab === "Import" && <ImportPanel addOutput={addOutput} />}
            {activeTab === "DAG" && <DAGPanel onOpenFile={openFile} />}
            {activeTab === "Docs" && <DocsPanel />}
            {activeTab === "History" && <HistoryPanel />}
            {activeTab === "Settings" && <SettingsPanel />}
          </div>

          {/* Output */}
          <OutputPanel output={output} onClear={() => setOutput([])} />
        </div>
      </div>
    </div>
  );
}

const styles = {
  container: { display: "flex", flexDirection: "column", height: "100vh", background: "var(--dp-bg)", color: "var(--dp-text)", fontFamily: "var(--dp-font)" },
  loading: { display: "flex", alignItems: "center", justifyContent: "center", height: "100vh", background: "var(--dp-bg)", color: "var(--dp-text-secondary)", fontFamily: "var(--dp-font)", fontSize: "14px" },
  header: { display: "flex", alignItems: "center", justifyContent: "space-between", padding: "8px 16px", borderBottom: "1px solid var(--dp-border)", background: "var(--dp-bg-secondary)", minHeight: "44px" },
  logo: { fontSize: "18px", fontWeight: "bold", fontFamily: "var(--dp-font-mono)", color: "var(--dp-accent)", letterSpacing: "-0.5px" },
  actions: { display: "flex", gap: "6px", flex: 1, justifyContent: "center", flexWrap: "wrap" },
  userInfo: { display: "flex", alignItems: "center", gap: "8px" },
  userName: { fontSize: "12px", color: "var(--dp-text)", fontWeight: 500 },
  userRole: { fontSize: "10px", color: "var(--dp-text-secondary)", background: "var(--dp-btn-bg)", padding: "2px 8px", borderRadius: "10px", fontWeight: 500, textTransform: "capitalize" },
  logoutBtn: { padding: "3px 8px", background: "none", border: "1px solid var(--dp-border-light)", borderRadius: "var(--dp-radius)", color: "var(--dp-text-secondary)", cursor: "pointer", fontSize: "11px" },
  main: { display: "flex", flex: 1, overflow: "hidden" },
  sidebar: { width: "240px", borderRight: "1px solid var(--dp-border)", overflow: "auto", background: "var(--dp-bg-tertiary)", padding: "8px 0" },
  content: { flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" },
  tabs: { display: "flex", alignItems: "center", borderBottom: "1px solid var(--dp-border)", padding: "0 8px", background: "var(--dp-bg-secondary)", overflowX: "auto", minHeight: "36px" },
  tab: { padding: "8px 14px", background: "none", border: "none", borderBottom: "2px solid transparent", color: "var(--dp-text-secondary)", cursor: "pointer", fontSize: "13px", whiteSpace: "nowrap", fontWeight: 500 },
  tabActive: { padding: "8px 14px", background: "none", border: "none", borderBottom: "2px solid var(--dp-accent)", color: "var(--dp-text)", cursor: "pointer", fontSize: "13px", whiteSpace: "nowrap", fontWeight: 600 },
  fileActions: { marginLeft: "auto", display: "flex", alignItems: "center", gap: "8px", paddingLeft: "16px" },
  fileName: { fontSize: "12px", color: "var(--dp-text-secondary)", fontFamily: "var(--dp-font-mono)" },
  modifiedDot: { color: "var(--dp-accent)", fontWeight: 700 },
  panel: { flex: 1, overflow: "hidden" },
  btn: { padding: "5px 12px", background: "var(--dp-btn-bg)", border: "1px solid var(--dp-btn-border)", borderRadius: "var(--dp-radius-lg)", color: "var(--dp-text)", cursor: "pointer", fontSize: "12px", fontWeight: 500 },
  btnPrimary: { padding: "5px 12px", background: "var(--dp-green)", border: "1px solid var(--dp-green-border)", borderRadius: "var(--dp-radius-lg)", color: "#fff", cursor: "pointer", fontSize: "12px", fontWeight: 500 },
};
