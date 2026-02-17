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

const TABS = ["Editor", "Query", "Tables", "DAG", "Docs", "History"];

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
    loadFiles();
    loadStreams();
  }, [loadFiles, loadStreams]);

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
                onClick={() => setActiveTab(tab)}
                style={activeTab === tab ? styles.tabActive : styles.tab}
              >
                {tab}
              </button>
            ))}
            {activeFile && activeTab === "Editor" && (
              <div style={styles.fileActions}>
                <span style={styles.fileName}>
                  {activeFile} {dirty ? "(modified)" : ""}
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
            {activeTab === "Tables" && <TablesPanel />}
            {activeTab === "DAG" && <DAGPanel onOpenFile={openFile} />}
            {activeTab === "Docs" && <DocsPanel />}
            {activeTab === "History" && <HistoryPanel />}
          </div>

          {/* Output */}
          <OutputPanel output={output} onClear={() => setOutput([])} />
        </div>
      </div>
    </div>
  );
}

const styles = {
  container: {
    display: "flex",
    flexDirection: "column",
    height: "100vh",
    background: "#0f1117",
    color: "#e1e4e8",
  },
  header: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "8px 16px",
    borderBottom: "1px solid #21262d",
    background: "#161b22",
  },
  logo: {
    fontSize: "18px",
    fontWeight: "bold",
    fontFamily: "monospace",
    color: "#58a6ff",
  },
  actions: { display: "flex", gap: "6px" },
  main: { display: "flex", flex: 1, overflow: "hidden" },
  sidebar: {
    width: "240px",
    borderRight: "1px solid #21262d",
    overflow: "auto",
    background: "#0d1117",
    padding: "8px 0",
  },
  content: { flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" },
  tabs: {
    display: "flex",
    alignItems: "center",
    borderBottom: "1px solid #21262d",
    padding: "0 8px",
    background: "#161b22",
  },
  tab: {
    padding: "8px 16px",
    background: "none",
    border: "none",
    color: "#8b949e",
    cursor: "pointer",
    fontSize: "13px",
  },
  tabActive: {
    padding: "8px 16px",
    background: "none",
    border: "none",
    borderBottom: "2px solid #58a6ff",
    color: "#e1e4e8",
    cursor: "pointer",
    fontSize: "13px",
  },
  fileActions: { marginLeft: "auto", display: "flex", alignItems: "center", gap: "8px" },
  fileName: { fontSize: "12px", color: "#8b949e", fontFamily: "monospace" },
  panel: { flex: 1, overflow: "hidden" },
  btn: {
    padding: "4px 12px",
    background: "#21262d",
    border: "1px solid #30363d",
    borderRadius: "6px",
    color: "#e1e4e8",
    cursor: "pointer",
    fontSize: "12px",
  },
  btnPrimary: {
    padding: "4px 12px",
    background: "#238636",
    border: "1px solid #2ea043",
    borderRadius: "6px",
    color: "#fff",
    cursor: "pointer",
    fontSize: "12px",
  },
};
