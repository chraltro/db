import React, { useState, useEffect, useCallback, useRef } from "react";
import { createPortal } from "react-dom";
import { api } from "./api";
import FileTree from "./FileTree";
import Editor from "./Editor";
import OutputPanel from "./OutputPanel";
import QueryPanel from "./QueryPanel";
import TablesPanel from "./TablesPanel";
import HistoryPanel from "./HistoryPanel";
import DAGPanel from "./DAGPanel";
import DiffPanel from "./DiffPanel";
import DocsPanel from "./DocsPanel";
import NotebookPanel from "./NotebookPanel";
import DataSourcesPanel from "./DataSourcesPanel";
import OverviewPanel from "./OverviewPanel";
import RunSummary from "./RunSummary";
import SettingsPanel from "./SettingsPanel";
import LoginPage from "./LoginPage";
import ResizeHandle from "./ResizeHandle";
import useResizable from "./useResizable";
import SortableTable from "./SortableTable";
import GuideTour from "./GuideTour";
import ErrorBoundary from "./ErrorBoundary";
import Hint from "./Hint";
import { useHintTriggerFn } from "./HintSystem";
import EnvironmentSwitcher from "./EnvironmentSwitcher";
import ModelNotebookView from "./ModelNotebookView";
import NewModelDialog from "./NewModelDialog";

const GUIDE_STEPS = [
  {
    id: "welcome",
    title: "Welcome to dp",
    description: "dp is your self-hosted data platform. Let's take a quick tour of the interface.",
    position: "center",
  },
  {
    id: "sidebar",
    title: "File Tree",
    description: "Browse your project files here. SQL transforms, Python ingest/export scripts, and notebooks are organized by folder.",
    position: "right",
  },
  {
    id: "editor",
    title: "Code Editor",
    description: "Edit SQL transforms and Python scripts with syntax highlighting, auto-complete, and one-click execution.",
    position: "left",
  },
  {
    id: "tabs",
    title: "Navigation Tabs",
    description: "The Overview shows pipeline health at a glance. Use Data Sources to connect your data, Query to explore, and Tables to browse.",
    position: "bottom",
  },
  {
    id: "actions",
    title: "Action Buttons",
    description: "Run transforms, streams, and lint checks from here. These buttons execute your data pipeline steps.",
    position: "bottom",
  },
  {
    id: "output",
    title: "Output Panel",
    description: "Execution logs, errors, and results appear here. After a pipeline runs, you'll see a summary with links to explore your data.",
    position: "top",
  },
  {
    id: "ready",
    title: "You're Ready!",
    description: "Start by connecting a data source from the Overview tab, or run a stream to kick off your pipeline. You can replay this guide from Settings.",
    position: "center",
  },
];

// Primary tabs always visible; secondary tabs collapsed under "More"
const PRIMARY_TABS = ["Overview", "Editor", "Query", "Tables", "Data Sources"];
const SECONDARY_TABS = ["Notebooks", "DAG", "Diff", "Docs", "History", "Settings"];
const ALL_TABS = [...PRIMARY_TABS, ...SECONDARY_TABS];

function ActionDropdown({ label, onClick, options, disabled, primary }) {
  const [open, setOpen] = useState(false);
  const ref = useRef(null);

  useEffect(() => {
    if (!open) return;
    const handler = (e) => {
      if (ref.current && !ref.current.contains(e.target)) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  const btnStyle = primary ? adStyles.btnPrimary : adStyles.btn;
  const hasOptions = options && options.length > 0;

  return (
    <div ref={ref} style={adStyles.wrapper}>
      <button
        onClick={onClick}
        disabled={disabled}
        style={{
          ...btnStyle,
          ...(hasOptions ? { borderTopRightRadius: 0, borderBottomRightRadius: 0 } : {}),
        }}
      >
        {label}
      </button>
      {hasOptions && (
        <>
          <button
            onClick={() => setOpen(!open)}
            disabled={disabled}
            style={{
              ...btnStyle,
              padding: "5px 5px",
              borderTopLeftRadius: 0,
              borderBottomLeftRadius: 0,
              borderLeft: `1px solid ${primary ? "rgba(255,255,255,0.2)" : "var(--dp-border-light)"}`,
              marginLeft: "-1px",
              fontSize: "10px",
            }}
          >
            {"\u25BE"}
          </button>
          {open && (
            <div style={adStyles.menu}>
              {options.map((opt) => (
                <button
                  key={opt.label}
                  onClick={() => { opt.action(); setOpen(false); }}
                  disabled={disabled}
                  style={adStyles.item}
                  onMouseEnter={(e) => e.currentTarget.style.background = "var(--dp-btn-bg)"}
                  onMouseLeave={(e) => e.currentTarget.style.background = "none"}
                >
                  {opt.label}
                </button>
              ))}
            </div>
          )}
        </>
      )}
    </div>
  );
}

const adStyles = {
  wrapper: { position: "relative", display: "inline-flex" },
  btn: { padding: "5px 12px", background: "var(--dp-btn-bg)", border: "1px solid var(--dp-btn-border)", borderRadius: "var(--dp-radius-lg)", color: "var(--dp-text)", cursor: "pointer", fontSize: "12px", fontWeight: 500 },
  btnPrimary: { padding: "5px 12px", background: "var(--dp-green)", border: "1px solid var(--dp-green-border)", borderRadius: "var(--dp-radius-lg)", color: "#fff", cursor: "pointer", fontSize: "12px", fontWeight: 500 },
  menu: { position: "absolute", top: "100%", left: 0, marginTop: "4px", background: "var(--dp-bg-secondary)", border: "1px solid var(--dp-border)", borderRadius: "var(--dp-radius)", zIndex: 100, minWidth: "120px", boxShadow: "0 4px 12px rgba(0,0,0,0.3)" },
  item: { display: "block", width: "100%", padding: "7px 12px", background: "none", border: "none", color: "var(--dp-text)", cursor: "pointer", fontSize: "12px", textAlign: "left", whiteSpace: "nowrap" },
};

function groupBySchema(tables) {
  const schemas = {};
  for (const t of tables) {
    if (!schemas[t.schema]) schemas[t.schema] = [];
    schemas[t.schema].push(t);
  }
  return schemas;
}

function SchemaTree({ tables, selectedTable, onSelectTable }) {
  const schemas = groupBySchema(tables);
  const SCHEMA_ORDER = ["landing", "bronze", "silver", "gold"];
  const schemaNames = Object.keys(schemas).sort((a, b) => {
    const ai = SCHEMA_ORDER.indexOf(a);
    const bi = SCHEMA_ORDER.indexOf(b);
    if (ai !== -1 && bi !== -1) return ai - bi;
    if (ai !== -1) return -1;
    if (bi !== -1) return 1;
    return a.localeCompare(b);
  });
  const [expanded, setExpanded] = useState(() => {
    const m = {};
    for (const s of schemaNames) m[s] = true;
    return m;
  });

  // Expand new schemas automatically
  useEffect(() => {
    setExpanded((prev) => {
      const next = { ...prev };
      for (const s of schemaNames) {
        if (!(s in next)) next[s] = true;
      }
      return next;
    });
  }, [tables]);

  if (tables.length === 0) {
    return <div style={stStyles.empty}>No tables yet</div>;
  }

  return (
    <div>
      {schemaNames.map((schema) => (
        <div key={schema}>
          <div
            style={stStyles.schemaRow}
            onClick={() => setExpanded((prev) => ({ ...prev, [schema]: !prev[schema] }))}
          >
            <span style={{ ...stStyles.arrow, transform: expanded[schema] ? "rotate(0deg)" : "rotate(-90deg)" }}>
              {"\u25BE"}
            </span>
            <span style={stStyles.schemaName}>{schema}</span>
            <span style={stStyles.schemaCount}>{schemas[schema].length}</span>
          </div>
          {expanded[schema] && schemas[schema].map((t) => {
            const key = `${t.schema}.${t.name}`;
            const isActive = selectedTable === key;
            return (
              <div
                key={key}
                style={{
                  ...stStyles.tableRow,
                  background: isActive ? "var(--dp-bg-secondary)" : "transparent",
                  borderLeft: isActive ? "2px solid var(--dp-accent)" : "2px solid transparent",
                }}
                onClick={() => onSelectTable(t.schema, t.name)}
              >
                <span style={{
                  ...stStyles.typeIcon,
                  color: t.type === "VIEW" ? "var(--dp-purple)" : "var(--dp-accent)",
                }}>{t.type === "VIEW" ? "V" : "T"}</span>
                <span style={isActive ? stStyles.tableNameActive : stStyles.tableName}>{t.name}</span>
              </div>
            );
          })}
        </div>
      ))}
    </div>
  );
}

const stStyles = {
  empty: { padding: "12px", color: "var(--dp-text-dim)", fontSize: "12px", textAlign: "center" },
  schemaRow: { display: "flex", alignItems: "center", gap: "6px", padding: "4px 8px", cursor: "pointer", margin: "0 4px", borderRadius: "3px" },
  arrow: { fontSize: "10px", color: "var(--dp-text-secondary)", width: "10px", display: "inline-block", transition: "transform 0.12s ease" },
  schemaName: { fontSize: "13px", fontWeight: 500, color: "var(--dp-text)" },
  schemaCount: { fontSize: "10px", color: "var(--dp-text-dim)", marginLeft: "auto" },
  tableRow: { display: "flex", alignItems: "center", gap: "6px", padding: "3px 8px 3px 30px", cursor: "pointer", fontSize: "12px", fontFamily: "var(--dp-font-mono)", margin: "0 4px", borderRadius: "3px" },
  typeIcon: { fontSize: "9px", fontWeight: 700, flexShrink: 0 },
  tableName: { color: "var(--dp-text)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" },
  tableNameActive: { color: "var(--dp-accent)", fontWeight: 500, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" },
};

export default function App() {
  const [files, setFiles] = useState([]);
  const [activeFile, setActiveFile] = useState(null);
  const [fileContent, setFileContent] = useState("");
  const [fileLang, setFileLang] = useState("sql");
  const [dirty, setDirty] = useState(false);
  const [preview, setPreview] = useState(null); // { columns, rows } | null
  const [previewError, setPreviewError] = useState(null);
  const [previewRunning, setPreviewRunning] = useState(false);
  const [previewHeight, onPreviewResize, onPreviewResizeStart] = useResizable("dp_editor_preview_height", 200, 80, 600);
  const [output, setOutput] = useState([]);
  const [activeTab, setActiveTab] = useState("Overview");
  const [streams, setStreams] = useState({});
  const [moreOpen, setMoreOpen] = useState(false);
  const moreRef = useRef(null);
  const moreBtnRef = useRef(null);
  const moreMenuRef = useRef(null);
  const [moreMenuPos, setMoreMenuPos] = useState({ top: 0, left: 0 });

  // Run summary state
  const [runSummary, setRunSummary] = useState(null);
  const [running, setRunning] = useState(false);
  const [warehouseTables, setWarehouseTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState(null);

  // Resizable panels
  const [sidebarWidth, onSidebarResize, onSidebarResizeStart] = useResizable("dp_sidebar_width", 240, 150, 500);
  const [outputHeight, onOutputResize, onOutputResizeStart] = useResizable("dp_output_height", 180, 80, 500);

  // Auth state
  const [authChecked, setAuthChecked] = useState(false);
  const [authRequired, setAuthRequired] = useState(false);
  const [needsSetup, setNeedsSetup] = useState(false);
  const [currentUser, setCurrentUser] = useState(null);

  // Editor navigation
  const editorRef = useRef(null);
  const [goToLine, setGoToLine] = useState(null);

  // Guide state
  const [guideOpen, setGuideOpen] = useState(() => !localStorage.getItem("dp_guide_completed"));

  // Hint system triggers
  const setHintTrigger = useHintTriggerFn();
  const tabSwitchCountRef = useRef(0);

  // Keep warehouseHasTables hint flag in sync
  useEffect(() => {
    setHintTrigger("warehouseHasTables", warehouseTables.length > 0);
  }, [warehouseTables, setHintTrigger]);

  function handleGuideComplete() {
    setGuideOpen(false);
    localStorage.setItem("dp_guide_completed", "true");
  }

  function showGuide() {
    setGuideOpen(true);
  }

  function navigateToTab(tab) {
    setActiveTab(tab);
    setMoreOpen(false);
    tabSwitchCountRef.current += 1;
    setHintTrigger("tabSwitchCount", tabSwitchCountRef.current);
  }

  // Close "More" dropdown on outside click
  useEffect(() => {
    if (!moreOpen) return;
    const handler = (e) => {
      if (
        moreRef.current && !moreRef.current.contains(e.target) &&
        moreMenuRef.current && !moreMenuRef.current.contains(e.target)
      ) setMoreOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [moreOpen]);

  // Keyboard shortcuts: Alt+1..5 for primary tabs
  useEffect(() => {
    function handleKeyDown(e) {
      if (!e.altKey) return;
      if (e.ctrlKey || e.metaKey || e.shiftKey) return;
      const num = parseInt(e.key);
      if (num >= 1 && num <= PRIMARY_TABS.length) {
        e.preventDefault();
        navigateToTab(PRIMARY_TABS[num - 1]);
      }
    }
    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, []);

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
      setFiles(data.map((f) => typeof f === "string" ? f.replace(/\\/g, "/") : { ...f, path: f.path?.replace(/\\/g, "/") }));
    } catch (e) {
      addOutput("error", `Failed to load files: ${e.message}`);
    }
  }, []);

  const loadStreams = useCallback(async () => {
    try {
      const data = await api.listStreams();
      setStreams(data);
    } catch (e) { console.warn("Failed to load streams:", e.message); }
  }, []);

  const loadTables = useCallback(async () => {
    try {
      const data = await api.listTables();
      setWarehouseTables(data);
    } catch (e) { console.warn("Failed to load tables:", e.message); }
  }, []);

  useEffect(() => {
    if (!authRequired && authChecked) {
      loadFiles();
      loadStreams();
      loadTables();
    }
  }, [authRequired, authChecked, loadFiles, loadStreams, loadTables]);

  function addOutput(type, message) {
    const ts = new Date().toLocaleTimeString();
    setOutput((prev) => [...prev, { type, message, ts }]);
  }

  const [notebookPath, setNotebookPath] = useState(null);
  const [modelNotebookName, setModelNotebookName] = useState(null);
  const [showNewDialog, setShowNewDialog] = useState(false);

  async function openFile(path, opts = {}) {
    path = path.replace(/\\/g, "/");
    // Open .dpnb files in Notebooks tab
    if (path.endsWith(".dpnb")) {
      setNotebookPath(path);
      setActiveTab("Notebooks");
      return;
    }
    // Open SQL models in notebook view if requested or if it's a transform model
    if (path.endsWith(".sql") && path.startsWith("transform/") && opts.notebookView) {
      // Extract schema.name from path: transform/silver/model.sql -> silver.model
      const parts = path.replace("transform/", "").replace(".sql", "").split("/");
      if (parts.length >= 2) {
        setModelNotebookName(`${parts[0]}.${parts[1]}`);
        return;
      }
    }
    if (dirty && activeFile) {
      if (!confirm("Unsaved changes. Discard?")) return;
    }
    try {
      const data = await api.readFile(path);
      setActiveFile(path);
      setFileContent(data.content);
      setFileLang(data.language);
      setDirty(false);
      setPreview(null);
      setPreviewError(null);
      setActiveTab("Editor");
    } catch (e) {
      addOutput("error", `Failed to open: ${e.message}`);
    }
  }

  function resolveFilePath(ref) {
    const normalized = ref.replace(/\\/g, "/");
    const hasExtension = /\.(sql|py|yml|yaml|json|csv|md|txt|dpnb)$/i.test(normalized);

    // If it looks like schema.model (no slashes, no file extension), map to transform SQL
    if (!hasExtension && !normalized.includes("/") && /^\w+\.\w+$/.test(normalized)) {
      const [schema, model] = normalized.split(".");
      return `transform/${schema}/${model}.sql`;
    }

    // If it's just a bare filename (no slashes), search the file tree
    if (!normalized.includes("/")) {
      const allPaths = [];
      const collect = (nodes) => {
        for (const n of nodes) {
          if (n.type === "file") allPaths.push(n.path);
          if (n.children) collect(n.children);
        }
      };
      collect(files);
      const match = allPaths.find((f) => f.endsWith("/" + normalized) || f === normalized);
      if (match) return match;
    }

    return normalized;
  }

  async function openFileAtLine(ref, line, col) {
    const path = resolveFilePath(ref);
    if (activeFile === path) {
      setGoToLine({ line, col: col || 1 });
      setActiveTab("Editor");
    } else {
      try {
        const data = await api.readFile(path);
        setActiveFile(path);
        setFileContent(data.content);
        setFileLang(data.language);
        setDirty(false);
        setActiveTab("Editor");
        setTimeout(() => setGoToLine({ line, col: col || 1 }), 50);
      } catch (e) {
        addOutput("error", `Failed to open: ${e.message}`);
      }
    }
  }

  async function saveFile() {
    if (!activeFile) return;
    try {
      await api.saveFile(activeFile, fileContent);
      setDirty(false);
      addOutput("info", `Saved ${activeFile}`);
      setHintTrigger("firstFileEdited", true);
    } catch (e) {
      addOutput("error", `Failed to save: ${e.message}`);
    }
  }

  async function createFile(path) {
    path = path.replace(/\\/g, "/");
    if (!path.trim()) return;
    const defaultContent = path.endsWith(".py")
      ? '# A DuckDB connection is available as `db`\n\n'
      : path.endsWith(".sql")
      ? `-- config: materialized=table\n\nSELECT 1\n`
      : "";
    try {
      await api.saveFile(path, defaultContent);
      addOutput("info", `Created ${path}`);
      await loadFiles();
      openFile(path);
    } catch (e) {
      addOutput("error", `Failed to create: ${e.message}`);
    }
  }

  async function deleteFile(path) {
    path = path.replace(/\\/g, "/");
    if (!confirm(`Delete ${path}?`)) return;
    try {
      await api.deleteFile(path);
      addOutput("info", `Deleted ${path}`);
      if (activeFile === path) {
        setActiveFile(null);
        setFileContent("");
        setDirty(false);
      }
      await loadFiles();
    } catch (e) {
      addOutput("error", `Failed to delete: ${e.message}`);
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
        if (data.log_output) data.log_output.split("\n").filter((l) => l.trim()).forEach((l) => addOutput("log", l));
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
    setRunSummary(null);
    addOutput("info", `Running transform (force=${force})...`);
    try {
      const data = await api.runTransform(null, force);
      const models = [];
      for (const [model, status] of Object.entries(data.results || {})) {
        addOutput(status === "error" ? "error" : "info", `${model}: ${status}`);
        models.push({ name: model, result: status });
      }
      loadTables();

      const transformSummary = {
        type: "transform",
        status: models.some((m) => m.result === "error") ? "failed" : "success",
        models,
        totalRows: 0,
        duration: 0,
        errors: models.filter((m) => m.result === "error").length,
      };
      setRunSummary(transformSummary);
      if (transformSummary.status === "success") {
        setHintTrigger("pipelineJustCompleted", true);
        setHintTrigger("pipelineRanThisSession", true);
      }
    } catch (e) {
      addOutput("error", e.message);
    } finally {
      setRunning(false);
    }
  }

  async function runStream(name, force = false) {
    setRunning(true);
    setRunSummary(null);
    addOutput("info", `Running pipeline${force ? " (full refresh)" : ""}...`);
    try {
      const data = await api.runStream(name, force);
      const models = [];
      let totalRows = 0;
      let totalDuration = 0;
      let hasError = false;

      for (const step of data.steps || []) {
        addOutput("info", `--- ${step.action} ---`);
        if (step.action === "transform") {
          for (const [model, status] of Object.entries(step.results || {})) {
            addOutput(status === "error" ? "error" : "info", `${model}: ${status}`);
            models.push({ name: model, result: status });
            if (status === "error") hasError = true;
          }
        } else {
          for (const r of step.results || []) {
            const label = r.script || step.action;
            const msg = r.status === "error" ? `${label}: error — ${r.error}` : `${label}: success (${r.duration_ms}ms)`;
            addOutput(r.status === "error" ? "error" : "info", msg);
            if (r.log_output && r.log_output.trim()) {
              r.log_output.split("\n").filter((l) => l.trim()).forEach((l) => addOutput("log", l.trim()));
            }
            if (r.rows_affected) totalRows += r.rows_affected;
            if (r.duration_ms) totalDuration += r.duration_ms;
            if (r.status === "error") hasError = true;
          }
        }
      }

      const durationS = data.duration_seconds ? data.duration_seconds * 1000 : totalDuration;
      addOutput("info", `Pipeline completed.`);
      loadTables();

      // Show run summary
      const streamSummary = {
        type: "stream",
        status: hasError ? "failed" : "success",
        models,
        totalRows,
        duration: Math.round(durationS),
        errors: models.filter((m) => m.result === "error").length,
      };
      setRunSummary(streamSummary);
      if (!hasError) {
        setHintTrigger("pipelineJustCompleted", true);
        setHintTrigger("pipelineRanThisSession", true);
      }
    } catch (e) {
      addOutput("error", e.message);
      setRunSummary({
        type: "stream",
        status: "failed",
        models: [],
        totalRows: 0,
        duration: 0,
        errors: 1,
      });
    } finally {
      setRunning(false);
    }
  }

  async function runLint(fix = false) {
    setRunning(true);
    addOutput("info", fix ? "Fixing SQL..." : "Linting SQL...");
    try {
      const data = await api.runLint(fix);
      for (const v of data.violations || []) {
        const tag = fix && !v.fixable ? " (unfixable)" : "";
        addOutput("warn", `${v.file}:${v.line}:${v.col} [${v.code}] ${v.description}${tag}`);
      }
      if (fix) {
        const fixed = data.fixed ?? 0;
        const remaining = data.count;
        const parts = [];
        if (fixed > 0) parts.push(`${fixed} fixed`);
        if (remaining > 0) parts.push(`${remaining} violation(s) remain (unfixable by SQLFluff)`);
        addOutput("info", parts.length > 0 ? parts.join(", ") + "." : "All fixable violations resolved.");
      } else {
        addOutput("info", data.count === 0 ? "No lint violations found." : `${data.count} violation(s) found.`);
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

  async function formatCurrentFile() {
    if (!activeFile || !activeFile.endsWith(".sql")) return;
    addOutput("info", `Formatting ${activeFile}...`);
    try {
      const data = await api.lintFile(activeFile, true, fileContent);
      for (const v of data.violations || []) {
        addOutput("warn", `${activeFile}:${v.line}:${v.col} [${v.code}] ${v.description} (unfixable)`);
      }
      const fixed = data.fixed ?? 0;
      if (fixed > 0) addOutput("info", `${fixed} issue(s) fixed.`);
      else if (data.count === 0) addOutput("info", "No violations found.");
      if (data.content != null) setFileContent(data.content);
    } catch (e) {
      addOutput("error", e.message);
    }
  }

  async function previewCurrentFile() {
    if (!activeFile || !activeFile.endsWith(".sql")) return;
    // Strip -- config: / -- depends_on: header lines before running
    const lines = fileContent.split("\n");
    let start = 0;
    for (const line of lines) {
      const s = line.trim();
      if (s.startsWith("-- config:") || s.startsWith("-- depends_on:") || s === "") { start++; } else break;
    }
    const sql = lines.slice(start).join("\n").trim();
    if (!sql) return;
    setPreviewRunning(true);
    setPreviewError(null);
    try {
      const data = await api.runQuery(sql);
      setPreview(data);
    } catch (e) {
      setPreviewError(e.message);
      setPreview(null);
    } finally {
      setPreviewRunning(false);
    }
  }

  function handleSelectTable(schema, name) {
    setSelectedTable(`${schema}.${name}`);
    setActiveTab("Tables");
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

  // Navigate to Query with pre-filled SQL (for "Query this table" from TablesPanel)
  function queryTable(schema, table) {
    navigateToTab("Query");
    // Store the query intent for QueryPanel to pick up
    window.__dp_prefill_query = { sql: `SELECT * FROM ${schema}.${table} LIMIT 1000`, run: true };
  }

  // Run a specific transform model from the editor
  async function runSingleModel() {
    if (!activeFile || !activeFile.includes("transform/") || !activeFile.endsWith(".sql")) return;
    if (dirty) await saveFile();
    setRunning(true);
    setRunSummary(null);
    const modelName = activeFile.replace(/^transform\//, "").replace(/\.sql$/, "").replace(/\//g, ".");
    addOutput("info", `Running transform for ${modelName}...`);
    try {
      const data = await api.runTransform([modelName], false);
      const models = [];
      for (const [model, status] of Object.entries(data.results || {})) {
        addOutput(status === "error" ? "error" : "info", `${model}: ${status}`);
        models.push({ name: model, result: status });
      }
      loadTables();
      setRunSummary({
        type: "transform",
        status: models.some((m) => m.result === "error") ? "failed" : "success",
        models,
        totalRows: 0,
        duration: 0,
        errors: models.filter((m) => m.result === "error").length,
      });
    } catch (e) {
      addOutput("error", e.message);
    } finally {
      setRunning(false);
    }
  }

  const isTransformFile = activeFile && activeFile.includes("transform/") && activeFile.endsWith(".sql");

  return (
    <div style={styles.container}>
      {/* Header */}
      <header style={styles.header}>
        <span style={styles.logo}>dp</span>
        <div style={styles.actions} data-dp-guide="actions">
          <ActionDropdown
            label="Run"
            onClick={() => {
              const names = Object.keys(streams);
              if (names.length > 0) runStream(names[0]);
              else addOutput("warn", "No streams defined in project.yml");
            }}
            options={[{
              label: "Full Refresh",
              action: () => {
                const names = Object.keys(streams);
                if (names.length > 0) runStream(names[0], true);
                else addOutput("warn", "No streams defined in project.yml");
              },
            }]}
            disabled={running}
            primary
          />
          <ActionDropdown
            label="Transform"
            onClick={() => runTransformAll(false)}
            options={[{ label: "Force", action: () => runTransformAll(true) }]}
            disabled={running}
          />
          <ActionDropdown
            label="Lint"
            onClick={() => runLint(false)}
            options={[{ label: "Fix", action: () => runLint(true) }]}
            disabled={running}
          />
          <button onClick={() => setShowNewDialog(true)} style={styles.btn}>+ New</button>
          <EnvironmentSwitcher />
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
        <aside style={{ ...styles.sidebar, width: sidebarWidth }} data-dp-guide="sidebar">
          <FileTree files={files} onSelect={openFile} activeFile={activeFile} onNewFile={createFile} onDeleteFile={deleteFile} onRefresh={() => { loadFiles(); loadTables(); loadStreams(); }} />
          <div style={styles.sidebarDivider} />
          <div style={styles.sidebarSectionHeader}>TABLES</div>
          <SchemaTree
            tables={warehouseTables}
            selectedTable={selectedTable}
            onSelectTable={handleSelectTable}
          />
        </aside>

        <ResizeHandle
          direction="horizontal"
          onResize={onSidebarResize}
          onResizeStart={onSidebarResizeStart}
        />

        {/* Content */}
        <div style={styles.content}>
          {/* Tabs — primary tabs + "More" dropdown for secondary */}
          <div style={styles.tabs} data-dp-guide="tabs" data-dp-hint="tab-bar">
            {PRIMARY_TABS.map((tab, i) => (
              <button
                key={tab}
                data-dp-tab=""
                data-dp-active={activeTab === tab ? "true" : "false"}
                onClick={() => navigateToTab(tab)}
                style={activeTab === tab ? styles.tabActive : styles.tab}
                title={`${tab} (Ctrl+${i + 1})`}
              >
                {tab === "Editor" && dirty ? tab + " *" : tab}
              </button>
            ))}
            {/* More dropdown for secondary tabs */}
            <div ref={moreRef} style={styles.moreWrapper}>
              <button
                ref={moreBtnRef}
                onClick={() => {
                  if (!moreOpen && moreBtnRef.current) {
                    const rect = moreBtnRef.current.getBoundingClientRect();
                    setMoreMenuPos({ top: rect.bottom + 2, left: rect.left });
                  }
                  setMoreOpen(!moreOpen);
                }}
                style={SECONDARY_TABS.includes(activeTab) ? styles.tabActive : styles.tab}
              >
                {SECONDARY_TABS.includes(activeTab) ? activeTab : "More"}
                <span style={styles.moreArrow}>{"\u25BE"}</span>
              </button>
              {moreOpen && createPortal(
                <div ref={moreMenuRef} style={{ ...styles.moreMenu, position: "fixed", top: moreMenuPos.top, left: moreMenuPos.left }}>
                  {SECONDARY_TABS.map((tab) => (
                    <button
                      key={tab}
                      onClick={() => navigateToTab(tab)}
                      style={{
                        ...styles.moreItem,
                        ...(activeTab === tab ? { color: "var(--dp-accent)", fontWeight: 600 } : {}),
                      }}
                      onMouseEnter={(e) => e.currentTarget.style.background = "var(--dp-btn-bg)"}
                      onMouseLeave={(e) => e.currentTarget.style.background = "none"}
                    >
                      {tab}
                    </button>
                  ))}
                </div>,
                document.body
              )}
            </div>
            {activeFile && activeTab === "Editor" && (
              <div style={styles.fileActions} data-dp-hint="editor-toolbar">
                <span style={styles.fileName}>
                  {activeFile}
                  {dirty && <span style={styles.modifiedDot}> *</span>}
                </span>
                <button onClick={saveFile} disabled={!dirty} style={styles.btn}>
                  Save
                </button>
                {isTransformFile && (
                  <button onClick={runSingleModel} disabled={running} style={styles.btn} title="Run just this model">
                    Run Model
                  </button>
                )}
                <button onClick={runCurrentFile} disabled={running} style={styles.btnPrimary}>
                  Run
                </button>
              </div>
            )}
          </div>

          {/* Panel */}
          <div style={styles.panel} data-dp-guide="editor">
            {/* Breadcrumb for secondary tabs */}
            {SECONDARY_TABS.includes(activeTab) && (
              <div style={styles.breadcrumb}>
                <button onClick={() => navigateToTab("Overview")} style={styles.breadcrumbLink}>Overview</button>
                <span style={styles.breadcrumbSep}>/</span>
                <span style={styles.breadcrumbCurrent}>{activeTab}</span>
              </div>
            )}
            {activeTab === "Overview" && (
              <ErrorBoundary name="Overview">
                <OverviewPanel
                  onNavigate={navigateToTab}
                  onRunStream={(name, force) => {
                    if (name) runStream(name, force);
                    else {
                      const names = Object.keys(streams);
                      if (names.length > 0) runStream(names[0], force);
                      else addOutput("warn", "No streams defined in project.yml");
                    }
                  }}
                  streams={streams}
                />
              </ErrorBoundary>
            )}
            {activeTab === "Editor" && (
              <ErrorBoundary name="Editor">
                <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
                  <div style={{ flex: 1, overflow: "hidden", minHeight: 0 }}>
                    <Editor
                      content={fileContent}
                      language={fileLang}
                      onChange={(val) => {
                        setFileContent(val);
                        setDirty(true);
                      }}
                      activeFile={activeFile}
                      onMount={(editor) => { editorRef.current = editor; }}
                      goToLine={goToLine}
                      onFormat={activeFile?.endsWith(".sql") ? formatCurrentFile : undefined}
                      onPreview={activeFile?.endsWith(".sql") ? previewCurrentFile : undefined}
                    />
                  </div>
                  {(preview || previewError || previewRunning) && (
                    <>
                      <ResizeHandle direction="vertical" onResize={(d) => onPreviewResize(-d)} onResizeStart={onPreviewResizeStart} />
                      <div style={{ height: previewHeight, flexShrink: 0, borderTop: "1px solid var(--dp-border)", display: "flex", flexDirection: "column", overflow: "hidden" }}>
                        <div style={{ padding: "4px 12px", fontSize: "11px", color: "var(--dp-text-secondary)", borderBottom: "1px solid var(--dp-border)", display: "flex", alignItems: "center", justifyContent: "space-between", flexShrink: 0 }}>
                          <span>
                            {previewRunning ? "Running…" : previewError ? "Error" : `${preview.rows.length} row${preview.rows.length !== 1 ? "s" : ""}, ${preview.columns.length} col${preview.columns.length !== 1 ? "s" : ""}`}
                          </span>
                          <button onClick={() => { setPreview(null); setPreviewError(null); }} style={{ background: "none", border: "none", color: "var(--dp-text-dim)", cursor: "pointer", fontSize: "14px", lineHeight: 1 }}>×</button>
                        </div>
                        <div style={{ flex: 1, overflow: "auto" }}>
                          {previewError
                            ? <div style={{ padding: "8px 12px", color: "var(--dp-red)", fontFamily: "var(--dp-font-mono)", fontSize: "12px", whiteSpace: "pre-wrap" }}>{previewError}</div>
                            : preview && <SortableTable columns={preview.columns} rows={preview.rows} />
                          }
                        </div>
                      </div>
                    </>
                  )}
                </div>
              </ErrorBoundary>
            )}
            {activeTab === "Query" && <ErrorBoundary name="Query"><QueryPanel addOutput={addOutput} /></ErrorBoundary>}
            {activeTab === "Tables" && <ErrorBoundary name="Tables"><TablesPanel selectedTable={selectedTable} onQueryTable={queryTable} /></ErrorBoundary>}
            {activeTab === "Data Sources" && <ErrorBoundary name="Data Sources"><DataSourcesPanel addOutput={addOutput} /></ErrorBoundary>}
            {activeTab === "Notebooks" && <ErrorBoundary name="Notebooks"><NotebookPanel openPath={notebookPath} /></ErrorBoundary>}
            {activeTab === "DAG" && <ErrorBoundary name="DAG"><DAGPanel onOpenFile={openFile} /></ErrorBoundary>}
            {activeTab === "Diff" && <ErrorBoundary name="Diff"><DiffPanel api={api} addOutput={addOutput} /></ErrorBoundary>}
            {activeTab === "Docs" && <ErrorBoundary name="Docs"><DocsPanel /></ErrorBoundary>}
            {activeTab === "History" && <ErrorBoundary name="History"><HistoryPanel onOpenFile={openFile} /></ErrorBoundary>}
            {activeTab === "Settings" && <ErrorBoundary name="Settings"><SettingsPanel onShowGuide={showGuide} /></ErrorBoundary>}
          </div>

          {/* Run summary (post-pipeline feedback) */}
          {runSummary && (
            <div data-dp-hint="run-summary">
              <RunSummary
                summary={runSummary}
                onNavigate={navigateToTab}
                onDismiss={() => setRunSummary(null)}
              />
            </div>
          )}

          {/* Output */}
          <ResizeHandle
            direction="vertical"
            onResize={(delta) => onOutputResize(-delta)}
            onResizeStart={onOutputResizeStart}
          />
          <div data-dp-guide="output">
            <OutputPanel output={output} onClear={() => setOutput([])} height={outputHeight} onOpenFile={openFileAtLine} />
          </div>
        </div>
      </div>

      <Hint onNavigate={navigateToTab} />
      <GuideTour steps={GUIDE_STEPS} onComplete={handleGuideComplete} isOpen={guideOpen} />

      {/* Model notebook view overlay */}
      {modelNotebookName && (
        <div style={{ position: "fixed", inset: 0, background: "var(--dp-bg)", zIndex: 900, overflow: "auto", padding: "16px" }}>
          <ModelNotebookView
            modelName={modelNotebookName}
            onClose={() => setModelNotebookName(null)}
            onSaved={() => { loadFiles(); }}
          />
        </div>
      )}

      {/* New model/notebook/ingest dialog */}
      {showNewDialog && (
        <NewModelDialog
          onClose={() => setShowNewDialog(false)}
          onCreated={(result) => {
            loadFiles();
            if (result.path) {
              addOutput("info", `Created ${result.path}`);
            }
          }}
        />
      )}
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
  sidebar: { borderRight: "1px solid var(--dp-border)", overflow: "auto", background: "var(--dp-bg-tertiary)", padding: "8px 0", flexShrink: 0 },
  sidebarDivider: { height: "1px", background: "var(--dp-border)", margin: "8px 12px" },
  sidebarSectionHeader: { padding: "6px 12px 8px", fontSize: "10px", fontWeight: "600", color: "var(--dp-text-dim)", letterSpacing: "1px", textTransform: "uppercase" },
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
  // "More" dropdown
  moreWrapper: { position: "relative" },
  moreArrow: { marginLeft: "4px", fontSize: "10px" },
  moreMenu: { background: "var(--dp-bg-secondary)", border: "1px solid var(--dp-border)", borderRadius: "var(--dp-radius)", zIndex: 9999, minWidth: "130px", boxShadow: "0 4px 12px rgba(0,0,0,0.3)" },
  moreItem: { display: "block", width: "100%", padding: "7px 14px", background: "none", border: "none", color: "var(--dp-text)", cursor: "pointer", fontSize: "13px", textAlign: "left", whiteSpace: "nowrap" },
  // Breadcrumb for secondary tabs
  breadcrumb: { display: "flex", alignItems: "center", gap: "6px", padding: "6px 16px", fontSize: "12px", borderBottom: "1px solid var(--dp-border)", background: "var(--dp-bg-tertiary)" },
  breadcrumbLink: { background: "none", border: "none", color: "var(--dp-accent)", cursor: "pointer", fontSize: "12px", padding: 0, fontWeight: 500 },
  breadcrumbSep: { color: "var(--dp-text-dim)" },
  breadcrumbCurrent: { color: "var(--dp-text-secondary)", fontWeight: 500 },
};
