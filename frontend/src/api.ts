// ---- Type definitions ----

export interface AuthStatus {
  auth_enabled: boolean;
  needs_setup?: boolean;
}

export interface UserInfo {
  username: string;
  role: string;
  display_name?: string;
}

export interface LoginResult {
  token: string;
  username: string;
  role: string;
}

export interface TableInfo {
  schema: string;
  name: string;
  type: string;
  column_count?: number;
  row_count?: number;
}

export interface TableDescription {
  schema: string;
  name: string;
  columns: ColumnInfo[];
  row_count?: number;
}

export interface ColumnInfo {
  name: string;
  type: string;
  nullable?: boolean;
}

export interface QueryResult {
  columns: string[];
  rows: unknown[][];
  row_count?: number;
  truncated?: boolean;
}

export interface TransformResult {
  results: Record<string, string>;
}

export interface ScriptResult {
  status: string;
  duration_ms: number;
  log_output?: string;
  error?: string;
  rows_affected?: number;
}

export interface StreamStep {
  action: string;
  results?: Record<string, string> | ScriptResult[];
}

export interface StreamResult {
  steps: StreamStep[];
  duration_seconds?: number;
}

export interface LintViolation {
  file: string;
  line: number;
  col: number;
  code: string;
  description: string;
  fixable?: boolean;
}

export interface LintResult {
  violations: LintViolation[];
  count: number;
  fixed?: number;
  content?: string;
}

export interface FileEntry {
  path: string;
  type: string;
  children?: FileEntry[];
}

export interface FileContent {
  content: string;
  language: string;
}

export interface OutputEntry {
  type: "info" | "error" | "warn" | "log";
  message: string;
  ts: string;
}

export interface ModelResult {
  name: string;
  result: string;
}

export interface RunSummary {
  type: "transform" | "stream";
  status: "success" | "failed";
  models: ModelResult[];
  totalRows: number;
  duration: number;
  errors: number;
}

export interface SecretEntry {
  key: string;
  is_set: boolean;
  masked_value: string;
}

export interface UserEntry {
  username: string;
  role: string;
  display_name?: string;
  created_at?: string;
  last_login?: string;
}

export interface StreamConfig {
  description?: string;
  schedule?: string;
  steps: { ingest?: string[]; transform?: string[]; export?: string[] }[];
}

// ---- API client ----

const BASE = "/api";

let authToken: string | null = localStorage.getItem("dp_token") || null;

async function request<T = unknown>(path: string, options: RequestInit = {}): Promise<T> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(options.headers as Record<string, string> || {}),
  };
  if (authToken) {
    headers["Authorization"] = `Bearer ${authToken}`;
  }
  const res = await fetch(`${BASE}${path}`, { ...options, headers });
  if (res.status === 401) {
    authToken = null;
    localStorage.removeItem("dp_token");
    window.dispatchEvent(new Event("dp_auth_required"));
    throw new Error("Authentication required");
  }
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || res.statusText);
  }
  return res.json() as Promise<T>;
}

export const api = {
  // Auth
  setToken: (token: string | null) => {
    authToken = token;
    if (token) localStorage.setItem("dp_token", token);
    else localStorage.removeItem("dp_token");
  },
  getToken: () => authToken,
  getAuthStatus: () => request<AuthStatus>("/auth/status"),
  login: (username: string, password: string) =>
    request<LoginResult>("/auth/login", { method: "POST", body: JSON.stringify({ username, password }) }),
  setup: (username: string, password: string, display_name?: string) =>
    request<LoginResult>("/auth/setup", {
      method: "POST",
      body: JSON.stringify({ username, password, display_name }),
    }),
  getMe: () => request<UserInfo>("/auth/me"),

  // Users
  listUsers: () => request<UserEntry[]>("/users"),
  createUser: (username: string, password: string, role: string, display_name?: string) =>
    request("/users", {
      method: "POST",
      body: JSON.stringify({ username, password, role, display_name }),
    }),
  updateUser: (username: string, data: Partial<{ role: string; password: string; display_name: string }>) =>
    request(`/users/${username}`, { method: "PUT", body: JSON.stringify(data) }),
  deleteUser: (username: string) => request(`/users/${username}`, { method: "DELETE" }),

  // Secrets
  listSecrets: () => request<SecretEntry[]>("/secrets"),
  setSecret: (key: string, value: string) =>
    request("/secrets", { method: "POST", body: JSON.stringify({ key, value }) }),
  deleteSecret: (key: string) => request(`/secrets/${key}`, { method: "DELETE" }),

  // Files
  listFiles: () => request<FileEntry[]>("/files"),
  readFile: (path: string) => request<FileContent>(`/files/${path}`),
  saveFile: (path: string, content: string) =>
    request(`/files/${path}`, { method: "PUT", body: JSON.stringify({ content }) }),
  deleteFile: (path: string) =>
    request(`/files/${path}`, { method: "DELETE" }),

  // Models
  listModels: () => request("/models"),

  // Transform
  runTransform: (targets: string[] | null = null, force: boolean = false) =>
    request<TransformResult>("/transform", {
      method: "POST",
      body: JSON.stringify({ targets, force }),
    }),

  // Run script
  runScript: (scriptPath: string) =>
    request<ScriptResult>("/run", {
      method: "POST",
      body: JSON.stringify({ script_path: scriptPath }),
    }),

  // Streams
  listStreams: () => request<Record<string, StreamConfig>>("/streams"),
  runStream: (name: string, force: boolean = false) =>
    request<StreamResult>(`/stream/${name}?force=${force}`, { method: "POST" }),

  // Query
  runQuery: (sql: string, limit: number = 1000) =>
    request<QueryResult>("/query", { method: "POST", body: JSON.stringify({ sql, limit }) }),

  // Tables
  listTables: (schema: string | null = null) =>
    request<TableInfo[]>(`/tables${schema ? `?schema=${schema}` : ""}`),
  describeTable: (schema: string, table: string) =>
    request<TableDescription>(`/tables/${schema}/${table}`),
  sampleTable: (schema: string, table: string, limit: number = 100) =>
    request<QueryResult>(`/tables/${schema}/${table}/sample?limit=${limit}`),

  // History
  getHistory: (limit: number = 50) => request(`/history?limit=${limit}`),

  // Lint
  runLint: (fix: boolean = false) =>
    request<LintResult>(`/lint?fix=${fix}`, { method: "POST" }),
  lintFile: (path: string, fix: boolean = false, content: string | null = null) =>
    request<LintResult>("/lint/file", { method: "POST", body: JSON.stringify({ path, fix, content }) }),
  getLintConfig: () => request("/lint/config"),
  saveLintConfig: (content: string) =>
    request("/lint/config", { method: "PUT", body: JSON.stringify({ content }) }),
  deleteLintConfig: () =>
    request("/lint/config", { method: "DELETE" }),

  // DAG
  getDAG: () => request("/dag"),

  // Docs
  getDocs: () => request("/docs/markdown"),
  getStructuredDocs: () => request("/docs/structured"),

  // Overview
  getOverview: () => request("/overview"),

  // Connector health
  getConnectorHealth: () => request("/connectors/health"),

  // Scheduler
  getScheduler: () => request("/scheduler"),

  // Notebooks
  listNotebooks: () => request("/notebooks"),
  getNotebook: (name: string) => request(`/notebooks/open/${name}`),
  saveNotebook: (name: string, notebook: unknown) =>
    request(`/notebooks/save/${name}`, {
      method: "POST",
      body: JSON.stringify({ notebook }),
    }),
  createNotebook: (name: string, title: string = "") =>
    request(`/notebooks/create/${name}?title=${encodeURIComponent(title)}`, {
      method: "POST",
    }),
  runNotebook: (name: string) => request(`/notebooks/run/${name}`, { method: "POST" }),
  runCell: (name: string, source: string, { reset = false, cell_type = "code" }: { reset?: boolean; cell_type?: string } = {}) =>
    request(`/notebooks/run-cell/${name}`, {
      method: "POST",
      body: JSON.stringify({ source, cell_type, reset }),
    }),

  // Import
  previewFile: (file_path: string, target_schema: string, target_table: string) =>
    request("/import/preview-file", {
      method: "POST",
      body: JSON.stringify({ file_path, target_schema, target_table }),
    }),
  importFile: (file_path: string, target_schema: string, target_table: string) =>
    request("/import/file", {
      method: "POST",
      body: JSON.stringify({ file_path, target_schema, target_table }),
    }),
  testConnection: (connection_type: string, params: Record<string, unknown>) =>
    request("/import/test-connection", {
      method: "POST",
      body: JSON.stringify({ connection_type, params }),
    }),
  importFromConnection: (connection_type: string, params: Record<string, unknown>, source_table: string, target_schema: string, target_table: string) =>
    request("/import/from-connection", {
      method: "POST",
      body: JSON.stringify({ connection_type, params, source_table, target_schema, target_table }),
    }),

  // Connectors
  listAvailableConnectors: () => request("/connectors/available"),
  listConfiguredConnectors: () => request("/connectors"),
  testConnector: (connector_type: string, config: Record<string, unknown>) =>
    request("/connectors/test", {
      method: "POST",
      body: JSON.stringify({ connector_type, config }),
    }),
  discoverConnector: (connector_type: string, config: Record<string, unknown>) =>
    request("/connectors/discover", {
      method: "POST",
      body: JSON.stringify({ connector_type, config }),
    }),
  setupConnector: (connector_type: string, connection_name: string, config: Record<string, unknown>, tables: string[], target_schema: string, schedule?: string) =>
    request("/connectors/setup", {
      method: "POST",
      body: JSON.stringify({ connector_type, connection_name, config, tables, target_schema, schedule }),
    }),
  regenerateConnector: (connection_name: string, config?: Record<string, unknown>) =>
    request(`/connectors/regenerate/${connection_name}`, {
      method: "POST",
      body: JSON.stringify(config || {}),
    }),
  syncConnector: (connection_name: string) =>
    request(`/connectors/sync/${connection_name}`, { method: "POST" }),
  removeConnector: (connection_name: string) =>
    request(`/connectors/${connection_name}`, { method: "DELETE" }),

  // Diff
  runDiff: (targets: string[] | null = null, target_schema: string | null = null, full: boolean = false) =>
    request("/diff", {
      method: "POST",
      body: JSON.stringify({ targets, target_schema, full }),
    }),

  // Git status
  getGitStatus: () => request("/git/status"),

  // Upload
  uploadFile: async (file: File) => {
    const formData = new FormData();
    formData.append("file", file);
    const headers: Record<string, string> = {};
    if (authToken) headers["Authorization"] = `Bearer ${authToken}`;
    const res = await fetch(`${BASE}/upload`, { method: "POST", body: formData, headers });
    if (!res.ok) throw new Error(await res.text());
    return res.json();
  },

  // Environment
  getEnvironment: () => request("/environment"),
  switchEnvironment: (envName: string) =>
    request(`/environment/${envName}`, { method: "PUT" }),

  // Seeds
  listSeeds: () => request("/seeds"),
  runSeeds: (force: boolean = false, schema_name: string = "seeds") =>
    request("/seeds", {
      method: "POST",
      body: JSON.stringify({ force, schema_name }),
    }),

  // Sources
  listSources: () => request("/sources"),
  checkSourceFreshness: () => request("/sources/freshness"),

  // Exposures
  listExposures: () => request("/exposures"),

  // Autocomplete
  getAutocomplete: () => request("/autocomplete"),

  // Full DAG (with seeds, sources, exposures)
  getFullDAG: () => request("/dag/full"),

  // Model notebook view
  getModelNotebookView: (modelName: string) => request(`/models/${modelName}/notebook-view`),

  // Create model
  createModel: (name: string, schema_name: string = "bronze", materialized: string = "table", sql: string = "") =>
    request("/models/create", {
      method: "POST",
      body: JSON.stringify({ name, schema_name, materialized, sql }),
    }),

  // Check (validation)
  runCheck: () => request("/check", { method: "POST" }),

  // Lineage
  getLineage: (modelName: string) => request(`/lineage/${modelName}`),
  getAllLineage: () => request("/lineage"),
};
