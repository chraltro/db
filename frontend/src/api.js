const BASE = "/api";

let authToken = localStorage.getItem("dp_token") || null;

async function request(path, options = {}) {
  const headers = { "Content-Type": "application/json", ...options.headers };
  if (authToken) {
    headers["Authorization"] = `Bearer ${authToken}`;
  }
  const res = await fetch(`${BASE}${path}`, { headers, ...options });
  if (res.status === 401) {
    // Clear invalid token
    authToken = null;
    localStorage.removeItem("dp_token");
    window.dispatchEvent(new Event("dp_auth_required"));
    throw new Error("Authentication required");
  }
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || res.statusText);
  }
  return res.json();
}

export const api = {
  // Auth
  setToken: (token) => {
    authToken = token;
    if (token) localStorage.setItem("dp_token", token);
    else localStorage.removeItem("dp_token");
  },
  getToken: () => authToken,
  getAuthStatus: () => request("/auth/status"),
  login: (username, password) =>
    request("/auth/login", { method: "POST", body: JSON.stringify({ username, password }) }),
  setup: (username, password, display_name) =>
    request("/auth/setup", {
      method: "POST",
      body: JSON.stringify({ username, password, display_name }),
    }),
  getMe: () => request("/auth/me"),

  // Users
  listUsers: () => request("/users"),
  createUser: (username, password, role, display_name) =>
    request("/users", {
      method: "POST",
      body: JSON.stringify({ username, password, role, display_name }),
    }),
  updateUser: (username, data) =>
    request(`/users/${username}`, { method: "PUT", body: JSON.stringify(data) }),
  deleteUser: (username) => request(`/users/${username}`, { method: "DELETE" }),

  // Secrets
  listSecrets: () => request("/secrets"),
  setSecret: (key, value) =>
    request("/secrets", { method: "POST", body: JSON.stringify({ key, value }) }),
  deleteSecret: (key) => request(`/secrets/${key}`, { method: "DELETE" }),

  // Files
  listFiles: () => request("/files"),
  readFile: (path) => request(`/files/${path}`),
  saveFile: (path, content) =>
    request(`/files/${path}`, { method: "PUT", body: JSON.stringify({ content }) }),

  // Models
  listModels: () => request("/models"),

  // Transform
  runTransform: (targets = null, force = false) =>
    request("/transform", {
      method: "POST",
      body: JSON.stringify({ targets, force }),
    }),

  // Run script
  runScript: (scriptPath) =>
    request("/run", {
      method: "POST",
      body: JSON.stringify({ script_path: scriptPath }),
    }),

  // Streams
  listStreams: () => request("/streams"),
  runStream: (name, force = false) =>
    request(`/stream/${name}?force=${force}`, { method: "POST" }),

  // Query
  runQuery: (sql, limit = 1000) =>
    request("/query", { method: "POST", body: JSON.stringify({ sql, limit }) }),

  // Tables
  listTables: (schema = null) =>
    request(`/tables${schema ? `?schema=${schema}` : ""}`),
  describeTable: (schema, table) => request(`/tables/${schema}/${table}`),
  sampleTable: (schema, table, limit = 100) =>
    request(`/tables/${schema}/${table}/sample?limit=${limit}`),

  // History
  getHistory: (limit = 50) => request(`/history?limit=${limit}`),

  // Lint
  runLint: (fix = false) =>
    request(`/lint?fix=${fix}`, { method: "POST" }),

  // DAG
  getDAG: () => request("/dag"),

  // Docs
  getDocs: () => request("/docs/markdown"),

  // Scheduler
  getScheduler: () => request("/scheduler"),

  // Notebooks
  listNotebooks: () => request("/notebooks"),
  getNotebook: (name) => request(`/notebooks/${name}`),
  saveNotebook: (name, notebook) =>
    request(`/notebooks/${name}`, {
      method: "POST",
      body: JSON.stringify({ notebook }),
    }),
  createNotebook: (name, title = "") =>
    request(`/notebooks/create/${name}?title=${encodeURIComponent(title)}`, {
      method: "POST",
    }),
  runNotebook: (name) => request(`/notebooks/${name}/run`, { method: "POST" }),
  runCell: (name, source) =>
    request(`/notebooks/${name}/run-cell`, {
      method: "POST",
      body: JSON.stringify({ source }),
    }),

  // Import
  previewFile: (file_path, target_schema, target_table) =>
    request("/import/preview-file", {
      method: "POST",
      body: JSON.stringify({ file_path, target_schema, target_table }),
    }),
  importFile: (file_path, target_schema, target_table) =>
    request("/import/file", {
      method: "POST",
      body: JSON.stringify({ file_path, target_schema, target_table }),
    }),
  testConnection: (connection_type, params) =>
    request("/import/test-connection", {
      method: "POST",
      body: JSON.stringify({ connection_type, params }),
    }),
  importFromConnection: (connection_type, params, source_table, target_schema, target_table) =>
    request("/import/from-connection", {
      method: "POST",
      body: JSON.stringify({ connection_type, params, source_table, target_schema, target_table }),
    }),

  // Upload
  uploadFile: async (file) => {
    const formData = new FormData();
    formData.append("file", file);
    const headers = {};
    if (authToken) headers["Authorization"] = `Bearer ${authToken}`;
    const res = await fetch(`${BASE}/upload`, { method: "POST", body: formData, headers });
    if (!res.ok) throw new Error(await res.text());
    return res.json();
  },
};
