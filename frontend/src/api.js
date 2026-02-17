const BASE = "/api";

async function request(path, options = {}) {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...options.headers },
    ...options,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || res.statusText);
  }
  return res.json();
}

export const api = {
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
};
