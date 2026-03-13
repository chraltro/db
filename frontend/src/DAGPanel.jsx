import React, { useState, useEffect, useRef, useCallback, useMemo } from "react";
import { api } from "./api";
import { useHintTriggerFn } from "./HintSystem";

const SCHEMA_COLORS = {
  landing: "#8b949e",
  bronze: "#d2a04a",
  silver: "#8b949e",
  gold: "#e3b341",
  source: "#484f58",
  ingest: "#58a6ff",
  import: "#bc8cff",
  seed: "#92400e",
  exposure: "#7c3aed",
};

const NODE_W = 160;
const NODE_H = 56;
const LAYER_GAP_X = 220;
const NODE_GAP_Y = 78;

function getCV(prop) {
  return getComputedStyle(document.documentElement).getPropertyValue(prop).trim();
}

function formatRowDelta(current, previous) {
  if (previous == null || current == null) return null;
  const delta = current - previous;
  if (delta === 0) return null;
  const sign = delta > 0 ? "+" : "";
  return `${sign}${delta.toLocaleString()}`;
}

function layoutDAG(nodes, edges) {
  const adj = {};
  const radj = {};
  const inDeg = {};
  for (const n of nodes) {
    adj[n.id] = [];
    radj[n.id] = [];
    inDeg[n.id] = 0;
  }
  for (const e of edges) {
    if (adj[e.source]) adj[e.source].push(e.target);
    if (radj[e.target]) radj[e.target].push(e.source);
    if (inDeg[e.target] !== undefined) inDeg[e.target]++;
  }

  const layer = {};
  const queue = nodes.filter((n) => inDeg[n.id] === 0).map((n) => n.id);
  for (const id of queue) layer[id] = 0;

  const visited = new Set();
  const stack = [...queue];
  while (stack.length > 0) {
    const id = stack.shift();
    if (visited.has(id)) continue;
    visited.add(id);
    for (const next of adj[id] || []) {
      layer[next] = Math.max(layer[next] || 0, (layer[id] || 0) + 1);
      inDeg[next]--;
      if (inDeg[next] === 0) stack.push(next);
    }
  }

  const layers = {};
  for (const n of nodes) {
    const l = layer[n.id] || 0;
    if (!layers[l]) layers[l] = [];
    layers[l].push(n);
  }

  const maxLayer = Math.max(...Object.keys(layers).map(Number), 0);

  const posIndex = {};
  for (let l = 0; l <= maxLayer; l++) {
    const group = layers[l] || [];
    group.forEach((n, i) => { posIndex[n.id] = i; });
  }

  for (let sweep = 0; sweep < 4; sweep++) {
    if (sweep % 2 === 0) {
      for (let l = 1; l <= maxLayer; l++) {
        const group = layers[l] || [];
        for (const n of group) {
          const preds = radj[n.id] || [];
          if (preds.length > 0) {
            posIndex[n.id] = preds.reduce((s, p) => s + (posIndex[p] || 0), 0) / preds.length;
          }
        }
        group.sort((a, b) => (posIndex[a.id] || 0) - (posIndex[b.id] || 0));
        group.forEach((n, i) => { posIndex[n.id] = i; });
        layers[l] = group;
      }
    } else {
      for (let l = maxLayer - 1; l >= 0; l--) {
        const group = layers[l] || [];
        for (const n of group) {
          const succs = adj[n.id] || [];
          if (succs.length > 0) {
            posIndex[n.id] = succs.reduce((s, c) => s + (posIndex[c] || 0), 0) / succs.length;
          }
        }
        group.sort((a, b) => (posIndex[a.id] || 0) - (posIndex[b.id] || 0));
        group.forEach((n, i) => { posIndex[n.id] = i; });
        layers[l] = group;
      }
    }
  }

  const positions = {};
  const maxNodes = Math.max(...Object.values(layers).map((g) => g.length), 1);
  const canvasH = 80 + maxNodes * (NODE_H + NODE_GAP_Y);

  for (let l = 0; l <= maxLayer; l++) {
    const group = layers[l] || [];
    const totalH = group.length * (NODE_H + NODE_GAP_Y) - NODE_GAP_Y;
    const startY = Math.max(40, (canvasH - totalH) / 2);
    group.forEach((n, i) => {
      positions[n.id] = {
        x: 60 + l * LAYER_GAP_X,
        y: startY + i * (NODE_H + NODE_GAP_Y),
      };
    });
  }

  const edgeRoutes = {};
  const ROUTE_MARGIN = 20;
  for (const e of edges) {
    const srcLayer = layer[e.source] || 0;
    const tgtLayer = layer[e.target] || 0;
    const span = tgtLayer - srcLayer;
    if (span < 2) continue;

    const from = positions[e.source];
    const to = positions[e.target];
    if (!from || !to) continue;

    const y1 = from.y + NODE_H / 2;
    const y2 = to.y + NODE_H / 2;
    const waypoints = [];

    for (let il = srcLayer + 1; il < tgtLayer; il++) {
      const layerX = 60 + il * LAYER_GAP_X;
      const wpXBefore = layerX - (LAYER_GAP_X - NODE_W) / 2;
      const wpXAfter = layerX + NODE_W + (LAYER_GAP_X - NODE_W) / 2;

      const t = (il - srcLayer) / span;
      const naturalY = y1 + (y2 - y1) * t;
      const group = layers[il] || [];

      let blocked = false;
      for (const n of group) {
        const np = positions[n.id];
        if (np && naturalY >= np.y - ROUTE_MARGIN && naturalY <= np.y + NODE_H + ROUTE_MARGIN) {
          blocked = true;
          break;
        }
      }

      if (!blocked) {
        waypoints.push({ x: wpXBefore, y: naturalY });
        waypoints.push({ x: wpXAfter, y: naturalY });
        continue;
      }

      const ys = group.map((n) => positions[n.id].y).sort((a, b) => a - b);
      let bestGapY = null;
      let bestDist = Infinity;

      const aboveY = ys[0] - ROUTE_MARGIN - 5;
      if (aboveY > 0) {
        const d = Math.abs(naturalY - aboveY);
        if (d < bestDist) { bestDist = d; bestGapY = aboveY; }
      }
      for (let i = 0; i < ys.length - 1; i++) {
        const gapTop = ys[i] + NODE_H + ROUTE_MARGIN;
        const gapBot = ys[i + 1] - ROUTE_MARGIN;
        if (gapBot > gapTop) {
          const mid = (gapTop + gapBot) / 2;
          const d = Math.abs(naturalY - mid);
          if (d < bestDist) { bestDist = d; bestGapY = mid; }
        }
      }
      const belowY = ys[ys.length - 1] + NODE_H + ROUTE_MARGIN + 5;
      const d = Math.abs(naturalY - belowY);
      if (d < bestDist) { bestDist = d; bestGapY = belowY; }

      const safeY = bestGapY ?? naturalY;
      waypoints.push({ x: wpXBefore, y: safeY });
      waypoints.push({ x: wpXAfter, y: safeY });
    }

    if (waypoints.length > 0) {
      edgeRoutes[e.source + "|" + e.target] = waypoints;
    }
  }

  const width = 120 + (maxLayer + 1) * LAYER_GAP_X;

  return { positions, width, height: canvasH, edgeRoutes };
}

// ---------------------------------------------------------------------------
// Detail Panel (shown when a node is clicked in rewind mode)
// ---------------------------------------------------------------------------

function DetailPanel({ modelName, runId, runs, snapshotsByRun, onClose, onRestore, showConfirm }) {
  const [sample, setSample] = useState(null);
  const [loading, setLoading] = useState(false);
  const [restoring, setRestoring] = useState(false);

  const snapshot = snapshotsByRun?.[runId]?.[modelName];

  useEffect(() => {
    if (!runId || !modelName || !snapshot?.file_path) { setSample(null); return; }
    setLoading(true);
    api.getSnapshotSample(runId, modelName, 50)
      .then(setSample)
      .catch(() => setSample(null))
      .finally(() => setLoading(false));
  }, [runId, modelName]);

  // Row count history across runs
  const history = useMemo(() => {
    if (!runs || !snapshotsByRun) return [];
    return runs.slice().reverse().map(r => {
      const s = snapshotsByRun[r.run_id]?.[modelName];
      return s ? { run_id: r.run_id, ts: r.started_at, row_count: s.row_count } : null;
    }).filter(Boolean);
  }, [runs, snapshotsByRun, modelName]);

  async function handleRestore() {
    if (showConfirm && !(await showConfirm("Restore Snapshot", `Restore ${modelName} from this run? Downstream models will be re-built.`, "Restore", true))) return;
    if (!showConfirm && !confirm(`Restore ${modelName} from this run? Downstream models will be re-built.`)) return;
    setRestoring(true);
    try {
      await onRestore(runId, modelName);
    } finally {
      setRestoring(false);
    }
  }

  return (
    <div style={ds.panel}>
      <div style={ds.panelHeader}>
        <span style={ds.panelTitle}>{modelName}</span>
        <button onClick={onClose} style={ds.closeBtn}>x</button>
      </div>

      {snapshot ? (
        <div style={ds.panelBody}>
          <div style={ds.statRow}>
            <span style={ds.statLabel}>Rows</span>
            <span style={ds.statValue}>{snapshot.row_count?.toLocaleString()}</span>
          </div>
          <div style={ds.statRow}>
            <span style={ds.statLabel}>Columns</span>
            <span style={ds.statValue}>{snapshot.col_count}</span>
          </div>
          <div style={ds.statRow}>
            <span style={ds.statLabel}>Size</span>
            <span style={ds.statValue}>
              {snapshot.size_bytes < 1048576
                ? `${(snapshot.size_bytes / 1024).toFixed(1)} KB`
                : `${(snapshot.size_bytes / 1048576).toFixed(1)} MB`}
            </span>
          </div>
          <div style={ds.statRow}>
            <span style={ds.statLabel}>Status</span>
            <span style={{ ...ds.statValue, color: snapshot.file_path ? "var(--havn-green)" : "var(--havn-text-dim)" }}>
              {snapshot.file_path ? "Restorable" : "Expired"}
            </span>
          </div>

          {/* Row count history sparkline */}
          {history.length > 1 && (
            <div style={ds.histSection}>
              <span style={ds.statLabel}>Row count history</span>
              <div style={ds.sparkContainer}>
                {(() => {
                  const counts = history.map(h => h.row_count);
                  const max = Math.max(...counts, 1);
                  const w = 200, h = 40;
                  return (
                    <svg width={w} height={h} style={{ display: "block" }}>
                      <polyline
                        fill="none"
                        stroke="var(--havn-accent, #58a6ff)"
                        strokeWidth="1.5"
                        points={counts.map((c, i) =>
                          `${(i / (counts.length - 1)) * w},${h - (c / max) * (h - 4) - 2}`
                        ).join(" ")}
                      />
                    </svg>
                  );
                })()}
              </div>
            </div>
          )}

          {/* Sample data preview */}
          {loading && <div style={ds.loadingText}>Loading sample...</div>}
          {sample && sample.columns && sample.rows && sample.rows.length > 0 && (
            <div style={ds.sampleSection}>
              <span style={ds.statLabel}>Sample data</span>
              <div style={ds.sampleTable}>
                <table style={ds.table}>
                  <thead>
                    <tr>{sample.columns.map((c, i) => <th key={i} style={ds.th}>{c}</th>)}</tr>
                  </thead>
                  <tbody>
                    {sample.rows.slice(0, 10).map((row, ri) => (
                      <tr key={ri}>
                        {row.map((v, ci) => <td key={ci} style={ds.td}>{v ?? ""}</td>)}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Restore button */}
          {snapshot.file_path && (
            <button
              onClick={handleRestore}
              disabled={restoring}
              style={ds.restoreBtn}
            >
              {restoring ? "Restoring..." : "Restore to this point"}
            </button>
          )}
        </div>
      ) : (
        <div style={ds.panelBody}>
          <div style={ds.loadingText}>No snapshot for this model at this run.</div>
        </div>
      )}
    </div>
  );
}

// Detail panel styles
const ds = {
  panel: { width: 320, borderLeft: "1px solid var(--havn-border)", display: "flex", flexDirection: "column", overflow: "hidden", background: "var(--havn-bg-secondary)" },
  panelHeader: { display: "flex", justifyContent: "space-between", alignItems: "center", padding: "8px 12px", borderBottom: "1px solid var(--havn-border)" },
  panelTitle: { fontWeight: 600, fontSize: 13 },
  closeBtn: { background: "none", border: "none", color: "var(--havn-text-secondary)", cursor: "pointer", fontSize: 14, padding: "2px 6px" },
  panelBody: { flex: 1, overflow: "auto", padding: "10px 12px", fontSize: 12 },
  statRow: { display: "flex", justifyContent: "space-between", padding: "4px 0", borderBottom: "1px solid var(--havn-border-light)" },
  statLabel: { color: "var(--havn-text-secondary)", fontSize: 11, fontWeight: 500 },
  statValue: { fontWeight: 600, fontSize: 12 },
  histSection: { marginTop: 12 },
  sparkContainer: { marginTop: 4 },
  sampleSection: { marginTop: 12 },
  sampleTable: { marginTop: 4, overflow: "auto", maxHeight: 200, border: "1px solid var(--havn-border-light)", borderRadius: 4 },
  table: { width: "100%", borderCollapse: "collapse", fontSize: 10 },
  th: { textAlign: "left", padding: "3px 6px", background: "var(--havn-bg-tertiary)", borderBottom: "1px solid var(--havn-border-light)", position: "sticky", top: 0, fontWeight: 600 },
  td: { padding: "2px 6px", borderBottom: "1px solid var(--havn-border-light)", maxWidth: 120, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" },
  restoreBtn: { marginTop: 12, padding: "4px 12px", background: "var(--havn-green)", color: "#fff", border: "1px solid var(--havn-green-border)", borderRadius: "var(--havn-radius-lg)", cursor: "pointer", fontSize: 11, fontWeight: 500, width: "100%" },
  loadingText: { color: "var(--havn-text-dim)", fontStyle: "italic", padding: "8px 0" },
};

// ---------------------------------------------------------------------------
// Main DAG Panel
// ---------------------------------------------------------------------------

export default function DAGPanel({ onOpenFile, showConfirm }) {
  const canvasRef = useRef(null);
  const [dag, setDag] = useState(null);
  const [hovered, setHovered] = useState(null);
  const setHintTrigger = useHintTriggerFn();
  const [error, setError] = useState(null);
  const [dagMode, setDagMode] = useState('basic'); // 'basic' | 'full'
  const [selectedNode, setSelectedNode] = useState(null);

  // Rewind state
  const [runs, setRuns] = useState([]);
  const [snapshots, setSnapshots] = useState([]);
  const [sliderIndex, setSliderIndex] = useState(-1);
  const [rewindMode, setRewindMode] = useState(false);

  useEffect(() => {
    setError(null);
    if (dagMode === 'full') {
      (api.getFullDAG ? api.getFullDAG() : Promise.reject(new Error('not available')))
        .then(setDag)
        .catch(() => {
          // Fallback to basic DAG if full is not available
          api.getDAG().then(setDag).catch((e) => setError(e.message || "Failed to load DAG"));
        });
    } else {
      api.getDAG().then(setDag).catch((e) => setError(e.message || "Failed to load DAG"));
    }
    setHintTrigger("dagOpened", true);
  }, [dagMode]);

  // Load rewind data when entering rewind mode
  useEffect(() => {
    if (!rewindMode) return;
    Promise.all([
      api.getRewindRuns(),
      api.getRewindSnapshots(),
    ]).then(([r, s]) => {
      setRuns(r);
      setSnapshots(s);
      if (r.length > 0) setSliderIndex(0);
    }).catch(() => {});
  }, [rewindMode]);

  // Index snapshots by run_id -> model_name
  const snapshotsByRun = useMemo(() => {
    const map = {};
    for (const s of snapshots) {
      if (!map[s.run_id]) map[s.run_id] = {};
      map[s.run_id][s.model_name] = s;
    }
    return map;
  }, [snapshots]);

  // Previous run snapshots (for delta calculation)
  const currentRunId = runs[sliderIndex]?.run_id;
  const prevRunId = runs[sliderIndex + 1]?.run_id;

  const currentSnaps = snapshotsByRun[currentRunId] || {};
  const prevSnaps = snapshotsByRun[prevRunId] || {};

  // Memoize layout
  const layout = useMemo(() => {
    if (!dag || dag.nodes.length === 0) return null;
    return layoutDAG(dag.nodes, dag.edges);
  }, [dag]);

  const draw = useCallback(() => {
    if (!layout || !canvasRef.current) return;
    const canvas = canvasRef.current;
    const ctx = canvas.getContext("2d");
    const { nodes, edges } = dag;
    const { positions, width, height, edgeRoutes } = layout;

    const dpr = window.devicePixelRatio || 1;
    canvas.width = width * dpr;
    canvas.height = height * dpr;
    canvas.style.width = width + "px";
    canvas.style.height = height + "px";
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, width, height);

    // Draw edges
    for (const e of edges) {
      const from = positions[e.source];
      const to = positions[e.target];
      if (!from || !to) continue;

      const isHighlighted = hovered === e.source || hovered === e.target;
      ctx.strokeStyle = isHighlighted ? getCV("--havn-accent") : getCV("--havn-border-light");
      ctx.lineWidth = isHighlighted ? 2 : 1.5;
      ctx.globalAlpha = isHighlighted ? 1 : (hovered ? 0.3 : 0.8);

      const x1 = from.x + NODE_W;
      const y1 = from.y + NODE_H / 2;
      const x2 = to.x;
      const y2 = to.y + NODE_H / 2;

      const routeKey = e.source + "|" + e.target;
      const waypoints = edgeRoutes[routeKey];

      if (waypoints && waypoints.length > 0) {
        const pts = [{ x: x1, y: y1 }, ...waypoints, { x: x2, y: y2 }];
        ctx.beginPath();
        ctx.moveTo(pts[0].x, pts[0].y);
        const tension = 0.5;
        for (let i = 0; i < pts.length - 1; i++) {
          const p0 = pts[Math.max(i - 1, 0)];
          const p1 = pts[i];
          const p2 = pts[i + 1];
          const p3 = pts[Math.min(i + 2, pts.length - 1)];
          const cp1x = p1.x + (p2.x - p0.x) / (6 / tension);
          const cp1y = p1.y + (p2.y - p0.y) / (6 / tension);
          const cp2x = p2.x - (p3.x - p1.x) / (6 / tension);
          const cp2y = p2.y - (p3.y - p1.y) / (6 / tension);
          ctx.bezierCurveTo(cp1x, cp1y, cp2x, cp2y, p2.x, p2.y);
        }
        ctx.stroke();
      } else {
        ctx.beginPath();
        const cpx = (x1 + x2) / 2;
        ctx.moveTo(x1, y1);
        ctx.bezierCurveTo(cpx, y1, cpx, y2, x2, y2);
        ctx.stroke();
      }

      ctx.fillStyle = ctx.strokeStyle;
      ctx.beginPath();
      ctx.moveTo(x2, y2);
      ctx.lineTo(x2 - 8, y2 - 4);
      ctx.lineTo(x2 - 8, y2 + 4);
      ctx.closePath();
      ctx.fill();
    }

    ctx.globalAlpha = 1;

    // Draw nodes
    const fontFamily = getCV("--havn-font") || "-apple-system, sans-serif";
    const monoFamily = getCV("--havn-font-mono") || "monospace";

    for (const n of nodes) {
      const pos = positions[n.id];
      if (!pos) continue;

      const color = SCHEMA_COLORS[n.schema] || getCV("--havn-accent");
      const isHovered = hovered === n.id;
      const isSelected = selectedNode === n.id;
      const isTable = n.type === "table";
      const snap = rewindMode ? currentSnaps[n.id] : null;
      const prevSnap = rewindMode ? prevSnaps[n.id] : null;

      if (hovered && !isHovered) {
        const connected = dag.edges.some(
          (e) => (e.source === hovered && e.target === n.id) || (e.target === hovered && e.source === n.id)
        );
        ctx.globalAlpha = connected ? 1 : 0.35;
      } else {
        ctx.globalAlpha = 1;
      }

      // Node background
      ctx.fillStyle = isHovered || isSelected ? getCV("--havn-bg") : getCV("--havn-bg-secondary");
      ctx.strokeStyle = isSelected ? getCV("--havn-accent") : color;
      ctx.lineWidth = isHovered || isSelected ? 2.5 : (isTable ? 2 : 1.5);

      const r = 6;
      if (isHovered || isSelected) {
        ctx.shadowColor = isSelected ? getCV("--havn-accent") : color;
        ctx.shadowBlur = 12;
        ctx.shadowOffsetX = 0;
        ctx.shadowOffsetY = 2;
      }
      ctx.beginPath();
      ctx.roundRect(pos.x, pos.y, NODE_W, NODE_H, r);
      ctx.fill();
      ctx.stroke();
      ctx.shadowColor = "transparent";
      ctx.shadowBlur = 0;

      // Label
      ctx.fillStyle = isHovered ? getCV("--havn-accent") : getCV("--havn-text");
      ctx.font = `500 11px ${fontFamily}`;
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillText(n.label, pos.x + NODE_W / 2, pos.y + 16, NODE_W - 20);

      // Rewind mode: show row count + delta
      if (rewindMode && snap) {
        const rowStr = snap.row_count?.toLocaleString() ?? "?";
        ctx.fillStyle = "var(--havn-text-secondary, #8b949e)";
        ctx.font = `500 10px ${monoFamily}`;
        ctx.textAlign = "center";
        ctx.fillText(`${rowStr} rows`, pos.x + NODE_W / 2, pos.y + NODE_H - 14, NODE_W - 16);

        // Delta from previous run
        const delta = formatRowDelta(snap.row_count, prevSnap?.row_count);
        if (delta) {
          ctx.fillStyle = delta.startsWith("+") ? "#3fb950" : "#f85149";
          ctx.font = `bold 9px ${monoFamily}`;
          ctx.textAlign = "right";
          ctx.fillText(delta, pos.x + NODE_W - 6, pos.y + NODE_H - 4);
        }

        // Schema change indicator
        if (prevSnap && snap.schema_hash && prevSnap.schema_hash && snap.schema_hash !== prevSnap.schema_hash) {
          ctx.fillStyle = "#d29922";
          ctx.beginPath();
          ctx.arc(pos.x + 10, pos.y + 10, 4, 0, Math.PI * 2);
          ctx.fill();
        }

        // Restorable indicator
        if (!snap.file_path) {
          ctx.globalAlpha = 0.5;
          ctx.fillStyle = "#8b949e";
          ctx.font = `9px ${monoFamily}`;
          ctx.textAlign = "left";
          ctx.fillText("expired", pos.x + 4, pos.y + NODE_H - 4);
          ctx.globalAlpha = 1;
        }
      } else if (!rewindMode) {
        // Type badge
        const badge = n.type === "ingest" ? "I" : n.type === "import" ? "\u2191" : n.type === "source" ? "S" : n.type === "seed" ? "D" : n.type === "exposure" ? "E" : n.type === "table" ? "T" : "V";
        ctx.fillStyle = color;
        ctx.font = `bold 9px ${monoFamily}`;
        ctx.textAlign = "right";
        ctx.fillText(badge, pos.x + NODE_W - 6, pos.y + 12);
      }
    }

    ctx.globalAlpha = 1;
  }, [dag, layout, hovered, rewindMode, currentSnaps, prevSnaps, selectedNode]);

  useEffect(() => {
    draw();
  }, [draw]);

  function handleMouseMove(e) {
    if (!layout || !canvasRef.current) return;
    const rect = canvasRef.current.getBoundingClientRect();
    const mx = (e.clientX - rect.left) * (canvasRef.current.width / rect.width / (window.devicePixelRatio || 1));
    const my = (e.clientY - rect.top) * (canvasRef.current.height / rect.height / (window.devicePixelRatio || 1));

    const { positions } = layout;
    let found = null;
    for (const n of dag.nodes) {
      const p = positions[n.id];
      if (p && mx >= p.x && mx <= p.x + NODE_W && my >= p.y && my <= p.y + NODE_H) {
        found = n.id;
        break;
      }
    }
    setHovered(found);
    canvasRef.current.style.cursor = found ? "pointer" : "default";
  }

  function handleClick(e) {
    if (!layout || !canvasRef.current) return;
    const rect = canvasRef.current.getBoundingClientRect();
    const mx = (e.clientX - rect.left) * (canvasRef.current.width / rect.width / (window.devicePixelRatio || 1));
    const my = (e.clientY - rect.top) * (canvasRef.current.height / rect.height / (window.devicePixelRatio || 1));

    const { positions } = layout;
    for (const n of dag.nodes) {
      const p = positions[n.id];
      if (p && mx >= p.x && mx <= p.x + NODE_W && my >= p.y && my <= p.y + NODE_H) {
        if (rewindMode) {
          setSelectedNode(selectedNode === n.id ? null : n.id);
        } else if (onOpenFile && n.path) {
          onOpenFile(n.path);
        }
        return;
      }
    }
    setSelectedNode(null);
  }

  async function handleRestore(runId, modelName) {
    try {
      const result = await api.restoreSnapshot(runId, modelName, true);
      if (result.status === "success") {
        alert(`Restored ${modelName}. ${result.cascade_results ? Object.keys(result.cascade_results).length + " downstream models rebuilt." : ""}`);
        // Refresh rewind data
        const [r, s] = await Promise.all([api.getRewindRuns(), api.getRewindSnapshots()]);
        setRuns(r);
        setSnapshots(s);
      }
    } catch (err) {
      alert("Restore failed: " + (err.message || err));
    }
  }

  if (error) {
    return <div style={{ padding: "24px", color: "var(--havn-red)", textAlign: "center" }}>{error}</div>;
  }

  if (!dag) {
    return <div style={styles.loading}>Loading DAG...</div>;
  }

  if (dag.nodes.length === 0) {
    return (
      <div style={styles.empty}>
        No models found. Add SQL files to transform/ to see the DAG.
      </div>
    );
  }

  const currentRun = runs[sliderIndex];

  return (
    <div style={styles.container}>
      <div style={styles.header}>
        <div style={styles.headerRight}>
          {!rewindMode && (
            <div style={styles.legend}>
              <span style={styles.legendItem}>
                <span style={{ ...styles.legendDot, background: SCHEMA_COLORS.import }} />imported
              </span>
              <span style={styles.legendItem}>
                <span style={{ ...styles.legendDot, background: SCHEMA_COLORS.ingest }} />ingest
              </span>
              <span style={styles.legendItem}>
                <span style={{ ...styles.legendDot, background: SCHEMA_COLORS.bronze }} />bronze
              </span>
              <span style={styles.legendItem}>
                <span style={{ ...styles.legendDot, background: SCHEMA_COLORS.silver }} />silver
              </span>
              <span style={styles.legendItem}>
                <span style={{ ...styles.legendDot, background: SCHEMA_COLORS.gold }} />gold
              </span>
              <span style={styles.legendItem}>
                <span style={{ ...styles.legendDot, background: SCHEMA_COLORS.seed }} />seed
              </span>
              <span style={styles.legendItem}>
                <span style={{ ...styles.legendDot, background: SCHEMA_COLORS.exposure }} />exposure
              </span>
            </div>
          )}
          {!rewindMode && (
            <div style={{ display: 'flex', gap: 4, alignItems: 'center' }}>
              {['basic', 'full'].map(mode => (
                <button key={mode} onClick={() => setDagMode(mode)} style={{
                  padding: '4px 12px', borderRadius: 'var(--havn-radius-lg)', fontSize: 11, cursor: 'pointer', fontWeight: 500,
                  background: dagMode === mode ? 'var(--havn-accent)' : 'var(--havn-btn-bg)',
                  color: dagMode === mode ? '#fff' : 'var(--havn-text)',
                  border: dagMode === mode ? '1px solid var(--havn-accent)' : '1px solid var(--havn-btn-border)'
                }}>{mode === 'basic' ? 'Basic' : 'Full'}</button>
              ))}
            </div>
          )}
          <button
            onClick={() => { setRewindMode(!rewindMode); setSelectedNode(null); }}
            style={{
              ...styles.rewindBtn,
              background: rewindMode ? "var(--havn-accent, #58a6ff)" : "transparent",
              color: rewindMode ? "#fff" : "var(--havn-text-secondary)",
            }}
          >
            Rewind
          </button>
        </div>
      </div>

      {/* Timeline — placed above the canvas so it's always visible */}
      {rewindMode && runs.length > 0 && (() => {
        const sorted = runs.slice().reverse(); // oldest first
        const earliest = new Date(sorted[0].started_at).getTime();
        const latest = new Date(sorted[sorted.length - 1].started_at).getTime();
        const span = latest - earliest || 1;
        const fmtDate = (s) => s?.slice(0, 10) || "";
        const fmtTime = (s) => s?.slice(11, 16) || "";
        // Map sliderIndex (0=newest in runs[]) to position in sorted array
        const selectedSortedIdx = sorted.length - 1 - sliderIndex;

        return (
          <div style={styles.sliderContainer}>
            <span style={{ fontSize: 10, color: "var(--havn-text-dim)", whiteSpace: "nowrap", flexShrink: 0 }}>
              {fmtDate(sorted[0].started_at)}<br />{fmtTime(sorted[0].started_at)}
            </span>
            <div
              style={{ flex: 1, position: "relative", height: 32, cursor: "pointer", margin: "0 8px" }}
              onClick={(e) => {
                const rect = e.currentTarget.getBoundingClientRect();
                const pct = (e.clientX - rect.left) / rect.width;
                // Find closest run by position
                let bestIdx = 0;
                let bestDist = Infinity;
                sorted.forEach((r, i) => {
                  const t = new Date(r.started_at).getTime();
                  const pos = sorted.length === 1 ? 0.5 : (t - earliest) / span;
                  const d = Math.abs(pos - pct);
                  if (d < bestDist) { bestDist = d; bestIdx = i; }
                });
                setSliderIndex(sorted.length - 1 - bestIdx);
              }}
            >
              {/* Track line */}
              <div style={{ position: "absolute", top: 14, left: 0, right: 0, height: 2, background: "var(--havn-border)", borderRadius: 1 }} />
              {/* Run dots */}
              {sorted.map((r, i) => {
                const t = new Date(r.started_at).getTime();
                const pct = sorted.length === 1 ? 50 : ((t - earliest) / span) * 100;
                const isSelected = i === selectedSortedIdx;
                const statusColor = r.status === "success" ? "var(--havn-green)" : r.status === "failed" ? "var(--havn-red)" : "var(--havn-yellow)";
                return (
                  <div
                    key={r.run_id}
                    title={`${r.started_at?.slice(0, 19)} — ${r.status} (${r.trigger})`}
                    style={{
                      position: "absolute",
                      left: `${pct}%`,
                      top: isSelected ? 7 : 10,
                      width: isSelected ? 14 : 8,
                      height: isSelected ? 14 : 8,
                      borderRadius: "50%",
                      background: isSelected ? "var(--havn-accent, #58a6ff)" : statusColor,
                      border: isSelected ? "2px solid #fff" : "1px solid var(--havn-bg)",
                      transform: "translateX(-50%)",
                      transition: "all 0.15s ease",
                      zIndex: isSelected ? 2 : 1,
                      boxShadow: isSelected ? "0 0 6px rgba(88,166,255,0.5)" : "none",
                    }}
                  />
                );
              })}
            </div>
            <span style={{ fontSize: 10, color: "var(--havn-text-dim)", whiteSpace: "nowrap", flexShrink: 0, textAlign: "right" }}>
              {fmtDate(sorted[sorted.length - 1].started_at)}<br />{fmtTime(sorted[sorted.length - 1].started_at)}
            </span>
            {/* Selected run info */}
            {currentRun && (
              <div style={{ marginLeft: 12, fontSize: 11, whiteSpace: "nowrap", flexShrink: 0 }}>
                <span style={{ fontWeight: 600 }}>{currentRun.started_at?.slice(11, 19)}</span>
                <span style={{
                  marginLeft: 6, fontSize: 10, fontWeight: 600,
                  color: currentRun.status === "success" ? "var(--havn-green)" : currentRun.status === "failed" ? "var(--havn-red)" : "var(--havn-yellow)",
                }}>
                  {currentRun.status}
                </span>
                <span style={{ color: "var(--havn-text-dim)", marginLeft: 6, fontSize: 10 }}>
                  {currentRun.trigger}
                </span>
              </div>
            )}
          </div>
        );
      })()}

      {rewindMode && runs.length === 0 && (
        <div style={styles.sliderContainer}>
          <span style={{ color: "var(--havn-text-dim)", fontSize: 12 }}>
            No pipeline runs recorded. Run a transform to start capturing snapshots.
          </span>
        </div>
      )}

      <div style={styles.mainArea}>
        <div style={{ flex: 1, overflow: "auto", background: "var(--havn-bg-tertiary)" }} data-havn-hint="dag-canvas">
          <canvas
            ref={canvasRef}
            onMouseMove={handleMouseMove}
            onMouseLeave={() => setHovered(null)}
            onClick={handleClick}
            style={styles.canvas}
          />
        </div>

        {/* Rewind detail panel */}
        {rewindMode && selectedNode && currentRun && (
          <DetailPanel
            modelName={selectedNode}
            runId={currentRun.run_id}
            runs={runs}
            snapshotsByRun={snapshotsByRun}
            onClose={() => setSelectedNode(null)}
            onRestore={handleRestore}
            showConfirm={showConfirm}
          />
        )}
      </div>
    </div>
  );
}

const styles = {
  container: { display: "flex", flexDirection: "column", flex: 1, height: "100%", minHeight: 0, overflow: "hidden" },
  header: { display: "flex", alignItems: "center", justifyContent: "space-between", padding: "8px 12px", borderBottom: "1px solid var(--havn-border)", fontSize: "13px", flexShrink: 0 },
  headerTitle: { fontWeight: 600 },
  headerRight: { display: "flex", alignItems: "center", gap: 12 },
  legend: { display: "flex", gap: "10px", fontSize: "11px", color: "var(--havn-text-secondary)", alignItems: "center", flexWrap: "wrap" },
  legendItem: { display: "flex", alignItems: "center", gap: "4px" },
  legendDot: { width: "8px", height: "8px", borderRadius: "50%", display: "inline-block" },
  mainArea: { flex: 1, display: "flex", overflow: "hidden", minHeight: 0 },
  canvas: { display: "block" },
  loading: { padding: "24px", color: "var(--havn-text-secondary)", textAlign: "center" },
  empty: { padding: "24px", color: "var(--havn-text-dim)", textAlign: "center" },
  rewindBtn: { border: "1px solid var(--havn-btn-border)", borderRadius: "var(--havn-radius-lg)", padding: "4px 12px", fontSize: 11, fontWeight: 500, cursor: "pointer" },
  sliderContainer: { display: "flex", alignItems: "center", gap: 10, padding: "8px 16px", borderBottom: "1px solid var(--havn-border)", background: "var(--havn-bg-secondary)", fontSize: 12, flexShrink: 0 },
};
