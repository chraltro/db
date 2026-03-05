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
};

const NODE_W = 160;
const NODE_H = 40;
const LAYER_GAP_X = 220;
const NODE_GAP_Y = 70;

function getCV(prop) {
  return getComputedStyle(document.documentElement).getPropertyValue(prop).trim();
}

function layoutDAG(nodes, edges) {
  // Assign layers via longest path from sources
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

  // Topological layer assignment (longest path)
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

  // Group by layer
  const layers = {};
  for (const n of nodes) {
    const l = layer[n.id] || 0;
    if (!layers[l]) layers[l] = [];
    layers[l].push(n);
  }

  const maxLayer = Math.max(...Object.keys(layers).map(Number), 0);

  // Barycenter crossing minimization (4 alternating sweeps)
  // Build position indices for each node within its layer
  const posIndex = {};
  for (let l = 0; l <= maxLayer; l++) {
    const group = layers[l] || [];
    group.forEach((n, i) => { posIndex[n.id] = i; });
  }

  for (let sweep = 0; sweep < 4; sweep++) {
    if (sweep % 2 === 0) {
      // Forward sweep: reorder based on predecessors
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
      // Backward sweep: reorder based on successors
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

  // Position nodes - center each layer vertically
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

  // Waypoint edge routing for long-range edges (spanning 2+ layers)
  // Waypoints are placed in the horizontal gap between layer columns so
  // the curve never crosses through a node box.
  const edgeRoutes = {};
  const ROUTE_MARGIN = 10;
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
      // Place waypoint X in the gap *before* this layer's nodes
      // (halfway between previous layer's right edge and this layer's left edge)
      const layerX = 60 + il * LAYER_GAP_X;
      const wpX = layerX - (LAYER_GAP_X - NODE_W) / 2;

      const t = (il - srcLayer) / span;
      const naturalY = y1 + (y2 - y1) * t;
      const group = layers[il] || [];

      // Check if the straight-line Y would pass through any node in this layer
      let blocked = false;
      for (const n of group) {
        const np = positions[n.id];
        if (np && naturalY >= np.y - ROUTE_MARGIN && naturalY <= np.y + NODE_H + ROUTE_MARGIN) {
          blocked = true;
          break;
        }
      }

      if (!blocked) {
        waypoints.push({ x: wpX, y: naturalY });
        continue;
      }

      // Find nearest vertical gap between nodes in this layer
      const ys = group.map((n) => positions[n.id].y).sort((a, b) => a - b);
      let bestGapY = null;
      let bestDist = Infinity;

      // Gap above first node
      const aboveY = ys[0] - ROUTE_MARGIN - 5;
      if (aboveY > 0) {
        const d = Math.abs(naturalY - aboveY);
        if (d < bestDist) { bestDist = d; bestGapY = aboveY; }
      }
      // Gaps between nodes
      for (let i = 0; i < ys.length - 1; i++) {
        const gapTop = ys[i] + NODE_H + ROUTE_MARGIN;
        const gapBot = ys[i + 1] - ROUTE_MARGIN;
        if (gapBot > gapTop) {
          const mid = (gapTop + gapBot) / 2;
          const d = Math.abs(naturalY - mid);
          if (d < bestDist) { bestDist = d; bestGapY = mid; }
        }
      }
      // Gap below last node
      const belowY = ys[ys.length - 1] + NODE_H + ROUTE_MARGIN + 5;
      const d = Math.abs(naturalY - belowY);
      if (d < bestDist) { bestDist = d; bestGapY = belowY; }

      waypoints.push({ x: wpX, y: bestGapY ?? naturalY });
    }

    if (waypoints.length > 0) {
      edgeRoutes[e.source + "|" + e.target] = waypoints;
    }
  }

  const width = 120 + (maxLayer + 1) * LAYER_GAP_X;

  return { positions, width, height: canvasH, edgeRoutes };
}

export default function DAGPanel({ onOpenFile }) {
  const canvasRef = useRef(null);
  const [dag, setDag] = useState(null);
  const [hovered, setHovered] = useState(null);
  const setHintTrigger = useHintTriggerFn();

  const [error, setError] = useState(null);

  useEffect(() => {
    api.getDAG().then(setDag).catch((e) => setError(e.message || "Failed to load DAG"));
    setHintTrigger("dagOpened", true);
  }, []);

  // Memoize layout so it only recomputes when dag data changes
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

    // High DPI support
    const dpr = window.devicePixelRatio || 1;
    canvas.width = width * dpr;
    canvas.height = height * dpr;
    canvas.style.width = width + "px";
    canvas.style.height = height + "px";
    ctx.scale(dpr, dpr);

    ctx.clearRect(0, 0, width, height);

    // Draw edges with smooth bezier curves
    for (const e of edges) {
      const from = positions[e.source];
      const to = positions[e.target];
      if (!from || !to) continue;

      const isHighlighted = hovered === e.source || hovered === e.target;
      ctx.strokeStyle = isHighlighted ? getCV("--dp-accent") : getCV("--dp-border-light");
      ctx.lineWidth = isHighlighted ? 2 : 1.5;
      ctx.globalAlpha = isHighlighted ? 1 : (hovered ? 0.3 : 0.8);

      const x1 = from.x + NODE_W;
      const y1 = from.y + NODE_H / 2;
      const x2 = to.x;
      const y2 = to.y + NODE_H / 2;

      const routeKey = e.source + "|" + e.target;
      const waypoints = edgeRoutes[routeKey];

      if (waypoints && waypoints.length > 0) {
        // Draw Catmull-Rom spline through waypoints (curve passes through every point)
        const pts = [
          { x: x1, y: y1 },
          ...waypoints,
          { x: x2, y: y2 },
        ];
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
        // Simple single bezier for short edges
        ctx.beginPath();
        const cpx = (x1 + x2) / 2;
        ctx.moveTo(x1, y1);
        ctx.bezierCurveTo(cpx, y1, cpx, y2, x2, y2);
        ctx.stroke();
      }

      // Arrowhead
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
    const fontFamily = getCV("--dp-font") || "-apple-system, sans-serif";
    const monoFamily = getCV("--dp-font-mono") || "monospace";

    for (const n of nodes) {
      const pos = positions[n.id];
      if (!pos) continue;

      const color = SCHEMA_COLORS[n.schema] || getCV("--dp-accent");
      const isHovered = hovered === n.id;
      const isTable = n.type === "table";

      // Dim non-connected nodes when hovering
      if (hovered && !isHovered) {
        const connected = dag.edges.some(
          (e) => (e.source === hovered && e.target === n.id) || (e.target === hovered && e.source === n.id)
        );
        ctx.globalAlpha = connected ? 1 : 0.35;
      } else {
        ctx.globalAlpha = 1;
      }

      // Node background
      ctx.fillStyle = isHovered ? getCV("--dp-bg") : getCV("--dp-bg-secondary");
      ctx.strokeStyle = color;
      ctx.lineWidth = isHovered ? 2.5 : (isTable ? 2 : 1.5);

      // Rounded rect for all nodes, with subtle shadow on hover
      const r = 6;
      if (isHovered) {
        ctx.shadowColor = color;
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
      ctx.fillStyle = isHovered ? getCV("--dp-accent") : getCV("--dp-text");
      ctx.font = `500 12px ${fontFamily}`;
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillText(n.label, pos.x + NODE_W / 2, pos.y + NODE_H / 2, NODE_W - 20);

      // Type badge
      const badge = n.type === "ingest" ? "I" : n.type === "import" ? "↑" : n.type === "source" ? "S" : n.type === "table" ? "T" : "V";
      ctx.fillStyle = color;
      ctx.font = `bold 9px ${monoFamily}`;
      ctx.textAlign = "right";
      ctx.fillText(badge, pos.x + NODE_W - 6, pos.y + 12);
    }

    ctx.globalAlpha = 1;
  }, [dag, layout, hovered]);

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
    if (!layout || !canvasRef.current || !onOpenFile) return;
    const rect = canvasRef.current.getBoundingClientRect();
    const mx = (e.clientX - rect.left) * (canvasRef.current.width / rect.width / (window.devicePixelRatio || 1));
    const my = (e.clientY - rect.top) * (canvasRef.current.height / rect.height / (window.devicePixelRatio || 1));

    const { positions } = layout;
    for (const n of dag.nodes) {
      const p = positions[n.id];
      if (p && mx >= p.x && mx <= p.x + NODE_W && my >= p.y && my <= p.y + NODE_H) {
        if (n.path) onOpenFile(n.path);
        break;
      }
    }
  }

  if (error) {
    return <div style={{ padding: "24px", color: "var(--dp-red)", textAlign: "center" }}>{error}</div>;
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

  return (
    <div style={styles.container}>
      <div style={styles.header}>
        <span style={styles.headerTitle}>Model Lineage</span>
        <div style={styles.legend}>
          <span style={styles.legendItem}>
            <span style={{ ...styles.legendDot, background: SCHEMA_COLORS.import }} />
            imported
          </span>
          <span style={styles.legendItem}>
            <span style={{ ...styles.legendDot, background: SCHEMA_COLORS.ingest }} />
            ingest
          </span>
          <span style={styles.legendItem}>
            <span style={{ ...styles.legendDot, background: SCHEMA_COLORS.landing }} />
            landing
          </span>
          <span style={styles.legendItem}>
            <span style={{ ...styles.legendDot, background: SCHEMA_COLORS.bronze }} />
            bronze
          </span>
          <span style={styles.legendItem}>
            <span style={{ ...styles.legendDot, background: SCHEMA_COLORS.silver }} />
            silver
          </span>
          <span style={styles.legendItem}>
            <span style={{ ...styles.legendDot, background: SCHEMA_COLORS.gold }} />
            gold
          </span>
          <span style={styles.legendSep}>|</span>
          <span style={styles.legendItem}>↑ = imported file</span>
          <span style={styles.legendItem}>I = ingest</span>
          <span style={styles.legendItem}>S = source</span>
          <span style={styles.legendItem}>V = view</span>
          <span style={styles.legendItem}>T = table</span>
        </div>
      </div>
      <div style={styles.canvasWrap} data-dp-hint="dag-canvas">
        <canvas
          ref={canvasRef}
          onMouseMove={handleMouseMove}
          onMouseLeave={() => setHovered(null)}
          onClick={handleClick}
          style={styles.canvas}
        />
      </div>
    </div>
  );
}

const styles = {
  container: { display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" },
  header: { display: "flex", alignItems: "center", justifyContent: "space-between", padding: "8px 12px", borderBottom: "1px solid var(--dp-border)", fontSize: "13px" },
  headerTitle: { fontWeight: 600 },
  legend: { display: "flex", gap: "12px", fontSize: "11px", color: "var(--dp-text-secondary)", alignItems: "center" },
  legendItem: { display: "flex", alignItems: "center", gap: "4px" },
  legendDot: { width: "8px", height: "8px", borderRadius: "50%", display: "inline-block" },
  legendSep: { color: "var(--dp-border-light)" },
  canvasWrap: { flex: 1, overflow: "auto", background: "var(--dp-bg-tertiary)" },
  canvas: { display: "block" },
  loading: { padding: "24px", color: "var(--dp-text-secondary)", textAlign: "center" },
  empty: { padding: "24px", color: "var(--dp-text-dim)", textAlign: "center" },
};
