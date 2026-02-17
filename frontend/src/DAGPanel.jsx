import React, { useState, useEffect, useRef, useCallback } from "react";
import { api } from "./api";

const SCHEMA_COLORS = {
  landing: "#8b949e",
  bronze: "#d2a04a",
  silver: "#8b949e",
  gold: "#e3b341",
  source: "#484f58",
};

const TYPE_SHAPES = {
  source: "diamond",
  view: "rect",
  table: "rect-bold",
};

const NODE_W = 160;
const NODE_H = 40;
const LAYER_GAP_X = 220;
const NODE_GAP_Y = 60;

function layoutDAG(nodes, edges) {
  // Assign layers via longest path from sources
  const adj = {};
  const inDeg = {};
  for (const n of nodes) {
    adj[n.id] = [];
    inDeg[n.id] = 0;
  }
  for (const e of edges) {
    if (adj[e.source]) adj[e.source].push(e.target);
    if (inDeg[e.target] !== undefined) inDeg[e.target]++;
  }

  // Topological layer assignment (longest path)
  const layer = {};
  const queue = nodes.filter((n) => inDeg[n.id] === 0).map((n) => n.id);
  for (const id of queue) layer[id] = 0;

  const visited = new Set();
  const order = [];
  const stack = [...queue];
  while (stack.length > 0) {
    const id = stack.shift();
    if (visited.has(id)) continue;
    visited.add(id);
    order.push(id);
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

  // Position nodes
  const positions = {};
  const maxLayer = Math.max(...Object.keys(layers).map(Number), 0);
  for (let l = 0; l <= maxLayer; l++) {
    const group = layers[l] || [];
    const totalH = group.length * (NODE_H + NODE_GAP_Y) - NODE_GAP_Y;
    const startY = 60;
    group.forEach((n, i) => {
      positions[n.id] = {
        x: 60 + l * LAYER_GAP_X,
        y: startY + i * (NODE_H + NODE_GAP_Y),
      };
    });
  }

  const width = 120 + (maxLayer + 1) * LAYER_GAP_X;
  const maxNodes = Math.max(...Object.values(layers).map((g) => g.length), 1);
  const height = 120 + maxNodes * (NODE_H + NODE_GAP_Y);

  return { positions, width, height };
}

export default function DAGPanel({ onOpenFile }) {
  const canvasRef = useRef(null);
  const [dag, setDag] = useState(null);
  const [hovered, setHovered] = useState(null);

  useEffect(() => {
    api.getDAG().then(setDag).catch(() => {});
  }, []);

  const draw = useCallback(() => {
    if (!dag || !canvasRef.current) return;
    const canvas = canvasRef.current;
    const ctx = canvas.getContext("2d");
    const { nodes, edges } = dag;
    if (nodes.length === 0) return;

    const { positions, width, height } = layoutDAG(nodes, edges);
    canvas.width = width;
    canvas.height = height;

    ctx.clearRect(0, 0, width, height);

    // Draw edges
    ctx.lineWidth = 1.5;
    for (const e of edges) {
      const from = positions[e.source];
      const to = positions[e.target];
      if (!from || !to) continue;

      ctx.strokeStyle = "#30363d";
      ctx.beginPath();
      const x1 = from.x + NODE_W;
      const y1 = from.y + NODE_H / 2;
      const x2 = to.x;
      const y2 = to.y + NODE_H / 2;
      const cpx = (x1 + x2) / 2;
      ctx.moveTo(x1, y1);
      ctx.bezierCurveTo(cpx, y1, cpx, y2, x2, y2);
      ctx.stroke();

      // Arrow
      const angle = Math.atan2(y2 - y1, x2 - (cpx));
      ctx.fillStyle = "#30363d";
      ctx.beginPath();
      ctx.moveTo(x2, y2);
      ctx.lineTo(x2 - 8, y2 - 4);
      ctx.lineTo(x2 - 8, y2 + 4);
      ctx.closePath();
      ctx.fill();
    }

    // Draw nodes
    for (const n of nodes) {
      const pos = positions[n.id];
      if (!pos) continue;

      const color = SCHEMA_COLORS[n.schema] || "#58a6ff";
      const isHovered = hovered === n.id;
      const isTable = n.type === "table";

      // Background
      ctx.fillStyle = isHovered ? "#1f2937" : "#161b22";
      ctx.strokeStyle = color;
      ctx.lineWidth = isTable ? 2.5 : 1.5;

      if (n.type === "source") {
        // Diamond shape
        ctx.beginPath();
        const cx = pos.x + NODE_W / 2;
        const cy = pos.y + NODE_H / 2;
        ctx.moveTo(cx, pos.y);
        ctx.lineTo(pos.x + NODE_W, cy);
        ctx.lineTo(cx, pos.y + NODE_H);
        ctx.lineTo(pos.x, cy);
        ctx.closePath();
        ctx.fill();
        ctx.stroke();
      } else {
        // Rounded rect
        const r = 6;
        ctx.beginPath();
        ctx.roundRect(pos.x, pos.y, NODE_W, NODE_H, r);
        ctx.fill();
        ctx.stroke();
      }

      // Label
      ctx.fillStyle = "#e1e4e8";
      ctx.font = "12px -apple-system, sans-serif";
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillText(n.label, pos.x + NODE_W / 2, pos.y + NODE_H / 2, NODE_W - 16);

      // Type badge
      if (n.type !== "source") {
        const badge = n.type === "table" ? "T" : "V";
        ctx.fillStyle = color;
        ctx.font = "bold 9px monospace";
        ctx.textAlign = "right";
        ctx.fillText(badge, pos.x + NODE_W - 6, pos.y + 12);
      }
    }
  }, [dag, hovered]);

  useEffect(() => {
    draw();
  }, [draw]);

  function handleMouseMove(e) {
    if (!dag || !canvasRef.current) return;
    const rect = canvasRef.current.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const my = e.clientY - rect.top;

    const { positions } = layoutDAG(dag.nodes, dag.edges);
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
    if (!dag || !canvasRef.current || !onOpenFile) return;
    const rect = canvasRef.current.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const my = e.clientY - rect.top;

    const { positions } = layoutDAG(dag.nodes, dag.edges);
    for (const n of dag.nodes) {
      const p = positions[n.id];
      if (p && mx >= p.x && mx <= p.x + NODE_W && my >= p.y && my <= p.y + NODE_H) {
        if (n.path) onOpenFile(n.path);
        break;
      }
    }
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
        <span>Model Lineage</span>
        <div style={styles.legend}>
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
          <span style={styles.legendItem}>V = view</span>
          <span style={styles.legendItem}>T = table</span>
        </div>
      </div>
      <div style={styles.canvasWrap}>
        <canvas
          ref={canvasRef}
          onMouseMove={handleMouseMove}
          onClick={handleClick}
          style={styles.canvas}
        />
      </div>
    </div>
  );
}

const styles = {
  container: { display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" },
  header: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "8px 12px",
    borderBottom: "1px solid #21262d",
    fontSize: "13px",
    fontWeight: 600,
  },
  legend: { display: "flex", gap: "12px", fontSize: "11px", color: "#8b949e" },
  legendItem: { display: "flex", alignItems: "center", gap: "4px" },
  legendDot: { width: "8px", height: "8px", borderRadius: "50%", display: "inline-block" },
  canvasWrap: { flex: 1, overflow: "auto", background: "#0d1117" },
  canvas: { display: "block" },
  loading: { padding: "24px", color: "#8b949e", textAlign: "center" },
  empty: { padding: "24px", color: "#484f58", textAlign: "center" },
};
