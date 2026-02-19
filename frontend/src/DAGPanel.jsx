import React, { useState, useEffect, useRef, useCallback } from "react";
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
const NODE_GAP_Y = 60;

function getCV(prop) {
  return getComputedStyle(document.documentElement).getPropertyValue(prop).trim();
}

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

  // Position nodes - center each layer vertically
  const positions = {};
  const maxLayer = Math.max(...Object.keys(layers).map(Number), 0);
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

  const width = 120 + (maxLayer + 1) * LAYER_GAP_X;

  return { positions, width, height: canvasH };
}

export default function DAGPanel({ onOpenFile }) {
  const canvasRef = useRef(null);
  const [dag, setDag] = useState(null);
  const [hovered, setHovered] = useState(null);
  const setHintTrigger = useHintTriggerFn();

  useEffect(() => {
    api.getDAG().then(setDag).catch(() => {});
    setHintTrigger("dagOpened", true);
  }, []);

  const draw = useCallback(() => {
    if (!dag || !canvasRef.current) return;
    const canvas = canvasRef.current;
    const ctx = canvas.getContext("2d");
    const { nodes, edges } = dag;
    if (nodes.length === 0) return;

    const { positions, width, height } = layoutDAG(nodes, edges);

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

      ctx.beginPath();
      const x1 = from.x + NODE_W;
      const y1 = from.y + NODE_H / 2;
      const x2 = to.x;
      const y2 = to.y + NODE_H / 2;
      const cpx = (x1 + x2) / 2;
      ctx.moveTo(x1, y1);
      ctx.bezierCurveTo(cpx, y1, cpx, y2, x2, y2);
      ctx.stroke();

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
  }, [dag, hovered]);

  useEffect(() => {
    draw();
  }, [draw]);

  function handleMouseMove(e) {
    if (!dag || !canvasRef.current) return;
    const rect = canvasRef.current.getBoundingClientRect();
    const mx = (e.clientX - rect.left) * (canvasRef.current.width / rect.width / (window.devicePixelRatio || 1));
    const my = (e.clientY - rect.top) * (canvasRef.current.height / rect.height / (window.devicePixelRatio || 1));

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
    const mx = (e.clientX - rect.left) * (canvasRef.current.width / rect.width / (window.devicePixelRatio || 1));
    const my = (e.clientY - rect.top) * (canvasRef.current.height / rect.height / (window.devicePixelRatio || 1));

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
