import React, { useState, useRef, useEffect } from "react";
import { api } from "./api";
import SortableTable from "./SortableTable";

const CHART_TYPES = ["bar", "line", "pie", "scatter"];
const COLORS = ["#58a6ff", "#3fb950", "#d29922", "#f85149", "#bc8cff", "#79c0ff", "#56d364", "#e3b341"];

function getCV(prop) {
  return getComputedStyle(document.documentElement).getPropertyValue(prop).trim();
}

function getFont() {
  return getCV("--dp-font-mono") || "monospace";
}

function drawBar(ctx, data, labels, w, h, seriesColors) {
  const padding = { top: 20, right: 20, bottom: 40, left: 60 };
  const plotW = w - padding.left - padding.right;
  const plotH = h - padding.top - padding.bottom;
  const allValues = data.flat();
  const maxVal = Math.max(...allValues, 0) || 1;
  const barGroupWidth = plotW / labels.length;
  const barWidth = Math.min(barGroupWidth * 0.7 / data.length, 60);
  const font = getFont();

  ctx.strokeStyle = getCV("--dp-border-light");
  ctx.fillStyle = getCV("--dp-text-secondary");
  ctx.font = `11px ${font}`;
  ctx.textAlign = "right";
  for (let i = 0; i <= 5; i++) {
    const y = padding.top + plotH - (plotH * i / 5);
    const val = (maxVal * i / 5).toFixed(maxVal > 10 ? 0 : 1);
    ctx.globalAlpha = 0.3;
    ctx.beginPath();
    ctx.moveTo(padding.left, y);
    ctx.lineTo(w - padding.right, y);
    ctx.stroke();
    ctx.globalAlpha = 1;
    ctx.fillText(val, padding.left - 8, y + 4);
  }

  data.forEach((series, si) => {
    ctx.fillStyle = seriesColors[si % seriesColors.length];
    series.forEach((val, i) => {
      const x = padding.left + i * barGroupWidth + (barGroupWidth - barWidth * data.length) / 2 + si * barWidth;
      const barH = (val / maxVal) * plotH;
      const r = Math.min(3, barWidth / 4);
      const bx = x;
      const by = padding.top + plotH - barH;
      ctx.beginPath();
      ctx.roundRect(bx, by, barWidth - 1, barH, [r, r, 0, 0]);
      ctx.fill();
    });
  });

  ctx.fillStyle = getCV("--dp-text-secondary");
  ctx.font = `11px ${font}`;
  ctx.textAlign = "center";
  labels.forEach((label, i) => {
    const x = padding.left + i * barGroupWidth + barGroupWidth / 2;
    const text = String(label).length > 10 ? String(label).slice(0, 10) + ".." : String(label);
    ctx.fillText(text, x, h - padding.bottom + 16);
  });
}

function drawLine(ctx, data, labels, w, h, seriesColors) {
  const padding = { top: 20, right: 20, bottom: 40, left: 60 };
  const plotW = w - padding.left - padding.right;
  const plotH = h - padding.top - padding.bottom;
  const allValues = data.flat();
  const maxVal = Math.max(...allValues, 0) || 1;
  const minVal = Math.min(...allValues, 0);
  const range = maxVal - minVal || 1;
  const font = getFont();

  ctx.strokeStyle = getCV("--dp-border-light");
  ctx.fillStyle = getCV("--dp-text-secondary");
  ctx.font = `11px ${font}`;
  ctx.textAlign = "right";
  for (let i = 0; i <= 5; i++) {
    const y = padding.top + plotH - (plotH * i / 5);
    const val = (minVal + range * i / 5).toFixed(range > 10 ? 0 : 1);
    ctx.globalAlpha = 0.3;
    ctx.beginPath();
    ctx.moveTo(padding.left, y);
    ctx.lineTo(w - padding.right, y);
    ctx.stroke();
    ctx.globalAlpha = 1;
    ctx.fillText(val, padding.left - 8, y + 4);
  }

  data.forEach((series, si) => {
    ctx.strokeStyle = seriesColors[si % seriesColors.length];
    ctx.lineWidth = 2;
    ctx.lineJoin = "round";
    ctx.lineCap = "round";
    ctx.beginPath();
    series.forEach((val, i) => {
      const x = padding.left + (i / Math.max(series.length - 1, 1)) * plotW;
      const y = padding.top + plotH - ((val - minVal) / range) * plotH;
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.stroke();

    ctx.fillStyle = seriesColors[si % seriesColors.length];
    series.forEach((val, i) => {
      const x = padding.left + (i / Math.max(series.length - 1, 1)) * plotW;
      const y = padding.top + plotH - ((val - minVal) / range) * plotH;
      ctx.beginPath();
      ctx.arc(x, y, 3, 0, Math.PI * 2);
      ctx.fill();
    });
  });

  ctx.fillStyle = getCV("--dp-text-secondary");
  ctx.font = `11px ${font}`;
  ctx.textAlign = "center";
  const step = Math.max(1, Math.floor(labels.length / 10));
  labels.forEach((label, i) => {
    if (i % step !== 0 && i !== labels.length - 1) return;
    const x = padding.left + (i / Math.max(labels.length - 1, 1)) * plotW;
    const text = String(label).length > 8 ? String(label).slice(0, 8) + ".." : String(label);
    ctx.fillText(text, x, h - padding.bottom + 16);
  });
}

function drawPie(ctx, values, labels, w, h, seriesColors) {
  const cx = w / 2;
  const cy = h / 2 - 10;
  const r = Math.min(cx, cy) - 40;
  const total = values.reduce((a, b) => a + b, 0) || 1;
  let startAngle = -Math.PI / 2;
  const font = getFont();

  values.forEach((val, i) => {
    const sliceAngle = (val / total) * Math.PI * 2;
    ctx.fillStyle = seriesColors[i % seriesColors.length];
    ctx.beginPath();
    ctx.moveTo(cx, cy);
    ctx.arc(cx, cy, r, startAngle, startAngle + sliceAngle);
    ctx.closePath();
    ctx.fill();

    if (sliceAngle > 0.2) {
      const midAngle = startAngle + sliceAngle / 2;
      const lx = cx + (r * 0.65) * Math.cos(midAngle);
      const ly = cy + (r * 0.65) * Math.sin(midAngle);
      ctx.fillStyle = "#fff";
      ctx.font = `bold 11px ${font}`;
      ctx.textAlign = "center";
      const pct = ((val / total) * 100).toFixed(1) + "%";
      ctx.fillText(pct, lx, ly + 4);
    }
    startAngle += sliceAngle;
  });

  ctx.font = `11px ${font}`;
  ctx.textAlign = "left";
  const legendY = h - 20;
  let legendX = 10;
  labels.forEach((label, i) => {
    ctx.fillStyle = seriesColors[i % seriesColors.length];
    ctx.beginPath();
    ctx.roundRect(legendX, legendY - 8, 10, 10, 2);
    ctx.fill();
    ctx.fillStyle = getCV("--dp-text-secondary");
    const text = String(label).slice(0, 12);
    ctx.fillText(text, legendX + 14, legendY);
    legendX += ctx.measureText(text).width + 24;
  });
}

function drawScatter(ctx, xData, yData, w, h, color) {
  const padding = { top: 20, right: 20, bottom: 40, left: 60 };
  const plotW = w - padding.left - padding.right;
  const plotH = h - padding.top - padding.bottom;
  const xMin = Math.min(...xData); const xMax = Math.max(...xData);
  const yMin = Math.min(...yData); const yMax = Math.max(...yData);
  const xRange = xMax - xMin || 1; const yRange = yMax - yMin || 1;

  ctx.strokeStyle = getCV("--dp-border-light");
  for (let i = 0; i <= 5; i++) {
    const y = padding.top + plotH - (plotH * i / 5);
    ctx.globalAlpha = 0.3;
    ctx.beginPath();
    ctx.moveTo(padding.left, y);
    ctx.lineTo(w - padding.right, y);
    ctx.stroke();
  }
  ctx.globalAlpha = 1;

  ctx.fillStyle = color;
  xData.forEach((xv, i) => {
    const x = padding.left + ((xv - xMin) / xRange) * plotW;
    const y = padding.top + plotH - ((yData[i] - yMin) / yRange) * plotH;
    ctx.globalAlpha = 0.7;
    ctx.beginPath();
    ctx.arc(x, y, 4, 0, Math.PI * 2);
    ctx.fill();
  });
  ctx.globalAlpha = 1;
}

function ChartView({ results, chartType, setChartType, labelCol, setLabelCol, valueCols, setValueCols }) {
  const canvasRef = useRef(null);

  useEffect(() => {
    if (!results || !canvasRef.current) return;
    const canvas = canvasRef.current;
    const ctx = canvas.getContext("2d");
    const w = canvas.parentElement.clientWidth;
    const h = 360;

    const dpr = window.devicePixelRatio || 1;
    canvas.width = w * dpr;
    canvas.height = h * dpr;
    canvas.style.width = w + "px";
    canvas.style.height = h + "px";
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, w, h);

    const labels = results.rows.map((r) => r[labelCol] ?? "");
    const dataSeries = valueCols.map((ci) =>
      results.rows.map((r) => parseFloat(r[ci]) || 0)
    );

    if (chartType === "bar") {
      drawBar(ctx, dataSeries, labels, w, h, COLORS);
    } else if (chartType === "line") {
      drawLine(ctx, dataSeries, labels, w, h, COLORS);
    } else if (chartType === "pie") {
      drawPie(ctx, dataSeries[0] || [], labels, w, h, COLORS);
    } else if (chartType === "scatter" && dataSeries.length >= 1) {
      const xData = results.rows.map((r) => parseFloat(r[labelCol]) || 0);
      drawScatter(ctx, xData, dataSeries[0], w, h, COLORS[0]);
    }
  }, [results, chartType, labelCol, valueCols]);

  return (
    <div>
      <div style={styles.chartControls}>
        <div style={styles.controlGroup}>
          <label style={styles.controlLabel}>Chart</label>
          <select value={chartType} onChange={(e) => setChartType(e.target.value)} style={styles.controlSelect}>
            {CHART_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
          </select>
        </div>
        <div style={styles.controlGroup}>
          <label style={styles.controlLabel}>Label / X</label>
          <select value={labelCol} onChange={(e) => setLabelCol(parseInt(e.target.value))} style={styles.controlSelect}>
            {results.columns.map((c, i) => <option key={i} value={i}>{c}</option>)}
          </select>
        </div>
        <div style={styles.controlGroup}>
          <label style={styles.controlLabel}>Value / Y</label>
          <select value={valueCols[0]} onChange={(e) => setValueCols([parseInt(e.target.value)])} style={styles.controlSelect}>
            {results.columns.map((c, i) => <option key={i} value={i}>{c}</option>)}
          </select>
        </div>
      </div>
      <div style={styles.chartArea}>
        <canvas ref={canvasRef} style={{ width: "100%", display: "block" }} />
      </div>
    </div>
  );
}

const HISTORY_KEY = "dp_query_history";
const MAX_HISTORY = 50;

function loadHistory() {
  try {
    return JSON.parse(localStorage.getItem(HISTORY_KEY)) || [];
  } catch {
    return [];
  }
}

function saveToHistory(sql) {
  const trimmed = sql.trim();
  if (!trimmed) return;
  const history = loadHistory().filter((h) => h.sql !== trimmed);
  history.unshift({ sql: trimmed, ts: new Date().toLocaleString() });
  if (history.length > MAX_HISTORY) history.length = MAX_HISTORY;
  localStorage.setItem(HISTORY_KEY, JSON.stringify(history));
  return history;
}

export default function QueryPanel({ addOutput }) {
  const [sql, setSql] = useState("SELECT 1 AS hello");
  const [results, setResults] = useState(null);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState(null);
  const [viewMode, setViewMode] = useState("table"); // "table" or "chart"
  const [chartType, setChartType] = useState("bar");
  const [labelCol, setLabelCol] = useState(0);
  const [valueCols, setValueCols] = useState([1]);
  const [history, setHistory] = useState(loadHistory);
  const [showHistory, setShowHistory] = useState(false);

  async function runQuery() {
    setRunning(true);
    setError(null);
    try {
      const data = await api.runQuery(sql, viewMode === "chart" ? 5000 : undefined);
      setResults(data);
      setHistory(saveToHistory(sql));
      if (data.columns.length >= 2) {
        setLabelCol(0);
        setValueCols([1]);
      }
      addOutput("info", `Query returned ${data.rows.length} rows`);
    } catch (e) {
      setError(e.message);
      setHistory(saveToHistory(sql));
      addOutput("error", `Query error: ${e.message}`);
    } finally {
      setRunning(false);
    }
  }

  return (
    <div style={styles.container}>
      <div style={styles.inputArea}>
        <textarea
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          style={styles.textarea}
          placeholder="Enter SQL query..."
          onKeyDown={(e) => {
            if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
              e.preventDefault();
              runQuery();
            }
          }}
        />
        <div style={styles.runCol}>
          <button onClick={runQuery} disabled={running} style={styles.runBtn}>
            {running ? "Running..." : "Run"}
          </button>
          <span style={styles.hint}>Ctrl+Enter</span>
        </div>
      </div>
      <div style={styles.historyBar}>
        <button
          onClick={() => setShowHistory((v) => !v)}
          style={styles.historyToggle}
        >
          History {history.length > 0 ? `(${history.length})` : ""}
          <span style={{ marginLeft: "4px", fontSize: "9px" }}>{showHistory ? "\u25B2" : "\u25BC"}</span>
        </button>
        {showHistory && history.length > 0 && (
          <button
            onClick={() => { localStorage.removeItem(HISTORY_KEY); setHistory([]); }}
            style={styles.clearHistory}
          >
            Clear
          </button>
        )}
      </div>
      {showHistory && (
        <div style={styles.historyList}>
          {history.length === 0 && (
            <div style={styles.historyEmpty}>No queries yet. Run a query to see it here.</div>
          )}
          {history.map((h, i) => (
            <div
              key={i}
              onClick={() => { setSql(h.sql); setShowHistory(false); }}
              style={styles.historyItem}
            >
              <pre style={styles.historySQL}>{h.sql}</pre>
              <span style={styles.historyTs}>{h.ts}</span>
            </div>
          ))}
        </div>
      )}
      {error && <div style={styles.error}>{error}</div>}
      {results && (
        <>
          <div style={styles.viewToggle}>
            <div style={styles.resultsMeta}>
              {results.rows.length} row{results.rows.length !== 1 ? "s" : ""} returned
              {results.truncated && <span style={styles.truncatedMeta}> (truncated)</span>}
            </div>
            <div style={styles.toggleBtns}>
              <button
                onClick={() => setViewMode("table")}
                style={viewMode === "table" ? styles.toggleActive : styles.toggleBtn}
              >
                Table
              </button>
              <button
                onClick={() => setViewMode("chart")}
                style={viewMode === "chart" ? styles.toggleActive : styles.toggleBtn}
              >
                Chart
              </button>
            </div>
          </div>
          {viewMode === "table" ? (
            <div style={styles.tableWrap}>
              <SortableTable columns={results.columns} rows={results.rows} />
            </div>
          ) : (
            <div style={styles.tableWrap}>
              <ChartView
                results={results}
                chartType={chartType}
                setChartType={setChartType}
                labelCol={labelCol}
                setLabelCol={setLabelCol}
                valueCols={valueCols}
                setValueCols={setValueCols}
              />
            </div>
          )}
        </>
      )}
    </div>
  );
}

const styles = {
  container: { display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" },
  inputArea: { display: "flex", gap: "8px", padding: "8px", borderBottom: "1px solid var(--dp-border)" },
  textarea: { flex: 1, minHeight: "60px", maxHeight: "200px", background: "var(--dp-bg-tertiary)", color: "var(--dp-text)", border: "1px solid var(--dp-border-light)", borderRadius: "var(--dp-radius-lg)", padding: "8px", fontFamily: "var(--dp-font-mono)", fontSize: "13px", resize: "vertical" },
  runCol: { display: "flex", flexDirection: "column", alignItems: "center", gap: "4px", alignSelf: "flex-end" },
  runBtn: { padding: "8px 20px", background: "var(--dp-green)", border: "1px solid var(--dp-green-border)", borderRadius: "var(--dp-radius-lg)", color: "#fff", cursor: "pointer", fontSize: "12px", fontWeight: 500 },
  hint: { fontSize: "10px", color: "var(--dp-text-dim)" },
  error: { padding: "8px 12px", color: "var(--dp-red)", fontSize: "13px", fontFamily: "var(--dp-font-mono)", background: "color-mix(in srgb, var(--dp-red) 8%, transparent)", margin: "0 8px", borderRadius: "var(--dp-radius)" },
  viewToggle: { display: "flex", alignItems: "center", justifyContent: "space-between", padding: "4px 12px", borderBottom: "1px solid var(--dp-border)", background: "var(--dp-bg-secondary)" },
  resultsMeta: { fontSize: "11px", color: "var(--dp-text-secondary)" },
  truncatedMeta: { color: "var(--dp-yellow)" },
  toggleBtns: { display: "flex", gap: "2px" },
  toggleBtn: { padding: "3px 12px", background: "none", border: "1px solid var(--dp-border-light)", borderRadius: "var(--dp-radius)", color: "var(--dp-text-secondary)", cursor: "pointer", fontSize: "11px", fontWeight: 500 },
  toggleActive: { padding: "3px 12px", background: "var(--dp-btn-bg)", border: "1px solid var(--dp-accent)", borderRadius: "var(--dp-radius)", color: "var(--dp-accent)", cursor: "pointer", fontSize: "11px", fontWeight: 600 },
  tableWrap: { flex: 1, overflow: "auto", padding: "0 8px 8px" },
  chartControls: { display: "flex", gap: "16px", padding: "8px 4px", alignItems: "flex-end" },
  controlGroup: {},
  controlLabel: { display: "block", fontSize: "10px", color: "var(--dp-text-secondary)", marginBottom: "2px", textTransform: "uppercase", letterSpacing: "0.5px", fontWeight: 600 },
  controlSelect: { padding: "4px 8px", background: "var(--dp-bg-tertiary)", border: "1px solid var(--dp-border-light)", borderRadius: "var(--dp-radius)", color: "var(--dp-text)", fontSize: "12px" },
  chartArea: { padding: "8px 0" },
  historyBar: { display: "flex", alignItems: "center", gap: "8px", padding: "2px 8px", borderBottom: "1px solid var(--dp-border)", background: "var(--dp-bg-secondary)" },
  historyToggle: { background: "none", border: "none", color: "var(--dp-text-secondary)", cursor: "pointer", fontSize: "11px", fontWeight: 500, padding: "2px 4px" },
  clearHistory: { background: "none", border: "none", color: "var(--dp-text-dim)", cursor: "pointer", fontSize: "10px", marginLeft: "auto" },
  historyList: { maxHeight: "200px", overflow: "auto", borderBottom: "1px solid var(--dp-border)", background: "var(--dp-bg-tertiary)" },
  historyEmpty: { color: "var(--dp-text-dim)", fontSize: "12px", padding: "12px", textAlign: "center", fontStyle: "italic" },
  historyItem: { display: "flex", alignItems: "baseline", gap: "12px", padding: "4px 12px", cursor: "pointer", borderBottom: "1px solid var(--dp-border)" },
  historySQL: { flex: 1, margin: 0, fontSize: "12px", fontFamily: "var(--dp-font-mono)", color: "var(--dp-text)", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" },
  historyTs: { fontSize: "10px", color: "var(--dp-text-dim)", flexShrink: 0 },
};
