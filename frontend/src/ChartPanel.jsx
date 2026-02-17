import React, { useState, useRef, useEffect } from "react";
import { api } from "./api";

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

  // Y axis grid
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

  // Bars with rounded tops
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

  // X labels
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

  // Grid
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

  // Lines
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

    // Dots
    ctx.fillStyle = seriesColors[si % seriesColors.length];
    series.forEach((val, i) => {
      const x = padding.left + (i / Math.max(series.length - 1, 1)) * plotW;
      const y = padding.top + plotH - ((val - minVal) / range) * plotH;
      ctx.beginPath();
      ctx.arc(x, y, 3, 0, Math.PI * 2);
      ctx.fill();
    });
  });

  // X labels
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

    // Label
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

  // Legend
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

export default function ChartPanel() {
  const canvasRef = useRef(null);
  const [sql, setSql] = useState("SELECT 1 AS label, 42 AS value");
  const [chartType, setChartType] = useState("bar");
  const [results, setResults] = useState(null);
  const [error, setError] = useState(null);
  const [running, setRunning] = useState(false);
  const [labelCol, setLabelCol] = useState(0);
  const [valueCols, setValueCols] = useState([1]);

  async function runQuery() {
    setRunning(true);
    setError(null);
    try {
      const data = await api.runQuery(sql, 5000);
      setResults(data);
      if (data.columns.length >= 2) {
        setLabelCol(0);
        setValueCols([1]);
      }
    } catch (e) {
      setError(e.message);
    }
    setRunning(false);
  }

  useEffect(() => {
    if (!results || !canvasRef.current) return;
    const canvas = canvasRef.current;
    const ctx = canvas.getContext("2d");
    const w = canvas.parentElement.clientWidth;
    const h = 400;

    // High DPI support
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
    <div style={st.container}>
      <div style={st.queryArea}>
        <textarea
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          style={st.textarea}
          placeholder="Enter SQL query for chart data..."
          onKeyDown={(e) => {
            if ((e.ctrlKey || e.metaKey) && e.key === "Enter") { e.preventDefault(); runQuery(); }
          }}
        />
        <button onClick={runQuery} disabled={running} style={st.runBtn}>
          {running ? "..." : "Run"}
        </button>
      </div>

      {error && <div style={st.error}>{error}</div>}

      {results && (
        <div style={st.controls}>
          <div style={st.controlGroup}>
            <label style={st.label}>Chart</label>
            <select value={chartType} onChange={(e) => setChartType(e.target.value)} style={st.select}>
              {CHART_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
            </select>
          </div>
          <div style={st.controlGroup}>
            <label style={st.label}>Label / X</label>
            <select value={labelCol} onChange={(e) => setLabelCol(parseInt(e.target.value))} style={st.select}>
              {results.columns.map((c, i) => <option key={i} value={i}>{c}</option>)}
            </select>
          </div>
          <div style={st.controlGroup}>
            <label style={st.label}>Value / Y</label>
            <select value={valueCols[0]} onChange={(e) => setValueCols([parseInt(e.target.value)])} style={st.select}>
              {results.columns.map((c, i) => <option key={i} value={i}>{c}</option>)}
            </select>
          </div>
        </div>
      )}

      <div style={st.chartArea}>
        {!results && (
          <div style={st.placeholder}>Run a query to visualize data as a chart.</div>
        )}
        <canvas ref={canvasRef} style={st.canvas} />
      </div>
    </div>
  );
}

const st = {
  container: { display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" },
  queryArea: { display: "flex", gap: "8px", padding: "8px", borderBottom: "1px solid var(--dp-border)" },
  textarea: { flex: 1, minHeight: "40px", maxHeight: "120px", background: "var(--dp-bg-tertiary)", color: "var(--dp-text)", border: "1px solid var(--dp-border-light)", borderRadius: "var(--dp-radius-lg)", padding: "8px", fontFamily: "var(--dp-font-mono)", fontSize: "13px", resize: "vertical" },
  runBtn: { padding: "8px 16px", background: "var(--dp-green)", border: "1px solid var(--dp-green-border)", borderRadius: "var(--dp-radius-lg)", color: "#fff", cursor: "pointer", fontSize: "12px", fontWeight: 500, alignSelf: "flex-end" },
  error: { padding: "8px 12px", color: "var(--dp-red)", fontSize: "13px", background: "color-mix(in srgb, var(--dp-red) 8%, transparent)", margin: "0 8px", borderRadius: "var(--dp-radius)" },
  controls: { display: "flex", gap: "16px", padding: "8px 12px", borderBottom: "1px solid var(--dp-border)", background: "var(--dp-bg-secondary)", alignItems: "flex-end" },
  controlGroup: {},
  label: { display: "block", fontSize: "10px", color: "var(--dp-text-secondary)", marginBottom: "2px", textTransform: "uppercase", letterSpacing: "0.5px", fontWeight: 600 },
  select: { padding: "4px 8px", background: "var(--dp-bg-tertiary)", border: "1px solid var(--dp-border-light)", borderRadius: "var(--dp-radius)", color: "var(--dp-text)", fontSize: "12px" },
  chartArea: { flex: 1, overflow: "auto", padding: "16px" },
  placeholder: { color: "var(--dp-text-dim)", textAlign: "center", padding: "48px 0" },
  canvas: { width: "100%", display: "block" },
};
