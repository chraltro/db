import React, { useState, useEffect, useRef, useMemo, useCallback } from "react";

// ═══════════════════════════════════════════════════════════════════
// UTILITIES
// ═══════════════════════════════════════════════════════════════════

function niceScale(min, max, maxTicks = 6) {
  if (min === max) { min = min === 0 ? -1 : min * 0.9; max = max === 0 ? 1 : max * 1.1; }
  const range = max - min;
  const rough = range / maxTicks;
  const mag = Math.pow(10, Math.floor(Math.log10(rough)));
  const norm = rough / mag;
  let step;
  if (norm <= 1.5) step = mag;
  else if (norm <= 3) step = 2 * mag;
  else if (norm <= 7) step = 5 * mag;
  else step = 10 * mag;
  const nMin = Math.floor(min / step) * step;
  const nMax = Math.ceil(max / step) * step;
  const ticks = [];
  for (let v = nMin; v <= nMax + step * 0.001; v += step) {
    ticks.push(parseFloat(v.toPrecision(12)));
  }
  return { min: nMin, max: nMax, ticks };
}

function fmtNum(v) {
  if (v === null || v === undefined) return "";
  const n = Number(v);
  if (isNaN(n)) return String(v);
  const abs = Math.abs(n);
  if (abs >= 1e9) return (n / 1e9).toFixed(1) + "B";
  if (abs >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (abs >= 1e3) return (n / 1e3).toFixed(1) + "K";
  if (Number.isInteger(n)) return n.toLocaleString();
  return n.toFixed(2);
}

function fmtAxis(v) {
  const n = Number(v);
  if (isNaN(n)) return String(v);
  const abs = Math.abs(n);
  if (abs >= 1e9) return (n / 1e9).toFixed(1) + "B";
  if (abs >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (abs >= 1e4) return (n / 1e3).toFixed(0) + "K";
  if (Number.isInteger(n)) return String(n);
  return n.toFixed(abs < 1 ? 2 : 1);
}

function parseNum(v) {
  if (v === null || v === undefined || v === "") return NaN;
  return Number(v);
}

function trunc(s, max = 12) {
  s = String(s);
  return s.length > max ? s.slice(0, max - 1) + "\u2026" : s;
}

function arcPath(cx, cy, r, startAngle, endAngle) {
  const x1 = cx + r * Math.cos(startAngle);
  const y1 = cy + r * Math.sin(startAngle);
  const x2 = cx + r * Math.cos(endAngle);
  const y2 = cy + r * Math.sin(endAngle);
  const large = endAngle - startAngle > Math.PI ? 1 : 0;
  return `M ${cx} ${cy} L ${x1} ${y1} A ${r} ${r} 0 ${large} 1 ${x2} ${y2} Z`;
}

function donutArc(cx, cy, outer, inner, startAngle, endAngle) {
  const cos1 = Math.cos(startAngle), sin1 = Math.sin(startAngle);
  const cos2 = Math.cos(endAngle), sin2 = Math.sin(endAngle);
  const large = endAngle - startAngle > Math.PI ? 1 : 0;
  return [
    `M ${cx + outer * cos1} ${cy + outer * sin1}`,
    `A ${outer} ${outer} 0 ${large} 1 ${cx + outer * cos2} ${cy + outer * sin2}`,
    `L ${cx + inner * cos2} ${cy + inner * sin2}`,
    `A ${inner} ${inner} 0 ${large} 0 ${cx + inner * cos1} ${cy + inner * sin1}`,
    "Z",
  ].join(" ");
}

// ═══════════════════════════════════════════════════════════════════
// DATA ANALYSIS
// ═══════════════════════════════════════════════════════════════════

function analyzeColumns(columns, rows) {
  return columns.map((name, i) => {
    let numCount = 0, dateCount = 0, nullCount = 0;
    const sampleSize = Math.min(rows.length, 100);
    const unique = new Set();
    for (let r = 0; r < sampleSize; r++) {
      const val = rows[r][i];
      if (val === null || val === undefined || val === "") { nullCount++; continue; }
      unique.add(val);
      const str = String(val).trim();
      if (str === "true" || str === "false") continue;
      if (!isNaN(Number(str))) numCount++;
      else if (/^\d{4}-\d{2}-\d{2}/.test(str)) dateCount++;
    }
    const valid = sampleSize - nullCount;
    const isNumeric = valid > 0 && numCount / valid > 0.7;
    const isTemporal = valid > 0 && dateCount / valid > 0.7;
    return { name, index: i, isNumeric, isTemporal, isText: !isNumeric && !isTemporal, uniqueValues: unique.size };
  });
}

function detectBestChart(analysis, rowCount) {
  const numeric = analysis.filter((c) => c.isNumeric);
  const temporal = analysis.filter((c) => c.isTemporal);
  const text = analysis.filter((c) => c.isText);

  if (temporal.length >= 1 && numeric.length >= 1) {
    return { type: "line", x: temporal[0].index, y: numeric.slice(0, 4).map((c) => c.index) };
  }
  if (numeric.length >= 2 && text.length === 0) {
    return { type: "scatter", x: numeric[0].index, y: [numeric[1].index] };
  }
  if (text.length >= 1 && numeric.length >= 1) {
    if (rowCount <= 7 && numeric.length === 1) return { type: "donut", x: text[0].index, y: [numeric[0].index] };
    return { type: "bar", x: text[0].index, y: numeric.slice(0, 4).map((c) => c.index) };
  }
  if (numeric.length === 1) return { type: "bar", x: 0, y: [numeric[0].index] };
  return { type: "bar", x: 0, y: [Math.min(1, analysis.length - 1)] };
}

// ═══════════════════════════════════════════════════════════════════
// CONSTANTS
// ═══════════════════════════════════════════════════════════════════

const COLORS = [
  "#6366f1", "#22c55e", "#f59e0b", "#ef4444", "#8b5cf6",
  "#06b6d4", "#ec4899", "#14b8a6", "#f97316", "#a855f7",
];

const CHART_TYPES = [
  { id: "bar", label: "Bar" },
  { id: "line", label: "Line" },
  { id: "area", label: "Area" },
  { id: "scatter", label: "Scatter" },
  { id: "pie", label: "Pie" },
  { id: "donut", label: "Donut" },
  { id: "hbar", label: "H-Bar" },
  { id: "stacked", label: "Stacked" },
];

const PAD = { top: 28, right: 28, bottom: 52, left: 60 };
const PIE_PAD = { top: 16, bottom: 16 };

// ═══════════════════════════════════════════════════════════════════
// CHART TYPE ICONS (mini SVG)
// ═══════════════════════════════════════════════════════════════════

function ChartIcon({ type }) {
  const p = { width: 16, height: 14, viewBox: "0 0 16 14", fill: "currentColor", style: { display: "block" } };
  switch (type) {
    case "bar":
      return <svg {...p}><rect x="1" y="6" width="3" height="8" rx="0.5" /><rect x="6.5" y="2" width="3" height="12" rx="0.5" /><rect x="12" y="4" width="3" height="10" rx="0.5" /></svg>;
    case "line":
      return <svg {...p} fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"><polyline points="1,10 5,4 9,8 15,2" /></svg>;
    case "area":
      return <svg {...p}><path d="M1,10 L5,4 L9,8 L15,2 L15,14 L1,14 Z" opacity="0.3" /><polyline points="1,10 5,4 9,8 15,2" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" /></svg>;
    case "scatter":
      return <svg {...p}><circle cx="3" cy="10" r="1.5" /><circle cx="6" cy="5" r="1.5" /><circle cx="10" cy="8" r="1.5" /><circle cx="13" cy="3" r="1.5" /></svg>;
    case "pie":
      return <svg {...p} viewBox="0 0 16 16"><path d="M8,8 L8,1 A7,7 0 1,1 2,11 Z" opacity="0.7" /><path d="M8,8 L2,11 A7,7 0 0,1 8,1 Z" opacity="0.3" /></svg>;
    case "donut":
      return <svg {...p} viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="3"><path d="M8,1 A7,7 0 1,1 2,11" opacity="0.7" /><path d="M2,11 A7,7 0 0,1 8,1" opacity="0.3" /></svg>;
    case "hbar":
      return <svg {...p}><rect x="0" y="1" width="10" height="3" rx="0.5" /><rect x="0" y="5.5" width="14" height="3" rx="0.5" /><rect x="0" y="10" width="8" height="3" rx="0.5" /></svg>;
    case "stacked":
      return <svg {...p}><rect x="1" y="5" width="3" height="4" opacity="0.7" /><rect x="1" y="9" width="3" height="5" opacity="0.35" /><rect x="6.5" y="2" width="3" height="5" opacity="0.7" /><rect x="6.5" y="7" width="3" height="7" opacity="0.35" /><rect x="12" y="3" width="3" height="4" opacity="0.7" /><rect x="12" y="7" width="3" height="7" opacity="0.35" /></svg>;
    default:
      return null;
  }
}

// ═══════════════════════════════════════════════════════════════════
// EXPORT
// ═══════════════════════════════════════════════════════════════════

function exportChart(svgEl, format) {
  if (!svgEl) return;
  const serializer = new XMLSerializer();
  let svgStr = serializer.serializeToString(svgEl);

  if (format === "svg") {
    const blob = new Blob([svgStr], { type: "image/svg+xml" });
    const a = document.createElement("a");
    a.download = "chart.svg";
    a.href = URL.createObjectURL(blob);
    a.click();
    URL.revokeObjectURL(a.href);
    return;
  }

  // PNG
  const rect = svgEl.getBoundingClientRect();
  const scale = 2;
  const canvas = document.createElement("canvas");
  canvas.width = rect.width * scale;
  canvas.height = rect.height * scale;
  const ctx = canvas.getContext("2d");
  ctx.scale(scale, scale);
  const img = new Image();
  const blob = new Blob([svgStr], { type: "image/svg+xml;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  img.onload = () => {
    const bg = getComputedStyle(document.documentElement).getPropertyValue("--dp-bg").trim() || "#0c0e14";
    ctx.fillStyle = bg;
    ctx.fillRect(0, 0, rect.width, rect.height);
    ctx.drawImage(img, 0, 0, rect.width, rect.height);
    canvas.toBlob((b) => {
      const a = document.createElement("a");
      a.download = "chart.png";
      a.href = URL.createObjectURL(b);
      a.click();
    });
    URL.revokeObjectURL(url);
  };
  img.src = url;
}

// ═══════════════════════════════════════════════════════════════════
// MAIN COMPONENT
// ═══════════════════════════════════════════════════════════════════

export default function ChartPanel({ columns, rows }) {
  const [chartType, setChartType] = useState(null);
  const [xCol, setXCol] = useState(0);
  const [yCols, setYCols] = useState([1]);
  const [hoveredIndex, setHoveredIndex] = useState(null);
  const [tooltip, setTooltip] = useState(null);
  const [dims, setDims] = useState({ width: 800, height: 400 });
  const [addYOpen, setAddYOpen] = useState(false);
  const [exportOpen, setExportOpen] = useState(false);
  const containerRef = useRef(null);
  const svgRef = useRef(null);
  const addYRef = useRef(null);
  const exportRef = useRef(null);

  // Analyze columns
  const analysis = useMemo(() => analyzeColumns(columns, rows), [columns, rows]);

  // Auto-detect chart type on data change
  useEffect(() => {
    const d = detectBestChart(analysis, rows.length);
    setChartType(d.type);
    setXCol(d.x);
    setYCols(d.y);
    setHoveredIndex(null);
    setTooltip(null);
  }, [analysis, rows.length]);

  // ResizeObserver
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const { width, height } = entries[0].contentRect;
      if (width > 0 && height > 0) setDims({ width: Math.floor(width), height: Math.floor(height) });
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  // Close dropdowns on outside click
  useEffect(() => {
    if (!addYOpen && !exportOpen) return;
    const handler = (e) => {
      if (addYOpen && addYRef.current && !addYRef.current.contains(e.target)) setAddYOpen(false);
      if (exportOpen && exportRef.current && !exportRef.current.contains(e.target)) setExportOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [addYOpen, exportOpen]);

  // Prepare chart data
  const chartData = useMemo(() => {
    const labels = rows.map((r) => String(r[xCol] ?? ""));
    const series = yCols.map((ci, i) => ({
      name: columns[ci],
      color: COLORS[i % COLORS.length],
      values: rows.map((r) => { const n = parseNum(r[ci]); return isNaN(n) ? 0 : n; }),
    }));
    const allVals = series.flatMap((s) => s.values);
    const yMin = Math.min(0, ...allVals);
    const yMax = Math.max(0, ...allVals);

    // Scatter
    const scatterPoints = rows
      .map((r) => ({ x: parseNum(r[xCol]), y: parseNum(r[yCols[0]]) }))
      .filter((p) => !isNaN(p.x) && !isNaN(p.y));

    // Pie/Donut
    const slices = rows
      .map((r) => ({ label: String(r[xCol] ?? ""), value: Math.abs(parseNum(r[yCols[0]]) || 0) }))
      .filter((s) => s.value > 0);

    // Stacked: compute cumulative
    const stackedTotals = labels.map((_, i) => series.reduce((sum, s) => sum + Math.max(0, s.values[i]), 0));
    const stackMax = Math.max(0, ...stackedTotals);

    return { labels, series, yMin, yMax, scatterPoints, slices, stackMax };
  }, [rows, columns, xCol, yCols]);

  // Tooltip handlers
  const showTip = useCallback((e, label, values) => {
    const rect = containerRef.current?.getBoundingClientRect();
    if (!rect) return;
    setTooltip({ x: e.clientX - rect.left, y: e.clientY - rect.top, label, values });
  }, []);

  const hideTip = useCallback(() => {
    setTooltip(null);
    setHoveredIndex(null);
  }, []);

  // Available numeric columns not yet in yCols
  const availableYCols = analysis.filter((c) => c.isNumeric && !yCols.includes(c.index) && c.index !== xCol);

  const isPieType = chartType === "pie" || chartType === "donut";
  const isCartesian = !isPieType;

  // No data guard
  if (!columns || columns.length < 2 || rows.length === 0) {
    return <div style={st.empty}>Need at least 2 columns and 1 row to chart.</div>;
  }

  // Dimensions
  const { width: W, height: H } = dims;
  const plotW = W - PAD.left - PAD.right;
  const plotH = H - PAD.top - PAD.bottom;

  // ─── RENDERING ──────────────────────────────────────────
  function renderSVGContent() {
    if (plotW < 40 || plotH < 40) return null;

    switch (chartType) {
      case "bar": return renderBar();
      case "line": return renderLine(false);
      case "area": return renderLine(true);
      case "scatter": return renderScatter();
      case "pie": return renderPie(false);
      case "donut": return renderPie(true);
      case "hbar": return renderHBar();
      case "stacked": return renderStacked();
      default: return renderBar();
    }
  }

  // ─── BAR CHART ──────────────────────────────────────────
  function renderBar() {
    const { labels, series, yMin, yMax } = chartData;
    const scale = niceScale(yMin, yMax);
    const yRange = scale.max - scale.min || 1;
    const n = labels.length;
    const groupW = plotW / n;
    const seriesCount = series.length;
    const barW = Math.min(groupW * 0.7 / seriesCount, 48);
    const totalBarW = barW * seriesCount;
    const yPos = (v) => plotH - ((v - scale.min) / yRange) * plotH;
    const zeroY = yPos(0);
    const labelStep = Math.max(1, Math.ceil(n / 24));

    return (
      <g transform={`translate(${PAD.left},${PAD.top})`}>
        {/* Grid */}
        {scale.ticks.map((t) => (
          <g key={t}>
            <line x1={0} y1={yPos(t)} x2={plotW} y2={yPos(t)} stroke="var(--dp-border-light)" strokeDasharray="3,3" opacity={0.5} />
            <text x={-8} y={yPos(t) + 3.5} textAnchor="end" style={axLabelStyle}>{fmtAxis(t)}</text>
          </g>
        ))}
        {/* X labels */}
        {labels.map((l, i) => {
          if (i % labelStep !== 0 && i !== n - 1) return null;
          return <text key={i} x={i * groupW + groupW / 2} y={plotH + 18} textAnchor="middle" style={axLabelStyle}>{trunc(l)}</text>;
        })}
        {/* Bars */}
        {series.map((s, si) =>
          s.values.map((v, i) => {
            const x = i * groupW + (groupW - totalBarW) / 2 + si * barW;
            const barH = Math.abs(yPos(v) - zeroY);
            const y = v >= 0 ? yPos(v) : zeroY;
            const isHovered = hoveredIndex === i;
            return (
              <rect
                key={`${si}-${i}`}
                x={x} y={y} width={barW - 1} height={Math.max(barH, 1)}
                rx={Math.min(3, barW / 4)}
                fill={s.color}
                opacity={hoveredIndex !== null && !isHovered ? 0.3 : 1}
                style={{ transition: "opacity 0.15s" }}
                onMouseMove={(e) => {
                  setHoveredIndex(i);
                  showTip(e, labels[i], series.map((ss, ssi) => ({ name: ss.name, value: ss.values[i], color: ss.color })));
                }}
                onMouseLeave={hideTip}
              />
            );
          })
        )}
        {/* Zero line */}
        {scale.min < 0 && <line x1={0} y1={zeroY} x2={plotW} y2={zeroY} stroke="var(--dp-text-dim)" strokeWidth={1} opacity={0.6} />}
      </g>
    );
  }

  // ─── LINE / AREA CHART ──────────────────────────────────
  function renderLine(filled) {
    const { labels, series, yMin, yMax } = chartData;
    const scale = niceScale(yMin, yMax);
    const yRange = scale.max - scale.min || 1;
    const n = labels.length;
    const yPos = (v) => plotH - ((v - scale.min) / yRange) * plotH;
    const xPos = (i) => n === 1 ? plotW / 2 : (i / (n - 1)) * plotW;
    const labelStep = Math.max(1, Math.ceil(n / 20));

    return (
      <g transform={`translate(${PAD.left},${PAD.top})`}>
        {/* Grid */}
        {scale.ticks.map((t) => (
          <g key={t}>
            <line x1={0} y1={yPos(t)} x2={plotW} y2={yPos(t)} stroke="var(--dp-border-light)" strokeDasharray="3,3" opacity={0.5} />
            <text x={-8} y={yPos(t) + 3.5} textAnchor="end" style={axLabelStyle}>{fmtAxis(t)}</text>
          </g>
        ))}
        {/* X labels */}
        {labels.map((l, i) => {
          if (i % labelStep !== 0 && i !== n - 1) return null;
          return <text key={i} x={xPos(i)} y={plotH + 18} textAnchor="middle" style={axLabelStyle}>{trunc(l, 10)}</text>;
        })}
        {/* Area fills */}
        {filled && series.map((s, si) => {
          const pts = s.values.map((v, i) => `${xPos(i)},${yPos(v)}`).join(" ");
          const areaPath = `M ${xPos(0)},${yPos(s.values[0])} ${s.values.map((v, i) => `L ${xPos(i)},${yPos(v)}`).join(" ")} L ${xPos(n - 1)},${plotH} L ${xPos(0)},${plotH} Z`;
          return <path key={si} d={areaPath} fill={s.color} opacity={0.12} />;
        })}
        {/* Lines */}
        {series.map((s, si) => {
          const d = s.values.map((v, i) => `${i === 0 ? "M" : "L"} ${xPos(i)},${yPos(v)}`).join(" ");
          return (
            <path key={si} d={d} fill="none" stroke={s.color} strokeWidth={2}
              strokeLinecap="round" strokeLinejoin="round" />
          );
        })}
        {/* Data points */}
        {series.map((s, si) =>
          s.values.map((v, i) => (
            <circle key={`${si}-${i}`} cx={xPos(i)} cy={yPos(v)}
              r={hoveredIndex === i ? 5 : 3}
              fill={s.color} stroke="var(--dp-bg)" strokeWidth={2}
              opacity={hoveredIndex !== null && hoveredIndex !== i ? 0.3 : 1}
              style={{ transition: "r 0.15s, opacity 0.15s" }}
            />
          ))
        )}
        {/* Crosshair */}
        {hoveredIndex !== null && (
          <line x1={xPos(hoveredIndex)} y1={0} x2={xPos(hoveredIndex)} y2={plotH}
            stroke="var(--dp-text-dim)" strokeWidth={1} strokeDasharray="4,4" opacity={0.6} />
        )}
        {/* Hover overlay */}
        <rect x={0} y={0} width={plotW} height={plotH} fill="transparent" style={{ cursor: "crosshair" }}
          onMouseMove={(e) => {
            const svgRect = e.currentTarget.ownerSVGElement.getBoundingClientRect();
            const mx = e.clientX - svgRect.left - PAD.left;
            const idx = n === 1 ? 0 : Math.round(mx / (plotW / (n - 1)));
            const ci = Math.max(0, Math.min(n - 1, idx));
            setHoveredIndex(ci);
            showTip(e, labels[ci], series.map((s) => ({ name: s.name, value: s.values[ci], color: s.color })));
          }}
          onMouseLeave={hideTip}
        />
      </g>
    );
  }

  // ─── SCATTER CHART ──────────────────────────────────────
  function renderScatter() {
    const { scatterPoints } = chartData;
    if (scatterPoints.length === 0) return <text x={W / 2} y={H / 2} textAnchor="middle" style={{ fill: "var(--dp-text-dim)", fontSize: "13px" }}>No numeric data for scatter plot</text>;

    const xVals = scatterPoints.map((p) => p.x);
    const yVals = scatterPoints.map((p) => p.y);
    const xScale = niceScale(Math.min(...xVals), Math.max(...xVals));
    const yScale = niceScale(Math.min(...yVals), Math.max(...yVals));
    const xRange = xScale.max - xScale.min || 1;
    const yRange = yScale.max - yScale.min || 1;
    const xPos = (v) => ((v - xScale.min) / xRange) * plotW;
    const yPos = (v) => plotH - ((v - yScale.min) / yRange) * plotH;

    const labelStepX = Math.max(1, Math.ceil(xScale.ticks.length / 10));

    return (
      <g transform={`translate(${PAD.left},${PAD.top})`}>
        {/* Y grid */}
        {yScale.ticks.map((t) => (
          <g key={`y${t}`}>
            <line x1={0} y1={yPos(t)} x2={plotW} y2={yPos(t)} stroke="var(--dp-border-light)" strokeDasharray="3,3" opacity={0.5} />
            <text x={-8} y={yPos(t) + 3.5} textAnchor="end" style={axLabelStyle}>{fmtAxis(t)}</text>
          </g>
        ))}
        {/* X grid */}
        {xScale.ticks.map((t, i) => {
          if (i % labelStepX !== 0) return null;
          return (
            <g key={`x${t}`}>
              <line x1={xPos(t)} y1={0} x2={xPos(t)} y2={plotH} stroke="var(--dp-border-light)" strokeDasharray="3,3" opacity={0.3} />
              <text x={xPos(t)} y={plotH + 18} textAnchor="middle" style={axLabelStyle}>{fmtAxis(t)}</text>
            </g>
          );
        })}
        {/* Points */}
        {scatterPoints.map((p, i) => (
          <circle key={i} cx={xPos(p.x)} cy={yPos(p.y)}
            r={hoveredIndex === i ? 6 : 4}
            fill={COLORS[0]} opacity={hoveredIndex !== null && hoveredIndex !== i ? 0.2 : 0.7}
            stroke={COLORS[0]} strokeWidth={1}
            style={{ transition: "r 0.15s, opacity 0.15s", cursor: "pointer" }}
            onMouseMove={(e) => {
              setHoveredIndex(i);
              showTip(e, `Point ${i + 1}`, [
                { name: columns[xCol], value: p.x, color: COLORS[0] },
                { name: columns[yCols[0]], value: p.y, color: COLORS[1] },
              ]);
            }}
            onMouseLeave={hideTip}
          />
        ))}
      </g>
    );
  }

  // ─── PIE / DONUT CHART ──────────────────────────────────
  function renderPie(isDonut) {
    const { slices } = chartData;
    if (slices.length === 0) return <text x={W / 2} y={H / 2} textAnchor="middle" style={{ fill: "var(--dp-text-dim)", fontSize: "13px" }}>No data for chart</text>;

    const total = slices.reduce((s, d) => s + d.value, 0) || 1;
    const cx = W / 2;
    const cy = (H - 30) / 2;
    const radius = Math.min(cx - 40, cy - 20);
    const innerR = isDonut ? radius * 0.55 : 0;
    let angle = -Math.PI / 2;

    const arcs = slices.map((s, i) => {
      const sliceAngle = (s.value / total) * Math.PI * 2;
      const start = angle;
      angle += sliceAngle;
      const end = angle;
      const midAngle = start + sliceAngle / 2;
      return { ...s, start, end, midAngle, pct: ((s.value / total) * 100).toFixed(1), color: COLORS[i % COLORS.length], index: i };
    });

    return (
      <g>
        {arcs.map((a) => {
          const isHovered = hoveredIndex === a.index;
          const dx = isHovered ? Math.cos(a.midAngle) * 6 : 0;
          const dy = isHovered ? Math.sin(a.midAngle) * 6 : 0;
          const path = isDonut
            ? donutArc(cx + dx, cy + dy, radius, innerR, a.start, a.end)
            : arcPath(cx + dx, cy + dy, radius, a.start, a.end);
          return (
            <path key={a.index} d={path} fill={a.color}
              opacity={hoveredIndex !== null && !isHovered ? 0.35 : 1}
              style={{ transition: "opacity 0.15s", cursor: "pointer" }}
              onMouseMove={(e) => {
                setHoveredIndex(a.index);
                showTip(e, a.label, [{ name: `${a.pct}%`, value: a.value, color: a.color }]);
              }}
              onMouseLeave={hideTip}
            />
          );
        })}
        {/* Labels on large slices */}
        {arcs.map((a) => {
          if (a.end - a.start < 0.35) return null;
          const lr = isDonut ? (radius + innerR) / 2 : radius * 0.6;
          const lx = cx + lr * Math.cos(a.midAngle);
          const ly = cy + lr * Math.sin(a.midAngle);
          return (
            <text key={`l${a.index}`} x={lx} y={ly} textAnchor="middle" dominantBaseline="central"
              style={{ fill: "#fff", fontSize: "11px", fontWeight: 600, fontFamily: "var(--dp-font-mono)", pointerEvents: "none", textShadow: "0 1px 2px rgba(0,0,0,0.5)" }}>
              {a.pct}%
            </text>
          );
        })}
        {/* Center label for donut */}
        {isDonut && (
          <g>
            <text x={cx} y={cy - 6} textAnchor="middle" style={{ fill: "var(--dp-text)", fontSize: "18px", fontWeight: 700, fontFamily: "var(--dp-font-mono)" }}>
              {fmtNum(total)}
            </text>
            <text x={cx} y={cy + 12} textAnchor="middle" style={{ fill: "var(--dp-text-dim)", fontSize: "10px", textTransform: "uppercase", letterSpacing: "0.5px" }}>
              Total
            </text>
          </g>
        )}
        {/* Legend */}
        {renderPieLegend(arcs)}
      </g>
    );
  }

  function renderPieLegend(arcs) {
    const lx = 12;
    const ly = H - 22;
    const maxItems = Math.floor((W - 24) / 100);
    const items = arcs.slice(0, maxItems);
    let cx = lx;
    return (
      <g>
        {items.map((a) => {
          const x = cx;
          const textW = Math.min(trunc(a.label, 10).length * 6.5 + 20, 100);
          cx += textW;
          return (
            <g key={a.index}>
              <rect x={x} y={ly} width={8} height={8} rx={2} fill={a.color} />
              <text x={x + 12} y={ly + 8} style={{ fill: "var(--dp-text-secondary)", fontSize: "10px", fontFamily: "var(--dp-font-mono)" }}>
                {trunc(a.label, 10)}
              </text>
            </g>
          );
        })}
      </g>
    );
  }

  // ─── HORIZONTAL BAR CHART ──────────────────────────────
  function renderHBar() {
    const { labels, series, yMin, yMax } = chartData;
    const n = labels.length;
    const barH = Math.min((plotH / n) * 0.7, 32);
    const groupH = plotH / n;

    // For hbar, x-axis is the value axis
    const allVals = series.flatMap((s) => s.values);
    const valMin = Math.min(0, ...allVals);
    const valMax = Math.max(0, ...allVals);
    const scale = niceScale(valMin, valMax);
    const valRange = scale.max - scale.min || 1;
    const xPos = (v) => ((v - scale.min) / valRange) * plotW;
    const zeroX = xPos(0);

    const hPad = { ...PAD, left: 100 };
    const hPlotW = W - hPad.left - hPad.right;
    const xPosH = (v) => ((v - scale.min) / valRange) * hPlotW;
    const zeroXH = xPosH(0);

    return (
      <g transform={`translate(${hPad.left},${PAD.top})`}>
        {/* X grid (value axis) */}
        {scale.ticks.map((t) => (
          <g key={t}>
            <line x1={xPosH(t)} y1={0} x2={xPosH(t)} y2={plotH} stroke="var(--dp-border-light)" strokeDasharray="3,3" opacity={0.5} />
            <text x={xPosH(t)} y={plotH + 18} textAnchor="middle" style={axLabelStyle}>{fmtAxis(t)}</text>
          </g>
        ))}
        {/* Y labels (categories) */}
        {labels.map((l, i) => (
          <text key={i} x={-8} y={i * groupH + groupH / 2 + 4} textAnchor="end" style={axLabelStyle}>{trunc(l, 14)}</text>
        ))}
        {/* Bars */}
        {series.map((s, si) =>
          s.values.map((v, i) => {
            const y = i * groupH + (groupH - barH * series.length) / 2 + si * barH;
            const barW = Math.abs(xPosH(v) - zeroXH);
            const x = v >= 0 ? zeroXH : xPosH(v);
            const isHovered = hoveredIndex === i;
            return (
              <rect key={`${si}-${i}`}
                x={x} y={y} width={Math.max(barW, 1)} height={barH - 1}
                rx={Math.min(3, barH / 4)}
                fill={s.color}
                opacity={hoveredIndex !== null && !isHovered ? 0.3 : 1}
                style={{ transition: "opacity 0.15s", cursor: "pointer" }}
                onMouseMove={(e) => {
                  setHoveredIndex(i);
                  showTip(e, labels[i], series.map((ss) => ({ name: ss.name, value: ss.values[i], color: ss.color })));
                }}
                onMouseLeave={hideTip}
              />
            );
          })
        )}
        {/* Zero line */}
        {scale.min < 0 && <line x1={zeroXH} y1={0} x2={zeroXH} y2={plotH} stroke="var(--dp-text-dim)" strokeWidth={1} opacity={0.6} />}
      </g>
    );
  }

  // ─── STACKED BAR CHART ──────────────────────────────────
  function renderStacked() {
    const { labels, series, stackMax } = chartData;
    const scale = niceScale(0, stackMax);
    const yRange = scale.max - scale.min || 1;
    const n = labels.length;
    const groupW = plotW / n;
    const barW = Math.min(groupW * 0.7, 48);
    const yPos = (v) => plotH - ((v - scale.min) / yRange) * plotH;
    const labelStep = Math.max(1, Math.ceil(n / 24));

    return (
      <g transform={`translate(${PAD.left},${PAD.top})`}>
        {/* Grid */}
        {scale.ticks.map((t) => (
          <g key={t}>
            <line x1={0} y1={yPos(t)} x2={plotW} y2={yPos(t)} stroke="var(--dp-border-light)" strokeDasharray="3,3" opacity={0.5} />
            <text x={-8} y={yPos(t) + 3.5} textAnchor="end" style={axLabelStyle}>{fmtAxis(t)}</text>
          </g>
        ))}
        {/* X labels */}
        {labels.map((l, i) => {
          if (i % labelStep !== 0 && i !== n - 1) return null;
          return <text key={i} x={i * groupW + groupW / 2} y={plotH + 18} textAnchor="middle" style={axLabelStyle}>{trunc(l)}</text>;
        })}
        {/* Stacked segments */}
        {labels.map((_, i) => {
          let cumVal = 0;
          return series.map((s, si) => {
            const val = Math.max(0, s.values[i]);
            const y0 = yPos(cumVal);
            cumVal += val;
            const y1 = yPos(cumVal);
            const h = Math.max(y0 - y1, 0);
            const x = i * groupW + (groupW - barW) / 2;
            const isHovered = hoveredIndex === i;
            return (
              <rect key={`${si}-${i}`}
                x={x} y={y1} width={barW} height={Math.max(h, val > 0 ? 1 : 0)}
                fill={s.color}
                rx={si === series.length - 1 ? Math.min(3, barW / 4) : 0}
                opacity={hoveredIndex !== null && !isHovered ? 0.3 : 1}
                style={{ transition: "opacity 0.15s", cursor: "pointer" }}
                onMouseMove={(e) => {
                  setHoveredIndex(i);
                  const total = series.reduce((sum, ss) => sum + Math.max(0, ss.values[i]), 0);
                  showTip(e, labels[i], [
                    ...series.map((ss) => ({ name: ss.name, value: Math.max(0, ss.values[i]), color: ss.color })),
                    { name: "Total", value: total, color: "var(--dp-text-secondary)" },
                  ]);
                }}
                onMouseLeave={hideTip}
              />
            );
          });
        })}
      </g>
    );
  }

  // ─── RENDER ─────────────────────────────────────────────
  return (
    <div style={st.container}>
      {/* Toolbar */}
      <div style={st.toolbar}>
        {/* Chart type selector */}
        <div style={st.typeGroup}>
          {CHART_TYPES.map((t) => (
            <button key={t.id} onClick={() => setChartType(t.id)} title={t.label}
              style={chartType === t.id ? st.typeBtnActive : st.typeBtn}>
              <ChartIcon type={t.id} />
            </button>
          ))}
        </div>

        <div style={st.sep} />

        {/* X axis */}
        {!isPieType && (
          <div style={st.axisGroup}>
            <span style={st.axisLabel}>X</span>
            <select value={xCol} onChange={(e) => setXCol(parseInt(e.target.value))} style={st.select}>
              {columns.map((c, i) => <option key={i} value={i}>{c}</option>)}
            </select>
          </div>
        )}

        {/* Y axis / Value */}
        <div style={st.axisGroup}>
          <span style={st.axisLabel}>{isPieType ? "Label" : "Y"}</span>
          {isPieType ? (
            <select value={xCol} onChange={(e) => setXCol(parseInt(e.target.value))} style={st.select}>
              {columns.map((c, i) => <option key={i} value={i}>{c}</option>)}
            </select>
          ) : (
            <>
              {yCols.map((ci) => (
                <span key={ci} style={st.yTag}>
                  <span style={{ ...st.yTagDot, background: COLORS[yCols.indexOf(ci) % COLORS.length] }} />
                  {columns[ci]}
                  {yCols.length > 1 && (
                    <button onClick={() => setYCols(yCols.filter((c) => c !== ci))} style={st.yTagClose}>&times;</button>
                  )}
                </span>
              ))}
              {availableYCols.length > 0 && (
                <div ref={addYRef} style={{ position: "relative" }}>
                  <button onClick={() => setAddYOpen(!addYOpen)} style={st.addBtn}>+</button>
                  {addYOpen && (
                    <div style={st.dropdown}>
                      {availableYCols.map((c) => (
                        <button key={c.index} style={st.dropdownItem}
                          onClick={() => { setYCols([...yCols, c.index]); setAddYOpen(false); }}
                          onMouseEnter={(e) => e.currentTarget.style.background = "var(--dp-btn-bg)"}
                          onMouseLeave={(e) => e.currentTarget.style.background = "none"}>
                          {c.name}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </>
          )}
        </div>

        {isPieType && (
          <div style={st.axisGroup}>
            <span style={st.axisLabel}>Value</span>
            <select value={yCols[0]} onChange={(e) => setYCols([parseInt(e.target.value)])} style={st.select}>
              {columns.map((c, i) => <option key={i} value={i}>{c}</option>)}
            </select>
          </div>
        )}

        {/* Export */}
        <div ref={exportRef} style={{ position: "relative", marginLeft: "auto" }}>
          <button onClick={() => setExportOpen(!exportOpen)} style={st.exportBtn}>
            <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
              <path d="M7 2v8M4 7l3 3 3-3M2 12h10" />
            </svg>
          </button>
          {exportOpen && (
            <div style={{ ...st.dropdown, right: 0, left: "auto" }}>
              <button style={st.dropdownItem} onClick={() => { exportChart(svgRef.current, "png"); setExportOpen(false); }}
                onMouseEnter={(e) => e.currentTarget.style.background = "var(--dp-btn-bg)"}
                onMouseLeave={(e) => e.currentTarget.style.background = "none"}>
                Download PNG
              </button>
              <button style={st.dropdownItem} onClick={() => { exportChart(svgRef.current, "svg"); setExportOpen(false); }}
                onMouseEnter={(e) => e.currentTarget.style.background = "var(--dp-btn-bg)"}
                onMouseLeave={(e) => e.currentTarget.style.background = "none"}>
                Download SVG
              </button>
            </div>
          )}
        </div>
      </div>

      {/* Chart area */}
      <div ref={containerRef} style={st.chartArea}>
        <svg ref={svgRef} width={W} height={H} style={{ display: "block" }}
          xmlns="http://www.w3.org/2000/svg">
          {renderSVGContent()}
        </svg>

        {/* Tooltip */}
        {tooltip && (
          <div style={{
            ...st.tooltip,
            left: tooltip.x > W * 0.65 ? undefined : tooltip.x + 14,
            right: tooltip.x > W * 0.65 ? W - tooltip.x + 14 : undefined,
            top: tooltip.y < 60 ? tooltip.y + 20 : tooltip.y - 12,
            transform: tooltip.y < 60 ? "none" : "translateY(-100%)",
          }}>
            <div style={st.tipLabel}>{trunc(String(tooltip.label), 28)}</div>
            {tooltip.values.map((v, i) => (
              <div key={i} style={st.tipRow}>
                <span style={{ ...st.tipDot, background: v.color }} />
                <span style={st.tipName}>{v.name}</span>
                <span style={st.tipVal}>{fmtNum(v.value)}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Legend for multi-series cartesian charts */}
      {isCartesian && yCols.length > 1 && (
        <div style={st.legend}>
          {chartData.series.map((s, i) => (
            <div key={i} style={st.legendItem}>
              <span style={{ ...st.legendDot, background: s.color }} />
              <span style={st.legendName}>{s.name}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════
// SHARED SVG STYLES
// ═══════════════════════════════════════════════════════════════════

const axLabelStyle = {
  fill: "var(--dp-text-dim)",
  fontSize: "10px",
  fontFamily: "var(--dp-font-mono)",
};

// ═══════════════════════════════════════════════════════════════════
// COMPONENT STYLES
// ═══════════════════════════════════════════════════════════════════

const st = {
  container: {
    display: "flex",
    flexDirection: "column",
    height: "100%",
    overflow: "hidden",
  },
  empty: {
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    height: "100%",
    color: "var(--dp-text-dim)",
    fontSize: "13px",
  },

  // ─── Toolbar ──────────────────────────────────
  toolbar: {
    display: "flex",
    alignItems: "center",
    gap: "8px",
    padding: "6px 10px",
    borderBottom: "1px solid var(--dp-border)",
    background: "var(--dp-bg-secondary)",
    flexShrink: 0,
    flexWrap: "wrap",
    minHeight: "36px",
  },
  typeGroup: {
    display: "flex",
    gap: "1px",
    background: "var(--dp-border)",
    borderRadius: "var(--dp-radius-lg)",
    overflow: "hidden",
  },
  typeBtn: {
    padding: "5px 8px",
    background: "var(--dp-btn-bg)",
    border: "none",
    color: "var(--dp-text-dim)",
    cursor: "pointer",
    display: "flex",
    alignItems: "center",
  },
  typeBtnActive: {
    padding: "5px 8px",
    background: "var(--dp-bg)",
    border: "none",
    color: "var(--dp-accent)",
    cursor: "pointer",
    display: "flex",
    alignItems: "center",
  },
  sep: {
    width: "1px",
    height: "20px",
    background: "var(--dp-border)",
    flexShrink: 0,
  },
  axisGroup: {
    display: "flex",
    alignItems: "center",
    gap: "4px",
  },
  axisLabel: {
    fontSize: "10px",
    fontWeight: 600,
    color: "var(--dp-text-dim)",
    textTransform: "uppercase",
    letterSpacing: "0.5px",
    flexShrink: 0,
  },
  select: {
    padding: "3px 6px",
    background: "var(--dp-bg-tertiary)",
    border: "1px solid var(--dp-border-light)",
    borderRadius: "var(--dp-radius)",
    color: "var(--dp-text)",
    fontSize: "11px",
    fontFamily: "var(--dp-font-mono)",
    maxWidth: "140px",
  },
  yTag: {
    display: "inline-flex",
    alignItems: "center",
    gap: "4px",
    padding: "2px 8px 2px 4px",
    background: "var(--dp-bg-tertiary)",
    border: "1px solid var(--dp-border-light)",
    borderRadius: "var(--dp-radius)",
    fontSize: "11px",
    fontFamily: "var(--dp-font-mono)",
    color: "var(--dp-text)",
  },
  yTagDot: {
    width: "6px",
    height: "6px",
    borderRadius: "50%",
    flexShrink: 0,
  },
  yTagClose: {
    background: "none",
    border: "none",
    color: "var(--dp-text-dim)",
    cursor: "pointer",
    fontSize: "14px",
    lineHeight: 1,
    padding: "0 0 0 2px",
  },
  addBtn: {
    padding: "2px 8px",
    background: "var(--dp-btn-bg)",
    border: "1px solid var(--dp-border-light)",
    borderRadius: "var(--dp-radius)",
    color: "var(--dp-text-secondary)",
    cursor: "pointer",
    fontSize: "12px",
    fontWeight: 600,
  },
  exportBtn: {
    padding: "4px 8px",
    background: "var(--dp-btn-bg)",
    border: "1px solid var(--dp-btn-border)",
    borderRadius: "var(--dp-radius)",
    color: "var(--dp-text-secondary)",
    cursor: "pointer",
    display: "flex",
    alignItems: "center",
  },
  dropdown: {
    position: "absolute",
    top: "100%",
    left: 0,
    marginTop: "4px",
    background: "var(--dp-bg-secondary)",
    border: "1px solid var(--dp-border)",
    borderRadius: "var(--dp-radius)",
    zIndex: 100,
    minWidth: "120px",
    boxShadow: "0 4px 16px rgba(0,0,0,0.3)",
    overflow: "hidden",
  },
  dropdownItem: {
    display: "block",
    width: "100%",
    padding: "6px 12px",
    background: "none",
    border: "none",
    borderBottom: "1px solid var(--dp-border)",
    color: "var(--dp-text)",
    cursor: "pointer",
    fontSize: "12px",
    fontFamily: "var(--dp-font-mono)",
    textAlign: "left",
    whiteSpace: "nowrap",
  },

  // ─── Chart Area ───────────────────────────────
  chartArea: {
    flex: 1,
    position: "relative",
    overflow: "hidden",
    minHeight: 0,
  },

  // ─── Tooltip ──────────────────────────────────
  tooltip: {
    position: "absolute",
    pointerEvents: "none",
    zIndex: 50,
    background: "var(--dp-bg-secondary)",
    border: "1px solid var(--dp-border)",
    borderRadius: "var(--dp-radius-lg)",
    padding: "8px 12px",
    boxShadow: "0 6px 20px rgba(0,0,0,0.35)",
    maxWidth: "220px",
  },
  tipLabel: {
    fontSize: "11px",
    fontWeight: 600,
    color: "var(--dp-text)",
    marginBottom: "4px",
    fontFamily: "var(--dp-font-mono)",
  },
  tipRow: {
    display: "flex",
    alignItems: "center",
    gap: "6px",
    fontSize: "11px",
    lineHeight: "18px",
  },
  tipDot: {
    width: "7px",
    height: "7px",
    borderRadius: "2px",
    flexShrink: 0,
  },
  tipName: {
    color: "var(--dp-text-secondary)",
  },
  tipVal: {
    fontWeight: 600,
    color: "var(--dp-text)",
    fontFamily: "var(--dp-font-mono)",
    marginLeft: "auto",
  },

  // ─── Legend ───────────────────────────────────
  legend: {
    display: "flex",
    alignItems: "center",
    gap: "16px",
    padding: "6px 16px",
    borderTop: "1px solid var(--dp-border)",
    flexShrink: 0,
    flexWrap: "wrap",
    background: "var(--dp-bg-secondary)",
  },
  legendItem: {
    display: "flex",
    alignItems: "center",
    gap: "6px",
    fontSize: "11px",
  },
  legendDot: {
    width: "8px",
    height: "8px",
    borderRadius: "2px",
    flexShrink: 0,
  },
  legendName: {
    color: "var(--dp-text-secondary)",
    fontFamily: "var(--dp-font-mono)",
  },
};
