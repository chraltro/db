import React, { useState, useMemo } from "react";

function compareValues(a, b) {
  if (a === null && b === null) return 0;
  if (a === null) return 1;
  if (b === null) return -1;
  const numA = Number(a);
  const numB = Number(b);
  if (!isNaN(numA) && !isNaN(numB)) return numA - numB;
  return String(a).localeCompare(String(b));
}

const CLR = {
  int: "var(--dp-accent)",
  float: "var(--dp-accent)",
  text: "var(--dp-green)",
  bool: "var(--dp-purple)",
  temporal: "var(--dp-yellow)",
  other: "var(--dp-text-dim)",
};

function typeDisplay(dbType) {
  const t = dbType.toUpperCase().trim();
  // Integers
  if (/^(BIGINT|INT8)$/.test(t))                          return { label: "123", color: CLR.int };
  if (/^(INTEGER|INT4|INT|SIGNED)$/.test(t))               return { label: "123", color: CLR.int };
  if (/^(SMALLINT|INT2|SHORT)$/.test(t))                   return { label: "123", color: CLR.int };
  if (/^(TINYINT|INT1)$/.test(t))                          return { label: "123", color: CLR.int };
  if (/^(UBIGINT)$/.test(t))                               return { label: "123", color: CLR.int };
  if (/^(UINTEGER|UINT)$/.test(t))                         return { label: "123", color: CLR.int };
  if (/^(USMALLINT)$/.test(t))                             return { label: "123", color: CLR.int };
  if (/^(UTINYINT)$/.test(t))                              return { label: "123", color: CLR.int };
  if (/^(HUGEINT|UHUGEINT)$/.test(t))                     return { label: "123", color: CLR.int };
  // Floats / decimals
  if (/^(FLOAT|FLOAT4|REAL)$/.test(t))                     return { label: "1.2", color: CLR.float };
  if (/^(DOUBLE|FLOAT8)$/.test(t))                         return { label: "1.2", color: CLR.float };
  if (/^DECIMAL|^NUMERIC/.test(t))                         return { label: "1.2", color: CLR.float };
  // Boolean
  if (/^BOOL(EAN)?$/.test(t))                              return { label: "T/F", color: CLR.bool };
  // Text
  if (/^VARCHAR/.test(t))                                  return { label: "VARCHAR", color: CLR.text };
  if (/^TEXT$/.test(t))                                    return { label: "TEXT", color: CLR.text };
  if (/^CHAR/.test(t))                                     return { label: "CHAR", color: CLR.text };
  if (/^STRING$/.test(t))                                  return { label: "STRING", color: CLR.text };
  if (/^BLOB|^BYTEA$/.test(t))                             return { label: "BLOB", color: CLR.text };
  if (/^UUID$/.test(t))                                    return { label: "UUID", color: CLR.text };
  if (/^ENUM/.test(t))                                     return { label: "ENUM", color: CLR.text };
  // Temporal — each gets its own specific label
  if (/^TIMESTAMP\s*WITH\s*TIME\s*ZONE|^TIMESTAMPTZ/.test(t)) return { label: "TIMESTAMPTZ", color: CLR.temporal };
  if (/^TIMESTAMP/.test(t))                                return { label: "TIMESTAMP", color: CLR.temporal };
  if (/^DATETIME$/.test(t))                                return { label: "DATETIME", color: CLR.temporal };
  if (/^DATE$/.test(t))                                    return { label: "DATE", color: CLR.temporal };
  if (/^TIME\s*WITH\s*TIME\s*ZONE|^TIMETZ/.test(t))       return { label: "TIMETZ", color: CLR.temporal };
  if (/^TIME$/.test(t))                                    return { label: "TIME", color: CLR.temporal };
  if (/^INTERVAL/.test(t))                                 return { label: "INTERVAL", color: CLR.temporal };
  // Structured
  if (/^JSON$/.test(t))                                    return { label: "JSON", color: CLR.other };
  if (/^STRUCT|^MAP/.test(t))                              return { label: "{ }", color: CLR.other };
  if (/^LIST|^ARRAY|\[\]/.test(t))                         return { label: "[ ]", color: CLR.other };
  if (/^UNION/.test(t))                                    return { label: "UNION", color: CLR.other };
  // Fallback — show the raw type
  return { label: t.length > 10 ? t.slice(0, 10) : t, color: CLR.other };
}

function inferTypeDisplay(rows, colIndex) {
  for (let i = 0; i < Math.min(rows.length, 20); i++) {
    const v = rows[i][colIndex];
    if (v === null || v === undefined) continue;
    if (typeof v === "boolean" || v === "true" || v === "false")
      return { label: "T/F", color: CLR.bool };
    const s = String(v).trim();
    if (s === "") continue;
    if (!isNaN(Number(s))) {
      return s.includes(".") ? { label: "1.2", color: CLR.float } : { label: "123", color: CLR.int };
    }
    if (/^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}/.test(s))
      return { label: "TIMESTAMP", color: CLR.temporal };
    if (/^\d{4}-\d{2}-\d{2}$/.test(s))
      return { label: "DATE", color: CLR.temporal };
    if (/^\d{2}:\d{2}(:\d{2})?/.test(s))
      return { label: "TIME", color: CLR.temporal };
    return { label: "VARCHAR", color: CLR.text };
  }
  return { label: "VARCHAR", color: CLR.text };
}

export default function SortableTable({ columns, rows, columnTypes }) {
  const [sortCol, setSortCol] = useState(null);
  const [sortDir, setSortDir] = useState("asc");

  const resolvedTypes = useMemo(() => {
    return columns.map((_, i) => {
      if (columnTypes && columnTypes[i]) return typeDisplay(columnTypes[i]);
      return inferTypeDisplay(rows, i);
    });
  }, [columns, rows, columnTypes]);

  function handleSort(colIndex) {
    if (sortCol === colIndex) {
      if (sortDir === "asc") setSortDir("desc");
      else { setSortCol(null); setSortDir("asc"); }
    } else {
      setSortCol(colIndex);
      setSortDir("asc");
    }
  }

  const sortedRows = useMemo(() => {
    if (sortCol === null) return rows;
    const sorted = [...rows].sort((a, b) => compareValues(a[sortCol], b[sortCol]));
    return sortDir === "desc" ? sorted.reverse() : sorted;
  }, [rows, sortCol, sortDir]);

  return (
    <table style={styles.table}>
      <thead>
        <tr>
          {columns.map((col, i) => {
            const sym = resolvedTypes[i];
            const isActive = sortCol === i;
            return (
              <th key={col} style={styles.th} onClick={() => handleSort(i)}>
                <span style={styles.thInner}>
                  <span>{col}</span>
                  <span style={{ ...styles.typeSymbol, color: sym.color }}>{sym.label}</span>
                  <span style={{ ...styles.sortIcon, color: isActive ? "var(--dp-accent)" : "var(--dp-text-dim)" }}>
                    {isActive ? (sortDir === "asc" ? "\u25B4" : "\u25BE") : "\u25B4\u25BE"}
                  </span>
                </span>
              </th>
            );
          })}
        </tr>
      </thead>
      <tbody>
        {sortedRows.map((row, i) => (
          <tr key={i}>
            {row.map((val, j) => (
              <td key={j} style={styles.td}>
                {val === null ? <span style={styles.null}>NULL</span> : String(val)}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

const styles = {
  table: { width: "100%", borderCollapse: "collapse", fontSize: "12px", fontFamily: "var(--dp-font-mono)" },
  th: { textAlign: "left", padding: "6px 12px", borderBottom: "2px solid var(--dp-border-light)", color: "var(--dp-text-secondary)", fontWeight: 600, position: "sticky", top: 0, background: "var(--dp-bg)", cursor: "pointer", userSelect: "none" },
  thInner: { display: "flex", alignItems: "center", gap: "6px", width: "100%" },
  typeSymbol: { fontSize: "9px", fontWeight: 500, opacity: 0.8 },
  sortIcon: { fontSize: "8px", lineHeight: 1, marginLeft: "auto", flexShrink: 0 },
  td: { padding: "4px 12px", borderBottom: "1px solid var(--dp-border)", color: "var(--dp-text)" },
  null: { color: "var(--dp-text-dim)", fontStyle: "italic", fontSize: "11px" },
};
