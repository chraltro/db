import React, { useState } from "react";

function FileNode({ node, depth, onSelect, activeFile }) {
  const [expanded, setExpanded] = useState(depth < 2);
  const isActive = activeFile === node.path;

  if (node.type === "dir") {
    return (
      <div>
        <div
          data-dp-file=""
          style={{ ...styles.item, paddingLeft: 8 + depth * 16 }}
          onClick={() => setExpanded(!expanded)}
        >
          <span style={{ ...styles.icon, transform: expanded ? "rotate(0deg)" : "rotate(-90deg)" }}>
            {"\u25BE"}
          </span>
          <span style={styles.dirName}>{node.name}</span>
        </div>
        {expanded &&
          node.children?.map((child) => (
            <FileNode
              key={child.path}
              node={child}
              depth={depth + 1}
              onSelect={onSelect}
              activeFile={activeFile}
            />
          ))}
      </div>
    );
  }

  const ext = node.name.split(".").pop();
  const iconColor = ext === "sql" ? "var(--dp-accent)" : ext === "py" ? "var(--dp-green)" : "var(--dp-text-secondary)";

  return (
    <div
      data-dp-file=""
      style={{
        ...styles.item,
        paddingLeft: 8 + depth * 16,
        background: isActive ? "var(--dp-bg-secondary)" : "transparent",
        borderLeft: isActive ? "2px solid var(--dp-accent)" : "2px solid transparent",
      }}
      onClick={() => onSelect(node.path)}
    >
      <span style={{ ...styles.dot, background: iconColor }} />
      <span style={isActive ? styles.activeFileName : styles.fileName}>{node.name}</span>
    </div>
  );
}

export default function FileTree({ files, onSelect, activeFile }) {
  return (
    <div>
      <div style={styles.header}>FILES</div>
      {files.length === 0 && (
        <div style={styles.empty}>No files found</div>
      )}
      {files.map((f) => (
        <FileNode key={f.path} node={f} depth={0} onSelect={onSelect} activeFile={activeFile} />
      ))}
    </div>
  );
}

const styles = {
  header: { padding: "6px 12px 8px", fontSize: "10px", fontWeight: "600", color: "var(--dp-text-dim)", letterSpacing: "1px", textTransform: "uppercase" },
  item: { display: "flex", alignItems: "center", gap: "6px", padding: "4px 8px", cursor: "pointer", fontSize: "13px", whiteSpace: "nowrap", margin: "0 4px", borderRadius: "3px" },
  icon: { fontSize: "10px", color: "var(--dp-text-secondary)", width: "10px", display: "inline-block", transition: "transform 0.12s ease" },
  dirName: { color: "var(--dp-text)", fontWeight: 500 },
  fileName: { color: "var(--dp-text)" },
  activeFileName: { color: "var(--dp-accent)", fontWeight: 500 },
  dot: { width: "6px", height: "6px", borderRadius: "50%", flexShrink: 0 },
  empty: { padding: "12px", color: "var(--dp-text-dim)", fontSize: "12px", textAlign: "center" },
};
