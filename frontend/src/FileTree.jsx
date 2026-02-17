import React, { useState } from "react";

function FileNode({ node, depth, onSelect, activeFile }) {
  const [expanded, setExpanded] = useState(depth < 2);
  const isActive = activeFile === node.path;

  if (node.type === "dir") {
    return (
      <div>
        <div
          style={{ ...styles.item, paddingLeft: 8 + depth * 16 }}
          onClick={() => setExpanded(!expanded)}
        >
          <span style={styles.icon}>{expanded ? "\u25BE" : "\u25B8"}</span>
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
      style={{
        ...styles.item,
        paddingLeft: 8 + depth * 16,
        background: isActive ? "var(--dp-bg-secondary)" : "transparent",
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
      {files.map((f) => (
        <FileNode key={f.path} node={f} depth={0} onSelect={onSelect} activeFile={activeFile} />
      ))}
    </div>
  );
}

const styles = {
  header: { padding: "4px 12px 8px", fontSize: "11px", fontWeight: "600", color: "var(--dp-text-secondary)", letterSpacing: "0.5px" },
  item: { display: "flex", alignItems: "center", gap: "6px", padding: "3px 8px", cursor: "pointer", fontSize: "13px", whiteSpace: "nowrap" },
  icon: { fontSize: "10px", color: "var(--dp-text-secondary)", width: "10px" },
  dirName: { color: "var(--dp-text)" },
  fileName: { color: "var(--dp-text)" },
  activeFileName: { color: "var(--dp-accent)" },
  dot: { width: "6px", height: "6px", borderRadius: "50%", flexShrink: 0 },
};
