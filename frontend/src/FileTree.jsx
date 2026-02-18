import React, { useState } from "react";

function FileNode({ node, depth, onSelect, activeFile, onNewFile, onDeleteFile }) {
  const [expanded, setExpanded] = useState(depth < 2);
  const [creating, setCreating] = useState(false);
  const [newName, setNewName] = useState("");
  const [hovered, setHovered] = useState(false);
  const isActive = activeFile === node.path;

  if (node.type === "dir") {
    return (
      <div>
        <div
          data-dp-file=""
          style={{ ...styles.item, paddingLeft: 8 + depth * 16 }}
          onClick={() => setExpanded(!expanded)}
          onMouseEnter={() => setHovered(true)}
          onMouseLeave={() => setHovered(false)}
        >
          <span style={{ ...styles.icon, transform: expanded ? "rotate(0deg)" : "rotate(-90deg)" }}>
            {"\u25BE"}
          </span>
          <span style={styles.dirName}>{node.name}</span>
          {hovered && (
            <button
              onClick={(e) => {
                e.stopPropagation();
                setExpanded(true);
                setCreating(true);
                setNewName("");
              }}
              style={styles.addBtn}
              title={`New file in ${node.name}/`}
            >+</button>
          )}
        </div>
        {expanded && creating && (
          <div style={{ ...styles.newFileRow, paddingLeft: 24 + depth * 16 }}>
            <input
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
              placeholder="filename.sql"
              style={styles.newFileInput}
              autoFocus
              onKeyDown={(e) => {
                if (e.key === "Enter" && newName.trim()) {
                  onNewFile(`${node.path}/${newName.trim()}`);
                  setCreating(false);
                  setNewName("");
                }
                if (e.key === "Escape") { setCreating(false); setNewName(""); }
              }}
              onBlur={() => { setCreating(false); setNewName(""); }}
            />
          </div>
        )}
        {expanded &&
          node.children?.map((child) => (
            <FileNode
              key={child.path}
              node={child}
              depth={depth + 1}
              onSelect={onSelect}
              activeFile={activeFile}
              onNewFile={onNewFile}
              onDeleteFile={onDeleteFile}
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
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
    >
      <span style={{ ...styles.dot, background: iconColor }} />
      <span style={isActive ? styles.activeFileName : styles.fileName}>{node.name}</span>
      {hovered && onDeleteFile && (
        <button
          onClick={(e) => { e.stopPropagation(); onDeleteFile(node.path); }}
          style={styles.deleteBtn}
          title={`Delete ${node.name}`}
        >&times;</button>
      )}
    </div>
  );
}

export default function FileTree({ files, onSelect, activeFile, onNewFile, onDeleteFile, onRefresh }) {
  return (
    <div>
      <div style={styles.header}>
        <span>FILES</span>
        {onRefresh && (
          <button onClick={onRefresh} style={styles.refreshBtn} title="Refresh files &amp; tables">&#x21BB;</button>
        )}
      </div>
      {files.length === 0 && (
        <div style={styles.empty}>No files found</div>
      )}
      {files.map((f) => (
        <FileNode key={f.path} node={f} depth={0} onSelect={onSelect} activeFile={activeFile} onNewFile={onNewFile} onDeleteFile={onDeleteFile} />
      ))}
    </div>
  );
}

const styles = {
  header: { display: "flex", alignItems: "center", justifyContent: "space-between", padding: "6px 12px 8px", fontSize: "10px", fontWeight: "600", color: "var(--dp-text-dim)", letterSpacing: "1px", textTransform: "uppercase" },
  refreshBtn: { background: "none", border: "none", color: "var(--dp-text-secondary)", cursor: "pointer", fontSize: "13px", padding: "0 2px", lineHeight: 1 },
  item: { display: "flex", alignItems: "center", gap: "6px", padding: "4px 8px", cursor: "pointer", fontSize: "13px", whiteSpace: "nowrap", margin: "0 4px", borderRadius: "3px" },
  icon: { fontSize: "10px", color: "var(--dp-text-secondary)", width: "10px", display: "inline-block", transition: "transform 0.12s ease" },
  dirName: { color: "var(--dp-text)", fontWeight: 500 },
  addBtn: { marginLeft: "auto", width: "18px", height: "18px", background: "none", border: "none", color: "var(--dp-text-secondary)", cursor: "pointer", fontSize: "14px", lineHeight: "18px", textAlign: "center", padding: 0, flexShrink: 0 },
  newFileRow: { display: "flex", padding: "2px 8px 4px", margin: "0 4px" },
  newFileInput: { flex: 1, padding: "3px 6px", background: "var(--dp-bg)", border: "1px solid var(--dp-border-light)", borderRadius: "var(--dp-radius)", color: "var(--dp-text)", fontSize: "11px", fontFamily: "var(--dp-font-mono)", outline: "none" },
  fileName: { color: "var(--dp-text)" },
  activeFileName: { color: "var(--dp-accent)", fontWeight: 500 },
  dot: { width: "6px", height: "6px", borderRadius: "50%", flexShrink: 0 },
  deleteBtn: { marginLeft: "auto", width: "18px", height: "18px", background: "none", border: "none", color: "var(--dp-text-dim)", cursor: "pointer", fontSize: "14px", lineHeight: "18px", textAlign: "center", padding: 0, flexShrink: 0 },
  empty: { padding: "12px", color: "var(--dp-text-dim)", fontSize: "12px", textAlign: "center" },
};
