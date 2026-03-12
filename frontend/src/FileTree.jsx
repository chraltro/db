import React, { useState } from "react";

function FileNode({ node, depth, onSelect, activeFile, onNewFile, onDeleteFile, onMoveFile }) {
  const [expanded, setExpanded] = useState(depth < 2);
  const [creating, setCreating] = useState(false);
  const [newName, setNewName] = useState("");
  const [hovered, setHovered] = useState(false);
  const [dragOver, setDragOver] = useState(false);
  const isActive = activeFile === node.path;

  if (node.type === "dir") {
    return (
      <div>
        <div
          data-havn-file=""
          style={{
            ...styles.item,
            paddingLeft: 8 + depth * 16,
            background: dragOver ? "color-mix(in srgb, var(--havn-accent) 12%, transparent)" : "transparent",
          }}
          onClick={() => setExpanded(!expanded)}
          onMouseEnter={() => setHovered(true)}
          onMouseLeave={() => setHovered(false)}
          onDragOver={(e) => { e.preventDefault(); e.stopPropagation(); setDragOver(true); }}
          onDragLeave={(e) => { e.stopPropagation(); setDragOver(false); }}
          onDrop={(e) => {
            e.preventDefault();
            e.stopPropagation();
            setDragOver(false);
            const srcPath = e.dataTransfer.getData("text/plain");
            if (srcPath && onMoveFile) {
              const fileName = srcPath.split("/").pop();
              const dest = `${node.path}/${fileName}`;
              if (dest !== srcPath) onMoveFile(srcPath, dest);
            }
          }}
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
              onMoveFile={onMoveFile}
            />
          ))}
      </div>
    );
  }

  const ext = node.name.split(".").pop();
  const iconColor = ext === "sql" ? "var(--havn-accent)" : ext === "py" ? "var(--havn-green)" : "var(--havn-text-secondary)";

  return (
    <div
      data-havn-file=""
      draggable
      onDragStart={(e) => {
        e.dataTransfer.setData("text/plain", node.path);
        e.dataTransfer.effectAllowed = "move";
      }}
      style={{
        ...styles.item,
        paddingLeft: 8 + depth * 16,
        background: isActive ? "var(--havn-bg-secondary)" : "transparent",
        borderLeft: isActive ? "2px solid var(--havn-accent)" : "2px solid transparent",
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

export default function FileTree({ files, onSelect, activeFile, onNewFile, onDeleteFile, onMoveFile }) {
  return (
    <div>
      {files.length === 0 && (
        <div style={styles.empty}>No files found</div>
      )}
      {files.map((f) => (
        <FileNode key={f.path} node={f} depth={0} onSelect={onSelect} activeFile={activeFile} onNewFile={onNewFile} onDeleteFile={onDeleteFile} onMoveFile={onMoveFile} />
      ))}
    </div>
  );
}

const styles = {
  item: { display: "flex", alignItems: "center", gap: "6px", padding: "4px 8px", cursor: "pointer", fontSize: "13px", whiteSpace: "nowrap", margin: "0 4px", borderRadius: "3px" },
  icon: { fontSize: "10px", color: "var(--havn-text-secondary)", width: "10px", display: "inline-block", transition: "transform 0.12s ease" },
  dirName: { color: "var(--havn-text)", fontWeight: 500, fontFamily: "var(--havn-font-mono)" },
  addBtn: { marginLeft: "auto", width: "18px", height: "18px", background: "none", border: "none", color: "var(--havn-text-secondary)", cursor: "pointer", fontSize: "14px", lineHeight: "18px", textAlign: "center", padding: 0, flexShrink: 0 },
  newFileRow: { display: "flex", padding: "2px 8px 4px", margin: "0 4px" },
  newFileInput: { flex: 1, padding: "3px 6px", background: "var(--havn-bg)", border: "1px solid var(--havn-border-light)", borderRadius: "var(--havn-radius)", color: "var(--havn-text)", fontSize: "11px", fontFamily: "var(--havn-font-mono)", outline: "none" },
  fileName: { color: "var(--havn-text)", fontFamily: "var(--havn-font-mono)", fontSize: "12px" },
  activeFileName: { color: "var(--havn-accent)", fontWeight: 500, fontFamily: "var(--havn-font-mono)", fontSize: "12px" },
  dot: { width: "6px", height: "6px", borderRadius: "50%", flexShrink: 0 },
  deleteBtn: { marginLeft: "auto", width: "18px", height: "18px", background: "none", border: "none", color: "var(--havn-text-dim)", cursor: "pointer", fontSize: "14px", lineHeight: "18px", textAlign: "center", padding: 0, flexShrink: 0 },
  empty: { padding: "12px", color: "var(--havn-text-dim)", fontSize: "12px", textAlign: "center" },
};
