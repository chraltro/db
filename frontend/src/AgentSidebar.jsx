import React, { useState, useRef, useEffect, useCallback } from "react";

const AGENTS = [
  {
    id: "claude", name: "Claude Code", install: "npm install -g @anthropic-ai/claude-code",
    models: [
      { id: "", label: "Default" },
      { id: "claude-sonnet-4-5-20250514", label: "Sonnet 4.5" },
      { id: "claude-opus-4-6", label: "Opus 4.6" },
      { id: "claude-haiku-4-5-20251001", label: "Haiku 4.5" },
    ],
  },
  {
    id: "codex", name: "Codex", install: "npm install -g @openai/codex",
    models: [
      { id: "", label: "Default" },
      { id: "gpt-5", label: "GPT-5" },
      { id: "o3", label: "o3" },
      { id: "o4-mini", label: "o4-mini" },
    ],
  },
  {
    id: "gemini", name: "Gemini CLI", install: "npm install -g @google/gemini-cli",
    models: [
      { id: "gemini-3-flash-preview", label: "3 Flash" },
      { id: "gemini-3-pro-preview", label: "3 Pro" },
      { id: "", label: "Auto (Gemini 3)" },
      { id: "gemini-2.5-flash", label: "2.5 Flash" },
      { id: "gemini-2.5-pro", label: "2.5 Pro" },
    ],
  },
];

function timestamp() {
  const d = new Date();
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

/** Lightweight markdown rendering: headings, **bold**, `code`, ```blocks```, tables, links, and - lists. */
function renderMarkdown(text, actions) {
  if (!text) return null;
  const parts = [];
  const fenced = text.split(/(```[\s\S]*?```)/g);
  for (const segment of fenced) {
    if (segment.startsWith("```") && segment.endsWith("```")) {
      const inner = segment.slice(3, -3).replace(/^[^\n]*\n/, "");
      parts.push(<code key={parts.length} style={st.codeBlock}>{inner}</code>);
    } else {
      const lines = segment.split("\n");
      // Detect table blocks (consecutive lines starting with |)
      let li = 0;
      while (li < lines.length) {
        // Collect table rows
        if (/^\s*\|/.test(lines[li])) {
          const tableLines = [];
          while (li < lines.length && /^\s*\|/.test(lines[li])) {
            tableLines.push(lines[li]);
            li++;
          }
          parts.push(renderTable(tableLines, parts.length));
          continue;
        }
        if (li > 0 && parts.length > 0 && !React.isValidElement(parts[parts.length - 1])) {
          parts.push(<br key={`br-${parts.length}`} />);
        } else if (li > 0) {
          parts.push(<br key={`br-${parts.length}`} />);
        }
        const line = lines[li];
        const headingMatch = line.match(/^(#{1,6})\s+(.+)$/);
        if (headingMatch) {
          const level = headingMatch[1].length;
          parts.push(
            <span key={parts.length} style={st.heading(level)}>
              {renderInline(headingMatch[2], parts.length, actions)}
            </span>
          );
        } else if (/^(\s*[-*]\s)/.test(line)) {
          parts.push(<span key={parts.length} style={st.listItem}>{renderInline(line, parts.length, actions)}</span>);
        } else {
          parts.push(<span key={parts.length}>{renderInline(line, parts.length, actions)}</span>);
        }
        li++;
      }
    }
  }
  return parts;
}

/** Render a markdown table from lines like | col1 | col2 | */
function renderTable(lines, keyBase) {
  if (lines.length === 0) return null;
  const parseRow = (line) =>
    line.replace(/^\s*\|/, "").replace(/\|\s*$/, "").split("|").map((c) => c.trim());
  const headerCells = parseRow(lines[0]);
  // Skip separator row (e.g., |---|---|)
  const startIdx = lines.length > 1 && /^[\s|:-]+$/.test(lines[1]) ? 2 : 1;
  const bodyRows = lines.slice(startIdx).map(parseRow);
  return (
    <table key={keyBase} style={st.table}>
      <thead>
        <tr>
          {headerCells.map((cell, i) => (
            <th key={i} style={st.th}>{cell}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {bodyRows.map((row, ri) => (
          <tr key={ri}>
            {row.map((cell, ci) => (
              <td key={ci} style={st.td}>{cell}</td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

// Known havn schemas for detecting table references like gold.earthquake_summary
const _HAVN_SCHEMAS = new Set(["landing", "bronze", "silver", "gold"]);
// File extensions to detect as linkable paths
const _FILE_EXT_RE = /\.(?:sql|py|yml|yaml|json|csv|parquet|md|txt|toml|cfg|env|dpnb)$/;

function renderInline(text, keyBase, actions) {
  // First pass: handle **bold** and `code` (with linkable code detection)
  const tokens = [];
  const re = /(\*\*(.+?)\*\*|`([^`]+)`)/g;
  let last = 0;
  let m;
  while ((m = re.exec(text)) !== null) {
    if (m.index > last) tokens.push({ type: "plain", text: text.slice(last, m.index) });
    if (m[2]) {
      tokens.push({ type: "bold", text: m[2] });
    } else if (m[3]) {
      tokens.push({ type: "code", text: m[3] });
    }
    last = re.lastIndex;
  }
  if (last < text.length) tokens.push({ type: "plain", text: text.slice(last) });

  // Second pass: within plain text, detect file paths and table references
  const parts = [];
  let ki = 0;
  for (const token of tokens) {
    if (token.type === "bold") {
      parts.push(<strong key={`${keyBase}-b-${ki++}`} style={{ fontWeight: 600 }}>{token.text}</strong>);
    } else if (token.type === "code") {
      const link = _tryLink(token.text, keyBase, ki, actions);
      if (link) { parts.push(link); ki++; }
      else { parts.push(<code key={`${keyBase}-c-${ki++}`} style={st.inlineCode}>{token.text}</code>); }
    } else {
      // Scan plain text for bare file paths and table refs
      const linkRe = /((?:[\w./-]+\/)+[\w.-]+\.(?:sql|py|yml|yaml|json|csv|parquet|md|txt|toml|cfg|env|dpnb))|\b((?:landing|bronze|silver|gold)\.([\w]+))\b/g;
      let pLast = 0;
      let pm;
      while ((pm = linkRe.exec(token.text)) !== null) {
        if (pm.index > pLast) parts.push(token.text.slice(pLast, pm.index));
        if (pm[1] && actions?.onOpenFile) {
          const p = pm[1];
          parts.push(
            <span key={`${keyBase}-f-${ki++}`} style={st.fileLink} onClick={() => actions.onOpenFile(p)}>{p}</span>
          );
        } else if (pm[2] && actions?.onSelectTable) {
          const schema = pm[2].split(".")[0];
          const table = pm[3];
          const ref = pm[2];
          parts.push(
            <span key={`${keyBase}-t-${ki++}`} style={st.tableLink} onClick={() => actions.onSelectTable(schema, table)}>{ref}</span>
          );
        } else {
          parts.push(pm[0]);
        }
        pLast = linkRe.lastIndex;
      }
      if (pLast < token.text.length) parts.push(token.text.slice(pLast));
    }
  }
  return parts;
}

/** Check if a code-backtick string is a linkable file path or table ref. */
function _tryLink(code, keyBase, index, actions) {
  // File path inside backticks
  if (_FILE_EXT_RE.test(code) && code.includes("/") && actions?.onOpenFile) {
    return (
      <code key={`${keyBase}-fc-${index}`} style={{ ...st.inlineCode, ...st.fileLinkCode }} onClick={() => actions.onOpenFile(code)}>
        {code}
      </code>
    );
  }
  // Table reference inside backticks (schema.table)
  const tableMatch = code.match(/^(landing|bronze|silver|gold)\.([\w]+)$/);
  if (tableMatch && actions?.onSelectTable) {
    return (
      <code key={`${keyBase}-tc-${index}`} style={{ ...st.inlineCode, ...st.tableLinkCode }} onClick={() => actions.onSelectTable(tableMatch[1], tableMatch[2])}>
        {code}
      </code>
    );
  }
  return null;
}

/** Expandable tool use row */
function ToolEntry({ tool }) {
  const [expanded, setExpanded] = useState(false);
  const hasDetail = tool.detail || tool.toolInput;
  return (
    <div style={{ display: "inline-flex", flexDirection: "column", gap: "2px" }}>
      <span
        style={{ ...st.toolChip, cursor: hasDetail ? "pointer" : "default" }}
        onClick={() => hasDetail && setExpanded(!expanded)}
        title={hasDetail ? "Click to expand" : tool.name}
      >
        {tool.name}
        {tool.detail && <span style={st.toolDetailInline}>{tool.detail}</span>}
        {hasDetail && <span style={st.toolExpandIcon}>{expanded ? "\u25B4" : "\u25BE"}</span>}
      </span>
      {expanded && tool.toolInput && (
        <div style={st.toolInputBlock}>
          {tool.toolInput.old_string != null && (
            <div style={st.diffBlock}>
              <div style={st.diffRemoved}>{tool.toolInput.old_string}</div>
              <div style={st.diffAdded}>{tool.toolInput.new_string}</div>
            </div>
          )}
          {tool.toolInput.content != null && tool.name === "Write" && (
            <div style={st.diffAdded}>{tool.toolInput.content.length > 500 ? tool.toolInput.content.slice(0, 500) + "\n..." : tool.toolInput.content}</div>
          )}
          {tool.toolInput.file_path && (
            <div style={st.toolFilePath}>{tool.toolInput.file_path}</div>
          )}
        </div>
      )}
    </div>
  );
}

export default function AgentSidebar({ isOpen, onToggle, onFileChanged, onOpenFile, onSelectTable }) {
  const [selectedAgent, setSelectedAgent] = useState("claude");
  const [availableAgents, setAvailableAgents] = useState(null);
  // Per-agent state: { messages, isConnected, isStreaming, permissionMode, selectedModel }
  const [agentStates, setAgentStates] = useState(() => {
    const init = {};
    for (const a of AGENTS) init[a.id] = { messages: [], isConnected: false, isStreaming: false, permissionMode: "auto", selectedModel: "" };
    return init;
  });
  const [input, setInput] = useState("");
  // Per-agent WebSocket refs
  const socketsRef = useRef({});
  const reconnectsRef = useRef({});
  const intentionalCloseRef = useRef({});
  const messagesEndRef = useRef(null);
  const textareaRef = useRef(null);
  const isOpenRef = useRef(isOpen);
  const onFileChangedRef = useRef(onFileChanged);
  isOpenRef.current = isOpen;
  onFileChangedRef.current = onFileChanged;

  // Helpers to update per-agent state
  const updateAgent = useCallback((agentId, updater) => {
    setAgentStates((prev) => ({ ...prev, [agentId]: typeof updater === "function" ? updater(prev[agentId]) : { ...prev[agentId], ...updater } }));
  }, []);

  // Current agent shorthand
  const cur = agentStates[selectedAgent] || { messages: [], isConnected: false, isStreaming: false, permissionMode: "auto", selectedModel: "" };

  useEffect(() => {
    fetch("/api/agents")
      .then((r) => r.json())
      .then(setAvailableAgents)
      .catch(() => {});
  }, []);

  const connectAgent = useCallback((agentId) => {
    // Close existing connection for this agent
    if (socketsRef.current[agentId]) {
      intentionalCloseRef.current[agentId] = true;
      socketsRef.current[agentId].close();
    }

    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${protocol}//${window.location.host}/ws/agent`);
    socketsRef.current[agentId] = ws;

    ws.onopen = () => {
      const st = agentStates[agentId] || {};
      ws.send(JSON.stringify({ type: "start", agent: agentId, model: st.selectedModel || "" }));
    };

    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);

      if (data.type === "ready") {
        updateAgent(agentId, (s) => ({
          ...s,
          isConnected: true,
          messages: [...s.messages, { role: "system", content: `${AGENTS.find((a) => a.id === data.agent)?.name || data.agent} connected`, ts: timestamp() }],
        }));
      } else if (data.type === "mode_changed") {
        updateAgent(agentId, { permissionMode: data.mode });
      } else if (data.type === "model_changed") {
        updateAgent(agentId, { selectedModel: data.model });
      } else if (data.type === "chunk") {
        if (data.chunk_type === "tool_use" && /^(Edit|Write)$/.test(data.content) && onFileChangedRef.current) {
          setTimeout(() => onFileChangedRef.current?.(), 500);
        }
        updateAgent(agentId, (s) => {
          const prev = s.messages;
          const last = prev[prev.length - 1];
          let newMessages;

          if (data.chunk_type === "tool_use") {
            const updated = last && last.streaming ? [...prev.slice(0, -1), { ...last, streaming: false }] : [...prev];
            newMessages = [...updated, { role: "assistant", content: data.content, detail: data.detail || "", toolInput: data.tool_input || null, chunkType: "tool_use", streaming: false, ts: timestamp() }];
          } else if (last && last.role === "assistant" && last.streaming && last.chunkType === "text") {
            newMessages = [...prev.slice(0, -1), { ...last, content: last.content + data.content }];
          } else {
            const updated = last && last.streaming ? [...prev.slice(0, -1), { ...last, streaming: false }] : [...prev];
            newMessages = [...updated, { role: "assistant", content: data.content, chunkType: data.chunk_type || "text", streaming: true, ts: timestamp() }];
          }
          return { ...s, messages: newMessages };
        });
      } else if (data.type === "done") {
        updateAgent(agentId, (s) => {
          const last = s.messages[s.messages.length - 1];
          const messages = last && last.streaming ? [...s.messages.slice(0, -1), { ...last, streaming: false }] : s.messages;
          return { ...s, isStreaming: false, messages };
        });
      } else if (data.type === "error") {
        updateAgent(agentId, (s) => ({
          ...s,
          isStreaming: false,
          messages: [...s.messages, { role: "error", content: data.message, ts: timestamp() }],
        }));
      }
    };

    ws.onclose = () => {
      updateAgent(agentId, { isConnected: false });
      if (intentionalCloseRef.current[agentId]) {
        intentionalCloseRef.current[agentId] = false;
        return;
      }
      reconnectsRef.current[agentId] = setTimeout(() => {
        if (isOpenRef.current) connectAgent(agentId);
      }, 3000);
    };

    ws.onerror = () => {
      updateAgent(agentId, { isConnected: false });
    };
  }, [agentStates, updateAgent]);

  // Connect all available agents when sidebar opens
  useEffect(() => {
    if (isOpen) {
      for (const a of AGENTS) {
        if (!socketsRef.current[a.id] || socketsRef.current[a.id].readyState > WebSocket.OPEN) {
          connectAgent(a.id);
        }
      }
    } else {
      for (const a of AGENTS) {
        clearTimeout(reconnectsRef.current[a.id]);
        if (socketsRef.current[a.id]) {
          intentionalCloseRef.current[a.id] = true;
          socketsRef.current[a.id].close();
          socketsRef.current[a.id] = null;
        }
      }
    }
    return () => {
      for (const a of AGENTS) {
        clearTimeout(reconnectsRef.current[a.id]);
        if (socketsRef.current[a.id]) {
          intentionalCloseRef.current[a.id] = true;
          socketsRef.current[a.id].close();
          socketsRef.current[a.id] = null;
        }
      }
    };
  }, [isOpen, connectAgent]);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [cur.messages]);

  const sendMessage = () => {
    if (!input.trim() || !cur.isConnected || cur.isStreaming) return;
    const ws = socketsRef.current[selectedAgent];
    if (!ws || ws.readyState !== WebSocket.OPEN) return;

    updateAgent(selectedAgent, (s) => ({
      ...s,
      messages: [...s.messages, { role: "user", content: input, ts: timestamp() }],
      isStreaming: true,
    }));
    ws.send(JSON.stringify({ type: "message", message: input }));
    setInput("");
    setTimeout(() => textareaRef.current?.focus(), 0);
  };

  const switchAgent = (agentId) => {
    if (agentId === selectedAgent) return;
    setSelectedAgent(agentId);
  };

  const toggleMode = () => {
    const newMode = cur.permissionMode === "auto" ? "ask" : "auto";
    updateAgent(selectedAgent, { permissionMode: newMode });
    const ws = socketsRef.current[selectedAgent];
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: "set_mode", mode: newMode }));
    }
  };

  const changeModel = (modelId) => {
    updateAgent(selectedAgent, { selectedModel: modelId });
    const ws = socketsRef.current[selectedAgent];
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: "set_model", model: modelId }));
    }
  };

  if (!isOpen) return null;

  const agentAvailability = {};
  if (availableAgents) {
    for (const a of availableAgents) agentAvailability[a.id] = a.available;
  }

  const { messages, isConnected, isStreaming, permissionMode, selectedModel } = cur;

  // Group consecutive tool_use messages into clusters
  const groupedMessages = [];
  for (const msg of messages) {
    if (msg.role === "assistant" && msg.chunkType === "tool_use") {
      const prev = groupedMessages[groupedMessages.length - 1];
      if (prev && prev._type === "tool_group") {
        prev.tools.push({ name: msg.content, detail: msg.detail, toolInput: msg.toolInput });
        prev.ts = msg.ts;
      } else {
        groupedMessages.push({
          _type: "tool_group",
          tools: [{ name: msg.content, detail: msg.detail, toolInput: msg.toolInput }],
          ts: msg.ts,
        });
      }
    } else {
      groupedMessages.push(msg);
    }
  }

  return (
    <div style={st.sidebar} data-havn-agent-sidebar="">
      <style>{`@keyframes pulse { 0%, 100% { opacity: 0.3; } 50% { opacity: 1; } }`}</style>
      {/* Header */}
      <div style={st.header}>
        <span style={st.headerTitle}>AGENT</span>
        <span style={st.headerStatus}>
          <span
            style={{
              ...st.statusDot,
              background: isConnected ? "var(--havn-green)" : "var(--havn-text-dim)",
            }}
          />
          {isConnected ? "Connected" : "Connecting\u2026"}
        </span>
        <div style={{ display: "flex", gap: "2px", alignItems: "center", marginLeft: "auto" }}>
          {messages.length > 0 && (
            <button onClick={() => updateAgent(selectedAgent, { messages: [] })} style={st.clearBtn}>Clear</button>
          )}
          <button onClick={onToggle} style={st.closeBtn} title="Close agent sidebar">{"\u00D7"}</button>
        </div>
      </div>

      {/* Agent picker */}
      <div style={st.picker}>
        {AGENTS.map((agent) => {
          const available = agentAvailability[agent.id] !== false;
          const isActive = selectedAgent === agent.id;
          return (
            <button
              key={agent.id}
              data-havn-tab=""
              data-havn-active={isActive ? "true" : "false"}
              style={{
                ...st.agentTab,
                ...(isActive ? st.agentTabActive : {}),
                opacity: available ? 1 : 0.35,
              }}
              onClick={() => available && switchAgent(agent.id)}
              title={available ? agent.name : `${agent.name} (not installed)`}
              disabled={!available}
            >
              {agent.name}
            </button>
          );
        })}
      </div>

      {/* Message log */}
      <div style={st.log}>
        {messages.length === 0 && (() => {
          const noneAvailable = availableAgents && availableAgents.every((a) => !a.available);
          const currentAgent = AGENTS.find((a) => a.id === selectedAgent);
          const currentAvailable = agentAvailability[selectedAgent] !== false;
          if (noneAvailable) {
            return (
              <div style={st.emptyState}>
                <div style={st.emptyTitle}>No agents installed</div>
                <div style={st.emptyBody}>
                  Install a coding agent CLI to get started:
                </div>
                {AGENTS.map((a) => (
                  <div key={a.id} style={st.installHint}>
                    <span style={st.installName}>{a.name}</span>
                    <code style={st.installCmd}>{a.install}</code>
                  </div>
                ))}
                <div style={st.emptyBody}>Then restart the havn server.</div>
              </div>
            );
          }
          if (!currentAvailable && currentAgent) {
            return (
              <div style={st.emptyState}>
                <div style={st.emptyTitle}>{currentAgent.name} not installed</div>
                <div style={st.installHint}>
                  <code style={st.installCmd}>{currentAgent.install}</code>
                </div>
                <div style={st.emptyBody}>Then restart the havn server.</div>
              </div>
            );
          }
          return <div style={st.placeholder}>Ask the agent to edit your project.</div>;
        })()}
        {groupedMessages.map((msg, i) => {
          // Tool use cluster
          if (msg._type === "tool_group") {
            return (
              <div key={i} style={st.toolGroup}>
                <span style={{ ...st.indicator, background: "var(--havn-purple)" }} />
                <span style={st.toolList}>
                  {msg.tools.map((t, j) => (
                    <ToolEntry key={j} tool={t} />
                  ))}
                </span>
              </div>
            );
          }

          // User message
          if (msg.role === "user") {
            return (
              <div key={i} style={st.userEntry}>
                <div style={st.entryHeader}>
                  <span style={st.ts}>{msg.ts}</span>
                </div>
                <div style={st.userText}>{msg.content}</div>
              </div>
            );
          }

          // Assistant text
          if (msg.role === "assistant" && msg.chunkType !== "tool_use") {
            return (
              <div key={i} style={st.assistantEntry}>
                <div style={st.assistantBody}>
                  {renderMarkdown(msg.content, { onOpenFile, onSelectTable })}
                  {msg.streaming ? <span style={st.cursor}>{"\u2588"}</span> : null}
                </div>
              </div>
            );
          }

          // System
          if (msg.role === "system") {
            return (
              <div key={i} style={st.entry}>
                <span style={st.ts}>{msg.ts}</span>
                <span style={{ ...st.indicator, background: "var(--havn-text-dim)" }} />
                <span style={st.systemText}>{msg.content}</span>
              </div>
            );
          }

          // Error
          if (msg.role === "error") {
            return (
              <div key={i} style={st.entry}>
                <span style={st.ts}>{msg.ts}</span>
                <span style={{ ...st.indicator, background: "var(--havn-red)" }} />
                <span style={st.errorText}>{msg.content}</span>
              </div>
            );
          }

          return null;
        })}
        {isStreaming && (() => {
          const last = groupedMessages[groupedMessages.length - 1];
          const showSpinner = !last || last.role !== "assistant" || last.chunkType === "tool_use" || last._type === "tool_group";
          return showSpinner ? (
            <div style={st.loadingEntry}>
              <span style={st.loadingDots} />
              <span style={st.loadingText}>Working...</span>
            </div>
          ) : null;
        })()}
        <div ref={messagesEndRef} />
      </div>

      {/* Input + mode toggle */}
      <div style={st.inputArea}>
        <textarea
          ref={textareaRef}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              sendMessage();
            }
          }}
          placeholder={isConnected ? "Ask the agent\u2026" : "Connecting\u2026"}
          disabled={!isConnected || isStreaming}
          style={st.textarea}
          rows={3}
        />
        <div style={st.inputActions}>
          <button
            onClick={toggleMode}
            style={{
              ...st.modeBtn,
              ...(permissionMode === "auto" ? st.modeBtnAuto : st.modeBtnAsk),
            }}
            title={
              permissionMode === "auto"
                ? "Auto mode: agent can read and write files"
                : "Ask mode: agent can only read files"
            }
          >
            <span style={st.modeDot(permissionMode === "auto")} />
            {permissionMode === "auto" ? "Auto" : "Ask"}
          </button>
          {(() => {
            const agent = AGENTS.find((a) => a.id === selectedAgent);
            if (!agent?.models || agent.models.length <= 1) return null;
            return (
              <select
                value={selectedModel}
                onChange={(e) => changeModel(e.target.value)}
                style={st.modelSelect}
                title="Model"
              >
                {agent.models.map((m) => (
                  <option key={m.id} value={m.id}>{m.label}</option>
                ))}
              </select>
            );
          })()}
          <button
            onClick={sendMessage}
            disabled={!isConnected || isStreaming || !input.trim()}
            style={{
              ...st.sendBtn,
              opacity: !isConnected || isStreaming || !input.trim() ? 0.35 : 1,
            }}
          >
            {isStreaming ? "\u2026" : "Send"}
          </button>
        </div>
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Styles                                                               */
/* ------------------------------------------------------------------ */

const st = {
  sidebar: {
    display: "flex",
    flexDirection: "column",
    height: "100%",
    background: "var(--havn-bg-tertiary)",
    overflow: "hidden",
  },

  header: {
    display: "flex",
    alignItems: "center",
    gap: "8px",
    padding: "4px 12px",
    fontSize: "12px",
    fontWeight: 600,
    color: "var(--havn-text-secondary)",
    borderBottom: "1px solid var(--havn-border)",
    flexShrink: 0,
  },
  headerTitle: {
    fontSize: "11px",
    textTransform: "uppercase",
    letterSpacing: "0.5px",
  },
  headerStatus: {
    display: "flex",
    alignItems: "center",
    gap: "4px",
    fontSize: "10px",
    color: "var(--havn-text-dim)",
    fontWeight: 400,
  },
  statusDot: {
    width: "5px",
    height: "5px",
    borderRadius: "50%",
    flexShrink: 0,
  },
  clearBtn: {
    background: "none",
    border: "none",
    color: "var(--havn-text-secondary)",
    cursor: "pointer",
    fontSize: "11px",
    padding: "2px 4px",
  },
  closeBtn: {
    background: "none",
    border: "none",
    color: "var(--havn-text-secondary)",
    cursor: "pointer",
    fontSize: "16px",
    lineHeight: 1,
    padding: "0 2px",
  },

  picker: {
    display: "flex",
    alignItems: "center",
    borderBottom: "1px solid var(--havn-border)",
    padding: "0 8px",
    flexShrink: 0,
  },
  agentTab: {
    padding: "6px 12px",
    background: "none",
    border: "none",
    borderBottom: "2px solid transparent",
    color: "var(--havn-text-secondary)",
    cursor: "pointer",
    fontSize: "12px",
    whiteSpace: "nowrap",
    fontWeight: 500,
    fontFamily: "var(--havn-font)",
  },
  agentTabActive: {
    borderBottom: "2px solid var(--havn-accent)",
    color: "var(--havn-text)",
    fontWeight: 600,
  },

  log: {
    flex: 1,
    overflow: "auto",
    padding: "8px 12px",
    fontSize: "12px",
    fontFamily: "var(--havn-font-mono)",
    lineHeight: 1.5,
    minHeight: 0,
  },
  placeholder: {
    color: "var(--havn-text-dim)",
    fontStyle: "italic",
    padding: "8px 0",
    fontSize: "12px",
  },
  emptyState: {
    padding: "16px 0",
    display: "flex",
    flexDirection: "column",
    gap: "8px",
  },
  emptyTitle: {
    fontSize: "13px",
    fontWeight: 600,
    color: "var(--havn-text)",
  },
  emptyBody: {
    fontSize: "11px",
    color: "var(--havn-text-secondary)",
    lineHeight: 1.5,
  },
  installHint: {
    display: "flex",
    flexDirection: "column",
    gap: "2px",
    padding: "4px 0",
  },
  installName: {
    fontSize: "11px",
    fontWeight: 600,
    color: "var(--havn-text-secondary)",
  },
  installCmd: {
    fontSize: "11px",
    fontFamily: "var(--havn-font-mono)",
    color: "var(--havn-accent)",
    background: "var(--havn-bg)",
    padding: "3px 6px",
    borderRadius: "3px",
    border: "1px solid var(--havn-border)",
    userSelect: "all",
  },

  entry: {
    display: "flex",
    gap: "8px",
    alignItems: "baseline",
    marginBottom: "2px",
    fontFamily: "var(--havn-font-mono)",
  },
  ts: {
    color: "var(--havn-text-dim)",
    flexShrink: 0,
    fontSize: "10px",
    fontFamily: "var(--havn-font-mono)",
  },
  indicator: {
    width: "4px",
    height: "4px",
    borderRadius: "50%",
    flexShrink: 0,
    marginTop: "2px",
  },

  /* User message */
  userEntry: {
    margin: "8px 0 4px",
    borderLeft: "2px solid var(--havn-accent)",
    paddingLeft: "10px",
  },
  entryHeader: {
    display: "flex",
    gap: "6px",
    alignItems: "center",
    marginBottom: "2px",
  },
  userText: {
    color: "var(--havn-accent)",
    fontWeight: 500,
    wordBreak: "break-word",
    fontSize: "12px",
    fontFamily: "var(--havn-font-mono)",
  },

  /* Assistant text */
  assistantEntry: {
    margin: "8px 0 4px",
    padding: "6px 10px",
    background: "var(--havn-bg-secondary)",
    borderLeft: "2px solid var(--havn-purple)",
  },
  assistantBody: {
    color: "var(--havn-text)",
    wordBreak: "break-word",
    fontSize: "12px",
    fontFamily: "var(--havn-font-mono)",
    lineHeight: 1.5,
  },
  cursor: {
    color: "var(--havn-accent)",
  },

  /* Tool use cluster */
  toolGroup: {
    display: "flex",
    gap: "6px",
    alignItems: "flex-start",
    margin: "3px 0",
    paddingLeft: "12px",
  },
  toolList: {
    display: "flex",
    gap: "4px",
    flexWrap: "wrap",
  },
  toolChip: {
    display: "inline-flex",
    alignItems: "center",
    gap: "3px",
    padding: "1px 6px",
    background: "color-mix(in srgb, var(--havn-purple) 12%, transparent)",
    border: "1px solid color-mix(in srgb, var(--havn-purple) 25%, transparent)",
    borderRadius: "3px",
    color: "var(--havn-purple)",
    fontSize: "10px",
    fontFamily: "var(--havn-font-mono)",
    whiteSpace: "nowrap",
    flexWrap: "wrap",
  },
  toolExpandIcon: {
    fontSize: "8px",
    opacity: 0.6,
  },
  toolDetailInline: {
    color: "var(--havn-text-secondary)",
    fontSize: "10px",
    marginLeft: "2px",
  },
  toolInputBlock: {
    padding: "4px 6px",
    background: "var(--havn-bg)",
    border: "1px solid var(--havn-border)",
    borderRadius: "3px",
    fontSize: "10px",
    fontFamily: "var(--havn-font-mono)",
    maxHeight: "200px",
    overflow: "auto",
    whiteSpace: "pre-wrap",
    wordBreak: "break-all",
  },
  diffBlock: {
    display: "flex",
    flexDirection: "column",
    gap: "2px",
  },
  diffRemoved: {
    padding: "2px 4px",
    background: "color-mix(in srgb, var(--havn-red) 12%, transparent)",
    borderLeft: "2px solid var(--havn-red)",
    color: "var(--havn-text)",
    whiteSpace: "pre-wrap",
    fontSize: "10px",
    fontFamily: "var(--havn-font-mono)",
  },
  diffAdded: {
    padding: "2px 4px",
    background: "color-mix(in srgb, var(--havn-green) 12%, transparent)",
    borderLeft: "2px solid var(--havn-green)",
    color: "var(--havn-text)",
    whiteSpace: "pre-wrap",
    fontSize: "10px",
    fontFamily: "var(--havn-font-mono)",
  },
  toolFilePath: {
    color: "var(--havn-text-dim)",
    fontSize: "9px",
    marginTop: "2px",
  },

  /* Inline markdown */
  inlineCode: {
    padding: "1px 3px",
    background: "var(--havn-bg)",
    borderRadius: "2px",
    fontFamily: "inherit",
    fontSize: "inherit",
    color: "var(--havn-accent)",
  },
  fileLink: {
    color: "var(--havn-accent)",
    cursor: "pointer",
    textDecoration: "underline",
    textDecorationStyle: "dotted",
    textUnderlineOffset: "2px",
  },
  tableLink: {
    color: "var(--havn-purple)",
    cursor: "pointer",
    textDecoration: "underline",
    textDecorationStyle: "dotted",
    textUnderlineOffset: "2px",
  },
  fileLinkCode: {
    cursor: "pointer",
    textDecoration: "underline",
    textDecorationStyle: "dotted",
    textUnderlineOffset: "2px",
  },
  tableLinkCode: {
    color: "var(--havn-purple)",
    cursor: "pointer",
    textDecoration: "underline",
    textDecorationStyle: "dotted",
    textUnderlineOffset: "2px",
  },
  codeBlock: {
    display: "block",
    padding: "6px 8px",
    margin: "4px 0",
    background: "var(--havn-bg)",
    border: "1px solid var(--havn-border)",
    borderRadius: "var(--havn-radius)",
    fontFamily: "inherit",
    fontSize: "inherit",
    whiteSpace: "pre-wrap",
    overflowX: "auto",
  },
  listItem: {
    display: "block",
    paddingLeft: "8px",
  },
  heading: (level) => ({
    display: "block",
    fontWeight: 700,
    fontSize: level <= 2 ? "14px" : level === 3 ? "13px" : "12px",
    color: "var(--havn-text)",
    margin: level <= 2 ? "8px 0 4px" : "6px 0 2px",
    letterSpacing: level <= 2 ? "0.3px" : "0",
  }),
  table: {
    borderCollapse: "collapse",
    margin: "6px 0",
    fontSize: "11px",
    fontFamily: "var(--havn-font-mono)",
    width: "100%",
  },
  th: {
    padding: "3px 8px",
    borderBottom: "1px solid var(--havn-border)",
    textAlign: "left",
    fontWeight: 600,
    color: "var(--havn-text)",
    background: "var(--havn-bg)",
    fontSize: "10px",
    textTransform: "uppercase",
    letterSpacing: "0.3px",
  },
  td: {
    padding: "2px 8px",
    borderBottom: "1px solid color-mix(in srgb, var(--havn-border) 50%, transparent)",
    color: "var(--havn-text-secondary)",
    fontSize: "11px",
  },

  systemText: {
    color: "var(--havn-text-dim)",
    fontStyle: "italic",
    fontSize: "11px",
  },
  errorText: {
    color: "var(--havn-red)",
    wordBreak: "break-word",
  },

  inputArea: {
    borderTop: "1px solid var(--havn-border)",
    flexShrink: 0,
  },
  textarea: {
    width: "100%",
    resize: "none",
    background: "var(--havn-bg)",
    border: "none",
    borderBottom: "1px solid var(--havn-border)",
    color: "var(--havn-text)",
    padding: "8px 12px",
    fontSize: "12px",
    fontFamily: "var(--havn-font-mono)",
    lineHeight: 1.5,
    outline: "none",
    boxSizing: "border-box",
    display: "block",
  },
  inputActions: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "4px 8px",
    gap: "8px",
  },

  /* Mode toggle */
  modeBtn: {
    display: "inline-flex",
    alignItems: "center",
    gap: "5px",
    padding: "3px 10px",
    borderRadius: "var(--havn-radius-lg)",
    cursor: "pointer",
    fontSize: "11px",
    fontWeight: 600,
    fontFamily: "var(--havn-font-mono)",
    letterSpacing: "0.3px",
    flexShrink: 0,
  },
  modeBtnAuto: {
    background: "color-mix(in srgb, var(--havn-green) 15%, transparent)",
    border: "1px solid var(--havn-green)",
    color: "var(--havn-green)",
  },
  modeBtnAsk: {
    background: "color-mix(in srgb, var(--havn-accent) 15%, transparent)",
    border: "1px solid var(--havn-accent)",
    color: "var(--havn-accent)",
  },
  modeDot: (isAuto) => ({
    width: "5px",
    height: "5px",
    borderRadius: "50%",
    background: isAuto ? "var(--havn-green)" : "var(--havn-accent)",
    flexShrink: 0,
  }),

  modelSelect: {
    padding: "3px 4px",
    background: "var(--havn-bg)",
    border: "1px solid var(--havn-border)",
    borderRadius: "var(--havn-radius)",
    color: "var(--havn-text-secondary)",
    fontSize: "10px",
    fontFamily: "var(--havn-font-mono)",
    cursor: "pointer",
    outline: "none",
    flexShrink: 1,
    minWidth: 0,
  },

  sendBtn: {
    padding: "4px 14px",
    background: "var(--havn-green)",
    border: "1px solid var(--havn-green-border)",
    borderRadius: "var(--havn-radius-lg)",
    color: "#fff",
    cursor: "pointer",
    fontSize: "12px",
    fontWeight: 600,
    letterSpacing: "0.3px",
    flexShrink: 0,
  },

  /* Loading indicator */
  loadingEntry: {
    display: "flex",
    alignItems: "center",
    gap: "8px",
    padding: "6px 12px",
    margin: "4px 0",
    borderLeft: "2px solid var(--havn-purple)",
  },
  loadingDots: {
    display: "inline-block",
    width: "6px",
    height: "6px",
    borderRadius: "50%",
    background: "var(--havn-purple)",
    animation: "pulse 1.2s ease-in-out infinite",
  },
  loadingText: {
    color: "var(--havn-text-dim)",
    fontSize: "11px",
    fontFamily: "var(--havn-font-mono)",
    fontStyle: "italic",
  },
};
