import React, { useState, useRef, useEffect, useCallback } from "react";

const AGENTS = [
  { id: "claude", name: "Claude Code" },
  { id: "codex", name: "Codex" },
  { id: "gemini", name: "Gemini CLI" },
];

function timestamp() {
  const d = new Date();
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

export default function AgentSidebar({ isOpen, onToggle }) {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [selectedAgent, setSelectedAgent] = useState("claude");
  const [isConnected, setIsConnected] = useState(false);
  const [isStreaming, setIsStreaming] = useState(false);
  const [availableAgents, setAvailableAgents] = useState(null);
  const socketRef = useRef(null);
  const messagesEndRef = useRef(null);
  const reconnectRef = useRef(null);
  const isOpenRef = useRef(isOpen);
  isOpenRef.current = isOpen;

  // Fetch available agents on mount
  useEffect(() => {
    fetch("/api/agents")
      .then((r) => r.json())
      .then(setAvailableAgents)
      .catch(() => {});
  }, []);

  const connect = useCallback(() => {
    if (socketRef.current) {
      socketRef.current.close();
    }

    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${protocol}//${window.location.host}/ws/agent`);
    socketRef.current = ws;

    ws.onopen = () => {
      ws.send(JSON.stringify({ type: "start", agent: selectedAgent }));
    };

    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);

      if (data.type === "ready") {
        setIsConnected(true);
        setMessages((prev) => [
          ...prev,
          {
            role: "system",
            content: `${data.agent} connected`,
            ts: timestamp(),
          },
        ]);
      } else if (data.type === "chunk") {
        setMessages((prev) => {
          const last = prev[prev.length - 1];
          if (last && last.role === "assistant" && last.streaming) {
            return [
              ...prev.slice(0, -1),
              {
                ...last,
                content: last.content + data.content,
                chunkType: data.chunk_type,
              },
            ];
          }
          return [
            ...prev,
            {
              role: "assistant",
              content: data.content,
              chunkType: data.chunk_type,
              streaming: true,
              ts: timestamp(),
            },
          ];
        });
      } else if (data.type === "done") {
        setIsStreaming(false);
        setMessages((prev) => {
          const last = prev[prev.length - 1];
          if (last && last.streaming) {
            return [
              ...prev.slice(0, -1),
              { ...last, streaming: false },
            ];
          }
          return prev;
        });
      } else if (data.type === "error") {
        setMessages((prev) => [
          ...prev,
          { role: "error", content: data.message, ts: timestamp() },
        ]);
        setIsStreaming(false);
      }
    };

    ws.onclose = () => {
      setIsConnected(false);
      // Auto-reconnect after 3s if sidebar is still open (use ref to avoid stale closure)
      reconnectRef.current = setTimeout(() => {
        if (isOpenRef.current) connect();
      }, 3000);
    };

    ws.onerror = () => {
      setIsConnected(false);
    };
  }, [selectedAgent]);

  // Connect when sidebar opens, disconnect when it closes
  useEffect(() => {
    if (isOpen) {
      connect();
    } else {
      if (socketRef.current) {
        socketRef.current.close();
        socketRef.current = null;
      }
    }
    return () => {
      clearTimeout(reconnectRef.current);
      if (socketRef.current) {
        socketRef.current.close();
        socketRef.current = null;
      }
    };
  }, [isOpen, connect]);

  // Auto-scroll
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const sendMessage = () => {
    if (!input.trim() || !isConnected || isStreaming) return;
    const ws = socketRef.current;
    if (!ws || ws.readyState !== WebSocket.OPEN) return;

    setMessages((prev) => [
      ...prev,
      { role: "user", content: input, ts: timestamp() },
    ]);
    ws.send(JSON.stringify({ type: "message", message: input }));
    setInput("");
    setIsStreaming(true);
  };

  const switchAgent = (agentId) => {
    if (agentId === selectedAgent) return;
    setSelectedAgent(agentId);
    setMessages([]);
    setIsConnected(false);
  };

  if (!isOpen) return null;

  const agentAvailability = {};
  if (availableAgents) {
    for (const a of availableAgents) agentAvailability[a.id] = a.available;
  }

  return (
    <div style={st.sidebar} data-havn-agent-sidebar="">
      {/* Header — matches OutputPanel / sidebar section header pattern */}
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
            <button onClick={() => setMessages([])} style={st.clearBtn}>Clear</button>
          )}
          <button onClick={onToggle} style={st.closeBtn} title="Close agent sidebar">{"\u00D7"}</button>
        </div>
      </div>

      {/* Agent picker — tab-style, matches sub-tab bar */}
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

      {/* Message log — matches OutputPanel entry pattern */}
      <div style={st.log}>
        {messages.length === 0 && (
          <div style={st.placeholder}>
            Ask the agent to edit your project.
          </div>
        )}
        {messages.map((msg, i) => (
          <div key={i} style={st.entry} data-havn-agent-entry="">
            <span style={st.ts}>{msg.ts}</span>
            <span
              style={{
                ...st.indicator,
                background:
                  msg.role === "user"
                    ? "var(--havn-accent)"
                    : msg.role === "error"
                    ? "var(--havn-red)"
                    : msg.role === "system"
                    ? "var(--havn-text-dim)"
                    : msg.chunkType === "tool_use"
                    ? "var(--havn-purple)"
                    : "var(--havn-green)",
              }}
            />
            {msg.role === "user" && (
              <span style={st.userText}>{msg.content}</span>
            )}
            {msg.role === "assistant" && msg.chunkType === "tool_use" && (
              <span style={st.toolText}>{msg.content}</span>
            )}
            {msg.role === "assistant" && msg.chunkType !== "tool_use" && (
              <pre style={st.assistantText}>{msg.content}{msg.streaming ? "\u2588" : ""}</pre>
            )}
            {msg.role === "system" && (
              <span style={st.systemText}>{msg.content}</span>
            )}
            {msg.role === "error" && (
              <span style={st.errorText}>{msg.content}</span>
            )}
          </div>
        ))}
        <div ref={messagesEndRef} />
      </div>

      {/* Input — matches QueryPanel textarea styling */}
      <div style={st.inputArea}>
        <textarea
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              sendMessage();
            }
          }}
          placeholder={
            isConnected
              ? "Ask the agent\u2026"
              : "Connecting\u2026"
          }
          disabled={!isConnected || isStreaming}
          style={st.textarea}
          rows={3}
        />
        <div style={st.inputActions}>
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
          <span style={st.inputHint}>Enter to send, Shift+Enter for newline</span>
        </div>
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Styles — aligned with havn design system                            */
/* ------------------------------------------------------------------ */

const st = {
  sidebar: {
    display: "flex",
    flexDirection: "column",
    height: "100%",
    background: "var(--havn-bg-tertiary)",
    overflow: "hidden",
  },

  /* Header — matches OutputPanel header */
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

  /* Agent picker — matches sub-tab bar */
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

  /* Message log — matches OutputPanel */
  log: {
    flex: 1,
    overflow: "auto",
    padding: "4px 12px",
    fontFamily: "var(--havn-font-mono)",
    fontSize: "12px",
    lineHeight: 1.7,
    minHeight: 0,
  },
  placeholder: {
    color: "var(--havn-text-dim)",
    fontStyle: "italic",
    padding: "8px 0",
    fontSize: "12px",
  },
  entry: {
    display: "flex",
    gap: "8px",
    alignItems: "baseline",
    marginBottom: "1px",
  },
  ts: {
    color: "var(--havn-text-dim)",
    flexShrink: 0,
    fontSize: "11px",
  },
  indicator: {
    width: "4px",
    height: "4px",
    borderRadius: "50%",
    flexShrink: 0,
    marginTop: "2px",
  },

  /* Message text styles — match OutputPanel typeStyles */
  userText: {
    color: "var(--havn-accent)",
    fontWeight: 500,
    wordBreak: "break-word",
  },
  assistantText: {
    margin: 0,
    color: "var(--havn-text)",
    whiteSpace: "pre-wrap",
    wordBreak: "break-word",
    fontFamily: "inherit",
    fontSize: "inherit",
    lineHeight: "inherit",
  },
  toolText: {
    color: "var(--havn-purple)",
    fontStyle: "italic",
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

  /* Input area — matches QueryPanel textarea */
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
  inputHint: {
    fontSize: "10px",
    color: "var(--havn-text-dim)",
  },
};
