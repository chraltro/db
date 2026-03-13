import React, { useState, useRef, useEffect, useCallback } from "react";

const AGENTS = [
  { id: "claude", name: "Claude Code", icon: "C" },
  { id: "codex", name: "Codex", icon: "X" },
  { id: "gemini", name: "Gemini", icon: "G" },
];

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
            content: `${data.agent} is ready. Ask it to edit your project.`,
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
          { role: "error", content: data.message },
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

    setMessages((prev) => [...prev, { role: "user", content: input }]);
    ws.send(JSON.stringify({ type: "message", message: input }));
    setInput("");
    setIsStreaming(true);
  };

  const switchAgent = (agentId) => {
    if (agentId === selectedAgent) return;
    setSelectedAgent(agentId);
    setMessages([]);
    setIsConnected(false);
    // Reconnect handled by useEffect dependency on connect (which depends on selectedAgent)
  };

  if (!isOpen) return null;

  const agentAvailability = {};
  if (availableAgents) {
    for (const a of availableAgents) agentAvailability[a.id] = a.available;
  }

  return (
    <div style={s.sidebar}>
      {/* Header */}
      <div style={s.header}>
        <span style={s.headerTitle}>Agent</span>
        <div style={{ display: "flex", gap: "4px", alignItems: "center" }}>
          {messages.length > 0 && (
            <button
              onClick={() => setMessages([])}
              style={s.clearBtn}
              title="Clear chat"
            >
              Clear
            </button>
          )}
          <button onClick={onToggle} style={s.closeBtn} title="Close sidebar">
            {"\u00D7"}
          </button>
        </div>
      </div>

      {/* Agent picker */}
      <div style={s.picker}>
        {AGENTS.map((agent) => {
          const available = agentAvailability[agent.id] !== false;
          const isActive = selectedAgent === agent.id;
          return (
            <button
              key={agent.id}
              style={{
                ...s.agentBtn,
                ...(isActive ? s.agentBtnActive : {}),
                opacity: available ? 1 : 0.4,
              }}
              onClick={() => available && switchAgent(agent.id)}
              title={
                available
                  ? agent.name
                  : `${agent.name} (not installed)`
              }
              disabled={!available}
            >
              <span style={s.agentIcon}>{agent.icon}</span>
              <span style={s.agentName}>{agent.name}</span>
            </button>
          );
        })}
      </div>

      {/* Status */}
      <div style={s.status}>
        <span
          style={{
            ...s.statusDot,
            background: isConnected ? "var(--havn-green, #48bb78)" : "var(--havn-text-dim)",
          }}
        />
        <span style={s.statusText}>
          {isConnected ? "Connected" : "Connecting..."}
        </span>
      </div>

      {/* Messages */}
      <div style={s.messages}>
        {messages.length === 0 && (
          <div style={s.emptyState}>
            Send a message to start working with the agent.
          </div>
        )}
        {messages.map((msg, i) => (
          <div key={i} style={s.msgWrapper}>
            {msg.role === "user" && (
              <div style={s.userMsg}>
                <div style={s.userBubble}>{msg.content}</div>
              </div>
            )}
            {msg.role === "assistant" && (
              <div style={s.assistantMsg}>
                {msg.chunkType === "tool_use" ? (
                  <div style={s.toolUse}>{msg.content}</div>
                ) : (
                  <div style={s.assistantBubble}>
                    <pre style={s.pre}>{msg.content}</pre>
                  </div>
                )}
              </div>
            )}
            {msg.role === "system" && (
              <div style={s.systemMsg}>{msg.content}</div>
            )}
            {msg.role === "error" && (
              <div style={s.errorMsg}>{msg.content}</div>
            )}
          </div>
        ))}
        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <div style={s.inputArea}>
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
              ? "Ask the agent to edit your project..."
              : "Connecting..."
          }
          disabled={!isConnected || isStreaming}
          style={s.textarea}
          rows={3}
        />
        <button
          onClick={sendMessage}
          disabled={!isConnected || isStreaming || !input.trim()}
          style={{
            ...s.sendBtn,
            opacity: !isConnected || isStreaming || !input.trim() ? 0.4 : 1,
          }}
        >
          {isStreaming ? "..." : "Send"}
        </button>
      </div>
    </div>
  );
}

const s = {
  sidebar: {
    display: "flex",
    flexDirection: "column",
    height: "100%",
    background: "var(--havn-bg)",
    borderLeft: "1px solid var(--havn-border)",
    overflow: "hidden",
  },
  header: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "8px 12px",
    borderBottom: "1px solid var(--havn-border)",
    background: "var(--havn-bg-secondary)",
    flexShrink: 0,
  },
  headerTitle: {
    fontSize: "13px",
    fontWeight: 600,
    color: "var(--havn-text)",
  },
  closeBtn: {
    background: "none",
    border: "none",
    color: "var(--havn-text-secondary)",
    cursor: "pointer",
    fontSize: "18px",
    lineHeight: 1,
    padding: "0 4px",
  },
  clearBtn: {
    background: "none",
    border: "1px solid var(--havn-border)",
    borderRadius: "var(--havn-radius)",
    color: "var(--havn-text-dim)",
    cursor: "pointer",
    fontSize: "10px",
    padding: "2px 6px",
  },
  picker: {
    display: "flex",
    gap: "4px",
    padding: "8px",
    borderBottom: "1px solid var(--havn-border)",
    flexShrink: 0,
  },
  agentBtn: {
    flex: 1,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    gap: "4px",
    padding: "5px 8px",
    background: "var(--havn-btn-bg)",
    border: "1px solid var(--havn-btn-border)",
    borderRadius: "var(--havn-radius)",
    color: "var(--havn-text-secondary)",
    cursor: "pointer",
    fontSize: "11px",
    fontWeight: 500,
  },
  agentBtnActive: {
    background: "var(--havn-bg-secondary)",
    borderColor: "var(--havn-accent)",
    color: "var(--havn-accent)",
  },
  agentIcon: {
    fontSize: "11px",
    fontWeight: 700,
    fontFamily: "var(--havn-font-mono)",
  },
  agentName: {
    fontSize: "11px",
  },
  status: {
    display: "flex",
    alignItems: "center",
    gap: "6px",
    padding: "4px 12px",
    borderBottom: "1px solid var(--havn-border)",
    flexShrink: 0,
  },
  statusDot: {
    width: "6px",
    height: "6px",
    borderRadius: "50%",
    flexShrink: 0,
  },
  statusText: {
    fontSize: "11px",
    color: "var(--havn-text-dim)",
  },
  messages: {
    flex: 1,
    overflow: "auto",
    padding: "8px",
    minHeight: 0,
  },
  emptyState: {
    padding: "24px 12px",
    textAlign: "center",
    color: "var(--havn-text-dim)",
    fontSize: "12px",
  },
  msgWrapper: {
    marginBottom: "8px",
  },
  userMsg: {
    display: "flex",
    justifyContent: "flex-end",
  },
  userBubble: {
    background: "var(--havn-accent)",
    color: "#fff",
    padding: "6px 10px",
    borderRadius: "10px 10px 2px 10px",
    fontSize: "12px",
    maxWidth: "85%",
    lineHeight: 1.4,
    whiteSpace: "pre-wrap",
    wordBreak: "break-word",
  },
  assistantMsg: {
    display: "flex",
    justifyContent: "flex-start",
  },
  assistantBubble: {
    background: "var(--havn-bg-secondary)",
    border: "1px solid var(--havn-border)",
    padding: "6px 10px",
    borderRadius: "10px 10px 10px 2px",
    maxWidth: "90%",
    overflow: "hidden",
  },
  pre: {
    margin: 0,
    fontSize: "12px",
    fontFamily: "var(--havn-font-mono)",
    whiteSpace: "pre-wrap",
    wordBreak: "break-word",
    color: "var(--havn-text)",
    lineHeight: 1.4,
  },
  toolUse: {
    padding: "4px 8px",
    fontSize: "11px",
    color: "var(--havn-text-dim)",
    fontFamily: "var(--havn-font-mono)",
    fontStyle: "italic",
  },
  systemMsg: {
    textAlign: "center",
    padding: "6px",
    fontSize: "11px",
    color: "var(--havn-text-dim)",
    fontStyle: "italic",
  },
  errorMsg: {
    padding: "6px 10px",
    fontSize: "12px",
    color: "var(--havn-red, #f56565)",
    background: "rgba(245, 101, 101, 0.1)",
    borderRadius: "6px",
    fontFamily: "var(--havn-font-mono)",
  },
  inputArea: {
    padding: "8px",
    borderTop: "1px solid var(--havn-border)",
    display: "flex",
    gap: "6px",
    alignItems: "flex-end",
    flexShrink: 0,
  },
  textarea: {
    flex: 1,
    resize: "none",
    background: "var(--havn-bg-secondary)",
    border: "1px solid var(--havn-border)",
    borderRadius: "var(--havn-radius)",
    color: "var(--havn-text)",
    padding: "8px 10px",
    fontSize: "12px",
    fontFamily: "var(--havn-font)",
    lineHeight: 1.4,
    outline: "none",
  },
  sendBtn: {
    padding: "8px 14px",
    background: "var(--havn-accent)",
    border: "none",
    borderRadius: "var(--havn-radius)",
    color: "#fff",
    cursor: "pointer",
    fontSize: "12px",
    fontWeight: 600,
    flexShrink: 0,
    alignSelf: "flex-end",
  },
};
