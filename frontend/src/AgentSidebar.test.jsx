import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import React from "react";
import { render, screen, fireEvent } from "@testing-library/react";
import AgentSidebar from "./AgentSidebar";

// Mock WebSocket
class MockWebSocket {
  static instances = [];

  constructor(url) {
    this.url = url;
    this.readyState = 0; // CONNECTING
    this.sentMessages = [];
    this._closed = false;
    MockWebSocket.instances.push(this);
    // Auto-connect
    setTimeout(() => {
      this.readyState = 1; // OPEN
      if (this.onopen) this.onopen();
    }, 0);
  }

  send(data) {
    this.sentMessages.push(JSON.parse(data));
  }

  close() {
    this._closed = true;
    this.readyState = 3; // CLOSED
    if (this.onclose) this.onclose();
  }

  // Helper to simulate server messages
  _receive(data) {
    if (this.onmessage) {
      this.onmessage({ data: JSON.stringify(data) });
    }
  }
}

// Mock fetch for /api/agents
const mockAgents = [
  { id: "claude", name: "Claude Code", available: true },
  { id: "codex", name: "Codex", available: false },
  { id: "gemini", name: "Gemini CLI", available: true },
];

describe("AgentSidebar", () => {
  let originalWebSocket;
  let originalFetch;

  beforeEach(() => {
    MockWebSocket.instances = [];
    originalWebSocket = global.WebSocket;
    global.WebSocket = MockWebSocket;
    // Also set WebSocket.OPEN constant
    global.WebSocket.OPEN = 1;

    originalFetch = global.fetch;
    global.fetch = vi.fn(() =>
      Promise.resolve({ json: () => Promise.resolve(mockAgents) })
    );

    // jsdom doesn't implement scrollIntoView
    Element.prototype.scrollIntoView = vi.fn();
  });

  afterEach(() => {
    global.WebSocket = originalWebSocket;
    global.fetch = originalFetch;
  });

  it("renders nothing when closed", () => {
    const { container } = render(
      <AgentSidebar isOpen={false} onToggle={() => {}} />
    );
    expect(container.innerHTML).toBe("");
  });

  it("renders sidebar when open", () => {
    render(<AgentSidebar isOpen={true} onToggle={() => {}} />);
    expect(screen.getByText("AGENT")).toBeInTheDocument();
    expect(screen.getByText("Claude Code")).toBeInTheDocument();
    expect(screen.getByText("Codex")).toBeInTheDocument();
    expect(screen.getByText("Gemini CLI")).toBeInTheDocument();
  });

  it("shows empty state initially", () => {
    render(<AgentSidebar isOpen={true} onToggle={() => {}} />);
    expect(
      screen.getByText("Ask the agent to edit your project.")
    ).toBeInTheDocument();
  });

  it("shows connecting status initially", () => {
    render(<AgentSidebar isOpen={true} onToggle={() => {}} />);
    expect(screen.getByText("Connecting\u2026")).toBeInTheDocument();
  });

  it("calls onToggle when close button clicked", () => {
    const onToggle = vi.fn();
    render(<AgentSidebar isOpen={true} onToggle={onToggle} />);
    fireEvent.click(screen.getByTitle("Close agent sidebar"));
    expect(onToggle).toHaveBeenCalledTimes(1);
  });

  it("creates WebSocket connection on open", async () => {
    render(<AgentSidebar isOpen={true} onToggle={() => {}} />);
    await new Promise((r) => setTimeout(r, 10));
    expect(MockWebSocket.instances.length).toBeGreaterThanOrEqual(1);
    const ws = MockWebSocket.instances[0];
    expect(ws.url).toContain("/ws/agent");
  });

  it("sends start message on WebSocket open", async () => {
    render(<AgentSidebar isOpen={true} onToggle={() => {}} />);
    await new Promise((r) => setTimeout(r, 10));
    const ws = MockWebSocket.instances[0];
    expect(ws.sentMessages.length).toBeGreaterThanOrEqual(1);
    expect(ws.sentMessages[0]).toEqual({
      type: "start",
      agent: "claude",
    });
  });

  it("fetches available agents on mount", async () => {
    render(<AgentSidebar isOpen={true} onToggle={() => {}} />);
    await new Promise((r) => setTimeout(r, 10));
    expect(global.fetch).toHaveBeenCalledWith("/api/agents");
  });

  it("uses data-havn attributes for styling hooks", () => {
    const { container } = render(
      <AgentSidebar isOpen={true} onToggle={() => {}} />
    );
    expect(container.querySelector("[data-havn-agent-sidebar]")).toBeTruthy();
    // Agent picker tabs use data-havn-tab
    const tabs = container.querySelectorAll("[data-havn-tab]");
    expect(tabs.length).toBe(3);
  });

  it("marks active agent tab", () => {
    const { container } = render(
      <AgentSidebar isOpen={true} onToggle={() => {}} />
    );
    const activeTab = container.querySelector('[data-havn-active="true"]');
    expect(activeTab).toBeTruthy();
    expect(activeTab.textContent).toBe("Claude Code");
  });
});
