"""Agent registry — maps agent names to adapter classes."""

from __future__ import annotations

from havn.engine.agents.base import AgentAdapter
from havn.engine.agents.claude_adapter import ClaudeCodeAdapter
from havn.engine.agents.codex_adapter import CodexAdapter
from havn.engine.agents.gemini_adapter import GeminiCLIAdapter

AGENT_REGISTRY: dict[str, type[AgentAdapter]] = {
    "claude": ClaudeCodeAdapter,
    "codex": CodexAdapter,
    "gemini": GeminiCLIAdapter,
}


def get_adapter(name: str) -> AgentAdapter | None:
    """Instantiate an agent adapter by name, or return None if unknown."""
    cls = AGENT_REGISTRY.get(name)
    if cls is None:
        return None
    return cls()


def list_available_agents() -> list[dict]:
    """Return info about all registered agents and their availability."""
    agents = []
    for name, cls in AGENT_REGISTRY.items():
        agents.append(
            {
                "id": name,
                "name": cls.display_name,
                "available": cls.is_available(),
            }
        )
    return agents
