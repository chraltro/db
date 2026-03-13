"""Base class for coding agent adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import AsyncIterator


class AgentAdapter(ABC):
    """Base class for all coding agent adapters.

    Each adapter wraps a specific coding agent (Claude Code, Codex, Gemini CLI)
    and provides a uniform streaming interface for the agent sidebar.
    """

    name: str = "base"
    display_name: str = "Base Agent"

    @abstractmethod
    async def start_session(
        self, project_path: str, system_prompt: str | None = None
    ) -> None:
        """Initialize a new agent session pointed at the project directory."""

    @abstractmethod
    async def send_message(self, message: str) -> AsyncIterator[dict]:
        """Send a user message and yield response chunks.

        Each chunk is a dict with at minimum:
          { "type": "text" | "tool_use" | "diff" | "error" | "done",
            "content": str }
        """

    @abstractmethod
    async def stop_session(self) -> None:
        """Clean up the agent subprocess."""

    @classmethod
    def is_available(cls) -> bool:
        """Check whether this agent's CLI/SDK is installed."""
        return False
