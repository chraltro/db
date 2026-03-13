"""Base class for coding agent adapters."""

from __future__ import annotations

import asyncio
import os
import re
import shutil
import sys
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from pathlib import Path


def _resolve_npm_wrapper(cmd_path: str) -> list[str] | None:
    """Extract [node, script_path] from an npm .cmd wrapper on Windows.

    npm-installed CLIs on Windows are thin .cmd wrappers that invoke node
    with a JS entry point.  We parse that path so we can call node directly
    via create_subprocess_exec, avoiding cmd.exe shell interpretation
    (and the command-injection risk that comes with it).
    """
    try:
        content = Path(cmd_path).read_text(encoding="utf-8")
        cmd_dir = str(Path(cmd_path).parent)
        # npm .cmd wrappers reference scripts relative to %~dp0
        # e.g.: "%~dp0\node_modules\@openai\codex\bin\codex.js" %*
        for m in re.finditer(r"%~dp0\\([^\"*\n]+\.(?:js|mjs|cjs))", content):
            script = os.path.join(cmd_dir, m.group(1))
            if os.path.isfile(script):
                node = shutil.which("node")
                if node:
                    return [node, script]
        return None
    except Exception:
        return None


async def spawn_cli(cmd: list[str], cwd: str | None = None) -> asyncio.subprocess.Process:
    """Spawn a CLI subprocess safely on all platforms.

    On Windows, npm-installed CLIs are .cmd wrappers that must run through
    cmd.exe — but piping user input through cmd.exe is a command-injection
    risk.  Instead we resolve the wrapper to its underlying node script and
    call node directly via create_subprocess_exec (no shell).
    """
    if sys.platform == "win32":
        resolved = shutil.which(cmd[0])
        if resolved and resolved.lower().endswith((".cmd", ".bat")):
            node_cmd = _resolve_npm_wrapper(resolved)
            if node_cmd:
                cmd = node_cmd + cmd[1:]
            else:
                # Last resort: resolved full path, let Windows try to exec it.
                # This may fail for .cmd files, but is safer than shell=True.
                cmd[0] = resolved
        elif resolved:
            cmd[0] = resolved

    return await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd,
    )


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
    async def send_message(self, message: str) -> AsyncGenerator[dict, None]:
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
