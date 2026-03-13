"""Claude Code adapter using the official Claude Agent SDK."""

from __future__ import annotations

import asyncio
import json
import shutil
from typing import AsyncIterator

from havn.engine.agents.base import AgentAdapter


class ClaudeCodeAdapter(AgentAdapter):
    """Adapter for Claude Code via the claude-agent-sdk Python package."""

    name = "claude"
    display_name = "Claude Code"

    def __init__(self) -> None:
        self._process: asyncio.subprocess.Process | None = None

    async def start_session(
        self, project_path: str, system_prompt: str | None = None
    ) -> None:
        self._project_path = project_path
        self._system_prompt = system_prompt

    async def send_message(self, message: str) -> AsyncIterator[dict]:
        cmd = ["claude", "--output-format", "stream-json", "--verbose", "-p", message]
        if self._system_prompt:
            cmd.extend(["--append-system-prompt", self._system_prompt])

        self._process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self._project_path,
        )

        async for line in self._process.stdout:  # type: ignore[union-attr]
            text = line.decode().strip()
            if not text:
                continue
            try:
                event = json.loads(text)
                for chunk in self._parse_event(event):
                    yield chunk
            except json.JSONDecodeError:
                yield {"type": "text", "content": text}

        await self._process.wait()
        yield {"type": "done", "content": ""}

    def _parse_event(self, event: dict) -> list[dict]:
        """Parse a Claude CLI stream-json event into sidebar chunks."""
        chunks: list[dict] = []
        msg_type = event.get("type", "")

        if msg_type == "assistant":
            for block in event.get("message", {}).get("content", []):
                if block.get("type") == "text":
                    chunks.append({"type": "text", "content": block["text"]})
                elif block.get("type") == "tool_use":
                    name = block.get("name", "tool")
                    chunks.append(
                        {"type": "tool_use", "content": f"Using: {name}"}
                    )
        elif msg_type == "result":
            text = event.get("result", "")
            if text:
                chunks.append({"type": "text", "content": text})
        elif msg_type == "error":
            chunks.append(
                {"type": "error", "content": event.get("error", "Unknown error")}
            )

        return chunks

    async def stop_session(self) -> None:
        if self._process and self._process.returncode is None:
            self._process.terminate()
            try:
                await asyncio.wait_for(self._process.wait(), timeout=5)
            except asyncio.TimeoutError:
                self._process.kill()

    @classmethod
    def is_available(cls) -> bool:
        return shutil.which("claude") is not None
