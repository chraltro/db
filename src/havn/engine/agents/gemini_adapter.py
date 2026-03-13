"""Gemini CLI adapter using subprocess."""

from __future__ import annotations

import asyncio
import json
import shutil
from collections.abc import AsyncGenerator

from havn.engine.agents.base import AgentAdapter, spawn_cli

# Permission mode → Gemini approval-mode flag
_APPROVAL_MODES = {
    "auto": "yolo",
    "ask": "plan",
}


class GeminiCLIAdapter(AgentAdapter):
    """Adapter for Google Gemini CLI via subprocess."""

    name = "gemini"
    display_name = "Gemini CLI"

    def __init__(self) -> None:
        self._project_path: str | None = None
        self._process: asyncio.subprocess.Process | None = None
        self.permission_mode: str = "auto"
        self.model: str = "gemini-3-flash-preview"

    async def start_session(
        self, project_path: str, system_prompt: str | None = None
    ) -> None:
        self._project_path = project_path
        self._system_prompt = system_prompt

    async def send_message(self, message: str) -> AsyncGenerator[dict, None]:
        # Gemini CLI has no system prompt flag, so prepend context to the message
        prompt = message
        if self._system_prompt:
            prompt = f"[Context: {self._system_prompt}]\n\n{message}"

        approval = _APPROVAL_MODES.get(self.permission_mode, "yolo")
        cmd = [
            "gemini",
            "-p", prompt,
            "--output-format", "stream-json",
            "--approval-mode", approval,
        ]
        if self.model:
            cmd.extend(["-m", self.model])
        try:
            self._process = await spawn_cli(cmd, cwd=self._project_path)
        except (FileNotFoundError, OSError):
            yield {"type": "error", "content": "Gemini CLI not found. Install it with: npm install -g @google/gemini-cli"}
            yield {"type": "done", "content": ""}
            return

        got_output = False
        async for line in self._process.stdout:  # type: ignore[union-attr]
            text = line.decode().strip()
            if not text:
                continue
            try:
                event = json.loads(text)
                for chunk in self._parse_event(event):
                    if chunk.get("type") == "text":
                        got_output = True
                    yield chunk
            except json.JSONDecodeError:
                if text:
                    got_output = True
                    yield {"type": "text", "content": text}

        await self._process.wait()

        if (not got_output or self._process.returncode) and self._process.stderr:
            stderr = (await self._process.stderr.read()).decode().strip()
            if stderr:
                yield {"type": "error", "content": stderr}

        yield {"type": "done", "content": ""}

    def _parse_event(self, event: dict) -> list[dict]:
        """Parse a Gemini CLI stream-json event into sidebar chunks."""
        chunks: list[dict] = []
        msg_type = event.get("type", "")

        if msg_type == "message":
            role = event.get("role", "")
            # Skip echoed user messages and system messages
            if role != "assistant":
                return chunks
            content = event.get("content", "")
            if isinstance(content, str) and content:
                chunks.append({"type": "text", "content": content})
            # Handle tool calls if present
            elif isinstance(content, list):
                for part in content:
                    if isinstance(part, dict):
                        text = part.get("text", "")
                        if text:
                            chunks.append({"type": "text", "content": text})
                        if part.get("functionCall"):
                            name = part["functionCall"].get("name", "tool")
                            chunks.append({"type": "tool_use", "content": name, "detail": ""})
        elif msg_type == "error":
            chunks.append({"type": "error", "content": event.get("error", str(event))})
        # Skip init/result — result has no text, just stats

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
        return shutil.which("gemini") is not None
