"""Codex adapter using the CLI in non-interactive mode."""

from __future__ import annotations

import asyncio
import json
import shutil
from collections.abc import AsyncGenerator

from havn.engine.agents.base import AgentAdapter, spawn_cli


class CodexAdapter(AgentAdapter):
    """Adapter for OpenAI Codex CLI via subprocess."""

    name = "codex"
    display_name = "Codex"

    def __init__(self) -> None:
        self._project_path: str | None = None
        self._system_prompt: str | None = None
        self._process: asyncio.subprocess.Process | None = None
        self.permission_mode: str = "auto"
        self.model: str = ""

    async def start_session(
        self, project_path: str, system_prompt: str | None = None
    ) -> None:
        self._project_path = project_path
        self._system_prompt = system_prompt

    async def send_message(self, message: str) -> AsyncGenerator[dict, None]:
        cmd = [
            "codex",
            "exec",
            "--json",
            "--approval-mode",
            "auto-edit",
        ]
        if self.model:
            cmd.extend(["-c", f"model={self.model}"])
        if self._system_prompt:
            cmd.extend(["--instructions", self._system_prompt])
        cmd.append(message)

        try:
            self._process = await spawn_cli(cmd, cwd=self._project_path)
        except (FileNotFoundError, OSError):
            yield {"type": "error", "content": "Codex CLI not found. Install it with: npm install -g @openai/codex"}
            yield {"type": "done", "content": ""}
            return

        got_output = False
        async for line in self._process.stdout:  # type: ignore[union-attr]
            text = line.decode().strip()
            if not text:
                continue
            got_output = True
            try:
                event = json.loads(text)
                yield {"type": "text", "content": self._parse_event(event)}
            except json.JSONDecodeError:
                yield {"type": "text", "content": text}

        await self._process.wait()

        # Surface stderr if the process failed silently (e.g. auth errors)
        if (not got_output or self._process.returncode) and self._process.stderr:
            stderr = (await self._process.stderr.read()).decode().strip()
            if stderr:
                yield {"type": "error", "content": stderr}

        yield {"type": "done", "content": ""}

    def _parse_event(self, event: dict) -> str:
        if "message" in event:
            return event["message"]
        return json.dumps(event)

    async def stop_session(self) -> None:
        if self._process and self._process.returncode is None:
            self._process.terminate()
            try:
                await asyncio.wait_for(self._process.wait(), timeout=5)
            except asyncio.TimeoutError:
                self._process.kill()

    @classmethod
    def is_available(cls) -> bool:
        return shutil.which("codex") is not None
