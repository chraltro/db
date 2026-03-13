"""Codex adapter using the CLI in non-interactive mode."""

from __future__ import annotations

import asyncio
import json
import shutil
from collections.abc import AsyncGenerator

from havn.engine.agents.base import AgentAdapter


class CodexAdapter(AgentAdapter):
    """Adapter for OpenAI Codex CLI via subprocess."""

    name = "codex"
    display_name = "Codex"

    def __init__(self) -> None:
        self._project_path: str | None = None
        self._system_prompt: str | None = None

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
        if self._system_prompt:
            cmd.extend(["--instructions", self._system_prompt])
        cmd.append(message)

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self._project_path,
        )

        async for line in process.stdout:  # type: ignore[union-attr]
            text = line.decode().strip()
            if not text:
                continue
            try:
                event = json.loads(text)
                yield {"type": "text", "content": self._parse_event(event)}
            except json.JSONDecodeError:
                yield {"type": "text", "content": text}

        await process.wait()
        yield {"type": "done", "content": ""}

    def _parse_event(self, event: dict) -> str:
        if "message" in event:
            return event["message"]
        return json.dumps(event)

    async def stop_session(self) -> None:
        pass  # exec mode is stateless per invocation

    @classmethod
    def is_available(cls) -> bool:
        return shutil.which("codex") is not None
