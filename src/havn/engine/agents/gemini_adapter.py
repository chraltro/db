"""Gemini CLI adapter using subprocess."""

from __future__ import annotations

import asyncio
import shutil
from typing import AsyncIterator

from havn.engine.agents.base import AgentAdapter


class GeminiCLIAdapter(AgentAdapter):
    """Adapter for Google Gemini CLI via subprocess."""

    name = "gemini"
    display_name = "Gemini CLI"

    def __init__(self) -> None:
        self._project_path: str | None = None

    async def start_session(
        self, project_path: str, system_prompt: str | None = None
    ) -> None:
        self._project_path = project_path

    async def send_message(self, message: str) -> AsyncIterator[dict]:
        process = await asyncio.create_subprocess_exec(
            "gemini",
            "--non-interactive",
            "-p",
            message,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self._project_path,
        )

        async for line in process.stdout:  # type: ignore[union-attr]
            text = line.decode().strip()
            if text:
                yield {"type": "text", "content": text}

        await process.wait()
        yield {"type": "done", "content": ""}

    async def stop_session(self) -> None:
        pass

    @classmethod
    def is_available(cls) -> bool:
        return shutil.which("gemini") is not None
