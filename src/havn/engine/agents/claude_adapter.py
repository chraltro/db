"""Claude Code adapter using the official Claude Agent SDK."""

from __future__ import annotations

import asyncio
import json
import shutil
from collections.abc import AsyncGenerator

from havn.engine.agents.base import AgentAdapter, spawn_cli

# Tools allowed in each permission mode
_READ_ONLY_TOOLS = "Read,Glob,Grep"
_FULL_TOOLS = "Edit,Write,Read,Glob,Grep,Bash"


class ClaudeCodeAdapter(AgentAdapter):
    """Adapter for Claude Code via the claude-agent-sdk Python package."""

    name = "claude"
    display_name = "Claude Code"

    def __init__(self) -> None:
        self._process: asyncio.subprocess.Process | None = None
        self.permission_mode: str = "auto"  # "ask" or "auto"
        self.model: str = ""
        self._has_conversation: bool = False  # True after first message

    async def start_session(
        self, project_path: str, system_prompt: str | None = None
    ) -> None:
        self._project_path = project_path
        self._system_prompt = system_prompt
        self._has_conversation = False

    async def send_message(self, message: str) -> AsyncGenerator[dict, None]:
        tools = _FULL_TOOLS if self.permission_mode == "auto" else _READ_ONLY_TOOLS
        cmd = [
            "claude",
            "--output-format", "stream-json",
            "--verbose",
            "--allowedTools", tools,
        ]
        if self.model:
            cmd.extend(["--model", self.model])
        # Continue previous conversation for follow-up messages
        if self._has_conversation:
            cmd.append("--continue")
        cmd.extend(["-p", message])
        if self._system_prompt:
            cmd.extend(["--append-system-prompt", self._system_prompt])

        try:
            self._process = await spawn_cli(cmd, cwd=self._project_path)
        except (FileNotFoundError, OSError) as exc:
            yield {"type": "error", "content": "Claude Code not found. Install it with: npm install -g @anthropic-ai/claude-code"}
            yield {"type": "done", "content": ""}
            return

        saw_assistant_text = False

        async for line in self._process.stdout:  # type: ignore[union-attr]
            text = line.decode().strip()
            if not text:
                continue
            try:
                event = json.loads(text)
                is_result = event.get("type") == "result"
                for chunk in self._parse_event(event):
                    # Skip result text if we already streamed assistant content
                    if is_result and chunk.get("type") == "text" and saw_assistant_text:
                        continue
                    if chunk.get("type") == "text":
                        # Strip leading newlines from the first text chunk
                        if not saw_assistant_text:
                            chunk["content"] = chunk["content"].lstrip("\n")
                            if not chunk["content"]:
                                continue
                        saw_assistant_text = True
                    yield chunk
            except json.JSONDecodeError:
                yield {"type": "text", "content": text}

        await self._process.wait()

        # Surface stderr if the process failed silently
        if (not saw_assistant_text or self._process.returncode) and self._process.stderr:
            stderr = (await self._process.stderr.read()).decode().strip()
            if stderr:
                yield {"type": "error", "content": stderr}

        self._has_conversation = True
        yield {"type": "done", "content": ""}

    def _parse_event(self, event: dict) -> list[dict]:
        """Parse a Claude CLI stream-json event into sidebar chunks."""
        chunks: list[dict] = []
        msg_type = event.get("type", "")

        if msg_type == "assistant":
            for block in event.get("message", {}).get("content", []):
                if block.get("type") == "text":
                    text = block.get("text", "")
                    if text:
                        chunks.append({"type": "text", "content": text})
                elif block.get("type") == "tool_use":
                    name = block.get("name", "tool")
                    inp = block.get("input", {})
                    detail = _summarize_tool_input(name, inp)
                    chunk = {
                        "type": "tool_use",
                        "content": name,
                        "detail": detail,
                    }
                    # Include full input for Edit/Write so UI can show the diff
                    if name in ("Edit", "Write") and inp:
                        chunk["tool_input"] = inp
                    chunks.append(chunk)
        elif msg_type == "result":
            result_text = event.get("result", "")
            if result_text:
                chunks.append({"type": "text", "content": result_text})
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
        self._has_conversation = False

    @classmethod
    def is_available(cls) -> bool:
        return shutil.which("claude") is not None


def _summarize_tool_input(name: str, inp: dict) -> str:
    """Return a short human-readable summary of a tool's input."""
    if not inp:
        return ""
    if name in ("Read", "Write", "Edit"):
        path = inp.get("file_path", "")
        if path:
            parts = path.replace("\\", "/").split("/")
            return "/".join(parts[-2:]) if len(parts) > 1 else parts[0]
    if name == "Glob":
        return inp.get("pattern", "")
    if name == "Grep":
        return inp.get("pattern", "")
    if name == "Bash":
        cmd = inp.get("command", "")
        return cmd[:80] + ("..." if len(cmd) > 80 else "")
    for k, v in inp.items():
        s = str(v)
        return f"{k}: {s[:60]}{'...' if len(s) > 60 else ''}"
    return ""
