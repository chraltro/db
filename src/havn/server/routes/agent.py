"""Agent sidebar: WebSocket endpoint and REST API for coding agents."""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

from fastapi import APIRouter, Request
from starlette.websockets import WebSocket, WebSocketDisconnect

from havn.server.deps import _require_permission

router = APIRouter()
log = logging.getLogger(__name__)

# Active agent sessions keyed by WebSocket id (object id)
_active_sessions: dict[int, dict] = {}


def _build_system_prompt(project_path: str) -> str:
    """Build a context-rich system prompt so the agent understands havn conventions."""
    parts = [
        "You are working inside a havn data platform project.",
        f"Project root: {project_path}",
        "",
        "CRITICAL SECURITY CONSTRAINT:",
        f"You MUST NEVER read, write, search, or access any files outside of {project_path}.",
        "- ALL file operations (Read, Write, Edit, Glob, Grep) must target paths within the project root.",
        "- ALL Bash commands must operate within the project root. Do not use cd to leave it.",
        "- Do not use absolute paths outside the project. Do not follow symlinks that lead outside.",
        "- If a user asks you to access files outside the project, refuse and explain why.",
        "- This is a hard security boundary. There are no exceptions.",
        "",
        "Key conventions:",
        "- DuckDB is the query engine (OLAP-optimized, embedded)",
        "- SQL transforms live in transform/ (bronze/ -> silver/ -> gold/)",
        "- Python ingest/export scripts live in ingest/ and export/",
        "- Config is in project.yml, secrets in .env",
        "- The warehouse is a single DuckDB file (warehouse.duckdb)",
        "",
        "SQL model headers use comments:",
        "  -- config: materialized=table, schema=silver",
        "  -- depends_on: bronze.customers",
        "",
        "When editing files:",
        "- Follow existing naming conventions in the project",
        "- For SQL models: use -- config and -- depends_on headers",
        "- For Python scripts: the `db` connection is pre-injected",
        "- Always explain what you changed and why",
    ]

    # Include project.yml content if it exists
    project_yml = Path(project_path) / "project.yml"
    if project_yml.exists():
        try:
            content = project_yml.read_text()
            parts.extend(["", "project.yml contents:", "```yaml", content, "```"])
        except Exception:
            pass

    return "\n".join(parts)


@router.get("/api/agents")
def list_agents(request: Request) -> list[dict]:
    """List available coding agents and their installation status."""
    _require_permission(request, "read")
    from havn.engine.agents.registry import list_available_agents

    return list_available_agents()


def register_agent_websocket(app) -> None:
    """Register the agent WebSocket endpoint on the FastAPI app."""
    try:

        @app.websocket("/ws/agent")
        async def websocket_agent(websocket: WebSocket) -> None:
            """WebSocket endpoint for agent sidebar communication.

            Protocol:
              Client sends:
                { "type": "start", "agent": "claude", "project_path": "/path" }
                { "type": "message", "message": "Add a customer table" }
                { "type": "stop" }

              Server sends:
                { "type": "ready", "agent": "claude" }
                { "type": "chunk", "chunk_type": "text"|"tool_use"|"diff", "content": "..." }
                { "type": "done" }
                { "type": "error", "message": "..." }
            """
            import havn.server.app as server_app

            await websocket.accept()
            ws_id = id(websocket)
            _active_sessions[ws_id] = {"adapter": None, "streaming": False}

            try:
                while True:
                    raw = await websocket.receive_text()
                    if len(raw) > 200_000:
                        await websocket.send_json(
                            {"type": "error", "message": "Message too large"}
                        )
                        continue

                    data = json.loads(raw)
                    msg_type = data.get("type", "")

                    if msg_type == "start":
                        await _handle_start(
                            websocket, ws_id, data, server_app.PROJECT_DIR
                        )
                    elif msg_type == "message":
                        await _handle_message(websocket, ws_id, data)
                    elif msg_type == "set_mode":
                        await _handle_set_mode(websocket, ws_id, data)
                    elif msg_type == "set_model":
                        await _handle_set_model(websocket, ws_id, data)
                    elif msg_type == "stop":
                        await _handle_stop(websocket, ws_id)
                    else:
                        await websocket.send_json(
                            {
                                "type": "error",
                                "message": f"Unknown message type: {msg_type}",
                            }
                        )

            except WebSocketDisconnect:
                pass
            except json.JSONDecodeError:
                try:
                    await websocket.send_json(
                        {"type": "error", "message": "Invalid JSON"}
                    )
                except Exception:
                    pass
            finally:
                await _cleanup_session(ws_id)

    except ImportError:
        pass  # starlette WebSocket support not available


async def _handle_start(
    websocket: WebSocket, ws_id: int, data: dict, project_dir: Path
) -> None:
    """Handle agent:start — spawn the adapter and start a session."""
    from havn.engine.agents import get_adapter

    agent_name = data.get("agent", "claude")
    # Always use server-side project dir — never trust client-provided paths.
    project_path = str(project_dir)

    # Clean up any existing session
    session = _active_sessions.get(ws_id)
    if session and session.get("adapter"):
        try:
            await session["adapter"].stop_session()
        except Exception:
            pass

    adapter = get_adapter(agent_name)
    if adapter is None:
        await websocket.send_json(
            {"type": "error", "message": f"Unknown agent: {agent_name}"}
        )
        return

    if not adapter.is_available():
        await websocket.send_json(
            {
                "type": "error",
                "message": f"{adapter.display_name} is not installed. "
                f"Install it and make sure the '{agent_name}' command is on PATH.",
            }
        )
        return

    system_prompt = _build_system_prompt(project_path)
    permission_mode = data.get("mode", "auto")
    if permission_mode not in ("ask", "auto"):
        permission_mode = "auto"

    model = _sanitize_model(data.get("model", ""))
    try:
        adapter.permission_mode = permission_mode
        adapter.model = model
        await adapter.start_session(project_path, system_prompt)
        _active_sessions[ws_id] = {"adapter": adapter, "agent": agent_name}
        await websocket.send_json({"type": "ready", "agent": agent_name})
    except Exception as exc:
        log.exception("Failed to start agent session")
        await websocket.send_json(
            {"type": "error", "message": f"Failed to start: {exc}"}
        )


async def _handle_message(websocket: WebSocket, ws_id: int, data: dict) -> None:
    """Handle agent:message — send message and stream response chunks."""
    session = _active_sessions.get(ws_id)
    if not session or not session.get("adapter"):
        await websocket.send_json(
            {"type": "error", "message": "No active agent session. Send 'start' first."}
        )
        return

    if session.get("streaming"):
        await websocket.send_json(
            {"type": "error", "message": "Already processing a message. Wait for it to finish."}
        )
        return

    message = data.get("message", "").strip()
    if not message:
        await websocket.send_json(
            {"type": "error", "message": "Empty message"}
        )
        return

    if len(message) > 100_000:
        await websocket.send_json(
            {"type": "error", "message": "Message too long (100K char limit)"}
        )
        return

    adapter = session["adapter"]
    session["streaming"] = True
    try:
        async for chunk in adapter.send_message(message):
            chunk_type = chunk.get("type", "text")
            content = chunk.get("content", "")

            if chunk_type == "done":
                await websocket.send_json({"type": "done"})
            else:
                msg = {
                    "type": "chunk",
                    "chunk_type": chunk_type,
                    "content": content,
                }
                detail = chunk.get("detail")
                if detail:
                    msg["detail"] = detail
                tool_input = chunk.get("tool_input")
                if tool_input:
                    msg["tool_input"] = tool_input
                await websocket.send_json(msg)
    except (WebSocketDisconnect, RuntimeError):
        # Client disconnected mid-stream — stop the agent process quietly
        try:
            await adapter.stop_session()
        except Exception:
            pass
    except Exception as exc:
        log.exception("Error streaming agent response")
        try:
            await websocket.send_json(
                {"type": "error", "message": f"Agent error: {exc}"}
            )
            await websocket.send_json({"type": "done"})
        except Exception:
            pass  # client already gone
    finally:
        session["streaming"] = False


async def _handle_set_mode(websocket: WebSocket, ws_id: int, data: dict) -> None:
    """Handle set_mode — switch between ask (read-only) and auto (full) permissions."""
    session = _active_sessions.get(ws_id)
    mode = data.get("mode", "auto")
    if mode not in ("ask", "auto"):
        await websocket.send_json(
            {"type": "error", "message": f"Unknown mode: {mode}. Use 'ask' or 'auto'."}
        )
        return
    if session and session.get("adapter"):
        session["adapter"].permission_mode = mode
    await websocket.send_json({"type": "mode_changed", "mode": mode})


async def _handle_set_model(websocket: WebSocket, ws_id: int, data: dict) -> None:
    """Handle set_model — change the model used by the agent."""
    session = _active_sessions.get(ws_id)
    model = _sanitize_model(data.get("model", ""))
    if session and session.get("adapter"):
        session["adapter"].model = model
    await websocket.send_json({"type": "model_changed", "model": model})


# Only allow model IDs that look like real model identifiers
_MODEL_RE = re.compile(r"^[a-zA-Z0-9._:-]*$")


def _sanitize_model(value: str) -> str:
    """Validate model string — reject anything that isn't a clean identifier."""
    if not value:
        return ""
    if len(value) > 100 or not _MODEL_RE.match(value):
        return ""
    return value


async def _handle_stop(websocket: WebSocket, ws_id: int) -> None:
    """Handle agent:stop — clean up the current session."""
    session = _active_sessions.get(ws_id)
    if session and session.get("adapter"):
        try:
            await session["adapter"].stop_session()
        except Exception:
            pass
        session["adapter"] = None
    await websocket.send_json({"type": "stopped"})


async def _cleanup_session(ws_id: int) -> None:
    """Clean up session on disconnect."""
    session = _active_sessions.pop(ws_id, None)
    if session and session.get("adapter"):
        try:
            await session["adapter"].stop_session()
        except Exception:
            pass
