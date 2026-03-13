"""Agent sidebar: WebSocket endpoint and REST API for coding agents."""

from __future__ import annotations

import json
import logging
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
            _active_sessions[ws_id] = {"adapter": None}

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
    websocket: WebSocket, ws_id: int, data: dict, default_project: Path
) -> None:
    """Handle agent:start — spawn the adapter and start a session."""
    from havn.engine.agents import get_adapter

    agent_name = data.get("agent", "claude")
    project_path = data.get("project_path") or str(default_project)

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

    try:
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

    message = data.get("message", "").strip()
    if not message:
        await websocket.send_json(
            {"type": "error", "message": "Empty message"}
        )
        return

    adapter = session["adapter"]
    try:
        async for chunk in adapter.send_message(message):
            chunk_type = chunk.get("type", "text")
            content = chunk.get("content", "")

            if chunk_type == "done":
                await websocket.send_json({"type": "done"})
            else:
                await websocket.send_json(
                    {
                        "type": "chunk",
                        "chunk_type": chunk_type,
                        "content": content,
                    }
                )
    except Exception as exc:
        log.exception("Error streaming agent response")
        await websocket.send_json(
            {"type": "error", "message": f"Agent error: {exc}"}
        )
        await websocket.send_json({"type": "done"})


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
