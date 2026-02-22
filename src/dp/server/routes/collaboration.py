"""Collaboration sessions and WebSocket endpoints."""

from __future__ import annotations

import json
import time

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from dp.server.deps import (
    DbConnReadOnly,
    _require_permission,
    _serialize,
)

router = APIRouter()


# --- Pydantic models ---


class CreateSessionRequest(BaseModel):
    name: str = Field("", max_length=200)


class SessionQueryRequest(BaseModel):
    sql: str = Field(..., max_length=100_000)
    user_id: str = Field("local", max_length=200)


# --- Session endpoints ---


@router.get("/api/sessions")
def list_sessions(request: Request) -> list[dict]:
    """List all active collaboration sessions."""
    _require_permission(request, "read")
    from dp.engine.collaboration import session_manager

    return session_manager.list_sessions()


@router.post("/api/sessions")
def create_session(request: Request, req: CreateSessionRequest) -> dict:
    """Create a new collaboration session."""
    _require_permission(request, "write")
    from dp.engine.collaboration import session_manager

    session = session_manager.create_session(req.name)
    return {
        "session_id": session.session_id,
        "name": session.name,
        "created_at": session.created_at,
    }


@router.get("/api/sessions/{session_id}")
def get_session_detail(request: Request, session_id: str) -> dict:
    """Get details of a collaboration session."""
    _require_permission(request, "read")
    from dp.engine.collaboration import session_manager

    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(404, "Session not found")
    return {
        "session_id": session.session_id,
        "name": session.name,
        "created_at": session.created_at,
        "participants": session_manager.get_participants(session_id),
        "shared_sql": session.shared_sql,
        "query_history": session.query_history[-20:],
    }


@router.delete("/api/sessions/{session_id}")
def delete_session_endpoint(request: Request, session_id: str) -> dict:
    """Delete a collaboration session."""
    _require_permission(request, "write")
    from dp.engine.collaboration import session_manager

    if not session_manager.delete_session(session_id):
        raise HTTPException(404, "Session not found")
    return {"status": "deleted"}


@router.post("/api/sessions/{session_id}/query")
def session_query(
    request: Request,
    session_id: str,
    req: SessionQueryRequest,
    conn: DbConnReadOnly,
) -> dict:
    """Execute a query within a collaboration session and record in history."""
    _require_permission(request, "read")
    from dp.engine.collaboration import session_manager

    session = session_manager.get_session(session_id)
    if not session:
        raise HTTPException(404, "Session not found")

    start = time.time()
    try:
        result = conn.execute(req.sql)
        columns = (
            [desc[0] for desc in result.description] if result.description else []
        )
        rows = [[_serialize(v) for v in row] for row in result.fetchall()]
        duration_ms = int((time.time() - start) * 1000)

        entry = session_manager.add_query_result(
            session_id,
            req.user_id,
            req.sql,
            columns,
            rows,
            duration_ms,
        )
        return {
            "columns": columns,
            "rows": rows,
            "duration_ms": duration_ms,
            "history_id": entry["id"] if entry else None,
        }
    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        session_manager.add_query_result(
            session_id,
            req.user_id,
            req.sql,
            [],
            [],
            duration_ms,
            error=str(e),
        )
        return {"error": str(e), "duration_ms": duration_ms}


# --- WebSocket for collaboration ---


def register_websocket(app) -> None:
    """Register the WebSocket endpoint on the given FastAPI app.

    Called from app.py after all routers are included, because WebSocket
    endpoints are registered directly on the app instance.
    """
    try:
        from starlette.websockets import WebSocket, WebSocketDisconnect

        @app.websocket("/ws/session/{session_id}")
        async def websocket_session(
            websocket: WebSocket, session_id: str
        ) -> None:
            """WebSocket endpoint for real-time collaboration in a session."""
            from dp.engine.collaboration import (
                broadcast_to_session,
                session_manager,
            )

            await websocket.accept()

            user_id = websocket.query_params.get("user_id", "anonymous")
            display_name = websocket.query_params.get("display_name", user_id)

            session = session_manager.join_session(
                session_id, user_id, display_name, websocket
            )
            if not session:
                await websocket.send_json(
                    {"type": "error", "message": "Session not found"}
                )
                await websocket.close()
                return

            await broadcast_to_session(
                session_id,
                {
                    "type": "user_joined",
                    "user_id": user_id,
                    "display_name": display_name,
                    "participants": session_manager.get_participants(session_id),
                },
            )

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

                    if msg_type == "cursor":
                        session_manager.update_cursor(
                            session_id, user_id, data.get("position", {})
                        )
                        await broadcast_to_session(
                            session_id,
                            {
                                "type": "cursor",
                                "user_id": user_id,
                                "position": data.get("position", {}),
                            },
                        )
                    elif msg_type == "sql_update":
                        sql_content = data.get("sql", "")[:100_000]
                        session_manager.update_shared_sql(
                            session_id, sql_content
                        )
                        await broadcast_to_session(
                            session_id,
                            {
                                "type": "sql_update",
                                "user_id": user_id,
                                "sql": sql_content,
                            },
                        )
                    elif msg_type == "message":
                        await broadcast_to_session(
                            session_id,
                            {
                                "type": "message",
                                "user_id": user_id,
                                "text": data.get("text", "")[:10_000],
                            },
                        )

            except WebSocketDisconnect:
                pass
            finally:
                session_manager.leave_session(
                    session_id, user_id, websocket
                )
                await broadcast_to_session(
                    session_id,
                    {
                        "type": "user_left",
                        "user_id": user_id,
                        "participants": session_manager.get_participants(
                            session_id
                        ),
                    },
                )

    except ImportError:
        pass  # starlette WebSocket support not available
