"""Live collaboration engine for multi-user query sessions.

Provides WebSocket-based real-time features:
- Shared query sessions where users see the same query + results
- Presence tracking (who is connected)
- Broadcast of query executions and results

Session lifecycle::

    1. Client connects via WebSocket to /ws/session/{session_id}
    2. Server tracks presence and broadcasts to all clients in the session
    3. Clients can submit queries, which are broadcast with results
    4. On disconnect, presence is removed and others are notified
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field

logger = logging.getLogger("dp.collaboration")


@dataclass
class Participant:
    """A single user in a collaboration session."""

    user_id: str
    display_name: str
    connected_at: float = field(default_factory=time.time)
    cursor_position: dict | None = None  # {line, column} in editor


@dataclass
class Session:
    """A live collaboration session."""

    session_id: str
    name: str
    created_at: float = field(default_factory=time.time)
    participants: dict[str, Participant] = field(default_factory=dict)
    query_history: list[dict] = field(default_factory=list)
    shared_sql: str = ""  # Current shared editor content


class SessionManager:
    """Manages all active collaboration sessions.

    Thread-safe via asyncio — all mutations happen in the event loop.
    """

    def __init__(self) -> None:
        self._sessions: dict[str, Session] = {}
        # websocket_id -> (session_id, websocket)
        self._connections: dict[str, tuple[str, object]] = {}
        self._max_sessions = 100
        self._max_history = 200
        self._max_sql_length = 100_000  # 100KB limit for shared SQL
        self._session_ttl = 86400  # 24 hours — stale sessions auto-evicted

    def create_session(self, name: str = "") -> Session:
        """Create a new collaboration session."""
        if len(self._sessions) >= self._max_sessions:
            self._evict_stale_sessions()
        if len(self._sessions) >= self._max_sessions:
            self._evict_empty_sessions()
        session_id = uuid.uuid4().hex[:12]
        # Sanitize session name to prevent XSS when rendered
        safe_name = (name or f"Session {session_id[:6]}")[:200]
        session = Session(session_id=session_id, name=safe_name)
        self._sessions[session_id] = session
        return session

    def get_session(self, session_id: str) -> Session | None:
        """Get a session by ID."""
        return self._sessions.get(session_id)

    def list_sessions(self) -> list[dict]:
        """List all active sessions with participant counts."""
        return [
            {
                "session_id": s.session_id,
                "name": s.name,
                "participants": len(s.participants),
                "created_at": s.created_at,
            }
            for s in self._sessions.values()
        ]

    def join_session(
        self,
        session_id: str,
        user_id: str,
        display_name: str,
        websocket: object,
    ) -> Session | None:
        """Add a participant to a session. Returns the session or None."""
        session = self._sessions.get(session_id)
        if not session:
            return None
        session.participants[user_id] = Participant(
            user_id=user_id,
            display_name=display_name,
        )
        ws_id = id(websocket)
        self._connections[str(ws_id)] = (session_id, websocket)
        return session

    def leave_session(self, session_id: str, user_id: str, websocket: object) -> None:
        """Remove a participant from a session."""
        session = self._sessions.get(session_id)
        if session:
            session.participants.pop(user_id, None)
        ws_id = id(websocket)
        self._connections.pop(str(ws_id), None)

    def delete_session(self, session_id: str) -> bool:
        """Delete a session entirely."""
        session = self._sessions.pop(session_id, None)
        if not session:
            return False
        # Clean up connections for this session
        to_remove = [
            ws_id for ws_id, (sid, _) in self._connections.items()
            if sid == session_id
        ]
        for ws_id in to_remove:
            self._connections.pop(ws_id, None)
        return True

    def add_query_result(
        self,
        session_id: str,
        user_id: str,
        sql: str,
        columns: list[str],
        rows: list[list],
        duration_ms: int,
        error: str | None = None,
    ) -> dict | None:
        """Record a query execution in the session history."""
        session = self._sessions.get(session_id)
        if not session:
            return None
        entry = {
            "id": uuid.uuid4().hex[:8],
            "user_id": user_id,
            "sql": sql,
            "columns": columns,
            "rows": rows,
            "duration_ms": duration_ms,
            "error": error,
            "timestamp": time.time(),
        }
        session.query_history.append(entry)
        if len(session.query_history) > self._max_history:
            session.query_history = session.query_history[-self._max_history:]
        return entry

    def update_shared_sql(self, session_id: str, sql: str) -> bool:
        """Update the shared SQL editor content."""
        session = self._sessions.get(session_id)
        if not session:
            return False
        # Enforce size limit to prevent memory exhaustion
        session.shared_sql = sql[:self._max_sql_length]
        return True

    def update_cursor(
        self,
        session_id: str,
        user_id: str,
        position: dict,
    ) -> bool:
        """Update a participant's cursor position."""
        session = self._sessions.get(session_id)
        if not session:
            return False
        participant = session.participants.get(user_id)
        if not participant:
            return False
        participant.cursor_position = position
        return True

    def get_participants(self, session_id: str) -> list[dict]:
        """Get all participants in a session."""
        session = self._sessions.get(session_id)
        if not session:
            return []
        return [
            {
                "user_id": p.user_id,
                "display_name": p.display_name,
                "connected_at": p.connected_at,
                "cursor_position": p.cursor_position,
            }
            for p in session.participants.values()
        ]

    def get_websockets_for_session(self, session_id: str) -> list[object]:
        """Get all WebSocket connections for a session."""
        return [
            ws for sid, ws in self._connections.values()
            if sid == session_id
        ]

    def _evict_empty_sessions(self) -> None:
        """Remove sessions with no participants (oldest first)."""
        empty = [
            sid for sid, s in self._sessions.items()
            if not s.participants
        ]
        for sid in empty:
            self._sessions.pop(sid, None)

    def _evict_stale_sessions(self) -> None:
        """Remove sessions older than _session_ttl with no participants."""
        now = time.time()
        stale = [
            sid for sid, s in self._sessions.items()
            if not s.participants and (now - s.created_at) > self._session_ttl
        ]
        for sid in stale:
            self._sessions.pop(sid, None)


# Global singleton — shared across the FastAPI app
session_manager = SessionManager()


async def broadcast_to_session(session_id: str, message: dict) -> None:
    """Send a JSON message to all WebSocket clients in a session."""
    websockets = session_manager.get_websockets_for_session(session_id)
    payload = json.dumps(message)
    for ws in websockets:
        try:
            await ws.send_text(payload)
        except Exception:
            pass  # Client may have disconnected
