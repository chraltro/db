"""FastAPI backend for the web UI.

This module creates the FastAPI application and assembles all route modules.
Route handlers are defined in dp.server.routes.* submodules.
Shared dependencies (DB injection, auth, caching) live in dp.server.deps.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

# ---------------------------------------------------------------------------
# Global state — set by CLI before starting uvicorn.
# Tests also set these directly via `import dp.server.app as server_app`.
# deps.py reads these lazily via function-level imports to avoid circular deps.
# ---------------------------------------------------------------------------

PROJECT_DIR: Path = Path.cwd()
AUTH_ENABLED: bool = False  # Set by CLI --auth flag
ACTIVE_ENV: str | None = None  # Set by CLI --env flag

# ---------------------------------------------------------------------------
# Create the FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI(title="dp", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Include all route modules
# ---------------------------------------------------------------------------

from dp.server.routes.auth import router as auth_router  # noqa: E402
from dp.server.routes.files import router as files_router  # noqa: E402
from dp.server.routes.models import router as models_router  # noqa: E402
from dp.server.routes.dag import router as dag_router  # noqa: E402
from dp.server.routes.query import router as query_router  # noqa: E402
from dp.server.routes.notebooks import router as notebooks_router  # noqa: E402
from dp.server.routes.connectors import router as connectors_router  # noqa: E402
from dp.server.routes.pipeline import router as pipeline_router  # noqa: E402
from dp.server.routes.quality import router as quality_router  # noqa: E402
from dp.server.routes.catalog import router as catalog_router  # noqa: E402
from dp.server.routes.collaboration import (  # noqa: E402
    register_websocket,
    router as collaboration_router,
)
from dp.server.routes.lint import router as lint_router  # noqa: E402

app.include_router(auth_router)
app.include_router(files_router)
app.include_router(models_router)
app.include_router(dag_router)
app.include_router(query_router)
app.include_router(notebooks_router)
app.include_router(connectors_router)
app.include_router(pipeline_router)
app.include_router(quality_router)
app.include_router(catalog_router)
app.include_router(collaboration_router)
app.include_router(lint_router)

# Register WebSocket endpoint (can't use APIRouter for WebSocket)
register_websocket(app)

# ---------------------------------------------------------------------------
# Backward compatibility — expose helpers used by tests
# ---------------------------------------------------------------------------

from dp.server.routes.notebooks import _resolve_notebook  # noqa: E402, F401

# ---------------------------------------------------------------------------
# Serve frontend
# ---------------------------------------------------------------------------

_FRONTEND_DIR = Path(__file__).parent.parent.parent.parent / "frontend" / "dist"

# Reserved paths that should NOT be caught by the SPA catch-all.
_RESERVED_PATHS = {"docs", "redoc", "openapi.json"}


@app.get("/", response_class=HTMLResponse)
@app.get("/{path:path}", response_class=HTMLResponse)
def serve_frontend(path: str = "") -> HTMLResponse:
    """Serve the frontend SPA (skips /docs, /redoc, /openapi.json)."""
    from fastapi import HTTPException

    if path in _RESERVED_PATHS:
        raise HTTPException(404, "Not found")

    file_path = _FRONTEND_DIR / path
    if file_path.is_file():
        content_type = {
            ".html": "text/html",
            ".js": "application/javascript",
            ".css": "text/css",
            ".svg": "image/svg+xml",
            ".png": "image/png",
            ".ico": "image/x-icon",
        }.get(file_path.suffix, "application/octet-stream")
        return HTMLResponse(content=file_path.read_bytes(), media_type=content_type)

    index = _FRONTEND_DIR / "index.html"
    if index.exists():
        return HTMLResponse(content=index.read_text())
    return HTMLResponse(
        content="<h1>dp</h1><p>Frontend not built. Run <code>cd frontend && npm run build</code></p>",
        status_code=200,
    )
