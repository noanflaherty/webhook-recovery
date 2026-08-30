"""The FastAPI app: ingest, reads, and the built Vite bundle.

One container serves the whole product. The SPA is mounted *after* the API
router so ``/api/*`` always wins, and unknown paths fall through to
``index.html`` for client-side routing.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.requests import Request

from app.api.routes import router
from app.core.db import dispose_engine
from app.core.settings import get_settings

log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    log.info("api starting")
    yield
    await dispose_engine()
    log.info("api stopped")


def create_app() -> FastAPI:
    app = FastAPI(
        title="webhook-recovery",
        version="0.1.0",
        summary="Fair backlog burndown and consumer-defined replay policy after a provider outage.",
        lifespan=lifespan,
    )
    app.include_router(router)
    _mount_frontend(app)
    return app


def _mount_frontend(app: FastAPI) -> None:
    """Serve the built bundle, if there is one.

    Absent in local development, where Vite's dev server proxies to this process
    instead -- so a missing bundle is a logged fact, not an error.
    """
    dist = Path(get_settings().frontend_dist)
    index = dist / "index.html"
    if not index.is_file():
        log.warning("no frontend bundle at %s; serving API only", dist.resolve())
        return

    assets = dist / "assets"
    if assets.is_dir():
        app.mount("/assets", StaticFiles(directory=assets), name="assets")

    @app.get("/", include_in_schema=False)
    async def spa_root() -> FileResponse:
        return FileResponse(index)

    # response_model=None: the union return type is a Response choice, not a
    # schema, and FastAPI would otherwise try to validate it as one.
    @app.get("/{path:path}", include_in_schema=False, response_model=None)
    async def spa_catchall(request: Request, path: str) -> FileResponse | JSONResponse:
        # A miss under /api is a 404, not the SPA shell -- otherwise a typo'd
        # endpoint returns 200 and a page of HTML, which is a miserable thing to
        # debug from the frontend side.
        if path.startswith("api/"):
            return JSONResponse({"detail": "Not Found"}, status_code=404)
        candidate = dist / path
        if path and candidate.is_file() and dist.resolve() in candidate.resolve().parents:
            return FileResponse(candidate)
        return FileResponse(index)

    log.info("serving frontend bundle from %s", dist.resolve())


app = create_app()
