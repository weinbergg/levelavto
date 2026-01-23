from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from .config import settings
import logging
import time
import os
import uuid
from .routers.pages import router as pages_router
from .routers.catalog import router as catalog_router
from .routers.auth import router as auth_router
from .routers.admin import router as admin_router
from .routers.account import router as account_router
from .routers.favorites import router as favorites_router
from .routers.calculator import router as calc_router
from pathlib import Path


def create_app() -> FastAPI:
    app = FastAPI(title="Auto Dealer Catalog", version="0.1.0")
    logger = logging.getLogger(__name__)

    static_dir = Path(__file__).resolve().parent / "static"
    templates_dir = Path(__file__).resolve().parent / "templates"
    media_dir = Path(__file__).resolve().parents[2] / "фото-видео"

    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
    if media_dir.exists():
        app.mount("/media", StaticFiles(directory=str(media_dir)), name="media")
    app.state.templates = Jinja2Templates(directory=str(templates_dir))
    app.add_middleware(SessionMiddleware, secret_key=settings.APP_SECRET)

    @app.middleware("http")
    async def timing_middleware(request: Request, call_next):
        t0 = time.perf_counter()
        req_id = uuid.uuid4().hex[:8]
        response = await call_next(request)
        total = time.perf_counter() - t0
        if os.environ.get("CAR_API_TIMING", "0") == "1":
            parts = getattr(request.state, "api_parts", None)
            if parts:
                logger.info(
                    "req_timing id=%s path=%s total=%.3f list=%.3f images=%.3f serialize=%.3f items=%s",
                    req_id,
                    request.url.path,
                    total,
                    parts.get("list", 0.0),
                    parts.get("images", 0.0),
                    parts.get("serialize", 0.0),
                    parts.get("items", 0),
                )
            else:
                logger.info(
                    "req_timing id=%s path=%s total=%.3f",
                    req_id,
                    request.url.path,
                    total,
                )
        response.headers["X-Process-Time"] = f"{total:.3f}"
        if os.environ.get("HTML_TIMING", "0") == "1" and request.url.path in {"/", "/catalog"}:
            parts = getattr(request.state, "html_parts", None) or {}
            logger.info(
                "html_timing id=%s path=%s total_ms=%.1f parts=%s",
                req_id,
                request.url.path,
                total * 1000,
                parts,
            )
        return response

    app.include_router(pages_router)
    app.include_router(auth_router)
    app.include_router(account_router)
    app.include_router(admin_router)
    app.include_router(favorites_router)
    app.include_router(catalog_router, prefix="/api", tags=["catalog"])
    app.include_router(calc_router)

    @app.get("/health", include_in_schema=False)
    def healthcheck():
        return {"status": "ok"}

    return app


app = create_app()
