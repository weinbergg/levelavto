from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from .config import settings
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

    static_dir = Path(__file__).resolve().parent / "static"
    templates_dir = Path(__file__).resolve().parent / "templates"
    media_dir = Path(__file__).resolve().parents[2] / "фото-видео"

    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
    if media_dir.exists():
        app.mount("/media", StaticFiles(directory=str(media_dir)), name="media")
    app.state.templates = Jinja2Templates(directory=str(templates_dir))
    app.add_middleware(SessionMiddleware, secret_key=settings.APP_SECRET)

    app.include_router(pages_router)
    app.include_router(auth_router)
    app.include_router(account_router)
    app.include_router(admin_router)
    app.include_router(favorites_router)
    app.include_router(catalog_router, prefix="/api", tags=["catalog"])
    app.include_router(calc_router)
    return app


app = create_app()
