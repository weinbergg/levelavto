from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from ..auth import require_user
from ..db import get_db
from ..models import User
from ..services.auth_service import AuthService
from ..services.favorites_service import FavoritesService
from ..utils.country_map import resolve_display_country
from ..utils.localization import display_body, display_color
from ..utils.price_utils import display_price_rub, price_without_util_note
from ..utils.taxonomy import ru_body, translate_payload_value, normalize_color, ru_color
from ..utils.thumbs import resolve_thumbnail_url


router = APIRouter()


def _prepare_favorites(favorites: list) -> list:
    for car in favorites:
        code, label = resolve_display_country(car)
        car.display_country_code = code
        car.display_country_label = label
        car.display_engine_type = translate_payload_value("engine_type", getattr(car, "engine_type", None)) or car.engine_type
        car.display_transmission = translate_payload_value("transmission", getattr(car, "transmission", None)) or car.transmission
        car.display_body_type = ru_body(getattr(car, "body_type", None)) or display_body(getattr(car, "body_type", None)) or car.body_type
        normalized_color = normalize_color(getattr(car, "color", None))
        car.display_color = (
            ru_color(getattr(car, "color", None))
            or display_color(getattr(car, "color", None))
            or (ru_color(normalized_color) if normalized_color else None)
            or (display_color(normalized_color) if normalized_color else None)
            or car.color
        )
        car.display_price_rub = display_price_rub(
            getattr(car, "total_price_rub_cached", None),
            getattr(car, "price_rub_cached", None),
            allow_price_fallback=True,
        )
        car.price_note = price_without_util_note(
            display_price=car.display_price_rub,
            total_price_rub_cached=getattr(car, "total_price_rub_cached", None),
            calc_breakdown=getattr(car, "calc_breakdown_json", None),
            country=getattr(car, "country", None),
        )
        thumb = resolve_thumbnail_url(
            getattr(car, "thumbnail_url", None),
            getattr(car, "thumbnail_local_path", None),
        )
        if thumb:
            car.thumbnail_url = thumb
        if not getattr(car, "thumbnail_url", None):
            car.thumbnail_url = "/static/img/no-photo.svg"
    return favorites


@router.get("/account")
def account_page(request: Request, user: User = Depends(require_user), db: Session = Depends(get_db)):
    templates = request.app.state.templates
    favs = _prepare_favorites(FavoritesService(db).list_cars(user))
    return templates.TemplateResponse("account/index.html", {"request": request, "user": user, "status": None, "favorites": favs})


@router.post("/account/profile")
def update_profile(
    request: Request,
    full_name: str = Form(None),
    user: User = Depends(require_user),
    db: Session = Depends(get_db),
):
    templates = request.app.state.templates
    user.full_name = full_name
    db.add(user)
    db.commit()
    status = {"success": True, "message": "Профиль обновлён"}
    favs = _prepare_favorites(FavoritesService(db).list_cars(user))
    return templates.TemplateResponse("account/index.html", {"request": request, "user": user, "status": status, "favorites": favs})


@router.post("/account/password")
def update_password(
    request: Request,
    current_password: str = Form(...),
    new_password: str = Form(...),
    user: User = Depends(require_user),
    db: Session = Depends(get_db),
):
    templates = request.app.state.templates
    auth = AuthService(db)
    if not auth.verify_password(user.password_hash, current_password):
        status = {"success": False, "message": "Текущий пароль неверный"}
        return templates.TemplateResponse(
            "account/index.html",
            {"request": request, "user": user, "status": status},
            status_code=400,
        )
    user.password_hash = auth._hash(new_password)
    db.add(user)
    db.commit()
    status = {"success": True, "message": "Пароль обновлён"}
    favs = _prepare_favorites(FavoritesService(db).list_cars(user))
    return templates.TemplateResponse("account/index.html", {"request": request, "user": user, "status": status, "favorites": favs})

