from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from ..auth import require_user
from ..db import get_db
from ..models import User
from ..services.auth_service import AuthService
from ..services.cars_service import CarsService
from ..services.favorites_service import FavoritesService
from ..services.notification_service import NotificationService
from ..utils.country_map import resolve_display_country
from ..utils.localization import display_body, display_color
from ..utils.price_utils import price_without_util_note, resolve_public_display_price_rub
from ..utils.taxonomy import ru_body, translate_payload_value, normalize_color, ru_color
from ..utils.thumbs import resolve_thumbnail_url


router = APIRouter()


def _prepare_favorites(service: CarsService, favorites: list) -> list:
    service.refresh_visible_price_cache(favorites)
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
        car.display_price_rub = resolve_public_display_price_rub(
            getattr(car, "total_price_rub_cached", None),
            getattr(car, "price_rub_cached", None),
            calc_breakdown=getattr(car, "calc_breakdown_json", None),
            raw_price=getattr(car, "price", None),
            currency=getattr(car, "currency", None),
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


def _account_context(request: Request, user: User, db: Session, status=None) -> dict:
    service = CarsService(db)
    favs = _prepare_favorites(service, FavoritesService(db).list_cars(user))
    notif_svc = NotificationService(db)
    notifications = notif_svc.list_for_user(user.id, limit=20)
    notifications_by_id = {n.id: n for n in notifications}
    car_lookup: dict[int, dict] = {}
    car_ids: set[int] = set()
    for n in notifications:
        for cid in (n.attached_car_ids or []):
            try:
                car_ids.add(int(cid))
            except (TypeError, ValueError):
                continue
    if car_ids:
        from sqlalchemy import select as _select
        from ..models import Car as _Car

        for car in db.execute(_select(_Car).where(_Car.id.in_(car_ids))).scalars():
            title_parts = [car.brand or "", car.model or ""]
            car_lookup[car.id] = {
                "id": car.id,
                "title": " ".join(p for p in title_parts if p).strip() or f"#{car.id}",
                "subtitle": car.variant or "",
                "year": car.year,
                "url": f"/car/{car.id}",
            }
    notification_views = []
    for n in notifications:
        cars_for_n = []
        for cid in (n.attached_car_ids or []):
            try:
                info = car_lookup.get(int(cid))
            except (TypeError, ValueError):
                info = None
            if info:
                cars_for_n.append(info)
        notification_views.append({"row": n, "cars": cars_for_n})
    unread_count = sum(1 for n in notifications if not n.is_read)
    return {
        "request": request,
        "user": user,
        "status": status,
        "favorites": favs,
        "notifications": notification_views,
        "notifications_unread": unread_count,
    }


@router.get("/account")
def account_page(request: Request, user: User = Depends(require_user), db: Session = Depends(get_db)):
    templates = request.app.state.templates
    return templates.TemplateResponse("account/index.html", _account_context(request, user, db))


@router.post("/account/notifications/read")
def account_notifications_read(
    user: User = Depends(require_user),
    db: Session = Depends(get_db),
):
    NotificationService(db).mark_read(user_id=user.id)
    return RedirectResponse(url="/account#messages", status_code=302)


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
    return templates.TemplateResponse("account/index.html", _account_context(request, user, db, status=status))


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
            _account_context(request, user, db, status=status),
            status_code=400,
        )
    user.password_hash = auth._hash(new_password)
    db.add(user)
    db.commit()
    status = {"success": True, "message": "Пароль обновлён"}
    return templates.TemplateResponse("account/index.html", _account_context(request, user, db, status=status))
