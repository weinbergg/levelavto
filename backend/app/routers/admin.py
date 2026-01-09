from fastapi import APIRouter, Depends, Form, Request, Query
from fastapi.responses import RedirectResponse
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from ..auth import require_admin
from ..db import get_db
from ..services.admin_service import AdminService
from ..services.cars_service import CarsService
from ..models import User


router = APIRouter()

RECOMMENDED_PLACEMENT = "recommended"
LEGACY_PLACEMENTS = ["home_recommended", "catalog_recommended"]

CONTENT_KEYS = {
    "hero_title": "Заголовок на главной",
    "hero_subtitle": "Подзаголовок на главной",
    "hero_note": "Текст примечания в hero",
    "contact_phone": "Телефон",
    "contact_email": "Email",
    "contact_address": "Адрес",
    "contact_tg": "Telegram",
    "contact_wa": "WhatsApp",
    "contact_ig": "Instagram",
}


@router.get("/admin")
def admin_dashboard(
    request: Request,
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    templates = request.app.state.templates
    admin_svc = AdminService(db)
    cars_svc = CarsService(db)
    content = admin_svc.list_site_content(CONTENT_KEYS.keys())
    featured = admin_svc.list_featured(RECOMMENDED_PLACEMENT)
    if not featured:
        # миграция со старых ключей
        legacy = []
        for p in LEGACY_PLACEMENTS:
            legacy.extend(admin_svc.list_featured(p))
        seen = set()
        merged = []
        for fc in legacy:
            if fc.car_id in seen:
                continue
            seen.add(fc.car_id)
            merged.append(fc)
        featured = merged
    featured_cards = [
        {
            "id": fc.car_id,
            "title": f"{fc.car.brand or ''} {fc.car.model or ''}".strip(),
        }
        for fc in featured if fc.car
    ]
    total_cars = cars_svc.total_cars()
    recent_cars = cars_svc.recent_with_thumbnails(limit=40)
    return templates.TemplateResponse(
        "admin/dashboard.html",
        {
            "request": request,
            "user": user,
            "content": content,
            "featured": featured_cards,
            "featured_cards": featured_cards,
            "total_cars": total_cars,
            "recent_cars": recent_cars,
        },
    )


@router.post("/admin/content")
def update_content(
    request: Request,
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
    hero_title: str = Form(""),
    hero_subtitle: str = Form(""),
    hero_note: str = Form(""),
    contact_phone: str = Form(""),
    contact_email: str = Form(""),
    contact_address: str = Form(""),
    contact_tg: str = Form(""),
    contact_wa: str = Form(""),
    contact_ig: str = Form(""),
):
    admin_svc = AdminService(db)
    admin_svc.set_site_content(
        {
            "hero_title": hero_title,
            "hero_subtitle": hero_subtitle,
            "hero_note": hero_note,
            "contact_phone": contact_phone,
            "contact_email": contact_email,
            "contact_address": contact_address,
            "contact_tg": contact_tg,
            "contact_wa": contact_wa,
            "contact_ig": contact_ig,
        }
    )
    return RedirectResponse(url="/admin", status_code=302)


@router.post("/admin/featured")
def update_featured(
    request: Request,
    placement: str = Form(RECOMMENDED_PLACEMENT),
    car_ids: str = Form(""),
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    admin_svc = AdminService(db)
    ids_clean = []
    for part in car_ids.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids_clean.append(int(part))
        except ValueError:
            continue
    admin_svc.set_featured(placement, ids_clean)
    return RedirectResponse(url="/admin", status_code=302)


@router.get("/admin/featured/search")
def search_featured(
    q: str = Query(default=None),
    user: User = Depends(require_admin),
    db: Session = Depends(get_db),
):
    cars_svc = CarsService(db)
    results = cars_svc.search_featured_candidates(q or "", limit=20) if q else []
    payload = [
        {
            "id": c.id,
            "title": f"{c.brand or ''} {c.model or ''} {c.year or ''}".strip(),
        }
        for c in results
    ]
    return JSONResponse(payload)
