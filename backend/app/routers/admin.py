from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from ..auth import require_admin
from ..db import get_db
from ..services.admin_service import AdminService
from ..services.cars_service import CarsService
from ..models import User


router = APIRouter()

PLACEMENTS = {
    "home_popular": "Главная — популярные",
    "home_recommended": "Главная — рекомендуемые",
    "catalog_popular": "Каталог — популярные",
    "catalog_recommended": "Каталог — рекомендуемые",
}

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
def admin_dashboard(request: Request, user: User = Depends(require_admin), db: Session = Depends(get_db)):
    templates = request.app.state.templates
    admin_svc = AdminService(db)
    cars_svc = CarsService(db)
    content = admin_svc.list_site_content(CONTENT_KEYS.keys())
    featured = {placement: admin_svc.list_featured(
        placement) for placement in PLACEMENTS}
    featured_cards = {
        placement: [
            {
                "id": fc.car_id,
                "title": f"{fc.car.brand or ''} {fc.car.model or ''}".strip(),
            }
            for fc in featured_list
            if fc.car
        ]
        for placement, featured_list in featured.items()
    }
    total_cars = cars_svc.total_cars()
    recent_cars = cars_svc.recent_with_thumbnails(limit=120)
    return templates.TemplateResponse(
        "admin/dashboard.html",
        {
            "request": request,
            "user": user,
            "content": content,
            "placements": PLACEMENTS,
            "featured": featured,
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
    placement: str = Form(...),
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
