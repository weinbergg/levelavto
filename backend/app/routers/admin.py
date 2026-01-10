from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from ..auth import require_admin
from ..db import get_db
from ..services.admin_service import AdminService
from ..services.cars_service import CarsService
from ..models import User


router = APIRouter()

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
    total_cars = cars_svc.total_cars()
    return templates.TemplateResponse(
        "admin/dashboard.html",
        {
            "request": request,
            "user": user,
            "content": content,
            "total_cars": total_cars,
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

