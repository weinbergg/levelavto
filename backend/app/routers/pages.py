from typing import Dict, Any, Optional
from pathlib import Path

from fastapi import APIRouter, Request, Depends, Query, Form
import os
import smtplib
from email.mime.text import MIMEText
from ..db import get_db
from ..services.cars_service import CarsService
from ..services.content_service import ContentService
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, func, exists
from ..models import Car, Source, CarImage
from ..auth import get_current_user


router = APIRouter()


def _home_context(request: Request, service: CarsService, db: Session, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    brand_stats = service.brand_stats()
    body_type_stats = service.body_type_stats()
    top_models = service.top_models_by_brand(max_brands=6, top_n=6)
    highlights = service.featured_for(
        "home_popular", limit=8, fallback_limit=8)
    recommended = service.featured_for(
        "home_recommended", limit=8, fallback_limit=4)
    content = ContentService(db).content_map(
        ["hero_title", "hero_subtitle", "hero_note"])

    # brand logos: map brands that have logo files in static/img/brand-logos
    static_root = Path(__file__).resolve().parent.parent / \
        "static" / "img" / "brand-logos"

    def _slug(brand: str) -> str:
        raw = brand.lower().strip()
        # manual fixes for known variants
        manual = {
            "mercedes": "mercedes-benz",
            "mercedes-benz": "mercedes-benz",
            "mercedes benz": "mercedes-benz",
            "land": "land-rover",
            "land rover": "land-rover",
            "mini": "mini",
            "ds": "ds-automobiles",
            "ds automobiles": "ds-automobiles",
            "citroen": "citro-n",
            "citroën": "citro-n",
        }
        if raw in manual:
            return manual[raw]
        safe = "".join(ch if ch.isalnum() or ch in (
            " ", "-", "_") else "" for ch in raw)
        safe = safe.replace(" ", "-")
        return safe

    brand_logos = []
    seen = set()
    for b in brand_stats:
        slug = _slug(b["brand"])
        logo_path = static_root / f"{slug}.webp"
        if logo_path.exists():
            brand_logos.append(
                {
                    "brand": b["brand"],
                    "count": b["count"],
                    "logo": f"/static/img/brand-logos/{slug}.webp",
                }
            )
            seen.add(b["brand"])

    context = {
        "request": request,
        "user": getattr(request.state, "user", None),
        "total_cars": service.total_cars(),
        "brands": service.brands(),
        "colors": service.colors(),
        "brand_stats": brand_stats,
        "brand_logos": brand_logos,
        "body_type_stats": body_type_stats,
        "top_models": top_models,
        "highlighted_cars": highlights,
        "recommended_cars": recommended,
        "content": content,
    }
    if extra:
        context.update(extra)
    return context


@router.get("/")
def index(request: Request, db=Depends(get_db), user=Depends(get_current_user)):
    templates = request.app.state.templates
    service = CarsService(db)
    return templates.TemplateResponse("home.html", _home_context(request, service, db))


@router.post("/lead")
def submit_lead(
    request: Request,
    name: str = Form(...),
    phone: str = Form(...),
    email: str = Form(None),
    preferred: str = Form(None),
    price_range: str = Form(None),
    comment: str = Form(None),
    db: Session = Depends(get_db),
    user=Depends(get_current_user),
):
    templates = request.app.state.templates
    service = CarsService(db)
    errors = []
    if not name.strip():
        errors.append("Введите имя")
    if not phone.strip():
        errors.append("Введите телефон")

    status = {"success": False, "message": ""}
    if errors:
        status["message"] = "; ".join(errors)
    else:
        # send email if configured
        content_service = ContentService(db)
        lead_email = content_service.content_map().get("lead_email") or "info@levelavto.ru"
        body = (
            f"Заявка с сайта\n"
            f"Имя: {name}\n"
            f"Телефон: {phone}\n"
            f"Email: {email or '—'}\n"
            f"Предпочтения: {preferred or '—'}\n"
            f"Бюджет: {price_range or '—'}\n"
            f"Комментарий: {comment or '—'}\n"
        )
        sent = False
        try:
            host = os.environ.get("EMAIL_HOST")
            port = int(os.environ.get("EMAIL_PORT", "587"))
            user = os.environ.get("EMAIL_HOST_USER")
            pwd = os.environ.get("EMAIL_HOST_PASSWORD")
            mail_from = os.environ.get("EMAIL_FROM", "info@levelavto.ru")
            if host and user and pwd:
                msg = MIMEText(body, "plain", "utf-8")
                msg["Subject"] = "Заявка с сайта Level Avto"
                msg["From"] = mail_from
                msg["To"] = lead_email
                with smtplib.SMTP(host, port, timeout=10) as smtp:
                    smtp.starttls()
                    smtp.login(user, pwd)
                    smtp.sendmail(mail_from, [lead_email], msg.as_string())
                sent = True
        except Exception as e:
            print("[LEAD][email_failed]", e)
        # always log to stdout
        print("[LEAD]", {"name": name, "phone": phone, "email": email, "preferred": preferred, "price_range": price_range, "comment": comment, "sent": sent})
        status["success"] = True
        status["message"] = "Спасибо! Мы свяжемся с вами в ближайшее время."
    extra = {"lead_status": status}
    return templates.TemplateResponse("home.html", _home_context(request, service, db, extra))


@router.get("/catalog")
def catalog_page(request: Request, db=Depends(get_db), user=Depends(get_current_user)):
    templates = request.app.state.templates
    service = CarsService(db)
    brands = service.brands()
    countries = ["DE", "KR", "RU"]
    raw_colors = service.colors()

    palette = {
        "black": "#1d1d1f",
        "white": "#f8f8f8",
        "gray": "#888888",
        "silver": "#c0c0c0",
        "red": "#e03a3a",
        "blue": "#2d7dd2",
        "green": "#4caf50",
        "yellow": "#f4c430",
        "orange": "#f97316",
        "brown": "#8b5a2b",
        "beige": "#d7c4a5",
        "purple": "#8b5cf6",
        "violet": "#7c3aed",
        "gold": "#d4af37",
        "pink": "#f472b6",
    }
    labels_ru = {
        "black": "Чёрный",
        "white": "Белый",
        "gray": "Серый",
        "silver": "Серебристый",
        "red": "Красный",
        "blue": "Синий",
        "green": "Зелёный",
        "yellow": "Жёлтый",
        "orange": "Оранжевый",
        "brown": "Коричневый",
        "beige": "Бежевый",
        "purple": "Фиолетовый",
        "violet": "Фиолетовый",
        "gold": "Золотой",
        "pink": "Розовый",
    }

    def normalize_color(val: str) -> str:
        if not val:
            return ""
        t = val.lower().strip()
        mapping = {
            "weiss": "white",
            "weiß": "white",
            "white": "white",
            "blanc": "white",
            "black": "black",
            "schwarz": "black",
            "gray": "gray",
            "grey": "gray",
            "grau": "gray",
            "сер": "gray",
            "silver": "silver",
            "silber": "silver",
            "red": "red",
            "rot": "red",
            "blau": "blue",
            "blue": "blue",
            "navy": "blue",
            "gruen": "green",
            "grün": "green",
            "green": "green",
            "gelb": "yellow",
            "yellow": "yellow",
            "orange": "orange",
            "braun": "brown",
            "brown": "brown",
            "beige": "beige",
            "violett": "violet",
            "violet": "violet",
            "purple": "purple",
            "gold": "gold",
            "golden": "gold",
            "pink": "pink",
            "rosa": "pink",
        }
        for key, norm in mapping.items():
            if key in t:
                return norm
        simple = "".join(ch for ch in t if ch.isalpha())
        return simple or t

    seen_colors = set()
    color_options = []
    for raw in raw_colors:
        key = normalize_color(raw)
        if not key or key not in palette or key in seen_colors:
            continue
        seen_colors.add(key)
        color_options.append(
            {
                "value": key,
                "key": key,
                "label": labels_ru.get(key, key.title()),
                "hex": palette.get(key, "#8a8a8a"),
            }
        )

    brand_filter = request.query_params.get("brand")
    featured_popular = service.featured_for(
        "catalog_popular", limit=6, fallback_limit=3)
    featured_recommended = service.featured_for(
        "catalog_recommended", limit=6, fallback_limit=3)
    if brand_filter:
        featured_popular = [c for c in featured_popular if (
            c.brand or "").lower() == brand_filter.lower()]
        featured_recommended = [c for c in featured_recommended if (
            c.brand or "").lower() == brand_filter.lower()]

    return templates.TemplateResponse(
        "catalog.html",
        {
            "request": request,
            "user": getattr(request.state, "user", None),
            "brands": brands,
            "countries": countries,
            "colors": color_options,
            "featured_popular": featured_popular,
            "featured_recommended": featured_recommended,
        },
    )


@router.get("/car/{car_id}")
def car_detail_page(car_id: int, request: Request, db=Depends(get_db), user=Depends(get_current_user)):
    templates = request.app.state.templates
    service = CarsService(db)
    car = service.get_car(car_id)
    return templates.TemplateResponse("car_detail.html", {"request": request, "car": car, "user": getattr(request.state, "user", None)})


@router.get("/debug/parsing/{source_key}")
def debug_parsing_page(
    source_key: str,
    request: Request,
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    only_with_images: bool = Query(default=True),
    db: Session = Depends(get_db),
):
    templates = request.app.state.templates
    # ensure source exists
    src = db.execute(select(Source).where(
        Source.key == source_key)).scalar_one_or_none()
    if not src:
        return templates.TemplateResponse(
            "base.html",
            {
                "request": request,
                "content": f"Unknown source: {source_key}",
            },
            status_code=404,
        )
    conditions = [Car.source_id == src.id]
    if only_with_images:
        conditions.append(
            exists(select(CarImage.id).where(CarImage.car_id == Car.id))
        )
    where_expr = and_(*conditions)
    total = db.execute(select(func.count()).select_from(
        Car).where(where_expr)).scalar_one()
    cars = (
        db.execute(
            select(Car).where(where_expr).order_by(
                Car.id.desc()).offset(offset).limit(limit)
        )
        .scalars()
        .all()
    )
    # lazy-load images per car (simple, OK for debug)
    return templates.TemplateResponse(
        "debug_parsing.html",
        {
            "request": request,
            "source_key": source_key,
            "cars": cars,
            "limit": limit,
            "offset": offset,
            "total": total,
            "only_with_images": only_with_images,
        },
    )


@router.get("/debug/catalog/{source_key}")
def debug_catalog_alias(
    source_key: str,
    request: Request,
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    only_with_images: bool = Query(default=True),
    db: Session = Depends(get_db),
):
    # Alias to debug_parsing_page for convenience
    return debug_parsing_page(
        source_key=source_key,
        request=request,
        limit=limit,
        offset=offset,
        only_with_images=only_with_images,
        db=db,
    )
