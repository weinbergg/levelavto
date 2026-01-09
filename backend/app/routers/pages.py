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
from urllib.parse import quote
from ..utils.localization import display_region, display_body, display_color
from ..utils.taxonomy import ru_body, ru_color


router = APIRouter()
RECOMMENDED_PLACEMENT = "recommended"


def _home_context(request: Request, service: CarsService, db: Session, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    brand_stats = service.brand_stats()
    body_type_stats = service.body_type_stats()
    top_models = service.top_models_by_brand(max_brands=6, top_n=6)
    recommended = service.featured_for(
        RECOMMENDED_PLACEMENT, limit=12, fallback_limit=4)
    if not recommended:
        # обратная совместимость со старыми ключами
        recommended = service.featured_for(
            "home_recommended", limit=12, fallback_limit=4)
    if not recommended:
        recommended = service.featured_for(
            "catalog_recommended", limit=12, fallback_limit=4)
    content = ContentService(db).content_map(
        [
            "hero_title",
            "hero_subtitle",
            "hero_note",
            "contact_phone",
            "contact_email",
            "contact_address",
            "contact_tg",
            "contact_wa",
            "contact_ig",
        ])
    fx_rates = service.get_fx_rates() or {}
    # медиа лежат рядом с корнем проекта: /code/фото-видео
    media_root = Path(__file__).resolve().parents[3] / "фото-видео"
    video_dir = media_root / "видео"
    car_photos_dir = media_root / "машины"

    hero_videos = []
    if video_dir.exists():
        prefix = video_dir.name
        for p in sorted(video_dir.iterdir()):
            if p.suffix.lower() in {".mp4", ".mov", ".webm"}:
                hero_videos.append(f"/media/{prefix}/{p.name}")

    collage_images = []
    thumb_dir = media_root / "машины_thumbs"
    if car_photos_dir.exists():
        thumb_prefix = thumb_dir.name
        orig_prefix = car_photos_dir.name

        def build_url(prefix: str, name: str) -> str:
            safe_name = name.replace("\u00a0", " ")
            return f"/media/{prefix}/{quote(safe_name)}"

        for p in sorted(car_photos_dir.iterdir()):
            if p.name.startswith("."):
                continue
            if p.suffix.lower() not in {".jpg", ".jpeg", ".webp", ".png"}:
                continue
            base = p.stem
            t320 = thumb_dir / f"{base}__w320.webp"
            t640 = thumb_dir / f"{base}__w640.webp"
            has_thumb = t320.exists()
            src = build_url(thumb_prefix if has_thumb else orig_prefix,
                            t320.name if has_thumb else p.name)
            srcset_parts = []
            if has_thumb:
                srcset_parts.append(f"{build_url(thumb_prefix, t320.name)} 320w")
                if t640.exists():
                    srcset_parts.append(f"{build_url(thumb_prefix, t640.name)} 640w")
            collage_images.append({
                "src": src,
                "srcset": ", ".join(srcset_parts),
                "width": 320,
                "height": 240,
                "fallback": build_url(orig_prefix, p.name),
            })

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
        "recommended_cars": recommended,
        "content": content,
        "fx_rates": fx_rates,
        "hero_videos": hero_videos,
        "collage_images": collage_images,
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
        lead_email = content_service.content_map().get(
            "lead_email") or "info@levelavto.ru"
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
        print("[LEAD]", {"name": name, "phone": phone, "email": email, "preferred": preferred,
              "price_range": price_range, "comment": comment, "sent": sent})
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
    contact_content = ContentService(db).content_map(
        [
            "contact_phone",
            "contact_email",
            "contact_address",
            "contact_tg",
            "contact_wa",
            "contact_ig",
        ])

    palette = {
        "black": "#1d1d1f",
        "white": "#f8f8f8",
        "gray": "#888888",
        "dark_gray": "#5f6570",
        "graphite": "#4b4f56",
        "silver": "#c0c0c0",
        "red": "#e03a3a",
        "blue": "#2d7dd2",
        "navy": "#1f3b73",
        "green": "#4caf50",
        "teal": "#14b8a6",
        "yellow": "#f4c430",
        "orange": "#f97316",
        "brown": "#8b5a2b",
        "beige": "#d7c4a5",
        "purple": "#8b5cf6",
        "violet": "#7c3aed",
        "gold": "#d4af37",
        "pink": "#f472b6",
        "light_blue": "#6ab8ff",
        "champagne": "#e6d4b3",
        "ivory": "#f6efe2",
    }
    labels_ru = {
        "black": "Чёрный",
        "white": "Белый",
        "gray": "Серый",
        "dark_gray": "Тёмно-серый",
        "graphite": "Графит",
        "silver": "Серебристый",
        "red": "Красный",
        "blue": "Синий",
        "light_blue": "Голубой",
        "green": "Зелёный",
        "teal": "Бирюзовый",
        "yellow": "Жёлтый",
        "orange": "Оранжевый",
        "brown": "Коричневый",
        "beige": "Бежевый",
        "purple": "Фиолетовый",
        "violet": "Фиолетовый",
        "gold": "Золотой",
        "pink": "Розовый",
        "champagne": "Шампань",
        "ivory": "Айвори",
    }

    seen_colors = set()
    color_options = []
    for raw in raw_colors:
        key = raw.get("value") if isinstance(raw, dict) else None
        label = raw.get("label") if isinstance(raw, dict) else None
        cnt = raw.get("count", 0) if isinstance(raw, dict) else 0
        if not key or key in seen_colors or cnt <= 0:
            continue
        seen_colors.add(key)
        color_options.append(
            {
                "value": key,
                "key": key,
                "label": label or key.title(),
                "hex": palette.get(key, ""),
                "count": cnt,
            }
        )

    brand_filter = request.query_params.get("brand")
    featured_recommended = service.featured_for(
        RECOMMENDED_PLACEMENT, limit=12, fallback_limit=6)
    if not featured_recommended:
        featured_recommended = service.featured_for(
            "catalog_recommended", limit=12, fallback_limit=6)
    if not featured_recommended:
        featured_recommended = service.featured_for(
            "home_recommended", limit=12, fallback_limit=6)
    # удаляем дубли по id
    seen_ids = set()
    dedup = []
    for c in featured_recommended:
        if c.id in seen_ids:
            continue
        dedup.append(c)
        seen_ids.add(c.id)
    featured_recommended = dedup
    if brand_filter:
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
            "featured_recommended": featured_recommended,
            "fx_rates": service.get_fx_rates() or {},
            "content": contact_content,
            "contact_phone": contact_content.get("contact_phone"),
            "contact_email": contact_content.get("contact_email"),
            "contact_address": contact_content.get("contact_address"),
            "contact_tg": contact_content.get("contact_tg"),
            "contact_wa": contact_content.get("contact_wa"),
            "contact_ig": contact_content.get("contact_ig"),
        },
    )


@router.get("/car/{car_id}")
def car_detail_page(car_id: int, request: Request, db=Depends(get_db), user=Depends(get_current_user)):
    templates = request.app.state.templates
    service = CarsService(db)
    car = service.get_car(car_id)
    if car and car.source:
        src_key = car.source.key or ""
        car.display_region = display_region(src_key) or car.country
    if car:
        car.display_body_type = ru_body(getattr(car, "body_type", None)) or display_body(getattr(car, "body_type", None)) or car.body_type
        car.display_color = ru_color(getattr(car, "color", None)) or display_color(getattr(car, "color", None)) or car.color
    return templates.TemplateResponse("car_detail.html", {"request": request, "car": car, "user": getattr(request.state, "user", None)})


@router.get("/privacy")
def privacy_page(request: Request):
    templates = request.app.state.templates
    return templates.TemplateResponse("privacy.html", {"request": request})


@router.get("/calculator")
def calculator_page():
    from fastapi import HTTPException
    raise HTTPException(status_code=404, detail="not found")


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
