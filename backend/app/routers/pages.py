from typing import Dict, Any, Optional, List
from pathlib import Path

from fastapi import APIRouter, Request, Depends, Query, Form
import os
import smtplib
import random
import math
from email.mime.text import MIMEText
from ..db import get_db
from ..services.cars_service import CarsService
from ..utils.recommended_config import load_config
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
MONTHS_RU = [
    "январь",
    "февраль",
    "март",
    "апрель",
    "май",
    "июнь",
    "июль",
    "август",
    "сентябрь",
    "октябрь",
    "ноябрь",
    "декабрь",
]

BASIC_COLORS = [
    "white",
    "black",
    "gray",
    "silver",
    "blue",
    "red",
    "green",
    "orange",
    "yellow",
    "brown",
    "beige",
    "purple",
    "pink",
]


def _range_steps(max_val: Optional[float], base_step: int, min_val: int, max_options: int) -> List[int]:
    if not max_val or max_val <= 0:
        max_val = min_val
    try:
        max_int = int(float(max_val))
    except (TypeError, ValueError):
        max_int = min_val
    step = base_step
    last = int(math.ceil(max_int / step) * step)
    start = int(math.ceil(min_val / step) * step)
    values = list(range(start, last + step, step))
    if len(values) > max_options:
        factor = max(2, int(math.ceil(len(values) / max_options)))
        step *= factor
        last = int(math.ceil(max_int / step) * step)
        values = list(range(step, last + step, step))
    return values


def _build_filter_context(service: CarsService, db: Session) -> Dict[str, Any]:
    regions = service.available_regions()
    reg_years = (
        db.execute(
            select(func.distinct(Car.registration_year))
            .where(Car.is_available.is_(True), Car.registration_year.is_not(None))
            .order_by(Car.registration_year.desc())
        )
        .scalars()
        .all()
    )
    reg_years = [y for y in reg_years if y]
    reg_months = (
        [{"value": i + 1, "label": MONTHS_RU[i]} for i in range(12)]
        if reg_years
        else []
    )
    price_max = db.execute(select(func.max(Car.price_rub_cached))).scalar_one_or_none()
    if price_max is None:
        price_max = db.execute(select(func.max(Car.price))).scalar_one_or_none()
    mileage_max = db.execute(select(func.max(Car.mileage))).scalar_one_or_none()
    generations = (
        db.execute(
            select(func.distinct(Car.generation))
            .where(Car.is_available.is_(True), Car.generation.is_not(None))
            .order_by(Car.generation.asc())
        )
        .scalars()
        .all()
    )
    generations = [g for g in generations if g]
    colors = service.colors()
    basic_set = set(BASIC_COLORS)
    basic_by_value = {c["value"]: c for c in colors if c.get("value") in basic_set}
    colors_basic = [basic_by_value[c] for c in BASIC_COLORS if c in basic_by_value]
    colors_other = [c for c in colors if c.get("value") not in basic_set]
    return {
        "regions": regions,
        "reg_years": reg_years,
        "reg_months": reg_months,
        "price_options": _range_steps(price_max, 500_000, 1_000_000, 12),
        "mileage_options": _range_steps(mileage_max, 10_000, 20_000, 12),
        "generations": generations,
        "colors_basic": colors_basic,
        "colors_other": colors_other,
        "engine_types": service.engine_types(),
        "transmissions": service.transmission_options(),
    }


def _home_context(request: Request, service: CarsService, db: Session, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    brand_stats = service.brand_stats()
    body_type_stats = service.body_type_stats()
    reco_cfg = load_config()
    recommended = service.recommended_auto(
        max_age_years=reco_cfg.get("max_age_years"),
        price_min=reco_cfg.get("price_min"),
        price_max=reco_cfg.get("price_max"),
        mileage_max=reco_cfg.get("mileage_max"),
        limit=12,
    )
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
    if len(hero_videos) > 1:
        hero_videos = [hero_videos[1]]

    collage_images = []
    thumb_dir = media_root / "машины_thumbs"
    if car_photos_dir.exists():
        thumb_prefix = thumb_dir.name
        orig_prefix = car_photos_dir.name

        def build_url(prefix: str, name: str) -> str:
            safe_name = name.replace("\u00a0", " ")
            return f"/media/{prefix}/{quote(safe_name)}"

        files = list(car_photos_dir.iterdir())
        rng_files = random.Random(42)
        rng_files.shuffle(files)
        for p in files:
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

    collage_display = []
    if collage_images:
        rng = random.Random(21)
        pool = collage_images.copy()
        rng.shuffle(pool)
        while len(collage_display) < 60:
            for img in pool:
                if collage_display and collage_display[-1]["src"] == img["src"]:
                    continue
                collage_display.append(img)
                if len(collage_display) >= 60:
                    break
            rng.shuffle(pool)

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

    filter_ctx = _build_filter_context(service, db)
    context = {
        "request": request,
        "user": getattr(request.state, "user", None),
        "total_cars": service.total_cars(),
        "brands": service.brands(),
        "regions": filter_ctx["regions"],
        "reg_years": filter_ctx["reg_years"],
        "reg_months": filter_ctx["reg_months"],
        "price_options": filter_ctx["price_options"],
        "mileage_options": filter_ctx["mileage_options"],
        "generations": filter_ctx["generations"],
        "colors_basic": filter_ctx["colors_basic"],
        "colors_other": filter_ctx["colors_other"],
        "engine_types": filter_ctx["engine_types"],
        "transmissions": filter_ctx["transmissions"],
        "brand_stats": brand_stats,
        "brand_logos": brand_logos,
        "body_type_stats": body_type_stats,
        "recommended_cars": recommended,
        "content": content,
        "fx_rates": fx_rates,
        "hero_videos": hero_videos,
        "collage_images": collage_display or collage_images,
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
    filter_ctx = _build_filter_context(service, db)
    contact_content = ContentService(db).content_map(
        [
            "contact_phone",
            "contact_email",
            "contact_address",
            "contact_tg",
            "contact_wa",
            "contact_ig",
        ])

    return templates.TemplateResponse(
        "catalog.html",
        {
            "request": request,
            "user": getattr(request.state, "user", None),
            "brands": brands,
            "regions": filter_ctx["regions"],
            "reg_years": filter_ctx["reg_years"],
            "reg_months": filter_ctx["reg_months"],
            "price_options": filter_ctx["price_options"],
            "mileage_options": filter_ctx["mileage_options"],
            "generations": filter_ctx["generations"],
            "colors_basic": filter_ctx["colors_basic"],
            "colors_other": filter_ctx["colors_other"],
            "engine_types": filter_ctx["engine_types"],
            "transmissions": filter_ctx["transmissions"],
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
        if src_key.startswith("mobile"):
            car.display_region = "Европа"
        elif "emavto" in src_key or "encar" in src_key or "m-auto" in src_key or "m_auto" in src_key:
            car.display_region = "Корея"
        else:
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
