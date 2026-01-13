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
from ..utils.localization import display_body, display_color
from ..utils.taxonomy import ru_body, ru_color, ru_fuel, ru_transmission, normalize_fuel
from ..utils.country_map import country_label_ru, resolve_display_country
from ..utils.home_content import build_home_content


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


def _mileage_suggestions(max_val: Optional[float]) -> List[int]:
    base = [0, 5_000, 10_000, 20_000, 50_000, 90_000]
    if not max_val:
        return base
    steps = _range_steps(max_val, 10_000, 0, 14)
    all_vals = sorted({*base, *steps})
    return all_vals


def _sort_numeric_strings(values: List[str]) -> List[str]:
    def to_num(v: str) -> int:
        digits = "".join(ch for ch in str(v) if ch.isdigit())
        return int(digits) if digits else 0

    return sorted(values, key=lambda v: (to_num(v), str(v)))


def _build_filter_context(service: CarsService, db: Session, include_payload: bool = True) -> Dict[str, Any]:
    regions = service.available_regions()
    eu_countries = service.available_eu_countries()
    eu_source_ids = service.source_ids_for_region("EU")
    kr_source_ids = service.source_ids_for_region("KR")
    has_air_suspension = service.has_air_suspension() if include_payload else False
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
    countries_sorted = sorted(eu_countries)
    body_types = []
    for row in service.body_type_stats():
        val = row.get("body_type")
        if not val:
            continue
        label = ru_body(val) or display_body(val) or val
        body_types.append({"value": val, "label": label, "count": row.get("count")})
    if include_payload:
        payload_keys = [
            "num_seats",
            "doors_count",
            "owners_count",
            "emission_class",
            "efficiency_class",
            "climatisation",
            "airbags",
            "interior_design",
            "price_rating_label",
        ]
        eu_payload = service.payload_values_bulk(payload_keys, source_ids=eu_source_ids)
        kr_payload = service.payload_values_bulk(payload_keys, source_ids=kr_source_ids)
        seats_options = []
        doors_options = []
        owners_options = []
        emission_classes = []
        efficiency_classes = []
        climatisation_options = []
        airbags_options = []
        interior_design_options = []
        price_rating_labels = []
        seats_options_eu = _sort_numeric_strings(eu_payload.get("num_seats", []))
        doors_options_eu = _sort_numeric_strings(eu_payload.get("doors_count", []))
        owners_options_eu = _sort_numeric_strings(eu_payload.get("owners_count", []))
        emission_classes_eu = eu_payload.get("emission_class", [])
        efficiency_classes_eu = eu_payload.get("efficiency_class", [])
        climatisation_options_eu = eu_payload.get("climatisation", [])
        airbags_options_eu = eu_payload.get("airbags", [])
        interior_design_options_eu = eu_payload.get("interior_design", [])
        price_rating_labels_eu = eu_payload.get("price_rating_label", [])
        seats_options_kr = _sort_numeric_strings(kr_payload.get("num_seats", []))
        doors_options_kr = _sort_numeric_strings(kr_payload.get("doors_count", []))
        owners_options_kr = _sort_numeric_strings(kr_payload.get("owners_count", []))
        emission_classes_kr = kr_payload.get("emission_class", [])
        efficiency_classes_kr = kr_payload.get("efficiency_class", [])
        climatisation_options_kr = kr_payload.get("climatisation", [])
        airbags_options_kr = kr_payload.get("airbags", [])
        interior_design_options_kr = kr_payload.get("interior_design", [])
        price_rating_labels_kr = kr_payload.get("price_rating_label", [])
    else:
        seats_options = []
        doors_options = []
        owners_options = []
        emission_classes = []
        efficiency_classes = []
        climatisation_options = []
        airbags_options = []
        interior_design_options = []
        price_rating_labels = []
        seats_options_eu = []
        doors_options_eu = []
        owners_options_eu = []
        emission_classes_eu = []
        efficiency_classes_eu = []
        climatisation_options_eu = []
        airbags_options_eu = []
        interior_design_options_eu = []
        price_rating_labels_eu = []
        seats_options_kr = []
        doors_options_kr = []
        owners_options_kr = []
        emission_classes_kr = []
        efficiency_classes_kr = []
        climatisation_options_kr = []
        airbags_options_kr = []
        interior_design_options_kr = []
        price_rating_labels_kr = []
    return {
        "regions": regions,
        "countries": countries_sorted,
        "country_labels": {**{c: country_label_ru(c) for c in countries_sorted}, "EU": "Европа", "KR": "Корея"},
        "kr_types": [],
        "reg_years": reg_years,
        "reg_months": reg_months,
        "price_options": _range_steps(price_max, 500_000, 1_000_000, 12),
        "mileage_options": _mileage_suggestions(mileage_max),
        "generations": generations,
        "colors_basic": colors_basic,
        "colors_other": colors_other,
        "body_types": body_types,
        "engine_types": service.engine_types(),
        "transmissions": service.transmission_options(),
        "drive_types": service.drive_types(),
        "seats_options": seats_options,
        "doors_options": doors_options,
        "owners_options": owners_options,
        "emission_classes": emission_classes,
        "efficiency_classes": efficiency_classes,
        "climatisation_options": climatisation_options,
        "airbags_options": airbags_options,
        "interior_design_options": interior_design_options,
        "price_rating_labels": price_rating_labels,
        "seats_options_eu": seats_options_eu,
        "doors_options_eu": doors_options_eu,
        "owners_options_eu": owners_options_eu,
        "emission_classes_eu": emission_classes_eu,
        "efficiency_classes_eu": efficiency_classes_eu,
        "climatisation_options_eu": climatisation_options_eu,
        "airbags_options_eu": airbags_options_eu,
        "interior_design_options_eu": interior_design_options_eu,
        "price_rating_labels_eu": price_rating_labels_eu,
        "seats_options_kr": seats_options_kr,
        "doors_options_kr": doors_options_kr,
        "owners_options_kr": owners_options_kr,
        "emission_classes_kr": emission_classes_kr,
        "efficiency_classes_kr": efficiency_classes_kr,
        "climatisation_options_kr": climatisation_options_kr,
        "airbags_options_kr": airbags_options_kr,
        "interior_design_options_kr": interior_design_options_kr,
        "price_rating_labels_kr": price_rating_labels_kr,
        "has_air_suspension": has_air_suspension,
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
    for car in recommended:
        code, label = resolve_display_country(car)
        car.display_country_code = code
        car.display_country_label = label
        car.display_engine_type = ru_fuel(car.engine_type) or ru_fuel(normalize_fuel(car.engine_type)) or car.engine_type
        car.display_transmission = ru_transmission(car.transmission) or car.transmission
    content = ContentService(db).content_map(
        [
            "home_content",
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
    home_content = build_home_content(content)
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

    filter_ctx = _build_filter_context(service, db, include_payload=False)
    context = {
        "request": request,
        "user": getattr(request.state, "user", None),
        "total_cars": service.total_cars(),
        "brands": service.brands(),
        "regions": filter_ctx["regions"],
        "countries": filter_ctx["countries"],
        "country_labels": filter_ctx["country_labels"],
        "kr_types": filter_ctx["kr_types"],
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
        "home": home_content,
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
    filter_ctx = _build_filter_context(service, db, include_payload=False)
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
            "countries": filter_ctx["countries"],
            "country_labels": filter_ctx["country_labels"],
            "kr_types": filter_ctx["kr_types"],
            "reg_years": filter_ctx["reg_years"],
            "reg_months": filter_ctx["reg_months"],
            "price_options": filter_ctx["price_options"],
            "mileage_options": filter_ctx["mileage_options"],
            "generations": filter_ctx["generations"],
            "colors_basic": filter_ctx["colors_basic"],
            "colors_other": filter_ctx["colors_other"],
            "body_types": filter_ctx["body_types"],
            "engine_types": filter_ctx["engine_types"],
            "transmissions": filter_ctx["transmissions"],
            "drive_types": filter_ctx["drive_types"],
            "has_air_suspension": filter_ctx["has_air_suspension"],
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


@router.get("/search")
def search_page(request: Request, db=Depends(get_db), user=Depends(get_current_user)):
    templates = request.app.state.templates
    service = CarsService(db)
    filter_ctx = _build_filter_context(service, db, include_payload=True)
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
        "search.html",
        {
            "request": request,
            "user": getattr(request.state, "user", None),
            "total_cars": service.total_cars(),
            "brands": service.brands(),
            "regions": filter_ctx["regions"],
            "countries": filter_ctx["countries"],
            "country_labels": filter_ctx["country_labels"],
            "kr_types": filter_ctx["kr_types"],
            "reg_years": filter_ctx["reg_years"],
            "reg_months": filter_ctx["reg_months"],
            "generations": filter_ctx["generations"],
            "colors_basic": filter_ctx["colors_basic"],
            "colors_other": filter_ctx["colors_other"],
            "body_types": filter_ctx["body_types"],
            "engine_types": filter_ctx["engine_types"],
            "transmissions": filter_ctx["transmissions"],
            "drive_types": filter_ctx["drive_types"],
            "seats_options": filter_ctx["seats_options"],
            "doors_options": filter_ctx["doors_options"],
            "owners_options": filter_ctx["owners_options"],
            "emission_classes": filter_ctx["emission_classes"],
            "efficiency_classes": filter_ctx["efficiency_classes"],
            "climatisation_options": filter_ctx["climatisation_options"],
            "airbags_options": filter_ctx["airbags_options"],
            "interior_design_options": filter_ctx["interior_design_options"],
            "price_rating_labels": filter_ctx["price_rating_labels"],
            "seats_options_eu": filter_ctx["seats_options_eu"],
            "doors_options_eu": filter_ctx["doors_options_eu"],
            "owners_options_eu": filter_ctx["owners_options_eu"],
            "emission_classes_eu": filter_ctx["emission_classes_eu"],
            "efficiency_classes_eu": filter_ctx["efficiency_classes_eu"],
            "climatisation_options_eu": filter_ctx["climatisation_options_eu"],
            "airbags_options_eu": filter_ctx["airbags_options_eu"],
            "interior_design_options_eu": filter_ctx["interior_design_options_eu"],
            "price_rating_labels_eu": filter_ctx["price_rating_labels_eu"],
            "seats_options_kr": filter_ctx["seats_options_kr"],
            "doors_options_kr": filter_ctx["doors_options_kr"],
            "owners_options_kr": filter_ctx["owners_options_kr"],
            "emission_classes_kr": filter_ctx["emission_classes_kr"],
            "efficiency_classes_kr": filter_ctx["efficiency_classes_kr"],
            "climatisation_options_kr": filter_ctx["climatisation_options_kr"],
            "airbags_options_kr": filter_ctx["airbags_options_kr"],
            "interior_design_options_kr": filter_ctx["interior_design_options_kr"],
            "price_rating_labels_kr": filter_ctx["price_rating_labels_kr"],
            "has_air_suspension": filter_ctx["has_air_suspension"],
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
    if car:
        code, label = resolve_display_country(car)
        car.display_country_code = code
        car.display_country_label = label
    details = []
    options = []
    calc = None
    if car:
        car.display_body_type = ru_body(getattr(car, "body_type", None)) or display_body(getattr(car, "body_type", None)) or car.body_type
        car.display_color = ru_color(getattr(car, "color", None)) or display_color(getattr(car, "color", None)) or car.color
        car.display_engine_type = ru_fuel(getattr(car, "engine_type", None)) or ru_fuel(normalize_fuel(getattr(car, "engine_type", None))) or car.engine_type
        car.display_transmission = ru_transmission(getattr(car, "transmission", None)) or car.transmission
        payload = car.source_payload or {}
        pricing = service.price_info(car)

        def translate_value(val: Any) -> Any:
            if not isinstance(val, str):
                return val
            s = val.strip()
            low = s.lower()
            repl = [
                ("automatic", "автоматический"),
                ("climatisation", "климат-контроль"),
                ("climatization", "климат-контроль"),
                ("2 zones", "2 зоны"),
                ("two zones", "2 зоны"),
                ("zone", "зона"),
                ("front and side", "фронтальные и боковые"),
                ("airbags", "подушки безопасности"),
                ("navigation", "навигация"),
                ("leather", "кожа"),
                ("sport package", "спорт пакет"),
                ("park assist", "помощь при парковке"),
            ]
            out = s
            for src, dst in repl:
                if src in low:
                    out = out.replace(src, dst).replace(src.title(), dst)
            return out

        def push(label: str, value: Any) -> None:
            if value is None:
                return
            if isinstance(value, str) and not value.strip():
                return
            details.append({"label": label, "value": translate_value(value)})

        push("Мест", payload.get("num_seats"))
        push("Дверей", payload.get("doors_count"))
        push("Владельцев", payload.get("owners_count"))
        push("Экокласс", payload.get("emission_class"))
        push("Эффективность", payload.get("efficiency_class"))
        push("Климат", payload.get("climatisation"))
        push("Интерьер", payload.get("interior_design"))
        push("Парктроники", payload.get("park_assists"))
        push("Подушки", payload.get("airbags"))
        push("Цвет производителя", payload.get("manufacturer_color"))
        push("Расход топлива", payload.get("fuel_consumption"))
        push("CO₂", payload.get("co_emission"))
        push("Оценка цены", payload.get("price_rating_label"))

        raw_options = payload.get("options")
        if isinstance(raw_options, list):
            options = [translate_value(str(opt).strip()) for opt in raw_options if str(opt).strip()]
        elif isinstance(raw_options, str):
            opt = raw_options.strip()
            options = [translate_value(opt)] if opt else []
        calc = service.ensure_calc_cache(car)
    return templates.TemplateResponse(
        "car_detail.html",
        {
            "request": request,
            "car": car,
            "user": getattr(request.state, "user", None),
            "car_details": details,
            "car_options": options,
            "calc": calc,
            "pricing": pricing,
        },
    )


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
