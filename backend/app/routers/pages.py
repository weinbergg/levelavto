import time
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
from ..utils.taxonomy import (
    ru_body,
    ru_color,
    ru_fuel,
    ru_transmission,
    normalize_fuel,
    normalize_color as _normalize_color,
    color_hex,
)
normalize_color = _normalize_color
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


def _build_filter_context(
    service: CarsService,
    db: Session,
    include_payload: bool = True,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    regions = service.available_regions()
    params = params or {}
    facet_filters = {
        "region": params.get("region"),
        "country": params.get("country"),
        "brand": params.get("brand"),
        "model": params.get("model"),
        "color": (params.get("color") or "").lower() or None,
        "engine_type": (params.get("engine_type") or "").lower() or None,
        "transmission": (params.get("transmission") or "").lower() or None,
        "body_type": (params.get("body_type") or "").lower() or None,
        "drive_type": (params.get("drive_type") or "").lower() or None,
        "reg_year": int(params["reg_year"]) if params.get("reg_year") else None,
    }

    eu_countries = [c["value"] for c in service.facet_counts(field="country", filters=facet_filters)]
    eu_source_ids = service.source_ids_for_region("EU")
    kr_source_ids = service.source_ids_for_region("KR")
    has_air_suspension = False
    reg_years = [int(r["value"]) for r in service.facet_counts(field="reg_year", filters=facet_filters)]
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
            .where(func.coalesce(Car.is_available, True).is_(True), Car.generation.is_not(None))
            .order_by(Car.generation.asc())
        )
        .scalars()
        .all()
    )
    generations = [g for g in generations if g]
    colors = service.facet_counts(field="color", filters=facet_filters)
    colors = [
        {
            "value": c["value"],
            "label": ru_color(c["value"]) or display_color(c["value"]) or c["value"],
            "hex": color_hex(c["value"]),
            "count": c["count"],
        }
        for c in colors
        if c.get("value")
    ]
    basic_set = set(BASIC_COLORS)
    basic_by_value = {c["value"]: c for c in colors if c.get("value") in basic_set}
    colors_basic = [basic_by_value[c] for c in BASIC_COLORS if c in basic_by_value]
    colors_other = [c for c in colors if c.get("value") not in basic_set]
    countries_sorted = sorted(eu_countries)
    body_types = []
    for row in service.facet_counts(field="body_type", filters=facet_filters):
        val = row.get("value")
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
    kr_types = []
    if service.has_korea():
        kr_types = [
            {"value": "KR_INTERNAL", "label": "Корея (внутренний рынок)"},
            {"value": "KR_IMPORT", "label": "Корея (импорт)"},
        ]

    return {
        "regions": regions,
        "countries": countries_sorted,
        "country_labels": {**{c: country_label_ru(c) for c in countries_sorted}, "EU": "Европа", "KR": "Корея"},
        "kr_types": kr_types,
        "reg_years": reg_years,
        "reg_months": reg_months,
        "price_options": _range_steps(price_max, 500_000, 1_000_000, 12),
        "mileage_options": _mileage_suggestions(mileage_max),
        "generations": generations,
        "colors_basic": colors_basic,
        "colors_other": colors_other,
        "body_types": body_types,
        "brands": [
            b["value"]
            for b in service.facet_counts(field="brand", filters=facet_filters)
            if b.get("value")
        ],
        "engine_types": [
            {"value": v["value"], "label": ru_fuel(v["value"]) or v["value"], "count": v["count"]}
            for v in service.facet_counts(field="engine_type", filters=facet_filters)
        ],
        "transmissions": [
            {"value": v["value"], "label": ru_transmission(v["value"]) or v["value"], "count": v["count"]}
            for v in service.facet_counts(field="transmission", filters=facet_filters)
        ],
        "drive_types": [
            {"value": v["value"], "label": v["value"], "count": v["count"]}
            for v in service.facet_counts(field="drive_type", filters=facet_filters)
        ],
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


def _home_context(
    request: Request,
    service: CarsService,
    db: Session,
    extra: Optional[Dict[str, Any]] = None,
    timing: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    t0 = time.perf_counter()
    brand_stats = service.brand_stats()
    if timing is not None:
        timing["brand_stats_ms"] = (time.perf_counter() - t0) * 1000
    t0 = time.perf_counter()
    body_type_stats = service.body_type_stats()
    if timing is not None:
        timing["body_type_stats_ms"] = (time.perf_counter() - t0) * 1000
    reco_cfg = load_config()
    t0 = time.perf_counter()
    recommended = service.recommended_auto(
        max_age_years=reco_cfg.get("max_age_years"),
        price_min=reco_cfg.get("price_min"),
        price_max=reco_cfg.get("price_max"),
        mileage_max=reco_cfg.get("mileage_max"),
        limit=12,
    )
    if timing is not None:
        timing["recommended_ms"] = (time.perf_counter() - t0) * 1000
    for car in recommended:
        code, label = resolve_display_country(car)
        car.display_country_code = code
        car.display_country_label = label
        car.display_engine_type = ru_fuel(car.engine_type) or ru_fuel(normalize_fuel(car.engine_type)) or car.engine_type
        car.display_transmission = ru_transmission(car.transmission) or car.transmission
    t0 = time.perf_counter()
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
    if timing is not None:
        timing["content_ms"] = (time.perf_counter() - t0) * 1000
    t0 = time.perf_counter()
    fx_rates = service.get_fx_rates() or {}
    if timing is not None:
        timing["fx_rates_ms"] = (time.perf_counter() - t0) * 1000
    # медиа лежат рядом с корнем проекта: /code/фото-видео
    media_root = Path(__file__).resolve().parents[3] / "фото-видео"
    video_dir = media_root / "видео"
    car_photos_dir = media_root / "машины"

    t0 = time.perf_counter()
    hero_videos = []
    if video_dir.exists():
        prefix = video_dir.name
        for p in sorted(video_dir.iterdir()):
            if p.suffix.lower() in {".mp4", ".mov", ".webm"}:
                hero_videos.append(f"/media/{prefix}/{p.name}")
    if len(hero_videos) > 1:
        hero_videos = [hero_videos[1]]
    if timing is not None:
        timing["hero_videos_ms"] = (time.perf_counter() - t0) * 1000

    t0 = time.perf_counter()
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
    if timing is not None:
        timing["collage_ms"] = (time.perf_counter() - t0) * 1000

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

    t0 = time.perf_counter()
    filter_ctx = _build_filter_context(service, db, include_payload=False, params={})
    if timing is not None:
        timing["filter_ctx_ms"] = (time.perf_counter() - t0) * 1000
    country_labels = filter_ctx.get("country_labels") or {}
    countries_list = filter_ctx.get("countries") or []
    countries_with_labels = [
        {"value": c, "label": country_labels.get(c, c)}
        for c in countries_list
    ]
    t0 = time.perf_counter()
    total_cars = service.total_cars()
    if timing is not None:
        timing["total_cars_ms"] = (time.perf_counter() - t0) * 1000
    context = {
        "request": request,
        "user": getattr(request.state, "user", None),
        "total_cars": total_cars,
        "brands": filter_ctx["brands"],
        "regions": filter_ctx["regions"],
        "countries": countries_list,
        "countries_labeled": countries_with_labels,
        "country_labels": country_labels,
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
    timing: Dict[str, float] = {}
    ctx = _home_context(request, service, db, timing=timing)
    if os.environ.get("HTML_TIMING", "0") == "1":
        request.state.html_parts = {"route": "home", **timing}
    return templates.TemplateResponse("home.html", ctx)


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
    timing: Dict[str, float] = {}
    ctx = _home_context(request, service, db, extra, timing=timing)
    if os.environ.get("HTML_TIMING", "0") == "1":
        request.state.html_parts = {"route": "home", **timing}
    return templates.TemplateResponse("home.html", ctx)


@router.get("/catalog")
def catalog_page(request: Request, db=Depends(get_db), user=Depends(get_current_user)):
    templates = request.app.state.templates
    service = CarsService(db)
    timing: Dict[str, float] = {}
    t0 = time.perf_counter()
    filter_ctx = _build_filter_context(service, db, include_payload=False, params=dict(request.query_params))
    timing["filter_ctx_ms"] = (time.perf_counter() - t0) * 1000
    t0 = time.perf_counter()
    contact_content = ContentService(db).content_map(
        [
            "contact_phone",
            "contact_email",
            "contact_address",
            "contact_tg",
            "contact_wa",
            "contact_ig",
        ])
    timing["content_ms"] = (time.perf_counter() - t0) * 1000
    if os.environ.get("HTML_TIMING", "0") == "1":
        request.state.html_parts = {"route": "catalog", **timing}

    return templates.TemplateResponse(
        "catalog.html",
        {
            "request": request,
            "user": getattr(request.state, "user", None),
            "brands": filter_ctx["brands"],
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
    filter_ctx = _build_filter_context(service, db, include_payload=True, params=dict(request.query_params))
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
            "brands": filter_ctx["brands"],
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
            exact = {
                "automatic climate control": "климат-контроль",
                "automatic climate control, 2 zones": "климат-контроль, 2 зоны",
                "automatic climate control, 3 zones": "климат-контроль, 3 зоны",
                "automatic climate control, 4 zones": "климат-контроль, 4 зоны",
                "automatic climatisation": "климат-контроль",
                "automatic climatisation, 2 zones": "климат-контроль, 2 зоны",
                "automatic climatisation, 3 zones": "климат-контроль, 3 зоны",
                "automatic climatisation, 4 zones": "климат-контроль, 4 зоны",
                "airbags": "подушки безопасности",
                "front and side airbags": "фронтальные и боковые подушки",
                "front, side and more airbags": "фронтальные, боковые и дополнительные подушки",
                "front and side and more airbags": "фронтальные, боковые и дополнительные подушки",
                "parking sensors front and rear": "парктроники спереди и сзади",
                "front and rear parking sensors": "парктроники спереди и сзади",
                "parking assists": "ассистенты парковки",
                "park assist": "ассистент парковки",
                "front, rear": "спереди и сзади",
                "360° camera": "камера 360°",
                "rear, front, 360° camera": "камеры спереди, сзади и 360°",
                "front, rear, 360° camera": "камеры спереди, сзади и 360°",
                "rear view camera": "камера заднего вида",
                "backup camera": "камера заднего вида",
                "reverse camera": "камера заднего вида",
                "full leather": "кожа",
                "part leather": "частичная кожа",
                "alcantara": "алькантара",
                "no_rating": "нет оценки",
                "very_good_price": "отличная цена",
                "good_price": "хорошая цена",
                "average_price": "средняя цена",
                "high_price": "высокая цена",
                "dealer": "дилер",
                "private": "частное лицо",
                "petrol": "бензин",
                "diesel": "дизель",
                "electric": "электромобиль",
                "hybrid": "гибрид",
                "automatic": "автомат",
                "manual": "механика",
                "awd": "полный привод",
                "4x4": "полный привод",
                "fwd": "передний привод",
                "rwd": "задний привод",
                "abs": "ABS",
                "alarm system": "сигнализация",
                "alloy wheels": "легкосплавные диски",
                "apple carplay": "Apple CarPlay",
                "android auto": "Android Auto",
                "air suspension": "пневмоподвеска",
                "navigation system": "навигация",
                "heated seats": "подогрев сидений",
                "heated steering wheel": "подогрев руля",
                "led headlights": "LED-фары",
                "cruise control": "круиз-контроль",
                "adaptive cruise control": "адаптивный круиз-контроль",
                "lane change assist": "ассистент смены полосы",
                "blind spot assist": "контроль слепых зон",
                "panoramic roof": "панорамная крыша",
                "sunroof": "люк",
                "keyless central locking": "бесключевой доступ",
                "isofix": "ISOFIX",
                "dab radio": "DAB-радио",
                "bluetooth": "Bluetooth",
                "head-up display": "проекционный дисплей",
                "hill-start assist": "помощь при старте в гору",
                "start-stop system": "система старт-стоп",
                "trailer coupling": "фаркоп",
                "tinted windows": "тонировка",
                "warranty": "гарантия",
                "full service history": "полная сервисная история",
                "non-smoker vehicle": "не курили в салоне",
                "rain sensor": "датчик дождя",
                "light sensor": "датчик света",
                "tyre pressure monitoring": "контроль давления в шинах",
                "usb port": "USB",
                "touchscreen": "сенсорный экран",
            }
            if low in exact:
                return exact[low]
            replacements = [
                ("climatisation", "климат-контроль"),
                ("climatization", "климат-контроль"),
                ("climate control", "климат-контроль"),
                ("airbags", "подушки безопасности"),
                ("navigation", "навигация"),
                ("leather", "кожа"),
                ("sport package", "спорт пакет"),
                ("park assist", "ассистент парковки"),
                ("360° camera", "камера 360°"),
                ("front, rear", "спереди и сзади"),
                ("rear view camera", "камера заднего вида"),
                ("backup camera", "камера заднего вида"),
                ("reverse camera", "камера заднего вида"),
                ("parking sensors", "парктроники"),
                ("multifunction steering wheel", "мульти-руль"),
                ("leather", "кожа"),
            ]
            out = s
            for src, dst in replacements:
                if src in low:
                    out = out.replace(src, dst).replace(src.title(), dst)
            return out

        def push(label: str, value: Any, *, as_color: bool = False) -> None:
            if value is None:
                return
            if isinstance(value, str) and not value.strip():
                return
            if as_color:
                norm = _normalize_color(value)
                ru = ru_color(norm) or translate_value(value)
                details.append({"label": label, "value": ru})
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
        push("Цвет производителя", payload.get("manufacturer_color"), as_color=True)
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
