import time
import logging
import re
from typing import Dict, Any, Optional, List
from pathlib import Path

from fastapi import APIRouter, Request, Depends, Query, Form
import time
import os
import smtplib
import random
import math
from email.mime.text import MIMEText
from ..db import get_db
from ..services.cars_service import (
    CarsService,
    normalize_brand,
    effective_engine_cc_value,
    effective_power_hp_value,
    effective_power_kw_value,
)
from ..utils.recommended_config import load_config
from ..services.content_service import ContentService
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, func, exists, case
from cachetools import TTLCache
from ..utils.redis_cache import (
    redis_get_json,
    redis_set_json,
    build_filter_ctx_key,
    build_filter_ctx_base_key,
    normalize_filter_params,
    build_cars_count_key,
    normalize_count_params,
)
from ..models import Car, Source, CarImage
from ..auth import get_current_user
from urllib.parse import quote, urlparse, parse_qs, unquote
from ..utils.localization import display_body, display_color
from ..utils.taxonomy import (
    build_body_type_options,
    ru_body,
    ru_color,
    ru_fuel,
    ru_transmission,
    ru_drivetrain,
    translate_payload_value,
    build_labeled_options,
    build_interior_options,
    build_interior_trim_options,
    normalize_fuel,
    normalize_color as _normalize_color,
    color_hex,
)
from ..utils.price_utils import display_price_rub, price_without_util_note
normalize_color = _normalize_color
from ..utils.country_map import country_label_ru, resolve_display_country, normalize_country_code
from ..utils.color_groups import split_color_facets
from ..utils.thumbs import local_media_exists, normalize_classistatic_url, resolve_thumbnail_url
from ..utils.home_content import build_home_content
from ..utils.telegram import send_telegram_message


router = APIRouter()
logger = logging.getLogger(__name__)
_FILTER_CTX_CACHE: TTLCache = TTLCache(maxsize=64, ttl=600)
_TOTAL_CARS_CACHE: TTLCache = TTLCache(maxsize=32, ttl=300)
_HOME_FILTER_CTX_CACHE: TTLCache = TTLCache(maxsize=4, ttl=900)
_HOME_MEDIA_CACHE: TTLCache = TTLCache(maxsize=2, ttl=3600)
_HOME_RECOMMENDED_CACHE: TTLCache = TTLCache(maxsize=4, ttl=900)


def _home_dataset_version() -> str:
    try:
        return build_filter_ctx_base_key({}).rsplit(":v", 1)[-1]
    except Exception:
        return "0"


def _home_media_redis_key() -> str:
    return "home_media_ctx:v4"


def _home_recommended_redis_key(cfg: Dict[str, Any], limit: int) -> str:
    return (
        "home_recommended:"
        f"{cfg.get('reg_year_min', 2021)}:{cfg.get('reg_year_max', 2023)}:"
        f"{cfg.get('power_hp_max', 160)}:{cfg.get('engine_cc_max', 1900)}:"
        f"{limit}:v{_home_dataset_version()}"
    )


def _load_cars_by_ids(db: Session, ids: List[int]) -> List[Car]:
    if not ids:
        return []
    ordering = case({car_id: idx for idx, car_id in enumerate(ids)}, value=Car.id)
    return list(
        db.execute(
            select(Car)
            .where(Car.id.in_(ids))
            .order_by(ordering)
        ).scalars().all()
    )


def _get_home_recommended(service: CarsService, db: Session, cfg: Dict[str, Any], limit: int = 12) -> List[Car]:
    cache_key = (
        cfg.get("reg_year_min", 2021),
        cfg.get("reg_year_max", 2023),
        cfg.get("power_hp_max", 160),
        cfg.get("engine_cc_max", 1900),
        limit,
        _home_dataset_version(),
    )
    cached_ids = _HOME_RECOMMENDED_CACHE.get(cache_key)
    if cached_ids:
        cached_items = _load_cars_by_ids(db, [int(car_id) for car_id in cached_ids if car_id])
        if cached_items:
            return cached_items
    redis_key = _home_recommended_redis_key(cfg, limit)
    cached_ids = redis_get_json(redis_key)
    if isinstance(cached_ids, list) and cached_ids:
        cached_items = _load_cars_by_ids(db, [int(car_id) for car_id in cached_ids if car_id])
        if cached_items:
            _HOME_RECOMMENDED_CACHE[cache_key] = cached_ids
            return cached_items
    items = service.recommended_auto(
        reg_year_min=cfg.get("reg_year_min", 2021),
        reg_year_max=cfg.get("reg_year_max", 2023),
        power_hp_max=cfg.get("power_hp_max", 160),
        engine_cc_max=cfg.get("engine_cc_max", 1900),
        limit=limit,
    )
    ids = [int(car.id) for car in items if getattr(car, "id", None)]
    if ids:
        _HOME_RECOMMENDED_CACHE[cache_key] = ids
        redis_set_json(redis_key, ids, ttl_sec=1800)
    return items


def _get_cars_count(service: CarsService, params: Dict[str, Any], timing_enabled: bool) -> int:
    def _to_int(value: Any) -> Optional[int]:
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _to_float(value: Any) -> Optional[float]:
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _to_bool(value: Any) -> Optional[bool]:
        if value is None or value == "":
            return None
        if isinstance(value, bool):
            return value
        s = str(value).strip().lower()
        if s in {"1", "true", "yes", "on"}:
            return True
        if s in {"0", "false", "no", "off"}:
            return False
        return None

    normalized = normalize_count_params(params)
    strict_photo_mode = (
        os.getenv("CATALOG_HIDE_NO_LOCAL_PHOTO", "0") == "1"
        and str(normalized.get("region") or "").upper() == "EU"
    )
    normalized["hide_no_local_photo"] = "1" if strict_photo_mode else "0"
    cache_key = build_cars_count_key(normalized)
    cached = redis_get_json(cache_key)
    if cached is not None:
        if timing_enabled:
            print(f"CARS_COUNT_CACHE hit=1 source=redis key={cache_key}", flush=True)
        return int(cached)
    cached = _TOTAL_CARS_CACHE.get(cache_key)
    if cached is not None:
        if timing_enabled:
            print(f"CARS_COUNT_CACHE hit=1 source=fallback key={cache_key}", flush=True)
        return int(cached)
    if timing_enabled:
        print(f"CARS_COUNT_CACHE hit=0 source=fallback key={cache_key}", flush=True)
    # Fast path for unfiltered homepage/search counters.
    # list_cars(count_only) can be too slow on cold cache for the full dataset.
    count_params = {k: v for k, v in normalized.items() if k != "hide_no_local_photo"}
    if not count_params:
        total = service.total_cars()
    elif any(
        count_params.get(key)
        for key in (
            "q",
            "line",
            "source",
            "num_seats",
            "doors_count",
            "emission_class",
            "efficiency_class",
            "climatisation",
            "airbags",
            "interior_design",
            "interior_color",
            "interior_material",
            "vat_reclaimable",
            "air_suspension",
            "price_rating_label",
            "owners_count",
            "year_min",
            "year_max",
            "reg_month_min",
            "reg_month_max",
            "generation",
        )
    ):
        _, total = service.list_cars(
            region=count_params.get("region"),
            country=count_params.get("country"),
            brand=count_params.get("brand"),
            model=count_params.get("model"),
            generation=count_params.get("generation"),
            color=count_params.get("color"),
            body_type=count_params.get("body_type"),
            engine_type=count_params.get("engine_type"),
            transmission=count_params.get("transmission"),
            drive_type=count_params.get("drive_type"),
            kr_type=count_params.get("kr_type"),
            price_min=_to_float(count_params.get("price_min")),
            price_max=_to_float(count_params.get("price_max")),
            power_hp_min=_to_float(count_params.get("power_hp_min")),
            power_hp_max=_to_float(count_params.get("power_hp_max")),
            engine_cc_min=_to_int(count_params.get("engine_cc_min")),
            engine_cc_max=_to_int(count_params.get("engine_cc_max")),
            year_min=_to_int(count_params.get("year_min")),
            year_max=_to_int(count_params.get("year_max")),
            mileage_min=_to_int(count_params.get("mileage_min")),
            mileage_max=_to_int(count_params.get("mileage_max")),
            reg_year_min=_to_int(count_params.get("reg_year_min")),
            reg_month_min=_to_int(count_params.get("reg_month_min")),
            reg_year_max=_to_int(count_params.get("reg_year_max")),
            reg_month_max=_to_int(count_params.get("reg_month_max")),
            condition=count_params.get("condition"),
            q=count_params.get("q"),
            lines=[count_params["line"]] if count_params.get("line") else None,
            source_key=count_params.get("source"),
            num_seats=count_params.get("num_seats"),
            doors_count=count_params.get("doors_count"),
            emission_class=count_params.get("emission_class"),
            efficiency_class=count_params.get("efficiency_class"),
            climatisation=count_params.get("climatisation"),
            airbags=count_params.get("airbags"),
            interior_design=count_params.get("interior_design"),
            interior_color=count_params.get("interior_color"),
            interior_material=count_params.get("interior_material"),
            vat_reclaimable=count_params.get("vat_reclaimable"),
            air_suspension=_to_bool(count_params.get("air_suspension")),
            price_rating_label=count_params.get("price_rating_label"),
            owners_count=count_params.get("owners_count"),
            page=1,
            page_size=1,
            light=True,
            count_only=True,
            use_fast_count=True,
            hide_no_local_photo=(normalized.get("hide_no_local_photo") == "1"),
        )
    else:
        total = service.count_cars(**count_params)
    _TOTAL_CARS_CACHE[cache_key] = int(total)
    redis_set_json(cache_key, int(total), ttl_sec=1200)
    return int(total)
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
    redis_key = build_filter_ctx_key(params, include_payload)
    cached = redis_get_json(redis_key)
    if cached:
        if os.environ.get("HTML_TIMING", "0") == "1":
            print("FILTER_CTX_CACHE hit=1 source=redis", flush=True)
        return cached
    cached = _FILTER_CTX_CACHE.get(redis_key)
    if cached:
        if os.environ.get("HTML_TIMING", "0") == "1":
            print("FILTER_CTX_CACHE hit=1 source=fallback", flush=True)
        return cached
    if os.environ.get("HTML_TIMING", "0") == "1":
        print("FILTER_CTX_CACHE hit=0 source=fallback", flush=True)
    timing_enabled = os.environ.get("HTML_TIMING", "0") == "1"
    t_ctx = time.perf_counter()
    regions = service.available_regions()
    if timing_enabled:
        print(f"FILTER_CTX_STAGE name=regions ms={(time.perf_counter()-t_ctx)*1000:.2f}", flush=True)
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

    t0 = time.perf_counter()
    eu_countries = []
    seen_countries = set()
    for row in service.facet_counts(field="country", filters=facet_filters):
        raw_val = row.get("value")
        if not raw_val:
            continue
        code = normalize_country_code(raw_val)
        if not code or code in seen_countries:
            continue
        eu_countries.append(code)
        seen_countries.add(code)
    if timing_enabled:
        print(f"FILTER_CTX_STAGE name=countries ms={(time.perf_counter()-t0)*1000:.2f}", flush=True)
    eu_source_ids = service.source_ids_for_region("EU")
    kr_source_ids = service.source_ids_for_region("KR")
    has_air_suspension = service.has_air_suspension()
    t0 = time.perf_counter()
    reg_years = [int(r["value"]) for r in service.facet_counts(field="reg_year", filters=facet_filters)]
    if timing_enabled:
        print(f"FILTER_CTX_STAGE name=reg_years ms={(time.perf_counter()-t0)*1000:.2f}", flush=True)
    reg_months = (
        [{"value": i + 1, "label": MONTHS_RU[i]} for i in range(12)]
        if reg_years
        else []
    )
    t0 = time.perf_counter()
    price_max = db.execute(select(func.max(Car.price_rub_cached))).scalar_one_or_none()
    if price_max is None:
        price_max = db.execute(select(func.max(Car.price))).scalar_one_or_none()
    mileage_max = db.execute(select(func.max(Car.mileage))).scalar_one_or_none()
    if timing_enabled:
        print(f"FILTER_CTX_STAGE name=max_price_mileage ms={(time.perf_counter()-t0)*1000:.2f}", flush=True)
    t0 = time.perf_counter()
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
    if timing_enabled:
        print(f"FILTER_CTX_STAGE name=generations ms={(time.perf_counter()-t0)*1000:.2f}", flush=True)
    t0 = time.perf_counter()
    colors = service.facet_counts(field="color_group", filters=facet_filters)
    if timing_enabled:
        print(f"FILTER_CTX_STAGE name=colors ms={(time.perf_counter()-t0)*1000:.2f}", flush=True)
    colors_basic, colors_other = split_color_facets(colors)
    countries_sorted = sorted(eu_countries, key=lambda c: (country_label_ru(c) or c).casefold())
    t0 = time.perf_counter()
    body_types = []
    for row in service.facet_counts(field="body_type", filters=facet_filters):
        val = row.get("value")
        if not val:
            continue
        label = ru_body(val) or display_body(val) or val
        body_types.append({"value": val, "label": label, "count": row.get("count")})
    if timing_enabled:
        print(f"FILTER_CTX_STAGE name=body_types ms={(time.perf_counter()-t0)*1000:.2f}", flush=True)
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
        t0 = time.perf_counter()
        eu_payload = service.payload_values_bulk(payload_keys, source_ids=eu_source_ids)
        kr_payload = service.payload_values_bulk(payload_keys, source_ids=kr_source_ids)
        if timing_enabled:
            print(f"FILTER_CTX_STAGE name=payload_values ms={(time.perf_counter()-t0)*1000:.2f}", flush=True)
        seats_options = []
        doors_options = []
        owners_options = []
        emission_classes = []
        efficiency_classes = []
        climatisation_options = []
        airbags_options = []
        interior_design_options = []
        interior_color_options = []
        interior_material_options = []
        price_rating_labels = []
        seats_options_eu = build_labeled_options(_sort_numeric_strings(eu_payload.get("num_seats", [])), "num_seats")
        doors_options_eu = build_labeled_options(_sort_numeric_strings(eu_payload.get("doors_count", [])), "doors_count")
        owners_options_eu = build_labeled_options(_sort_numeric_strings(eu_payload.get("owners_count", [])), "owners_count")
        emission_classes_eu = build_labeled_options(eu_payload.get("emission_class", []), "emission_class")
        efficiency_classes_eu = build_labeled_options(eu_payload.get("efficiency_class", []), "efficiency_class")
        climatisation_options_eu = build_labeled_options(eu_payload.get("climatisation", []), "climatisation")
        airbags_options_eu = build_labeled_options(eu_payload.get("airbags", []), "airbags")
        interior_design_options_eu = build_interior_trim_options(eu_payload.get("interior_design", []))
        interior_color_options_eu = build_interior_options(eu_payload.get("interior_design", []), "color")
        interior_material_options_eu = build_interior_options(eu_payload.get("interior_design", []), "material")
        price_rating_labels_eu = build_labeled_options(eu_payload.get("price_rating_label", []), "price_rating_label")
        seats_options_kr = build_labeled_options(_sort_numeric_strings(kr_payload.get("num_seats", [])), "num_seats")
        doors_options_kr = build_labeled_options(_sort_numeric_strings(kr_payload.get("doors_count", [])), "doors_count")
        owners_options_kr = build_labeled_options(_sort_numeric_strings(kr_payload.get("owners_count", [])), "owners_count")
        emission_classes_kr = build_labeled_options(kr_payload.get("emission_class", []), "emission_class")
        efficiency_classes_kr = build_labeled_options(kr_payload.get("efficiency_class", []), "efficiency_class")
        climatisation_options_kr = build_labeled_options(kr_payload.get("climatisation", []), "climatisation")
        airbags_options_kr = build_labeled_options(kr_payload.get("airbags", []), "airbags")
        interior_design_options_kr = build_interior_trim_options(kr_payload.get("interior_design", []))
        interior_color_options_kr = build_interior_options(kr_payload.get("interior_design", []), "color")
        interior_material_options_kr = build_interior_options(kr_payload.get("interior_design", []), "material")
        price_rating_labels_kr = build_labeled_options(kr_payload.get("price_rating_label", []), "price_rating_label")
    else:
        seats_options = []
        doors_options = []
        owners_options = []
        emission_classes = []
        efficiency_classes = []
        climatisation_options = []
        airbags_options = []
        interior_design_options = []
        interior_color_options = []
        interior_material_options = []
        price_rating_labels = []
        seats_options_eu = []
        doors_options_eu = []
        owners_options_eu = []
        emission_classes_eu = []
        efficiency_classes_eu = []
        climatisation_options_eu = []
        airbags_options_eu = []
        interior_design_options_eu = []
        interior_color_options_eu = []
        interior_material_options_eu = []
        price_rating_labels_eu = []
        seats_options_kr = []
        doors_options_kr = []
        owners_options_kr = []
        emission_classes_kr = []
        efficiency_classes_kr = []
        climatisation_options_kr = []
        airbags_options_kr = []
        interior_design_options_kr = []
        interior_color_options_kr = []
        interior_material_options_kr = []
        price_rating_labels_kr = []
    kr_types = []
    t0 = time.perf_counter()
    if service.has_korea():
        kr_types = [
            {"value": "KR_INTERNAL", "label": "Корея (внутренний рынок)"},
            {"value": "KR_IMPORT", "label": "Корея (импорт)"},
        ]
    if timing_enabled:
        print(f"FILTER_CTX_STAGE name=kr_types ms={(time.perf_counter()-t0)*1000:.2f}", flush=True)

    t0 = time.perf_counter()
    brands = [
        b["value"]
        for b in service.facet_counts(field="brand", filters=facet_filters)
        if b.get("value")
    ]
    if timing_enabled:
        print(f"FILTER_CTX_STAGE name=brands ms={(time.perf_counter()-t0)*1000:.2f}", flush=True)
    t0 = time.perf_counter()
    engine_types = [
        {
            "value": v["value"],
            "label": translate_payload_value("engine_type", v["value"]) or v["value"],
            "count": v["count"],
        }
        for v in service.facet_counts(field="engine_type", filters=facet_filters)
    ]
    if timing_enabled:
        print(f"FILTER_CTX_STAGE name=engine_types ms={(time.perf_counter()-t0)*1000:.2f}", flush=True)
    t0 = time.perf_counter()
    transmissions = [
        {
            "value": v["value"],
            "label": translate_payload_value("transmission", v["value"]) or v["value"],
            "count": v["count"],
        }
        for v in service.facet_counts(field="transmission", filters=facet_filters)
    ]
    if timing_enabled:
        print(f"FILTER_CTX_STAGE name=transmissions ms={(time.perf_counter()-t0)*1000:.2f}", flush=True)
    t0 = time.perf_counter()
    drive_types = [
        {
            "value": v["value"],
            "label": translate_payload_value("drive_type", v["value"]) or v["value"],
            "count": v["count"],
        }
        for v in service.facet_counts(field="drive_type", filters=facet_filters)
    ]
    if timing_enabled:
        print(f"FILTER_CTX_STAGE name=drive_types ms={(time.perf_counter()-t0)*1000:.2f}", flush=True)
    ctx = {
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
        "brands": brands,
        "engine_types": engine_types,
        "transmissions": transmissions,
        "drive_types": drive_types,
        "seats_options": seats_options,
        "doors_options": doors_options,
        "owners_options": owners_options,
        "emission_classes": emission_classes,
        "efficiency_classes": efficiency_classes,
        "climatisation_options": climatisation_options,
        "airbags_options": airbags_options,
        "interior_design_options": interior_design_options,
        "interior_color_options": interior_color_options,
        "interior_material_options": interior_material_options,
        "price_rating_labels": price_rating_labels,
        "seats_options_eu": seats_options_eu,
        "doors_options_eu": doors_options_eu,
        "owners_options_eu": owners_options_eu,
        "emission_classes_eu": emission_classes_eu,
        "efficiency_classes_eu": efficiency_classes_eu,
        "climatisation_options_eu": climatisation_options_eu,
        "airbags_options_eu": airbags_options_eu,
        "interior_design_options_eu": interior_design_options_eu,
        "interior_color_options_eu": interior_color_options_eu,
        "interior_material_options_eu": interior_material_options_eu,
        "price_rating_labels_eu": price_rating_labels_eu,
        "seats_options_kr": seats_options_kr,
        "doors_options_kr": doors_options_kr,
        "owners_options_kr": owners_options_kr,
        "emission_classes_kr": emission_classes_kr,
        "efficiency_classes_kr": efficiency_classes_kr,
        "climatisation_options_kr": climatisation_options_kr,
        "airbags_options_kr": airbags_options_kr,
        "interior_design_options_kr": interior_design_options_kr,
        "interior_color_options_kr": interior_color_options_kr,
        "interior_material_options_kr": interior_material_options_kr,
        "price_rating_labels_kr": price_rating_labels_kr,
        "has_air_suspension": has_air_suspension,
    }
    _FILTER_CTX_CACHE[redis_key] = ctx
    redis_set_json(redis_key, ctx, ttl_sec=900)
    return ctx


def _build_home_filter_context(service: CarsService) -> Dict[str, Any]:
    cache_key = "home_filter_ctx:all"
    cached = _HOME_FILTER_CTX_CACHE.get(cache_key)
    if cached:
        return cached

    base_ctx = redis_get_json(build_filter_ctx_base_key({}))
    if base_ctx:
        regions = [str(item.get("value") or "").strip() for item in base_ctx.get("regions") or [] if item.get("value")]
        countries = [str(item.get("value") or "").strip() for item in base_ctx.get("countries") or [] if item.get("value")]
        country_labels = base_ctx.get("country_labels") or {
            **{code: country_label_ru(code) or code for code in countries},
            "EU": "Европа",
            "KR": "Корея",
        }
        brand_stats = [
            {"brand": normalize_brand(item.get("value")), "count": int(item.get("count") or 0)}
            for item in base_ctx.get("brands") or []
            if item.get("value")
        ]
        body_type_stats = build_body_type_options(base_ctx.get("body_types") or [])
        payload = {
            "regions": regions,
            "countries": countries,
            "country_labels": country_labels,
            "kr_types": base_ctx.get("kr_types") or [],
            "reg_years": [int(v) for v in base_ctx.get("reg_years") or [] if v not in (None, "")],
            "reg_months": base_ctx.get("reg_months") or [{"value": i + 1, "label": MONTHS_RU[i]} for i in range(12)],
            "brands": [item["brand"] for item in brand_stats if item.get("brand")],
            "brand_stats": brand_stats,
            "body_type_stats": body_type_stats,
        }
        _HOME_FILTER_CTX_CACHE[cache_key] = payload
        return payload

    regions = [
        str(row.get("value") or "").strip()
        for row in service.facet_counts(field="region", filters={})
        if row.get("value")
    ]
    countries = []
    seen_countries = set()
    for row in service.facet_counts(field="country", filters={}):
        raw_val = row.get("value")
        if not raw_val:
            continue
        code = normalize_country_code(raw_val)
        if not code or code in seen_countries:
            continue
        seen_countries.add(code)
        countries.append(code)
    brand_stats = [
        {"brand": normalize_brand(row["value"]), "count": int(row["count"])}
        for row in service.facet_counts(field="brand", filters={})
        if row.get("value")
    ]
    body_type_stats = build_body_type_options(service.facet_counts(field="body_type", filters={}))
    payload = {
        "regions": regions,
        "countries": countries,
        "country_labels": {
            **{code: country_label_ru(code) or code for code in countries},
            "EU": "Европа",
            "KR": "Корея",
        },
        "kr_types": [
            {"value": "KR_INTERNAL", "label": "Корея (внутренний рынок)"},
            {"value": "KR_IMPORT", "label": "Корея (импорт)"},
        ] if "KR" in regions else [],
        "reg_years": sorted(
            [int(row["value"]) for row in service.facet_counts(field="reg_year", filters={}) if row.get("value")],
            reverse=True,
        ),
        "reg_months": [{"value": i + 1, "label": MONTHS_RU[i]} for i in range(12)],
        "brands": sorted(
            [item["brand"] for item in brand_stats if item.get("brand")],
            key=lambda value: value.casefold(),
        ),
        "brand_stats": brand_stats,
        "body_type_stats": body_type_stats,
    }
    _HOME_FILTER_CTX_CACHE[cache_key] = payload
    return payload


def _build_home_media_context(db: Session) -> Dict[str, Any]:
    cache_key = "home_media:default"
    cached = _HOME_MEDIA_CACHE.get(cache_key)
    if cached:
        return cached
    redis_key = _home_media_redis_key()
    cached = redis_get_json(redis_key)
    if cached:
        _HOME_MEDIA_CACHE[cache_key] = cached
        return cached

    app_root = Path(__file__).resolve().parents[1]
    static_collage_dir = app_root / "static" / "home-collage"
    media_root = Path(__file__).resolve().parents[3] / "фото-видео"
    video_dir = media_root / "видео"
    hero_videos: List[str] = []
    if video_dir.exists():
        prefix = video_dir.name
        for path_obj in sorted(video_dir.iterdir()):
            if path_obj.suffix.lower() in {".mp4", ".mov", ".webm"}:
                hero_videos.append(f"/media/{prefix}/{path_obj.name}")
    if len(hero_videos) > 1:
        hero_videos = [hero_videos[1]]

    image_exts = {".jpg", ".jpeg", ".webp", ".png"}

    def build_static_url(path_obj: Path) -> str:
        rel = path_obj.relative_to(app_root / "static").as_posix().replace("\u00a0", " ")
        return f"/static/{quote(rel, safe='/')}"

    def build_media_url(path_obj: Path) -> str:
        rel = path_obj.relative_to(media_root).as_posix().replace("\u00a0", " ")
        return f"/media/{quote(rel, safe='/')}"

    def collect_gallery_files(root_dir: Path) -> list[Path]:
        if not root_dir.exists():
            return []
        preferred_dirs = ["машины", "фото", "фото-машины", "gallery", "photos"]
        files: list[Path] = []
        for name in preferred_dirs:
            candidate = root_dir / name
            if not candidate.exists() or not candidate.is_dir():
                continue
            files.extend(
                p
                for p in candidate.rglob("*")
                if p.is_file()
                and p.suffix.lower() in image_exts
                and not any(part.startswith(".") for part in p.parts)
            )
            if files:
                break
        if files:
            return files
        return [
            p
            for p in root_dir.rglob("*")
            if p.is_file()
            and p.suffix.lower() in image_exts
            and not any(part.startswith(".") for part in p.parts)
            and "видео" not in p.parts
            and not any(part.endswith("_thumbs") for part in p.parts)
        ]

    collage_images: List[Dict[str, Any]] = []
    static_gallery_files = (
        sorted(
            p
            for p in static_collage_dir.rglob("*")
            if p.is_file()
            and p.suffix.lower() in image_exts
            and not any(part.startswith(".") for part in p.parts)
        )
        if static_collage_dir.exists()
        else []
    )
    if static_gallery_files:
        rng_files = random.Random(42)
        rng_files.shuffle(static_gallery_files)
        for path_obj in static_gallery_files:
            src = build_static_url(path_obj)
            collage_images.append(
                {
                    "src": src,
                    "srcset": "",
                    "width": 320,
                    "height": 240,
                    "fallback": src,
                }
            )
    else:
        gallery_files = collect_gallery_files(media_root)
        if gallery_files:
            rng_files = random.Random(42)
            rng_files.shuffle(gallery_files)
            for path_obj in gallery_files:
                base = path_obj.stem
                parent = path_obj.parent
                thumbs_parent = parent.parent / f"{parent.name}_thumbs"
                t320 = thumbs_parent / f"{base}__w320.webp"
                t640 = thumbs_parent / f"{base}__w640.webp"
                has_thumb = t320.exists()
                src = build_media_url(t320 if has_thumb else path_obj)
                srcset_parts = []
                if has_thumb:
                    srcset_parts.append(f"{build_media_url(t320)} 320w")
                    if t640.exists():
                        srcset_parts.append(f"{build_media_url(t640)} 640w")
                collage_images.append(
                    {
                        "src": src,
                        "srcset": ", ".join(srcset_parts),
                        "width": 320,
                        "height": 240,
                        "fallback": build_media_url(path_obj),
                    }
                )

    collage_display: List[Dict[str, Any]] = []
    if collage_images:
        rng = random.Random(21)
        pool = collage_images.copy()
        rng.shuffle(pool)
        while len(collage_display) < 75:
            for item in pool:
                if collage_display and collage_display[-1]["src"] == item["src"]:
                    continue
                collage_display.append(item)
                if len(collage_display) >= 75:
                    break
            rng.shuffle(pool)
    else:
        rows = (
            db.execute(
                select(Car.thumbnail_url)
                .where(Car.is_available.is_(True), Car.thumbnail_url.is_not(None), Car.thumbnail_url != "")
                .order_by(Car.updated_at.desc())
                .limit(180)
            )
            .scalars()
            .all()
        )
        for raw in rows:
            thumb = resolve_thumbnail_url(raw, None)
            if not thumb:
                continue
            if "img.classistatic.de" in thumb:
                src = f"/thumb?u={quote(thumb)}&w=360&fmt=webp&rev=2"
                fallback = thumb
            else:
                src = thumb
                fallback = "/static/img/no-photo.svg"
            collage_images.append(
                {
                    "src": src,
                    "srcset": "",
                    "width": 320,
                    "height": 240,
                    "fallback": fallback,
                }
            )
            if len(collage_images) >= 60:
                break
        collage_display = collage_images

    payload = {
        "hero_videos": hero_videos,
        "collage_images": collage_display or collage_images,
    }
    _HOME_MEDIA_CACHE[cache_key] = payload
    redis_set_json(redis_key, payload, ttl_sec=3600)
    return payload


def _home_context(
    request: Request,
    service: CarsService,
    db: Session,
    extra: Optional[Dict[str, Any]] = None,
    timing: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    timing_enabled = os.environ.get("HTML_TIMING", "0") == "1"

    def _stage(name: str, started_at: float):
        if timing is None:
            return
        ms = (time.perf_counter() - started_at) * 1000
        timing[name] = ms
        if timing_enabled:
            print(f"HOME_STAGE name={name} ms={ms:.2f}", flush=True)

    t0 = time.perf_counter()
    home_filter_ctx = _build_home_filter_context(service)
    _stage("home_filter_ctx_ms", t0)
    brand_stats = home_filter_ctx.get("brand_stats") or []
    body_type_stats = home_filter_ctx.get("body_type_stats") or []
    reco_cfg = load_config()
    t0 = time.perf_counter()
    recommended = _get_home_recommended(service, db, reco_cfg, limit=12)
    _stage("recommended_ms", t0)
    for car in recommended:
        code, label = resolve_display_country(car)
        car.display_country_code = code
        car.display_country_label = label
        car.display_engine_type = translate_payload_value("engine_type", car.engine_type) or car.engine_type
        car.display_transmission = translate_payload_value("transmission", car.transmission) or car.transmission
        car.display_drive_type = translate_payload_value("drive_type", car.drive_type) or car.drive_type
        car.display_body_type = ru_body(car.body_type) or display_body(car.body_type) or car.body_type
        normalized_color = normalize_color(getattr(car, "color", None))
        car.display_color = (
            ru_color(getattr(car, "color", None))
            or display_color(getattr(car, "color", None))
            or (ru_color(normalized_color) if normalized_color else None)
            or (display_color(normalized_color) if normalized_color else None)
            or car.color
        )
        car.display_price_rub = display_price_rub(
            car.total_price_rub_cached,
            car.price_rub_cached,
        )
        car.price_note = price_without_util_note(
            display_price=car.display_price_rub,
            total_price_rub_cached=car.total_price_rub_cached,
            calc_breakdown=car.calc_breakdown_json,
            country=car.country,
        )
        thumb = resolve_thumbnail_url(
            getattr(car, "thumbnail_url", None),
            getattr(car, "thumbnail_local_path", None),
        )
        if thumb:
            car.thumbnail_url = thumb
        if not getattr(car, "thumbnail_url", None):
            car.thumbnail_url = "/static/img/no-photo.svg"
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
            "contact_vk",
            "contact_avito",
            "contact_autoru",
            "contact_max",
            "contact_map_link",
        ])
    home_content = build_home_content(content)
    _stage("content_ms", t0)
    t0 = time.perf_counter()
    fx_rates = service.get_fx_rates(allow_fetch=False) or {}
    _stage("fx_rates_ms", t0)
    t0 = time.perf_counter()
    media_ctx = _build_home_media_context(db)
    _stage("media_ms", t0)
    hero_videos = media_ctx.get("hero_videos") or []
    collage_images = media_ctx.get("collage_images") or []

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

    partner_logo_dir = Path(__file__).resolve().parent.parent / "static" / "img" / "partners"

    def _partner_slug(name: str) -> str:
        raw = str(name or "").strip().lower()
        manual = {
            "сбер": "sber",
            "альфа-лизинг": "alfa-leasing",
            "европлан": "europlan",
            "ресо-лизинг": "reso-leasing",
            "псб лизинг": "psb-leasing",
            "совкомбанк лизинг": "sovcombank-leasing",
        }
        if raw in manual:
            return manual[raw]
        safe = "".join(ch if ch.isalnum() or ch in {" ", "-", "_"} else "" for ch in raw)
        return safe.strip().replace(" ", "-")

    partner_logos = []
    for item in home_content.get("hero", {}).get("why_items", []) or []:
        name = str(item or "").strip()
        if not name:
            continue
        slug = _partner_slug(name)
        logo = None
        for ext in ("svg", "webp", "png", "jpg", "jpeg"):
            candidate = partner_logo_dir / f"{slug}.{ext}"
            if candidate.exists():
                logo = f"/static/img/partners/{candidate.name}"
                break
        partner_logos.append({"name": name, "logo": logo})

    country_labels = home_filter_ctx.get("country_labels") or {}
    countries_list = home_filter_ctx.get("countries") or []
    countries_with_labels = [
        {"value": c, "label": country_labels.get(c, c)}
        for c in countries_list
    ]
    t0 = time.perf_counter()
    count_params = normalize_filter_params(dict(request.query_params))
    total_cars = _get_cars_count(service, count_params, timing_enabled)
    _stage("total_cars_ms", t0)
    context = {
        "request": request,
        "user": getattr(request.state, "user", None),
        "total_cars": total_cars,
        "brands": home_filter_ctx["brands"],
        "regions": home_filter_ctx["regions"],
        "countries": countries_list,
        "countries_labeled": countries_with_labels,
        "country_labels": country_labels,
        "kr_types": home_filter_ctx["kr_types"],
        "reg_years": home_filter_ctx["reg_years"],
        "reg_months": home_filter_ctx["reg_months"],
        "brand_stats": brand_stats,
        "brand_logos": brand_logos,
        "partner_logos": partner_logos,
        "body_type_stats": body_type_stats,
        "recommended_cars": recommended,
        "content": content,
        "home": home_content,
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
    timing: Dict[str, float] = {}
    t_start = time.perf_counter()
    ctx = _home_context(request, service, db, timing=timing)
    timing["total_ms"] = (time.perf_counter() - t_start) * 1000
    request.state.perf = {
        "db_ms": float(
            timing.get("home_filter_ctx_ms", 0)
            + timing.get("total_cars_ms", 0)
            + timing.get("recommended_ms", 0)
        ),
        "redis_ms": float(timing.get("fx_rates_ms", 0)),
    }
    if os.environ.get("HTML_TIMING", "0") == "1":
        request.state.html_parts = {"route": "home", **timing}
        print(
            "HOME_TIMING "
            + " ".join(f"{k}={v:.2f}" for k, v in timing.items()),
            flush=True,
        )
        logger.info("HOME_TIMING %s", request.state.html_parts)
    t_render = time.perf_counter()
    resp = templates.TemplateResponse("home.html", ctx)
    request.state.perf["render_ms"] = (time.perf_counter() - t_render) * 1000
    return resp


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
        content_map = content_service.content_map()
        lead_email = content_map.get("lead_email") or "Level.avto@mail.ru"
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
            mail_from = os.environ.get("EMAIL_FROM", "Level.avto@mail.ru")
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
        tg_token = os.environ.get("TELEGRAM_BOT_TOKEN")
        tg_chat = (
            os.environ.get("TELEGRAM_ADMIN_CHAT_ID")
            or os.environ.get("TELEGRAM_CHAT_ID")
            or (os.environ.get("TELEGRAM_ALLOWED_IDS", "").split(",")[0].strip() if os.environ.get("TELEGRAM_ALLOWED_IDS") else "")
        )
        if tg_token and tg_chat:
            tg_text = (
                "Новая заявка с сайта Level Avto\n"
                f"Имя: {name}\n"
                f"Телефон: {phone}\n"
                f"Email: {email or '—'}\n"
                f"Предпочтения: {preferred or '—'}\n"
                f"Бюджет: {price_range or '—'}\n"
                f"Комментарий: {comment or '—'}"
            )
            send_telegram_message(tg_token, tg_chat, tg_text)
        # always log to stdout
        print("[LEAD]", {"name": name, "phone": phone, "email": email, "preferred": preferred,
              "price_range": price_range, "comment": comment, "sent": sent})
        status["success"] = True
        status["message"] = "Спасибо! Мы свяжемся с вами в ближайшее время."
    extra = {"lead_status": status}
    timing: Dict[str, float] = {}
    t_start = time.perf_counter()
    ctx = _home_context(request, service, db, extra, timing=timing)
    timing["total_ms"] = (time.perf_counter() - t_start) * 1000
    if os.environ.get("HTML_TIMING", "0") == "1":
        request.state.html_parts = {"route": "home", **timing}
        print(
            "HOME_TIMING "
            + " ".join(f"{k}={v:.2f}" for k, v in timing.items()),
            flush=True,
        )
        logger.info("HOME_TIMING %s", request.state.html_parts)
    return templates.TemplateResponse("home.html", ctx)


@router.get("/catalog")
def catalog_page(request: Request, db=Depends(get_db), user=Depends(get_current_user)):
    templates = request.app.state.templates
    service = CarsService(db)
    timing: Dict[str, float] = {}
    t_start = time.perf_counter()
    t0 = time.perf_counter()
    raw_params = dict(request.query_params)
    ctx_params = normalize_filter_params(raw_params)
    region_param = ctx_params.get("region")
    regions = [r["value"] for r in service.facet_counts(field="region", filters={})]
    country_filters = {"region": region_param} if region_param else {}
    countries = []
    seen_countries = set()
    for row in service.facet_counts(field="country", filters=country_filters):
        raw_val = row.get("value")
        if not raw_val:
            continue
        code = normalize_country_code(raw_val)
        if not code or code in seen_countries:
            continue
        countries.append(code)
        seen_countries.add(code)
    country_labels = {**{c: country_label_ru(c) for c in countries}, "EU": "Европа", "KR": "Корея"}
    kr_types = []
    if "KR" in regions:
        kr_types = [
            {"value": "KR_INTERNAL", "label": "Корея (внутренний рынок)"},
            {"value": "KR_IMPORT", "label": "Корея (импорт)"},
        ]
    reg_months = [{"value": i + 1, "label": MONTHS_RU[i]} for i in range(12)]
    timing["base_ctx_ms"] = (time.perf_counter() - t0) * 1000
    qp = request.query_params
    params = dict(qp)
    # Backward-compat: map eu_country -> country (read-only alias)
    if not params.get("country") and params.get("eu_country"):
        params["country"] = params.get("eu_country")
    if "eu_country" in params:
        params.pop("eu_country", None)
    canon_region = str(params.get("region") or "").strip().upper() or None
    canon_country = str(params.get("country") or "").strip().upper() or None
    canon_kr_type = str(params.get("kr_type") or "").strip().upper() or None
    canon_brand = normalize_brand(params.get("brand")).strip() if params.get("brand") else None
    canon_model = str(params.get("model") or "").strip() or None
    def _int_val(key: str) -> Optional[int]:
        raw = params.get(key)
        if raw is None or raw == "":
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None
    def _float_val(key: str) -> Optional[float]:
        raw = params.get(key)
        if raw is None or raw == "":
            return None
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None
    def _bool_val(key: str) -> Optional[bool]:
        raw = params.get(key)
        if raw is None or raw == "":
            return None
        if isinstance(raw, bool):
            return raw
        value = str(raw).strip().lower()
        if value in {"1", "true", "yes", "on"}:
            return True
        if value in {"0", "false", "no", "off"}:
            return False
        return None
    initial_items: List[dict] = []
    initial_total: Optional[int] = None
    try:
        source_list = qp.getlist("source") if hasattr(qp, "getlist") else []
        source_value: Optional[str | List[str]] = source_list if source_list else params.get("source")
        items, initial_total = service.list_cars(
            region=canon_region,
            country=canon_country,
            kr_type=canon_kr_type,
            brand=canon_brand,
            lines=qp.getlist("line") if hasattr(qp, "getlist") else None,
            source_key=source_value,
            q=params.get("q"),
            model=canon_model,
            generation=params.get("generation"),
            color=params.get("color"),
            body_type=params.get("body_type"),
            engine_type=params.get("engine_type"),
            transmission=params.get("transmission"),
            drive_type=params.get("drive_type"),
            price_min=_float_val("price_min"),
            price_max=_float_val("price_max"),
            power_hp_min=_float_val("power_hp_min"),
            power_hp_max=_float_val("power_hp_max"),
            engine_cc_min=_int_val("engine_cc_min"),
            engine_cc_max=_int_val("engine_cc_max"),
            year_min=_int_val("year_min"),
            year_max=_int_val("year_max"),
            mileage_min=_int_val("mileage_min"),
            mileage_max=_int_val("mileage_max"),
            reg_year_min=_int_val("reg_year_min"),
            reg_month_min=_int_val("reg_month_min"),
            reg_year_max=_int_val("reg_year_max"),
            reg_month_max=_int_val("reg_month_max"),
            num_seats=params.get("num_seats"),
            doors_count=params.get("doors_count"),
            emission_class=params.get("emission_class"),
            efficiency_class=params.get("efficiency_class"),
            climatisation=params.get("climatisation"),
            airbags=params.get("airbags"),
            interior_design=params.get("interior_design"),
            interior_color=params.get("interior_color"),
            interior_material=params.get("interior_material"),
            vat_reclaimable=params.get("vat_reclaimable"),
            air_suspension=_bool_val("air_suspension"),
            price_rating_label=params.get("price_rating_label"),
            owners_count=params.get("owners_count"),
            condition=params.get("condition"),
            sort=params.get("sort") or "price_asc",
            page=_int_val("page") or 1,
            page_size=12,
            light=True,
        )
        def _normalize_thumb(url: str | None) -> str | None:
            normalized = normalize_classistatic_url(url)
            if normalized:
                return normalized
            raw = (url or "").strip()
            return raw or None

        if isinstance(items, list):
            initial_items = items
            for c in initial_items:
                if not isinstance(c, dict):
                    continue
                c["display_price_rub"] = display_price_rub(
                    c.get("total_price_rub_cached"),
                    c.get("price_rub_cached"),
                    allow_price_fallback=True,
                )
                c["price_note"] = price_without_util_note(
                    display_price=c.get("display_price_rub"),
                    total_price_rub_cached=c.get("total_price_rub_cached"),
                    calc_breakdown=c.get("calc_breakdown_json"),
                    region=params.get("region"),
                    country=c.get("country"),
                )
                c["display_engine_type"] = translate_payload_value("engine_type", c.get("engine_type")) or c.get("engine_type")
                c["display_transmission"] = translate_payload_value("transmission", c.get("transmission")) or c.get("transmission")
                c["display_drive_type"] = translate_payload_value("drive_type", c.get("drive_type")) or c.get("drive_type")
                c["engine_cc"] = effective_engine_cc_value(c)
                c["power_hp"] = effective_power_hp_value(c)
                c["power_kw"] = effective_power_kw_value(c)
                c["display_body_type"] = ru_body(c.get("body_type")) or display_body(c.get("body_type")) or c.get("body_type")
                normalized_color = normalize_color(c.get("color"))
                c["display_color"] = (
                    ru_color(c.get("color"))
                    or display_color(c.get("color"))
                    or (ru_color(normalized_color) if normalized_color else None)
                    or (display_color(normalized_color) if normalized_color else None)
                    or c.get("color")
                )
            ids = [c.get("id") for c in initial_items if isinstance(c, dict) and c.get("id")]
            if ids:
                rows = db.execute(
                    select(CarImage.car_id, func.min(CarImage.url))
                    .where(CarImage.car_id.in_(ids))
                    .group_by(CarImage.car_id)
                ).all()
                first_urls = {car_id: _normalize_thumb(url) for car_id, url in rows if url}
                for c in initial_items:
                    if not isinstance(c, dict):
                        continue
                    cid = c.get("id")
                    if cid in first_urls and first_urls[cid]:
                        c["thumbnail_url"] = first_urls[cid]
                    thumb = resolve_thumbnail_url(
                        _normalize_thumb(c.get("thumbnail_url")),
                        c.get("thumbnail_local_path"),
                    )
                    if thumb:
                        c["thumbnail_url"] = thumb
                    if not c.get("thumbnail_url"):
                        c["thumbnail_url"] = "/static/img/no-photo.svg"
    except Exception:
        logger.exception("catalog_initial_items_failed")
    t0 = time.perf_counter()
    fx_rates = service.get_fx_rates(allow_fetch=False) or {}
    timing["fx_rates_ms"] = (time.perf_counter() - t0) * 1000
    t0 = time.perf_counter()
    contact_content = ContentService(db).content_map(
        [
            "contact_phone",
            "contact_email",
            "contact_address",
            "contact_tg",
            "contact_wa",
            "contact_ig",
            "contact_vk",
            "contact_avito",
            "contact_autoru",
            "contact_max",
            "contact_map_link",
        ])
    timing["content_ms"] = (time.perf_counter() - t0) * 1000
    timing["total_ms"] = (time.perf_counter() - t_start) * 1000
    request.state.perf = {
        "db_ms": float(timing.get("base_ctx_ms", 0)),
        "redis_ms": float(timing.get("fx_rates_ms", 0)),
    }
    if os.environ.get("HTML_TIMING", "0") == "1":
        request.state.html_parts = {"route": "catalog", **timing}
        print(
        "CATALOG_TIMING "
        + " ".join(f"{k}={v:.2f}" for k, v in timing.items()),
        flush=True,
    )
        logger.info("CATALOG_TIMING %s", request.state.html_parts)
    print(
        "CATALOG_SSR "
        f"items={len(initial_items)} region={canon_region} country={canon_country} "
        f"brand={canon_brand} model={canon_model} sort={params.get('sort') or 'price_asc'}",
        flush=True,
    )

    t_render = time.perf_counter()
    resp = templates.TemplateResponse(
        "catalog.html",
        {
            "request": request,
            "user": getattr(request.state, "user", None),
            "brands": [],
            "regions": regions,
            "countries": countries,
            "country_labels": country_labels,
            "kr_types": kr_types,
            "reg_years": [],
            "reg_months": reg_months,
            "price_options": [],
            "mileage_options": [],
            "generations": [],
            "colors_basic": [],
            "colors_other": [],
            "body_types": [],
            "engine_types": [],
            "transmissions": [],
            "drive_types": [],
            "interior_color_options": [],
            "interior_material_options": [],
            "has_air_suspension": service.has_air_suspension(),
            "initial_items": initial_items,
            "fx_rates": fx_rates,
            "content": contact_content,
            "contact_phone": contact_content.get("contact_phone"),
            "contact_email": contact_content.get("contact_email"),
            "contact_address": contact_content.get("contact_address"),
            "contact_tg": contact_content.get("contact_tg"),
            "contact_wa": contact_content.get("contact_wa"),
            "contact_ig": contact_content.get("contact_ig"),
            "contact_vk": contact_content.get("contact_vk"),
            "contact_avito": contact_content.get("contact_avito"),
            "contact_autoru": contact_content.get("contact_autoru"),
            "contact_max": contact_content.get("contact_max"),
            "contact_map_link": contact_content.get("contact_map_link"),
        },
    )
    request.state.perf["render_ms"] = (time.perf_counter() - t_render) * 1000
    return resp


@router.get("/search")
def search_page(request: Request, db=Depends(get_db), user=Depends(get_current_user)):
    templates = request.app.state.templates
    service = CarsService(db)
    timing_enabled = os.environ.get("HTML_TIMING", "0") == "1"
    t0 = time.perf_counter()
    raw_params = dict(request.query_params)
    params = normalize_filter_params(raw_params)
    cache_key = build_filter_ctx_key(params, include_payload=False)
    cached = redis_get_json(cache_key)
    cache_hit = 0
    cache_source = "fallback"
    if cached:
        filter_ctx = cached
        cache_hit = 1
        cache_source = "redis"
    else:
        filter_ctx = _build_filter_context(service, db, include_payload=False, params=params)
    contact_content = ContentService(db).content_map(
        [
            "contact_phone",
            "contact_email",
            "contact_address",
            "contact_tg",
            "contact_wa",
            "contact_ig",
            "contact_vk",
            "contact_avito",
            "contact_autoru",
            "contact_max",
            "contact_map_link",
        ])
    total_cars = _get_cars_count(service, params, timing_enabled)
    request.state.perf = {
        "db_ms": float((time.perf_counter() - t0) * 1000),
        "redis_ms": 0.0,
    }
    if timing_enabled:
        total_ms = (time.perf_counter() - t0) * 1000
        print(
            f"SEARCH_TIMING total_ms={total_ms:.2f} filter_ctx_hit={cache_hit} filter_ctx_source={cache_source} filter_ctx_key={cache_key}",
            flush=True,
        )
    t_render = time.perf_counter()
    resp = templates.TemplateResponse(
        "search.html",
        {
            "request": request,
            "user": getattr(request.state, "user", None),
            "total_cars": total_cars,
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
            "interior_color_options": filter_ctx.get("interior_color_options") or [],
            "interior_material_options": filter_ctx.get("interior_material_options") or [],
            "price_rating_labels": filter_ctx["price_rating_labels"],
            "seats_options_eu": filter_ctx["seats_options_eu"],
            "doors_options_eu": filter_ctx["doors_options_eu"],
            "owners_options_eu": filter_ctx["owners_options_eu"],
            "emission_classes_eu": filter_ctx["emission_classes_eu"],
            "efficiency_classes_eu": filter_ctx["efficiency_classes_eu"],
            "climatisation_options_eu": filter_ctx["climatisation_options_eu"],
            "airbags_options_eu": filter_ctx["airbags_options_eu"],
            "interior_design_options_eu": filter_ctx["interior_design_options_eu"],
            "interior_color_options_eu": filter_ctx.get("interior_color_options_eu") or [],
            "interior_material_options_eu": filter_ctx.get("interior_material_options_eu") or [],
            "price_rating_labels_eu": filter_ctx["price_rating_labels_eu"],
            "seats_options_kr": filter_ctx["seats_options_kr"],
            "doors_options_kr": filter_ctx["doors_options_kr"],
            "owners_options_kr": filter_ctx["owners_options_kr"],
            "emission_classes_kr": filter_ctx["emission_classes_kr"],
            "efficiency_classes_kr": filter_ctx["efficiency_classes_kr"],
            "climatisation_options_kr": filter_ctx["climatisation_options_kr"],
            "airbags_options_kr": filter_ctx["airbags_options_kr"],
            "interior_design_options_kr": filter_ctx["interior_design_options_kr"],
            "interior_color_options_kr": filter_ctx.get("interior_color_options_kr") or [],
            "interior_material_options_kr": filter_ctx.get("interior_material_options_kr") or [],
            "price_rating_labels_kr": filter_ctx["price_rating_labels_kr"],
            "has_air_suspension": filter_ctx["has_air_suspension"],
            "content": contact_content,
            "contact_phone": contact_content.get("contact_phone"),
            "contact_email": contact_content.get("contact_email"),
            "contact_address": contact_content.get("contact_address"),
            "contact_tg": contact_content.get("contact_tg"),
            "contact_wa": contact_content.get("contact_wa"),
            "contact_ig": contact_content.get("contact_ig"),
            "contact_vk": contact_content.get("contact_vk"),
            "contact_avito": contact_content.get("contact_avito"),
            "contact_autoru": contact_content.get("contact_autoru"),
            "contact_max": contact_content.get("contact_max"),
            "contact_map_link": contact_content.get("contact_map_link"),
        },
    )
    request.state.perf["render_ms"] = (time.perf_counter() - t_render) * 1000
    return resp


@router.get("/car/{car_id}")
def car_detail_page(car_id: int, request: Request, db=Depends(get_db), user=Depends(get_current_user)):
    templates = request.app.state.templates
    service = CarsService(db)
    car = service.get_car(car_id)
    detail_images: list[str] = []
    if car:
        code, label = resolve_display_country(car)
        car.display_country_code = code
        car.display_country_label = label
        car.display_price_rub = display_price_rub(
            car.total_price_rub_cached,
            car.price_rub_cached,
            allow_price_fallback=True,
        )
        car.price_note = price_without_util_note(
            display_price=car.display_price_rub,
            total_price_rub_cached=car.total_price_rub_cached,
            calc_breakdown=car.calc_breakdown_json,
            region="KR" if str(car.country or "").upper().startswith("KR") else ("EU" if str(car.country or "").upper() and str(car.country or "").upper() != "RU" else None),
            country=car.country,
        )
        def _normalize_detail_image(url: str | None) -> str | None:
            raw = (url or "").strip()
            if not raw:
                return None
            if raw.startswith("/thumb?") or raw.startswith("http://") or raw.startswith("https://"):
                try:
                    parsed = urlparse(raw if raw.startswith("/thumb?") else str(raw))
                    if parsed.path == "/thumb" or parsed.path.endswith("/thumb"):
                        params = parse_qs(parsed.query or "")
                        src = (params.get("u") or [None])[0]
                        if src:
                            raw = unquote(src).strip()
                except Exception:
                    pass
            normalized = normalize_classistatic_url(raw)
            if normalized:
                return normalized
            if raw.startswith("//"):
                return f"https:{raw}"
            if raw.startswith("http://"):
                return f"https://{raw[7:]}"
            if raw.startswith("/media/"):
                return raw if local_media_exists(raw) else None
            if raw.startswith("https://"):
                return raw
            return None

        if getattr(car, "images", None):
            for im in car.images:
                try:
                    normalized = _normalize_detail_image(getattr(im, "url", None))
                    if normalized:
                        detail_images.append(normalized)
                except Exception:
                    continue
        try:
            local_detail = next((u for u in detail_images if isinstance(u, str) and u.startswith("/media/")), None)
            resolved_thumb = resolve_thumbnail_url(
                getattr(car, "thumbnail_url", None),
                getattr(car, "thumbnail_local_path", None),
            )
            if resolved_thumb:
                car.thumbnail_url = resolved_thumb
                if local_detail:
                    detail_images = [local_detail] + [u for u in detail_images if u != local_detail]
                    if resolved_thumb not in detail_images:
                        detail_images.insert(1 if len(detail_images) > 0 else 0, resolved_thumb)
                elif resolved_thumb in detail_images:
                    detail_images = [resolved_thumb] + [u for u in detail_images if u != resolved_thumb]
                else:
                    detail_images.insert(0, resolved_thumb)
        except Exception:
            pass
        if detail_images:
            deduped_images: list[str] = []
            seen_images: set[str] = set()
            for url in detail_images:
                key = _normalize_detail_image(url)
                if not key or key in seen_images:
                    continue
                seen_images.add(key)
                deduped_images.append(key)
            detail_images = deduped_images
    details = []
    options = []
    calc = None
    if car:
        calc = service.ensure_calc_cache(car)
        car.engine_cc = effective_engine_cc_value(car)
        car.power_hp = effective_power_hp_value(car)
        car.power_kw = effective_power_kw_value(car)
        car.display_body_type = ru_body(getattr(car, "body_type", None)) or display_body(getattr(car, "body_type", None)) or car.body_type
        normalized_color = normalize_color(getattr(car, "color", None))
        car.display_color = (
            ru_color(getattr(car, "color", None))
            or display_color(getattr(car, "color", None))
            or (ru_color(normalized_color) if normalized_color else None)
            or (display_color(normalized_color) if normalized_color else None)
            or car.color
        )
        car.display_engine_type = translate_payload_value("engine_type", getattr(car, "engine_type", None)) or car.engine_type
        car.display_transmission = translate_payload_value("transmission", getattr(car, "transmission", None)) or car.transmission
        car.display_drive_type = translate_payload_value("drive_type", getattr(car, "drive_type", None)) or car.drive_type
        car.display_price_rub = display_price_rub(
            car.total_price_rub_cached,
            car.price_rub_cached,
            allow_price_fallback=True,
        )
        car.price_note = price_without_util_note(
            display_price=car.display_price_rub,
            total_price_rub_cached=car.total_price_rub_cached,
            calc_breakdown=car.calc_breakdown_json,
            region="KR" if str(car.country or "").upper().startswith("KR") else ("EU" if str(car.country or "").upper() and str(car.country or "").upper() != "RU" else None),
            country=car.country,
        )
        payload = car.source_payload or {}
        pricing = service.price_info(car)
        raw_description = (getattr(car, "description", None) or payload.get("description") or "").strip()
        car.display_description = raw_description if raw_description else None

        def push(label: str, value: Any, *, as_color: bool = False) -> None:
            if value is None:
                return
            if isinstance(value, str) and not value.strip():
                return
            if as_color:
                details.append({"label": label, "value": translate_payload_value("manufacturer_color", str(value)) or value})
                return
            details.append({"label": label, "value": translate_payload_value(label, str(value)) or value})

        push("Мест", payload.get("num_seats"))
        push("Дверей", payload.get("doors_count"))
        push("Владельцев", payload.get("owners_count"))
        push("Экокласс", payload.get("emission_class"))
        push("Класс CO₂", payload.get("envkv_co2_class") or payload.get("envkv_co2_class_value"))
        push("Эффективность", payload.get("efficiency_class"))
        push("Климат", payload.get("climatisation"))
        push("Интерьер", payload.get("interior_design"))
        push("Парктроники", payload.get("park_assists"))
        push("Подушки", payload.get("airbags"))
        push("Цвет производителя", payload.get("manufacturer_color"), as_color=True)
        push("Расход топлива", payload.get("envkv_energy_consumption") or payload.get("fuel_consumption"))
        push("CO₂", payload.get("envkv_co2_emissions") or payload.get("co_emission"))
        push("Оценка цены", payload.get("price_rating_label"))

        raw_options = payload.get("options")
        raw_features = payload.get("features")
        merged_options: list[str] = []
        seen_options: set[str] = set()

        def collect_option(value: Any) -> None:
            raw = str(value or "").strip()
            if not raw or raw in seen_options:
                return
            seen_options.add(raw)
            merged_options.append(translate_payload_value("options", raw) or raw)

        if isinstance(raw_options, list):
            for opt in raw_options:
                collect_option(opt)
        elif isinstance(raw_options, str):
            collect_option(raw_options)

        if isinstance(raw_features, list):
            for opt in raw_features:
                collect_option(opt)
        elif isinstance(raw_features, str):
            collect_option(raw_features)

        options = merged_options
    return templates.TemplateResponse(
        "car_detail.html",
        {
            "request": request,
            "car": car,
            "detail_images": detail_images,
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
