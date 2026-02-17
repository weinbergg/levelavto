from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy.orm import Session
from typing import Optional, List, Any
import os
from ..db import get_db
from ..services.cars_service import CarsService, normalize_brand
from ..schemas import CarDetailOut
from ..utils.country_map import resolve_display_country, normalize_country_code, country_label_ru
from ..utils.taxonomy import (
    normalize_fuel,
    ru_fuel,
    ru_color,
    ru_body,
    ru_transmission,
    ru_drivetrain,
    color_hex,
)
from ..utils.localization import display_body, display_color
from ..utils.price_utils import display_price_rub, price_without_util_note
from ..utils.color_groups import split_color_facets
from ..utils.thumbs import normalize_classistatic_url, pick_classistatic_thumb
from ..utils.redis_cache import (
    redis_get_json,
    redis_set_json,
    redis_try_lock,
    redis_unlock,
    redis_wait_json,
    build_cars_list_key,
    build_cars_list_full_key,
    build_cars_count_simple_key,
    build_cars_count_key,
    build_filter_payload_key,
    build_filter_ctx_base_key,
    build_filter_ctx_brand_key,
    build_filter_ctx_model_key,
    normalize_filter_params,
    build_cars_count_key,
    normalize_count_params,
)
from ..models.car_image import CarImage
from sqlalchemy import select, func
import re
import time
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

TOP_BRANDS = [
    "BMW",
    "Mercedes-Benz",
    "Audi",
    "Porsche",
    "Skoda",
    "Toyota",
    "Volkswagen",
    "Volvo",
    "Aston Martin",
    "Bentley",
    "Bugatti",
    "BYD",
    "Cadillac",
    "Ferrari",
    "GMC",
    "Hummer",
    "Hyundai",
    "Jaguar",
    "Jeep",
    "Kia",
    "Lamborghini",
    "Land Rover",
    "Lexus",
    "Lincoln",
    "Lynk&Co",
    "Maybach",
    "Mazda",
    "McLaren",
    "Mini",
    "Rolls-Royce",
    "Tesla",
    "Zeekr",
]


def _parse_hot_cache_brands() -> set[str]:
    raw = os.getenv("HOT_CACHE_BRANDS", ",".join(TOP_BRANDS))
    out: set[str] = set()
    for part in raw.split(","):
        value = normalize_brand(part.strip())
        if value:
            out.add(value)
    return out


TOP_BRANDS_SET = _parse_hot_cache_brands()


def _cacheable_catalog_filters(
    *,
    region: Optional[str],
    country: Optional[str],
    brand: Optional[str],
    model: Optional[str],
    generation: Optional[str],
    color: Optional[str],
    body_type: Optional[str],
    engine_type: Optional[str],
    transmission: Optional[str],
    drive_type: Optional[str],
    price_min: Optional[float],
    price_max: Optional[float],
    power_hp_min: Optional[float],
    power_hp_max: Optional[float],
    engine_cc_min: Optional[int],
    engine_cc_max: Optional[int],
    year_min: Optional[int],
    year_max: Optional[int],
    mileage_min: Optional[int],
    mileage_max: Optional[int],
    kr_type: Optional[str],
    reg_year_min: Optional[int],
    reg_month_min: Optional[int],
    reg_year_max: Optional[int],
    reg_month_max: Optional[int],
    condition: Optional[str],
    q: Optional[str],
    line: Optional[List[str]],
    source: Optional[str | List[str]],
    num_seats: Optional[str],
    doors_count: Optional[str],
    emission_class: Optional[str],
    efficiency_class: Optional[str],
    climatisation: Optional[str],
    airbags: Optional[str],
    interior_design: Optional[str],
    air_suspension: Optional[bool],
    price_rating_label: Optional[str],
    owners_count: Optional[str],
) -> bool:
    if any(
        [
            model,
            generation,
            color,
            body_type,
            engine_type,
            transmission,
            drive_type,
            price_min is not None,
            price_max is not None,
            power_hp_min is not None,
            power_hp_max is not None,
            engine_cc_min is not None,
            engine_cc_max is not None,
            year_min is not None,
            year_max is not None,
            mileage_min is not None,
            mileage_max is not None,
            kr_type,
            reg_year_min is not None,
            reg_month_min is not None,
            reg_year_max is not None,
            reg_month_max is not None,
            condition,
            q,
            line,
            source,
            num_seats,
            doors_count,
            emission_class,
            efficiency_class,
            climatisation,
            airbags,
            interior_design,
            air_suspension is True,
            price_rating_label,
            owners_count,
        ]
    ):
        return False
    if brand and normalize_brand(brand) not in TOP_BRANDS_SET:
        return False
    return True


def _sort_by_label(items: list[dict]) -> list[dict]:
    return sorted(items, key=lambda x: (x.get("label") or x.get("value") or "").strip().casefold())


def _region_label(code: str | None) -> str:
    if not code:
        return ""
    key = code.upper()
    if key == "EU":
        return "Европа"
    if key == "KR":
        return "Корея"
    if key == "RU":
        return "Россия"
    return key


def _canonicalize_params(
    *,
    region: Optional[str] = None,
    country: Optional[str] = None,
    eu_country: Optional[str] = None,
    kr_type: Optional[str] = None,
    brand: Optional[str] = None,
    model: Optional[str] = None,
) -> dict:
    raw = {
        "region": region,
        "country": country,
        "eu_country": eu_country,
        "kr_type": kr_type,
        "brand": brand,
        "model": model,
    }
    normalized = normalize_count_params(raw)
    if normalized.get("brand"):
        normalized["brand"] = normalize_brand(normalized["brand"]).strip()
        if normalized["brand"] == "":
            normalized["brand"] = None
    if normalized.get("model"):
        normalized["model"] = str(normalized["model"]).strip()
        if normalized["model"] == "":
            normalized["model"] = None
    return normalized


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


@router.get("/cars")
def list_cars(
    request: Request,
    region: Optional[str] = Query(default=None),
    country: Optional[str] = Query(default=None),
    eu_country: Optional[str] = Query(default=None, alias="eu_country"),
    brand: Optional[str] = Query(default=None),
    line: Optional[List[str]] = Query(
        default=None, description="Advanced search lines brand|model|variant"),
    source: Optional[str | List[str]] = Query(
        default=None, description="Source key, e.g., mobile_de or emavto_klg"),
    q: Optional[str] = Query(
        default=None, description="Free-text brand/model search"),
    model: Optional[str] = Query(default=None),
    generation: Optional[str] = Query(default=None),
    color: Optional[str] = Query(default=None),
    body_type: Optional[str] = Query(default=None),
    engine_type: Optional[str] = Query(default=None),
    transmission: Optional[str] = Query(default=None),
    drive_type: Optional[str] = Query(default=None),
    num_seats: Optional[str] = Query(default=None),
    doors_count: Optional[str] = Query(default=None),
    emission_class: Optional[str] = Query(default=None),
    efficiency_class: Optional[str] = Query(default=None),
    climatisation: Optional[str] = Query(default=None),
    airbags: Optional[str] = Query(default=None),
    interior_design: Optional[str] = Query(default=None),
    air_suspension: Optional[bool] = Query(default=None),
    price_rating_label: Optional[str] = Query(default=None),
    owners_count: Optional[str] = Query(default=None),
    price_min: Optional[float] = Query(default=None),
    price_max: Optional[float] = Query(default=None),
    power_hp_min: Optional[float] = Query(default=None),
    power_hp_max: Optional[float] = Query(default=None),
    engine_cc_min: Optional[int] = Query(default=None),
    engine_cc_max: Optional[int] = Query(default=None),
    year_min: Optional[int] = Query(default=None),
    year_max: Optional[int] = Query(default=None),
    mileage_min: Optional[int] = Query(default=None),
    mileage_max: Optional[int] = Query(default=None),
    kr_type: Optional[str] = Query(
        default=None, description="KR_INTERNAL|KR_IMPORT"),
    reg_year_min: Optional[int] = Query(default=None),
    reg_month_min: Optional[int] = Query(default=None),
    reg_year_max: Optional[int] = Query(default=None),
    reg_month_max: Optional[int] = Query(default=None),
    condition: Optional[str] = Query(default=None),
    sort: Optional[str] = Query(
        default=None,
        description="price_asc|price_desc|mileage_asc|mileage_desc|reg_desc|reg_asc|listing_desc|listing_asc",
    ),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    service = CarsService(db)
    timing_enabled = os.environ.get("CAR_API_TIMING", "0") == "1"
    t0 = time.perf_counter()
    canon = _canonicalize_params(
        region=region,
        country=country,
        eu_country=eu_country,
        kr_type=kr_type,
        brand=brand,
        model=model,
    )
    cache_ok = _cacheable_catalog_filters(
        region=canon.get("region"),
        country=canon.get("country"),
        brand=canon.get("brand"),
        model=model,
        generation=generation,
        color=color,
        body_type=body_type,
        engine_type=engine_type,
        transmission=transmission,
        drive_type=drive_type,
        price_min=price_min,
        price_max=price_max,
        power_hp_min=power_hp_min,
        power_hp_max=power_hp_max,
        engine_cc_min=engine_cc_min,
        engine_cc_max=engine_cc_max,
        year_min=year_min,
        year_max=year_max,
        mileage_min=mileage_min,
        mileage_max=mileage_max,
        kr_type=canon.get("kr_type"),
        reg_year_min=reg_year_min,
        reg_month_min=reg_month_min,
        reg_year_max=reg_year_max,
        reg_month_max=reg_month_max,
        condition=condition,
        q=q,
        line=line,
        source=source,
        num_seats=num_seats,
        doors_count=doors_count,
        emission_class=emission_class,
        efficiency_class=efficiency_class,
        climatisation=climatisation,
        airbags=airbags,
        interior_design=interior_design,
        air_suspension=air_suspension,
        price_rating_label=price_rating_label,
        owners_count=owners_count,
    )
    cache_key = None
    cache_lock_key = None
    cache_lock_token = None
    if cache_ok:
        cache_key = build_cars_list_key(
            canon.get("region"),
            canon.get("country"),
            canon.get("brand"),
            sort,
            page,
            page_size,
        )
        cached = redis_get_json(cache_key)
        if cached is not None:
            print("CARS_LIST_CACHE hit=1 source=redis key=%s" % cache_key, flush=True)
            return cached
        print("CARS_LIST_CACHE hit=0 source=fallback key=%s" % cache_key, flush=True)
        cache_lock_key = f"{cache_key}:lock"
        cache_lock_token = redis_try_lock(cache_lock_key, ttl_sec=25)
        if cache_lock_token is None:
            waited = redis_wait_json(cache_key, timeout_ms=2200, poll_ms=120)
            if waited is not None:
                print("CARS_LIST_CACHE hit=1 source=redis_wait key=%s" % cache_key, flush=True)
                return waited
    else:
        full_cache_params = {
            "region": canon.get("region"),
            "country": canon.get("country"),
            "brand": canon.get("brand"),
            "model": canon.get("model"),
            "generation": generation,
            "color": color,
            "body_type": body_type,
            "engine_type": engine_type,
            "transmission": transmission,
            "drive_type": drive_type,
            "price_min": price_min,
            "price_max": price_max,
            "power_hp_min": power_hp_min,
            "power_hp_max": power_hp_max,
            "engine_cc_min": engine_cc_min,
            "engine_cc_max": engine_cc_max,
            "year_min": year_min,
            "year_max": year_max,
            "mileage_min": mileage_min,
            "mileage_max": mileage_max,
            "kr_type": canon.get("kr_type"),
            "reg_year_min": reg_year_min,
            "reg_month_min": reg_month_min,
            "reg_year_max": reg_year_max,
            "reg_month_max": reg_month_max,
            "condition": condition,
            "q": q,
            "line": "|".join(line or []),
            "source": ",".join(source) if isinstance(source, list) else source,
            "num_seats": num_seats,
            "doors_count": doors_count,
            "emission_class": emission_class,
            "efficiency_class": efficiency_class,
            "climatisation": climatisation,
            "airbags": airbags,
            "interior_design": interior_design,
            "air_suspension": air_suspension,
            "price_rating_label": price_rating_label,
            "owners_count": owners_count,
        }
        cache_key = build_cars_list_full_key(full_cache_params, sort, page, page_size)
        cached = redis_get_json(cache_key)
        if cached is not None:
            print("CARS_LIST_FULL_CACHE hit=1 source=redis key=%s" % cache_key, flush=True)
            return cached
        print("CARS_LIST_FULL_CACHE hit=0 source=fallback key=%s" % cache_key, flush=True)
        cache_lock_key = f"{cache_key}:lock"
        cache_lock_token = redis_try_lock(cache_lock_key, ttl_sec=25)
        if cache_lock_token is None:
            waited = redis_wait_json(cache_key, timeout_ms=2200, poll_ms=120)
            if waited is not None:
                print("CARS_LIST_FULL_CACHE hit=1 source=redis_wait key=%s" % cache_key, flush=True)
                return waited
    if os.getenv("FILTERS_CANON") == "1":
        print(
            "FILTERS_CANON cars_count "
            f"region={canon.get('region')} country={canon.get('country')} kr_type={canon.get('kr_type')} "
            f"brand={canon.get('brand')} model={canon.get('model')}",
            flush=True,
        )
    if os.getenv("FILTERS_CANON") == "1":
        print(
            "FILTERS_CANON cars "
            f"region={canon.get('region')} country={canon.get('country')} kr_type={canon.get('kr_type')} "
            f"brand={canon.get('brand')} model={canon.get('model')}",
            flush=True,
        )
    items, total = service.list_cars(
        region=canon.get("region"),
        country=canon.get("country"),
        brand=canon.get("brand"),
        lines=line,
        source_key=source,
        q=q,
        model=canon.get("model"),
        generation=generation,
        color=color,
        price_min=price_min,
        price_max=price_max,
        power_hp_min=power_hp_min,
        power_hp_max=power_hp_max,
        engine_cc_min=engine_cc_min,
        engine_cc_max=engine_cc_max,
        year_min=year_min,
        year_max=year_max,
        mileage_min=mileage_min,
        mileage_max=mileage_max,
        kr_type=canon.get("kr_type"),
        reg_year_min=reg_year_min,
        reg_month_min=reg_month_min,
        reg_year_max=reg_year_max,
        reg_month_max=reg_month_max,
        body_type=body_type,
        engine_type=engine_type,
        transmission=transmission,
        drive_type=drive_type,
        num_seats=num_seats,
        doors_count=doors_count,
        emission_class=emission_class,
        efficiency_class=efficiency_class,
        climatisation=climatisation,
        airbags=airbags,
        interior_design=interior_design,
        air_suspension=air_suspension,
        price_rating_label=price_rating_label,
        owners_count=owners_count,
        condition=condition,
        sort=sort,
        page=page,
        page_size=page_size,
        light=True,
        use_fast_count=os.getenv("CATALOG_USE_FAST_COUNT", "1") != "0",
    )
    t1 = time.perf_counter()
    if items and not isinstance(items[0], dict):
        items = [dict(row) for row in items]
    image_counts = {}
    image_first = {}
    with_photo_stats = os.getenv("CATALOG_WITH_PHOTO_STATS", "0") == "1"
    def _normalize_thumb(url: str | None) -> str | None:
        return normalize_classistatic_url(url)
    if items and with_photo_stats:
        ids = [c.get("id") for c in items if c.get("id")]
        if ids:
            rows = (
                db.execute(
                    select(CarImage.car_id, func.count(CarImage.id))
                    .where(CarImage.car_id.in_(ids))
                    .group_by(CarImage.car_id)
                )
                .all()
            )
            image_counts = {car_id: int(cnt) for car_id, cnt in rows}
            rows = (
                db.execute(
                    select(CarImage.car_id, func.min(CarImage.url))
                    .where(CarImage.car_id.in_(ids))
                    .group_by(CarImage.car_id)
                )
                .all()
            )
            image_first = {car_id: _normalize_thumb(url) for car_id, url in rows if url}
    elif items:
        ids = [c.get("id") for c in items if c.get("id") and not c.get("thumbnail_url")]
        if ids:
            rows = (
                db.execute(
                    select(CarImage.car_id, func.min(CarImage.url))
                    .where(CarImage.car_id.in_(ids))
                    .group_by(CarImage.car_id)
                )
                .all()
            )
            image_first = {car_id: _normalize_thumb(url) for car_id, url in rows if url}
    t2 = time.perf_counter()
    payload_items = []
    fx_rates = service.get_fx_rates() or {}
    fx_eur = float(fx_rates.get("EUR") or 0)
    fx_usd = float(fx_rates.get("USD") or 0)
    eu_sources = set(service._source_ids_for_europe())
    kr_sources = set(service._source_ids_for_hints(service.KOREA_SOURCE_HINTS))
    eu_countries = set(service.EU_COUNTRIES)
    thumb_replaced = 0
    for c in items:
        country_raw = c.get("country") if isinstance(c, dict) else None
        country_norm = normalize_country_code(
            country_raw) if country_raw else None
        source_id = c.get("source_id") if isinstance(c, dict) else None
        if country_norm == "KR" or (country_norm and country_norm.startswith("KR")) or source_id in kr_sources:
            region_val = "KR"
        elif country_norm == "RU":
            region_val = "RU"
        elif country_norm in eu_countries or source_id in eu_sources:
            region_val = "EU"
        else:
            region_val = "EU" if country_norm else None
        img_count = image_counts.get(c.get("id"), 0)
        raw_thumb = _normalize_thumb(c.get("thumbnail_url"))
        thumb_url = raw_thumb or image_first.get(c.get("id"))
        thumb_url = pick_classistatic_thumb(thumb_url)
        if not thumb_url:
            thumb_url = "/static/img/no-photo.svg"
        if isinstance(thumb_url, str) and "rule=mo-" in thumb_url:
            thumb_replaced += 1
        total_cached = c.get("total_price_rub_cached")
        price_cached = c.get("price_rub_cached")
        display_rub = display_price_rub(
            total_cached,
            price_cached,
            allow_price_fallback=str(c.get("country") or "").upper() == "KR",
        )
        if display_rub is None and c.get("price") is not None:
            cur = str(c.get("currency") or "").upper()
            if cur == "EUR" and fx_eur > 0:
                display_rub = display_price_rub(None, float(c.get("price")) * fx_eur, allow_price_fallback=True)
            elif cur == "USD" and fx_usd > 0:
                display_rub = display_price_rub(None, float(c.get("price")) * fx_usd, allow_price_fallback=True)
            elif cur in {"RUB", "₽"}:
                display_rub = display_price_rub(None, float(c.get("price")), allow_price_fallback=True)
        payload_items.append(
            {
                "id": c.get("id"),
                "brand": c.get("brand"),
                "model": c.get("model"),
                "year": c.get("year"),
                "registration_year": c.get("registration_year"),
                "registration_month": c.get("registration_month"),
                "mileage": c.get("mileage"),
                "total_price_rub_cached": total_cached,
                "price_rub_cached": price_cached,
                "display_price_rub": display_rub,
                "price_note": price_without_util_note(
                    display_price=display_rub,
                    total_price_rub_cached=total_cached,
                    region=region_val,
                    country=country_norm or country_raw,
                ),
                "calc_updated_at": c.get("calc_updated_at"),
                "thumbnail_url": thumb_url,
                "country": country_norm or country_raw,
                "region": region_val,
                "color": c.get("color"),
                "display_color": ru_color(c.get("color")) or display_color(c.get("color")) or c.get("color"),
                "color_hex": color_hex(c.get("color")),
                "engine_cc": c.get("engine_cc"),
                "power_hp": c.get("power_hp"),
                "body_type": c.get("body_type"),
                "display_body_type": ru_body(c.get("body_type")) or display_body(c.get("body_type")) or c.get("body_type"),
                "transmission": c.get("transmission"),
                "display_transmission": ru_transmission(c.get("transmission")) or c.get("transmission"),
                "drive_type": c.get("drive_type"),
                "display_drive_type": ru_drivetrain(c.get("drive_type")) or c.get("drive_type"),
                "images_count": img_count,
                "photos_count": img_count,
                "price": c.get("price"),
                "currency": c.get("currency"),
                "display_country_label": country_label_ru(country_norm or country_raw) or (country_norm or country_raw),
            }
        )
    t3 = time.perf_counter()
    if timing_enabled:
        parts = {
            "list_ms": (t1 - t0) * 1000,
            "photos_ms": (t2 - t1) * 1000,
            "map_ms": (t3 - t2) * 1000,
            "total_ms": (t3 - t0) * 1000,
            "items": len(payload_items),
            "thumb_replaced": thumb_replaced,
        }
        print(
            "API_CARS_TIMING db_ms={list_ms:.2f} photos_ms={photos_ms:.2f} map_ms={map_ms:.2f} total_ms={total_ms:.2f} sort={sort} filters=({region},{country},{brand},{model})".format(
                sort=sort,
                region=canon.get("region"),
                country=canon.get("country"),
                brand=canon.get("brand"),
                model=canon.get("model"),
                **parts,
            ),
            flush=True,
        )
    resp = {
        "items": payload_items,
        "total": total,
        "page": page,
        "page_size": page_size,
    }
    if cache_key:
        list_ttl = int(os.getenv("CARS_LIST_CACHE_TTL_SEC", "21600") or 21600)
        redis_set_json(cache_key, resp, ttl_sec=max(300, list_ttl))
    if cache_lock_key and cache_lock_token:
        redis_unlock(cache_lock_key, cache_lock_token)
    return resp


@router.get("/cars_count")
def cars_count(
    request: Request,
    region: Optional[str] = Query(default=None),
    country: Optional[str] = Query(default=None),
    eu_country: Optional[str] = Query(default=None, alias="eu_country"),
    brand: Optional[str] = Query(default=None),
    model: Optional[str] = Query(default=None),
    color: Optional[str] = Query(default=None),
    engine_type: Optional[str] = Query(default=None),
    transmission: Optional[str] = Query(default=None),
    body_type: Optional[str] = Query(default=None),
    drive_type: Optional[str] = Query(default=None),
    kr_type: Optional[str] = Query(default=None),
    price_min: Optional[float] = Query(default=None),
    price_max: Optional[float] = Query(default=None),
    power_hp_min: Optional[float] = Query(default=None),
    power_hp_max: Optional[float] = Query(default=None),
    engine_cc_min: Optional[int] = Query(default=None),
    engine_cc_max: Optional[int] = Query(default=None),
    year_min: Optional[int] = Query(default=None),
    year_max: Optional[int] = Query(default=None),
    mileage_min: Optional[int] = Query(default=None),
    mileage_max: Optional[int] = Query(default=None),
    reg_year_min: Optional[int] = Query(default=None),
    reg_year_max: Optional[int] = Query(default=None),
    condition: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
):
    service = CarsService(db)
    canon = _canonicalize_params(
        region=region,
        country=country,
        eu_country=eu_country,
        kr_type=kr_type,
        brand=brand,
        model=model,
    )
    params = {
        "region": canon.get("region"),
        "country": canon.get("country"),
        "brand": canon.get("brand"),
        "model": canon.get("model"),
        "color": color,
        "engine_type": engine_type,
        "transmission": transmission,
        "body_type": body_type,
        "drive_type": drive_type,
        "kr_type": canon.get("kr_type"),
        "price_min": price_min,
        "price_max": price_max,
        "power_hp_min": power_hp_min,
        "power_hp_max": power_hp_max,
        "engine_cc_min": engine_cc_min,
        "engine_cc_max": engine_cc_max,
        "year_min": year_min,
        "year_max": year_max,
        "mileage_min": mileage_min,
        "mileage_max": mileage_max,
        "reg_year_min": reg_year_min,
        "reg_year_max": reg_year_max,
        "condition": condition,
    }
    normalized = normalize_count_params(params)
    cache_ok = _cacheable_catalog_filters(
        region=canon.get("region"),
        country=canon.get("country"),
        brand=canon.get("brand"),
        model=model,
        generation=None,
        color=color,
        body_type=body_type,
        engine_type=engine_type,
        transmission=transmission,
        drive_type=drive_type,
        price_min=price_min,
        price_max=price_max,
        power_hp_min=power_hp_min,
        power_hp_max=power_hp_max,
        engine_cc_min=engine_cc_min,
        engine_cc_max=engine_cc_max,
        year_min=year_min,
        year_max=year_max,
        mileage_min=mileage_min,
        mileage_max=mileage_max,
        kr_type=canon.get("kr_type"),
        reg_year_min=reg_year_min,
        reg_month_min=None,
        reg_year_max=reg_year_max,
        reg_month_max=None,
        condition=condition,
        q=None,
        line=None,
        source=None,
        num_seats=None,
        doors_count=None,
        emission_class=None,
        efficiency_class=None,
        climatisation=None,
        airbags=None,
        interior_design=None,
        air_suspension=None,
        price_rating_label=None,
        owners_count=None,
    )
    cache_key = build_cars_count_key(normalized)
    cached = redis_get_json(cache_key)
    if cached is not None:
        print("CARS_COUNT_FULL_CACHE hit=1 source=redis key=%s" % cache_key, flush=True)
        return {"count": int(cached)}
    print("CARS_COUNT_FULL_CACHE hit=0 source=fallback key=%s" % cache_key, flush=True)

    cache_lock_key = f"{cache_key}:lock"
    cache_lock_token = redis_try_lock(cache_lock_key, ttl_sec=20)
    if cache_lock_token is None:
        waited = redis_wait_json(cache_key, timeout_ms=2000, poll_ms=120)
        if waited is not None:
            print("CARS_COUNT_FULL_CACHE hit=1 source=redis_wait key=%s" % cache_key, flush=True)
            return {"count": int(waited)}

    cache_key_simple = None
    if cache_ok:
        cache_key_simple = build_cars_count_simple_key(
            canon.get("region"),
            canon.get("country"),
            canon.get("brand"),
        )
        cached = redis_get_json(cache_key_simple)
        if cached is not None:
            print("CARS_COUNT_CACHE hit=1 source=redis key=%s" % cache_key_simple, flush=True)
            redis_set_json(cache_key, int(cached), ttl_sec=1800)
            if cache_lock_token:
                redis_unlock(cache_lock_key, cache_lock_token)
            return {"count": int(cached)}
        print("CARS_COUNT_CACHE hit=0 source=fallback key=%s" % cache_key_simple, flush=True)
    try:
        total = service.count_cars(
            region=canon.get("region"),
            country=canon.get("country"),
            brand=canon.get("brand"),
            model=canon.get("model"),
            color=color,
            engine_type=engine_type,
            transmission=transmission,
            body_type=body_type,
            drive_type=drive_type,
            kr_type=canon.get("kr_type"),
            price_min=price_min,
            price_max=price_max,
            power_hp_min=power_hp_min,
            power_hp_max=power_hp_max,
            engine_cc_min=engine_cc_min,
            engine_cc_max=engine_cc_max,
            year_min=year_min,
            year_max=year_max,
            mileage_min=mileage_min,
            mileage_max=mileage_max,
            reg_year_min=reg_year_min,
            reg_year_max=reg_year_max,
            condition=condition,
        )
        redis_set_json(cache_key, int(total), ttl_sec=1800)
        if cache_ok and cache_key_simple:
            redis_set_json(cache_key_simple, int(total), ttl_sec=1200)
        return {"count": int(total)}
    finally:
        if cache_lock_token:
            redis_unlock(cache_lock_key, cache_lock_token)


@router.get("/advanced_count")
def advanced_count(request: Request, db: Session = Depends(get_db)):
    service = CarsService(db)
    qp = request.query_params
    region = qp.get("region")
    country = qp.get("country")
    eu_country = qp.get("eu_country")
    kr_type = qp.get("kr_type")
    brand = qp.get("brand")
    model = qp.get("model")
    canon = _canonicalize_params(
        region=region,
        country=country,
        eu_country=eu_country,
        kr_type=kr_type,
        brand=brand,
        model=model,
    )
    lines = qp.getlist("line") if hasattr(qp, "getlist") else []
    source = qp.getlist("source") if hasattr(qp, "getlist") else []
    source_value: Optional[str | List[str]] = source if source else qp.get("source")
    raw_params = {
        "region": canon.get("region"),
        "country": canon.get("country"),
        "brand": canon.get("brand"),
        "model": canon.get("model"),
        "generation": qp.get("generation"),
        "color": qp.get("color"),
        "body_type": qp.get("body_type"),
        "engine_type": qp.get("engine_type"),
        "transmission": qp.get("transmission"),
        "drive_type": qp.get("drive_type"),
        "price_min": _to_float(qp.get("price_min")),
        "price_max": _to_float(qp.get("price_max")),
        "power_hp_min": _to_float(qp.get("power_hp_min")),
        "power_hp_max": _to_float(qp.get("power_hp_max")),
        "engine_cc_min": _to_int(qp.get("engine_cc_min")),
        "engine_cc_max": _to_int(qp.get("engine_cc_max")),
        "year_min": _to_int(qp.get("year_min")),
        "year_max": _to_int(qp.get("year_max")),
        "mileage_min": _to_int(qp.get("mileage_min")),
        "mileage_max": _to_int(qp.get("mileage_max")),
        "kr_type": canon.get("kr_type"),
        "reg_year_min": _to_int(qp.get("reg_year_min")),
        "reg_month_min": _to_int(qp.get("reg_month_min")),
        "reg_year_max": _to_int(qp.get("reg_year_max")),
        "reg_month_max": _to_int(qp.get("reg_month_max")),
        "condition": qp.get("condition"),
        "q": qp.get("q"),
        "line": "|".join(lines or []),
        "source": ",".join(source) if source else qp.get("source"),
        "num_seats": qp.get("num_seats"),
        "doors_count": qp.get("doors_count"),
        "emission_class": qp.get("emission_class"),
        "efficiency_class": qp.get("efficiency_class"),
        "climatisation": qp.get("climatisation"),
        "airbags": qp.get("airbags"),
        "interior_design": qp.get("interior_design"),
        "air_suspension": _to_bool(qp.get("air_suspension")),
        "price_rating_label": qp.get("price_rating_label"),
        "owners_count": qp.get("owners_count"),
    }
    normalized = normalize_count_params(raw_params)
    cache_key = build_cars_count_key(normalized)
    cached = redis_get_json(cache_key)
    if cached is not None:
        print("ADVANCED_COUNT_CACHE hit=1 source=redis key=%s" % cache_key, flush=True)
        return {"count": int(cached)}
    print("ADVANCED_COUNT_CACHE hit=0 source=fallback key=%s" % cache_key, flush=True)
    lock_key = f"{cache_key}:lock"
    token = redis_try_lock(lock_key, ttl_sec=20)
    if token is None:
        waited = redis_wait_json(cache_key, timeout_ms=2200, poll_ms=120)
        if waited is not None:
            print("ADVANCED_COUNT_CACHE hit=1 source=redis_wait key=%s" % cache_key, flush=True)
            return {"count": int(waited)}
    try:
        _, total = service.list_cars(
            region=canon.get("region"),
            country=canon.get("country"),
            kr_type=canon.get("kr_type"),
            brand=canon.get("brand"),
            model=canon.get("model"),
            generation=qp.get("generation"),
            color=qp.get("color"),
            price_min=_to_float(qp.get("price_min")),
            price_max=_to_float(qp.get("price_max")),
            power_hp_min=_to_float(qp.get("power_hp_min")),
            power_hp_max=_to_float(qp.get("power_hp_max")),
            engine_cc_min=_to_int(qp.get("engine_cc_min")),
            engine_cc_max=_to_int(qp.get("engine_cc_max")),
            year_min=_to_int(qp.get("year_min")),
            year_max=_to_int(qp.get("year_max")),
            mileage_min=_to_int(qp.get("mileage_min")),
            mileage_max=_to_int(qp.get("mileage_max")),
            reg_year_min=_to_int(qp.get("reg_year_min")),
            reg_month_min=_to_int(qp.get("reg_month_min")),
            reg_year_max=_to_int(qp.get("reg_year_max")),
            reg_month_max=_to_int(qp.get("reg_month_max")),
            body_type=qp.get("body_type"),
            engine_type=qp.get("engine_type"),
            transmission=qp.get("transmission"),
            drive_type=qp.get("drive_type"),
            num_seats=qp.get("num_seats"),
            doors_count=qp.get("doors_count"),
            emission_class=qp.get("emission_class"),
            efficiency_class=qp.get("efficiency_class"),
            climatisation=qp.get("climatisation"),
            airbags=qp.get("airbags"),
            interior_design=qp.get("interior_design"),
            air_suspension=_to_bool(qp.get("air_suspension")),
            price_rating_label=qp.get("price_rating_label"),
            owners_count=qp.get("owners_count"),
            condition=qp.get("condition"),
            lines=lines or None,
            source_key=source_value,
            q=qp.get("q"),
            sort=qp.get("sort") or "price_asc",
            page=1,
            page_size=1,
            light=True,
            count_only=True,
            use_fast_count=os.getenv("CATALOG_USE_FAST_COUNT", "1") != "0",
        )
        redis_set_json(cache_key, int(total), ttl_sec=1800)
        return {"count": int(total)}
    finally:
        if token:
            redis_unlock(lock_key, token)


@router.get("/cars/{car_id}")
def get_car(car_id: int, db: Session = Depends(get_db)):
    service = CarsService(db)
    car = service.get_car(car_id)
    if not car:
        raise HTTPException(status_code=404, detail="Car not found")
    if os.getenv("LAZY_RECALC_ENABLED", "1") != "0":
        try:
            service.ensure_calc_cache(car)
        except Exception:
            pass
    detail = CarDetailOut.model_validate(car)
    if car.images:
        detail.images = [im.url for im in car.images if im.url]
    detail.display_price_rub = display_price_rub(
        car.total_price_rub_cached,
        car.price_rub_cached,
        allow_price_fallback=str(car.country or "").upper() == "KR",
    )
    detail.price_note = price_without_util_note(
        display_price=detail.display_price_rub,
        total_price_rub_cached=car.total_price_rub_cached,
        country=car.country,
    )
    if car.source:
        detail.source_name = car.source.name
        detail.source_country = car.source.country
    display_code, display_label = resolve_display_country(car)
    detail.display_country_code = display_code
    detail.display_country_label = display_label
    detail.display_engine_type = ru_fuel(car.engine_type) or ru_fuel(
        normalize_fuel(car.engine_type)) or car.engine_type
    detail.display_transmission = ru_transmission(
        car.transmission) or car.transmission
    return detail.model_dump()


@router.get("/brands")
def list_brands(db: Session = Depends(get_db)):
    service = CarsService(db)
    return service.brand_stats()


@router.get("/brands/{brand}/models")
def list_models_for_brand(brand: str, db: Session = Depends(get_db)):
    service = CarsService(db)
    normalized = normalize_brand(brand)
    models = service.models_for_brand(normalized)
    return {"brand": normalized, "models": models}


@router.get("/filters/options")
def filter_options(
    region: Optional[str] = Query(default=None),
    country: Optional[str] = Query(default=None),
    brand: Optional[str] = Query(default=None),
    model: Optional[str] = Query(default=None),
    color: Optional[str] = Query(default=None),
    engine_type: Optional[str] = Query(default=None),
    transmission: Optional[str] = Query(default=None),
    body_type: Optional[str] = Query(default=None),
    drive_type: Optional[str] = Query(default=None),
    price_bucket: Optional[str] = Query(default=None),
    mileage_bucket: Optional[str] = Query(default=None),
    reg_year: Optional[int] = Query(default=None),
    db: Session = Depends(get_db),
):
    service = CarsService(db)
    base_filters = {
        "region": region,
        "country": country,
        "brand": brand,
        "model": model,
        "color": color,
        "engine_type": engine_type,
        "transmission": transmission,
        "body_type": body_type,
        "drive_type": drive_type,
        "price_bucket": price_bucket,
        "mileage_bucket": mileage_bucket,
        "reg_year": reg_year,
    }

    def facet(field: str):
        filters = dict(base_filters)
        filters[field] = None
        return service.facet_counts(field=field, filters=filters)

    fields = [
        "region",
        "country",
        "brand",
        "model",
        "color",
        "engine_type",
        "transmission",
        "body_type",
        "drive_type",
        "price_bucket",
        "mileage_bucket",
        "reg_year",
    ]
    return {field: facet(field) for field in fields}


def _split_colors(raw_colors: List[dict]) -> tuple[list[dict], list[dict]]:
    return split_color_facets(
        raw_colors,
        top_limit=12,
        label_for=lambda value: ru_color(value) or display_color(value) or value,
        hex_for=color_hex,
    )


@router.get("/filter_ctx_base")
def filter_ctx_base(
    request: Request,
    region: Optional[str] = Query(default=None),
    country: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
):
    service = CarsService(db)
    params = normalize_filter_params({"region": region, "country": country})
    if os.getenv("FILTERS_CANON") == "1":
        print(
            "FILTERS_CANON filter_ctx_base "
            f"region={params.get('region')} country={params.get('country')}",
            flush=True,
        )
    cache_key = build_filter_ctx_base_key(params)
    t0 = time.perf_counter()
    cached = redis_get_json(cache_key)
    if cached:
        print("FILTER_CTX_BASE_CACHE hit=1 source=redis", flush=True)
        if os.getenv("FILTER_CTX_DEBUG") == "1":
            total_ms = (time.perf_counter() - t0) * 1000
            print(
                f"FILTER_CTX_BASE ms={total_ms:.2f} regions={len(cached.get('regions', []))} countries={len(cached.get('countries', []))} brands={len(cached.get('brands', []))}",
                flush=True,
            )
        return cached
    print("FILTER_CTX_BASE_CACHE hit=0 source=fallback", flush=True)
    base_filters = {"region": params.get("region"), "country": params.get("country")}
    regions_raw = [r["value"] for r in service.facet_counts(field="region", filters={}) if r.get("value")]
    regions = _sort_by_label([{"value": r, "label": _region_label(r)} for r in regions_raw])
    countries_raw = []
    seen_countries = set()
    for c in service.facet_counts(field="country", filters={"region": params.get("region")}):
        raw_val = c.get("value")
        if not raw_val:
            continue
        code = normalize_country_code(raw_val)
        if not code or code in seen_countries:
            continue
        countries_raw.append(code)
        seen_countries.add(code)
    countries = _sort_by_label([{"value": c, "label": country_label_ru(c) or c} for c in countries_raw])
    country_labels = {**{c: country_label_ru(c) or c for c in countries_raw}, "EU": "Европа", "KR": "Корея"}
    kr_types = []
    if any(r["value"] == "KR" for r in regions):
        kr_types = [
            {"value": "KR_INTERNAL", "label": "Корея (внутренний рынок)"},
            {"value": "KR_IMPORT", "label": "Корея (импорт)"},
        ]
    brands = _sort_by_label(
        [
            {"value": b["value"], "label": b["value"], "count": b.get("count", 0)}
            for b in service.facet_counts(field="brand", filters=base_filters)
            if b.get("value")
        ]
    )
    reg_years = sorted(
        [int(r["value"]) for r in service.facet_counts(field="reg_year", filters=base_filters) if r.get("value")],
        reverse=True,
    )
    engine_types = _sort_by_label(
        [
            {"value": v["value"], "label": ru_fuel(v["value"]) or v["value"], "count": v.get("count", 0)}
            for v in service.facet_counts(field="engine_type", filters=base_filters)
            if v.get("value")
        ]
    )
    transmissions = _sort_by_label(
        [
            {"value": v["value"], "label": ru_transmission(v["value"]) or v["value"], "count": v.get("count", 0)}
            for v in service.facet_counts(field="transmission", filters=base_filters)
            if v.get("value")
        ]
    )
    drive_types = _sort_by_label(
        [
            {
                "value": v["value"],
                "label": ru_drivetrain(v["value"]) or v["value"],
                "count": v.get("count", 0),
            }
            for v in service.facet_counts(field="drive_type", filters=base_filters)
            if v.get("value")
        ]
    )
    body_types = _sort_by_label(
        [
            {
                "value": v["value"],
                "label": ru_body(v["value"]) or display_body(v["value"]) or v["value"],
                "count": v.get("count", 0),
            }
            for v in service.facet_counts(field="body_type", filters=base_filters)
            if v.get("value")
        ]
    )
    colors_basic, colors_other = _split_colors(service.facet_counts(field="color", filters=base_filters))
    payload = {
        "regions": regions,
        "countries": countries,
        "country_labels": country_labels,
        "kr_types": kr_types,
        "brands": brands,
        "body_types": body_types,
        "engine_types": engine_types,
        "transmissions": transmissions,
        "drive_types": drive_types,
        "colors_basic": colors_basic,
        "colors_other": colors_other,
        "reg_years": reg_years,
        "reg_months": [{"value": i + 1, "label": m} for i, m in enumerate(["Янв", "Фев", "Мар", "Апр", "Май", "Июн", "Июл", "Авг", "Сен", "Окт", "Ноя", "Дек"])],
    }
    redis_set_json(cache_key, payload, ttl_sec=86400)
    if os.getenv("FILTER_CTX_DEBUG") == "1":
        total_ms = (time.perf_counter() - t0) * 1000
        print(
            f"FILTER_CTX_BASE ms={total_ms:.2f} regions={len(regions)} countries={len(countries)} brands={len(brands)}",
            flush=True,
        )
    return payload


@router.get("/debug/filter_ctx_base_raw")
def filter_ctx_base_raw(
    region: Optional[str] = Query(default=None),
    country: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
):
    service = CarsService(db)
    params = normalize_filter_params({"region": region, "country": country})
    base_filters = {"region": params.get("region"), "country": params.get("country")}
    regions_raw = service.facet_counts(field="region", filters={})
    countries_raw = service.facet_counts(field="country", filters={"region": params.get("region")})
    brands_raw = service.facet_counts(field="brand", filters=base_filters)
    return {
        "region_param": params.get("region"),
        "country_param": params.get("country"),
        "regions_raw": regions_raw,
        "countries_raw": countries_raw,
        "brands_raw": brands_raw,
    }


@router.get("/filter_ctx_brand")
def filter_ctx_brand(
    request: Request,
    region: Optional[str] = Query(default=None),
    country: Optional[str] = Query(default=None),
    kr_type: Optional[str] = Query(default=None),
    brand: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
):
    service = CarsService(db)
    canon = _canonicalize_params(region=region, country=country, kr_type=kr_type, brand=brand)
    if os.getenv("FILTERS_CANON") == "1":
        print(
            "FILTERS_CANON filter_ctx_brand "
            f"region={canon.get('region')} country={canon.get('country')} kr_type={canon.get('kr_type')} "
            f"brand={canon.get('brand')}",
            flush=True,
        )
    params = normalize_filter_params(
        {
            "region": canon.get("region"),
            "country": canon.get("country"),
            "kr_type": canon.get("kr_type"),
            "brand": canon.get("brand"),
        }
    )
    cache_key = build_filter_ctx_brand_key(params)
    t0 = time.perf_counter()
    cached = redis_get_json(cache_key)
    if cached:
        print("FILTER_CTX_BRAND_CACHE hit=1 source=redis", flush=True)
        if os.getenv("FILTER_CTX_DEBUG") == "1":
            total_ms = (time.perf_counter() - t0) * 1000
            print(f"FILTER_CTX_BRAND ms={total_ms:.2f} models={len(cached.get('models', []))}", flush=True)
        return cached
    print("FILTER_CTX_BRAND_CACHE hit=0 source=fallback", flush=True)
    brand_norm = normalize_brand(canon.get("brand")).strip() if canon.get("brand") else None
    models = service.models_for_brand_filtered(
        region=canon.get("region"),
        country=canon.get("country"),
        kr_type=canon.get("kr_type"),
        brand=brand_norm,
    )
    model_groups = service.build_model_groups(brand=brand_norm, models=models)
    payload = {"models": models, "model_groups": model_groups}
    redis_set_json(cache_key, payload, ttl_sec=86400)
    if os.getenv("FILTER_CTX_DEBUG") == "1":
        total_ms = (time.perf_counter() - t0) * 1000
        print(f"FILTER_CTX_BRAND ms={total_ms:.2f} models={len(models)}", flush=True)
    return payload


@router.get("/filter_ctx_model")
def filter_ctx_model(
    request: Request,
    region: Optional[str] = Query(default=None),
    country: Optional[str] = Query(default=None),
    brand: Optional[str] = Query(default=None),
    model: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
):
    service = CarsService(db)
    canon = _canonicalize_params(region=region, country=country, brand=brand, model=model)
    if os.getenv("FILTERS_CANON") == "1":
        print(
            "FILTERS_CANON filter_ctx_model "
            f"region={canon.get('region')} country={canon.get('country')} kr_type={canon.get('kr_type')} "
            f"brand={canon.get('brand')} model={canon.get('model')}",
            flush=True,
        )
    params = normalize_filter_params(
        {
            "region": canon.get("region"),
            "country": canon.get("country"),
            "brand": canon.get("brand"),
            "model": canon.get("model"),
        }
    )
    cache_key = build_filter_ctx_model_key(params)
    t0 = time.perf_counter()
    cached = redis_get_json(cache_key)
    if cached:
        print("FILTER_CTX_MODEL_CACHE hit=1 source=redis", flush=True)
        if os.getenv("FILTER_CTX_DEBUG") == "1":
            total_ms = (time.perf_counter() - t0) * 1000
            print(f"FILTER_CTX_MODEL ms={total_ms:.2f} generations={len(cached.get('generations', []))}", flush=True)
        return cached
    print("FILTER_CTX_MODEL_CACHE hit=0 source=fallback", flush=True)
    from ..models import Car
    stmt = (
        select(func.distinct(Car.generation))
        .where(Car.generation.is_not(None))
        .where(Car.is_available.is_(True))
    )
    if canon.get("brand"):
        stmt = stmt.where(Car.brand == normalize_brand(canon.get("brand")).strip())
    if canon.get("model"):
        stmt = stmt.where(Car.model == canon.get("model"))
    if canon.get("country"):
        stmt = stmt.where(func.upper(Car.country) == canon.get("country"))
    elif canon.get("region") == "EU":
        stmt = stmt.where(func.upper(Car.country).in_(service.EU_COUNTRIES))
    elif canon.get("region") == "KR":
        stmt = stmt.where(func.upper(Car.country) == "KR")
    gens = [g for g in service.db.execute(stmt).scalars().all() if g]
    generations = _sort_by_label([{"value": g, "label": g} for g in gens])
    payload = {"generations": generations}
    redis_set_json(cache_key, payload, ttl_sec=86400)
    if os.getenv("FILTER_CTX_DEBUG") == "1":
        total_ms = (time.perf_counter() - t0) * 1000
        print(f"FILTER_CTX_MODEL ms={total_ms:.2f} generations={len(generations)}", flush=True)
    return payload


@router.get("/filter_payload")
def filter_payload(
    request: Request,
    region: Optional[str] = Query(default=None),
    country: Optional[str] = Query(default=None),
    brand: Optional[str] = Query(default=None),
    model: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
):
    service = CarsService(db)
    timing_enabled = os.environ.get("CAR_API_TIMING", "0") == "1"
    params = normalize_filter_params(
        {"region": region, "country": country, "brand": brand, "model": model}
    )
    cache_key = build_filter_payload_key(params)
    cached = redis_get_json(cache_key)
    if cached:
        if timing_enabled:
            print("FILTER_PAYLOAD_CACHE hit=1 source=redis payload_total_ms=0.0", flush=True)
        return cached
    start = time.perf_counter()
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
    eu_payload = service.payload_values_bulk(payload_keys, source_ids=service.source_ids_for_region("EU"))
    kr_payload = service.payload_values_bulk(payload_keys, source_ids=service.source_ids_for_region("KR"))
    data = {
        "seats_options_eu": eu_payload.get("num_seats", []),
        "doors_options_eu": eu_payload.get("doors_count", []),
        "owners_options_eu": eu_payload.get("owners_count", []),
        "emission_classes_eu": eu_payload.get("emission_class", []),
        "efficiency_classes_eu": eu_payload.get("efficiency_class", []),
        "climatisation_options_eu": eu_payload.get("climatisation", []),
        "airbags_options_eu": eu_payload.get("airbags", []),
        "interior_design_options_eu": eu_payload.get("interior_design", []),
        "price_rating_labels_eu": eu_payload.get("price_rating_label", []),
        "seats_options_kr": kr_payload.get("num_seats", []),
        "doors_options_kr": kr_payload.get("doors_count", []),
        "owners_options_kr": kr_payload.get("owners_count", []),
        "emission_classes_kr": kr_payload.get("emission_class", []),
        "efficiency_classes_kr": kr_payload.get("efficiency_class", []),
        "climatisation_options_kr": kr_payload.get("climatisation", []),
        "airbags_options_kr": kr_payload.get("airbags", []),
        "interior_design_options_kr": kr_payload.get("interior_design", []),
        "price_rating_labels_kr": kr_payload.get("price_rating_label", []),
    }
    redis_set_json(cache_key, data, ttl_sec=3600)
    total_ms = (time.perf_counter() - start) * 1000
    if timing_enabled:
        print(f"FILTER_PAYLOAD_CACHE hit=0 source=fallback payload_total_ms={total_ms:.2f}", flush=True)
    return data
