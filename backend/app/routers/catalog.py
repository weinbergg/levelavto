from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy.orm import Session
from typing import Optional, List
from ..db import get_db
from ..services.cars_service import CarsService, normalize_brand
from ..schemas import CarDetailOut
from ..utils.country_map import resolve_display_country, normalize_country_code, country_label_ru
from ..utils.taxonomy import (
    normalize_fuel,
    ru_fuel,
    ru_transmission,
    color_hex,
    normalize_color,
    ru_color,
    is_color_base,
)
from ..utils.thumbs import normalize_classistatic_url, pick_classistatic_thumb
from ..utils.redis_cache import (
    redis_get_json,
    redis_set_json,
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
import os
import time
import logging

router = APIRouter()
logger = logging.getLogger(__name__)


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
    if normalized.get("model"):
        normalized["model"] = str(normalized["model"]).strip()
    return normalized


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
        use_fast_count=False,
    )
    t1 = time.perf_counter()
    if items and not isinstance(items[0], dict):
        items = [dict(row) for row in items]
    image_counts = {}
    image_first = {}
    def _normalize_thumb(url: str | None) -> str | None:
        return normalize_classistatic_url(url)
    if items:
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
    t2 = time.perf_counter()
    payload_items = []
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
        if isinstance(thumb_url, str) and "rule=mo-" in thumb_url:
            thumb_replaced += 1
        payload_items.append(
            {
                "id": c.get("id"),
                "brand": c.get("brand"),
                "model": c.get("model"),
                "year": c.get("year"),
                "mileage": c.get("mileage"),
                "total_price_rub_cached": c.get("total_price_rub_cached"),
                "price_rub_cached": c.get("price_rub_cached"),
                "thumbnail_url": thumb_url,
                "country": country_norm or country_raw,
                "region": region_val,
                "color": c.get("color"),
                "color_hex": color_hex(c.get("color")),
                "engine_cc": c.get("engine_cc"),
                "power_hp": c.get("power_hp"),
                "images_count": img_count,
                "photos_count": img_count,
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
    return {
        "items": payload_items,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


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
    cache_key = build_cars_count_key(normalized)
    cached = redis_get_json(cache_key)
    if cached is not None:
        print("CARS_COUNT_CACHE hit=1 source=redis key=%s" % cache_key, flush=True)
        return {"count": int(cached)}
    print("CARS_COUNT_CACHE hit=0 source=fallback key=%s" % cache_key, flush=True)
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
    redis_set_json(cache_key, int(total), ttl_sec=600)
    return {"count": int(total)}


@router.get("/cars/{car_id}")
def get_car(car_id: int, db: Session = Depends(get_db)):
    service = CarsService(db)
    car = service.get_car(car_id)
    if not car:
        raise HTTPException(status_code=404, detail="Car not found")
    detail = CarDetailOut.model_validate(car)
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
    normalized = []
    for c in raw_colors:
        value = (c.get("value") or "").strip()
        if not value:
            continue
        norm = normalize_color(value) or value
        label = ru_color(norm) or value
        hex_value = color_hex(norm)
        normalized.append({"value": norm, "label": label, "hex": hex_value, "count": c.get("count", 0)})
    basic: list[dict] = []
    other: list[dict] = []
    for c in normalized:
        if is_color_base(c["value"]):
            basic.append(c)
        else:
            other.append(c)
    return basic, other


@router.get("/filter_ctx_base")
def filter_ctx_base(
    request: Request,
    region: Optional[str] = Query(default=None),
    country: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
):
    service = CarsService(db)
    params = normalize_filter_params({"region": region, "country": country})
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
    countries_raw = [
        c["value"]
        for c in service.facet_counts(field="country", filters={"region": params.get("region")})
        if c.get("value")
    ]
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
            {"value": v["value"], "label": v["value"], "count": v.get("count", 0)}
            for v in service.facet_counts(field="drive_type", filters=base_filters)
            if v.get("value")
        ]
    )
    body_types = _sort_by_label(
        [
            {"value": v["value"], "label": v["value"], "count": v.get("count", 0)}
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
    redis_set_json(cache_key, payload, ttl_sec=21600)
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
    brand: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
):
    service = CarsService(db)
    canon = _canonicalize_params(region=region, country=country, brand=brand)
    params = normalize_filter_params(
        {"region": canon.get("region"), "country": canon.get("country"), "brand": canon.get("brand")}
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
    filters = {"region": canon.get("region"), "country": canon.get("country"), "brand": brand_norm}
    models = _sort_by_label(
        [
            {"value": m["value"], "label": m["value"], "count": m.get("count", 0)}
            for m in service.facet_counts(field="model", filters=filters)
            if m.get("value")
        ]
    )
    payload = {"models": models}
    redis_set_json(cache_key, payload, ttl_sec=21600)
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
    redis_set_json(cache_key, payload, ttl_sec=3600)
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
