from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy.orm import Session
from typing import Optional, List
from ..db import get_db
from ..services.cars_service import CarsService, normalize_brand
from ..schemas import CarDetailOut
from ..utils.country_map import resolve_display_country, normalize_country_code
from ..utils.taxonomy import normalize_fuel, ru_fuel, ru_transmission, color_hex
from ..utils.redis_cache import redis_get_json, redis_set_json, build_filter_payload_key, normalize_filter_params
from ..models.car_image import CarImage
from sqlalchemy import select, func
import re
import os
import time
import logging

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/cars")
def list_cars(
    request: Request,
    region: Optional[str] = Query(default=None),
    country: Optional[str] = Query(default=None),
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
    items, total = service.list_cars(
        region=region,
        country=country,
        brand=brand,
        lines=line,
        source_key=source,
        q=q,
        model=model,
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
        kr_type=kr_type,
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
    )
    t1 = time.perf_counter()
    image_counts = {}
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
        thumb_url = c.get("thumbnail_url")
        if isinstance(thumb_url, str) and "rule=mo-" in thumb_url:
            new_thumb = re.sub(r"(rule=mo-)\d+(\.jpg)?",
                               r"\g<1>480\2", thumb_url)
            if new_thumb != thumb_url:
                thumb_replaced += 1
                thumb_url = new_thumb
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
        request.state.api_parts = {
            "list": (t1 - t0),
            "images": (t2 - t1),
            "serialize": (t3 - t2),
            "items": len(payload_items),
            "thumb_replaced": thumb_replaced,
        }
    return {
        "items": payload_items,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


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
