from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional, List
from ..db import get_db
from ..services.cars_service import CarsService, normalize_brand
from ..schemas import CarOut, CarDetailOut
from ..models import Car, CarImage, Source
from sqlalchemy import select, func
from ..utils.localization import display_body, display_color
from ..utils.country_map import resolve_display_country
from ..utils.taxonomy import ru_color, normalize_color, color_hex, normalize_fuel, ru_fuel, ru_transmission
import os
import time
import logging

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/cars")
def list_cars(
    region: Optional[str] = Query(default=None),
    country: Optional[str] = Query(default=None),
    brand: Optional[str] = Query(default=None),
    line: Optional[List[str]] = Query(default=None, description="Advanced search lines brand|model|variant"),
    source: Optional[str | List[str]] = Query(
        default=None, description="Source key, e.g., mobile_de or emavto_klg"),
    q: Optional[str] = Query(default=None, description="Free-text brand/model search"),
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
    kr_type: Optional[str] = Query(default=None, description="KR_INTERNAL|KR_IMPORT"),
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
    )
    t1 = time.perf_counter()
    # compute images_count per car (single grouped query)
    if items:
        ids = [c.id for c in items]
        counts = db.execute(
            select(CarImage.car_id, func.count()).where(
                CarImage.car_id.in_(ids)).group_by(CarImage.car_id)
        ).all()
        counts_map = {cid: cnt for cid, cnt in counts}
        image_rows = db.execute(
            select(CarImage.car_id, CarImage.url)
            .where(CarImage.car_id.in_(ids))
            .order_by(CarImage.car_id.asc(), CarImage.position.asc(), CarImage.id.asc())
        ).all()
        images_map: dict[int, list[str]] = {}
        for car_id, url in image_rows:
            if not url:
                continue
            bucket = images_map.setdefault(car_id, [])
            if len(bucket) >= 6:
                continue
            bucket.append(url)
    else:
        counts_map = {}
        images_map = {}
    t2 = time.perf_counter()
    # map source_id -> key
    src_rows = db.execute(
        select(Car.source_id, Source.key).join(Source, Car.source_id == Source.id)
    ).all()
    source_map = {sid: skey for sid, skey in src_rows}
    payload_items = []
    for c in items:
        co = CarOut.model_validate(c).model_dump()
        src_key = source_map.get(c.source_id, "") if c.source_id else ""
        display_code, display_label = resolve_display_country(c, source_key=src_key)
        co["display_country_code"] = display_code
        co["display_country_label"] = display_label
        co["display_body_type"] = display_body(co.get("body_type")) or co.get("body_type")
        engine_raw = co.get("engine_type")
        engine_norm = normalize_fuel(engine_raw)
        co["display_engine_type"] = ru_fuel(engine_raw) or ru_fuel(engine_norm) or engine_raw
        trans_raw = co.get("transmission")
        co["display_transmission"] = ru_transmission(trans_raw) or trans_raw
        raw_color = co.get("color")
        norm_color = normalize_color(raw_color)
        co["display_color"] = ru_color(raw_color) or ru_color(norm_color) or display_color(raw_color) or raw_color
        if raw_color:
            co["color_hex"] = color_hex(norm_color)
        co["images_count"] = counts_map.get(c.id, 0)
        co["images"] = images_map.get(c.id, []) if images_map else []
        # pricing and calc summary
        co["pricing"] = service.price_info(c)
        if c.total_price_rub_cached is not None:
            co["calc_total_rub"] = float(c.total_price_rub_cached)
        if c.calc_breakdown_json:
            co["calc_breakdown"] = c.calc_breakdown_json
        payload_items.append(co)
    t3 = time.perf_counter()
    if timing_enabled:
        logger.info(
            "cars_api_timing list=%.3f images=%.3f serialize=%.3f total=%.3f page_size=%s",
            (t1 - t0),
            (t2 - t1),
            (t3 - t2),
            (t3 - t0),
            page_size,
        )
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
    detail.display_engine_type = ru_fuel(car.engine_type) or ru_fuel(normalize_fuel(car.engine_type)) or car.engine_type
    detail.display_transmission = ru_transmission(car.transmission) or car.transmission
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
