from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional, List
from ..db import get_db
from ..services.cars_service import CarsService
from ..schemas import CarOut, CarDetailOut
from ..models import Car, CarImage, Source
from sqlalchemy import select, func
from ..utils.localization import display_body, display_color
from ..utils.country_map import resolve_display_country
from ..utils.taxonomy import ru_color, normalize_color, color_hex

router = APIRouter()


@router.get("/cars")
def list_cars(
    country: Optional[str] = Query(default=None),
    brand: Optional[str] = Query(default=None),
    source: Optional[str | List[str]] = Query(
        default=None, description="Source key, e.g., mobile_de or emavto_klg"),
    q: Optional[str] = Query(default=None, description="Free-text brand/model search"),
    model: Optional[str] = Query(default=None),
    generation: Optional[str] = Query(default=None),
    color: Optional[str] = Query(default=None),
    body_type: Optional[str] = Query(default=None),
    engine_type: Optional[str] = Query(default=None),
    transmission: Optional[str] = Query(default=None),
    price_min: Optional[float] = Query(default=None),
    price_max: Optional[float] = Query(default=None),
    year_min: Optional[int] = Query(default=None),
    year_max: Optional[int] = Query(default=None),
    mileage_min: Optional[int] = Query(default=None),
    mileage_max: Optional[int] = Query(default=None),
    reg_year_min: Optional[int] = Query(default=None),
    reg_month_min: Optional[int] = Query(default=None),
    reg_year_max: Optional[int] = Query(default=None),
    reg_month_max: Optional[int] = Query(default=None),
    sort: Optional[str] = Query(default=None, description="price_asc|price_desc|year_desc|year_asc|mileage_asc|mileage_desc|created_desc"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    service = CarsService(db)
    items, total = service.list_cars(
        country=country,
        brand=brand,
        source_key=source,
        q=q,
        model=model,
        generation=generation,
        color=color,
        price_min=price_min,
        price_max=price_max,
        year_min=year_min,
        year_max=year_max,
        mileage_min=mileage_min,
        mileage_max=mileage_max,
        reg_year_min=reg_year_min,
        reg_month_min=reg_month_min,
        reg_year_max=reg_year_max,
        reg_month_max=reg_month_max,
        body_type=body_type,
        engine_type=engine_type,
        transmission=transmission,
        sort=sort,
        page=page,
        page_size=page_size,
    )
    # compute images_count per car (single grouped query)
    if items:
        ids = [c.id for c in items]
        counts = db.execute(
            select(CarImage.car_id, func.count()).where(
                CarImage.car_id.in_(ids)).group_by(CarImage.car_id)
        ).all()
        counts_map = {cid: cnt for cid, cnt in counts}
    else:
        counts_map = {}
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
        raw_color = co.get("color")
        norm_color = normalize_color(raw_color)
        co["display_color"] = ru_color(raw_color) or ru_color(norm_color) or display_color(raw_color) or raw_color
        if raw_color:
            co["color_hex"] = color_hex(norm_color) or "#444"
        co["images_count"] = counts_map.get(c.id, 0)
        payload_items.append(co)
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
    return detail.model_dump()


@router.get("/brands")
def list_brands(db: Session = Depends(get_db)):
    service = CarsService(db)
    return service.brand_stats()


@router.get("/brands/{brand}/models")
def list_models_for_brand(brand: str, db: Session = Depends(get_db)):
    service = CarsService(db)
    models = service.models_for_brand(brand)
    return {"brand": brand, "models": models}
