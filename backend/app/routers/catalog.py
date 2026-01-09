from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional, List
from ..db import get_db
from ..services.cars_service import CarsService
from ..schemas import CarOut, CarDetailOut
from ..models import Car, CarImage, Source
from sqlalchemy import select, func
from ..utils.localization import display_region, display_body, display_color

router = APIRouter()


@router.get("/cars")
def list_cars(
    country: Optional[str] = Query(default=None),
    brand: Optional[str] = Query(default=None),
    source: Optional[str | List[str]] = Query(
        default=None, description="Source key, e.g., mobile_de or emavto_klg"),
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
        model=model,
        generation=generation,
        color=color,
        price_min=price_min,
        price_max=price_max,
        year_min=year_min,
        year_max=year_max,
        mileage_min=mileage_min,
        mileage_max=mileage_max,
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
        if src_key.startswith("mobile"):
            co["country"] = "Европа"
            co["region"] = "EU"
        elif "emavto" in src_key or "encar" in src_key or "m-auto" in src_key or "m_auto" in src_key:
            co["country"] = "Корея"
            co["region"] = "KR"
        display_reg = display_region(src_key) or co.get("country")
        co["display_region"] = display_reg
        co["display_body_type"] = display_body(co.get("body_type")) or co.get("body_type")
        co["display_color"] = display_color(co.get("color")) or co.get("color")
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
        src_key = car.source.key or ""
        if src_key.startswith("mobile"):
            detail.country = "Европа"
        elif "emavto" in src_key:
            detail.country = "Корея"
        detail.source_country = car.source.country
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
