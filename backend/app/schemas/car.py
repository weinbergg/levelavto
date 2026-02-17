from pydantic import BaseModel
from datetime import datetime
from typing import Optional, List, Any


class CarBase(BaseModel):
    id: int
    source_id: int
    external_id: str
    country: str
    brand: Optional[str] = None
    model: Optional[str] = None
    generation: Optional[str] = None
    year: Optional[int] = None
    mileage: Optional[int] = None
    price: Optional[float] = None
    currency: Optional[str] = None
    price_rub_cached: Optional[float] = None
    total_price_rub_cached: Optional[float] = None
    display_price_rub: Optional[float] = None
    price_note: Optional[str] = None
    calc_updated_at: Optional[datetime] = None
    registration_year: Optional[int] = None
    registration_month: Optional[int] = None
    body_type: Optional[str] = None
    engine_type: Optional[str] = None
    engine_cc: Optional[int] = None
    power_hp: Optional[float] = None
    power_kw: Optional[float] = None
    transmission: Optional[str] = None
    drive_type: Optional[str] = None
    color: Optional[str] = None
    vin: Optional[str] = None
    source_url: Optional[str] = None
    thumbnail_url: Optional[str] = None
    thumbnail_local_path: Optional[str] = None
    hash: Optional[str] = None
    first_seen_at: Optional[datetime] = None
    last_seen_at: Optional[datetime] = None
    listing_date: Optional[datetime] = None
    is_available: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class CarOut(CarBase):
    images_count: Optional[int] = None
    images: Optional[List[Any]] = None
    display_country_code: Optional[str] = None
    display_country_label: Optional[str] = None
    display_engine_type: Optional[str] = None
    display_transmission: Optional[str] = None
    pricing: Optional[dict] = None
    calc_total_rub: Optional[float] = None
    calc_breakdown: Optional[list] = None
    calc_used_price: Optional[dict] = None
    kr_market_type: Optional[str] = None


class CarDetailOut(CarBase):
    source_name: Optional[str] = None
    source_country: Optional[str] = None
    display_country_code: Optional[str] = None
    display_country_label: Optional[str] = None
    display_engine_type: Optional[str] = None
    display_transmission: Optional[str] = None
    images: Optional[List[Any]] = None
