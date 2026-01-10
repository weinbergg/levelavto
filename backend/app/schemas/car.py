from pydantic import BaseModel
from datetime import datetime
from typing import Optional


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
    is_available: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class CarOut(CarBase):
    images_count: Optional[int] = None
    display_country_code: Optional[str] = None
    display_country_label: Optional[str] = None


class CarDetailOut(CarBase):
    source_name: Optional[str] = None
    source_country: Optional[str] = None
    display_country_code: Optional[str] = None
    display_country_label: Optional[str] = None

