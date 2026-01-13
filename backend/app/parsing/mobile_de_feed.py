from __future__ import annotations

from typing import Iterable, Iterator, List, Optional, Any, Dict
from dataclasses import asdict
from decimal import Decimal
from datetime import datetime
from .base import CarParsed
from .config import SiteConfig
from ..importing.mobilede_csv import MobileDeCsvRow


class MobileDeFeedParser:
    def __init__(self, site_config: SiteConfig):
        self.config = site_config

    def _payload_from_row(self, row: MobileDeCsvRow) -> Dict[str, Any]:
        data = asdict(row)
        for key, value in list(data.items()):
            if isinstance(value, Decimal):
                data[key] = float(value)
            elif isinstance(value, datetime):
                data[key] = value.isoformat()
        return data

    def _normalize_transmission(self, raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        t = raw.lower()
        if "auto" in t:
            return "Automatic"
        if "schalt" in t or "manual" in t:
            return "Manual"
        return raw

    def _normalize_body(self, raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        mapping = {
            "estatecar": "wagon",
            "van": "van",
            "limousine": "sedan",
            "smallcar": "hatchback",
            "offroad": "suv",
            "cabrio": "cabrio",
        }
        r = raw.lower()
        return mapping.get(r, raw)

    def _normalize_engine(self, raw: Optional[str], full: Optional[str]) -> Optional[str]:
        val = (full or raw or "").lower()
        if not val:
            return None
        if "diesel" in val:
            return "Diesel"
        if "electric" in val or "ev" in val:
            return "Electric"
        if "hybrid" in val:
            return "Hybrid"
        if "petrol" in val or "benzin" in val or "gasoline" in val:
            return "Petrol"
        if "lpg" in val or "gas" in val:
            return "LPG"
        return full or raw

    def _detect_drive(self, options: List[str]) -> Optional[str]:
        opts = " ".join(options).lower()
        if "four-wheel drive" in opts or "all wheel" in opts or "four-wheel" in opts:
            return "AWD"
        if "front wheel drive" in opts:
            return "FWD"
        if "rear wheel drive" in opts:
            return "RWD"
        return None

    def iter_parsed_from_csv(self, rows: Iterable[MobileDeCsvRow]) -> Iterator[CarParsed]:
        for row in rows:
            images: List[str] = list(row.image_urls) if row.image_urls else []
            thumb = images[0] if images else None
            drive = self._detect_drive(row.options or [])
            payload = self._payload_from_row(row)
            # skip obviously broken rows
            if not row.mark and not row.model and not row.title:
                continue
            if not row.url:
                continue
            yield CarParsed(
                source_key=self.config.key,
                external_id=str(row.inner_id),
                country=row.seller_country or self.config.country or "DE",
                brand=(row.mark or None),
                model=(row.model or None),
                variant=(row.sub_title or None),
                year=row.year,
                registration_year=int(row.first_registration.split("/")[1]) if row.first_registration and "/" in row.first_registration else None,
                registration_month=int(row.first_registration.split("/")[0]) if row.first_registration and "/" in row.first_registration else None,
                mileage=row.km_age,
                price=float(
                    row.price_eur) if row.price_eur is not None else None,
                currency="EUR",
                listing_date=row.created_at,
                engine_cc=int(row.displacement * 1000) if row.displacement else None,
                power_hp=float(row.horse_power) if row.horse_power else None,
                power_kw=float(row.power_kw) if hasattr(row, "power_kw") and row.power_kw else (float(row.horse_power) /
                                                                                              1.35962 if row.horse_power else None),
                body_type=self._normalize_body(row.body_type),
                engine_type=self._normalize_engine(
                    row.engine_type, row.full_fuel_type),
                transmission=self._normalize_transmission(row.transmission),
                drive_type=drive,
                color=row.manufacturer_color or row.color,
                vin=None,
                source_url=row.url,
                thumbnail_url=thumb,
                images=images,
                source_payload=payload,
            )
