from __future__ import annotations

from typing import Iterable, Iterator, List, Optional, Any, Dict
from dataclasses import asdict
from decimal import Decimal, ROUND_HALF_UP
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
        if "hybrid" in val or "plug-in" in val or "plug in" in val:
            return "Hybrid"
        if "electric" in val or "ev" in val:
            return "Electric"
        if "petrol" in val or "benzin" in val or "gasoline" in val:
            return "Petrol"
        if "lpg" in val or "gas" in val:
            return "LPG"
        return full or raw

    def _resolve_engine_cc(self, row: MobileDeCsvRow) -> Optional[int]:
        raw_cc = row.displacement_orig
        if raw_cc is not None:
            try:
                return int(Decimal(raw_cc).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
            except Exception:
                pass
        if row.displacement is None:
            return None
        try:
            return int((Decimal(row.displacement) * Decimal("1000")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        except Exception:
            return None

    def _resolve_price_eur(self, row: MobileDeCsvRow) -> Optional[float]:
        if row.price_eur_nt is not None:
            try:
                return float(row.price_eur_nt)
            except Exception:
                pass
        if row.price_eur is not None:
            try:
                return float(row.price_eur)
            except Exception:
                pass
        return None

    def _resolve_power_kw(self, row: MobileDeCsvRow) -> Optional[float]:
        if row.power_kw is not None:
            try:
                value = float(row.power_kw)
                if 0 < value <= 2500:
                    return value
            except Exception:
                pass
        if row.horse_power:
            try:
                return float(row.horse_power) / 1.35962
            except Exception:
                return None
        return None

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
            option_values = list(row.options or [])
            if row.features:
                option_values.extend(row.features)
            drive = self._detect_drive(option_values)
            payload = self._payload_from_row(row)
            # skip obviously broken rows
            if not row.mark and not row.model and not row.title:
                continue
            if not row.url:
                continue
            yield CarParsed(
                source_key=self.config.key,
                external_id=str(row.inner_id),
                country=(row.seller_country or self.config.country or "DE").upper(),
                brand=(row.mark or None),
                model=(row.model or None),
                variant=(row.sub_title or None),
                year=row.year,
                registration_year=int(row.first_registration.split("/")[1]) if row.first_registration and "/" in row.first_registration else None,
                registration_month=int(row.first_registration.split("/")[0]) if row.first_registration and "/" in row.first_registration else None,
                mileage=row.km_age,
                price=self._resolve_price_eur(row),
                currency="EUR",
                listing_date=row.created_at,
                engine_cc=self._resolve_engine_cc(row),
                power_hp=float(row.horse_power) if row.horse_power else None,
                power_kw=self._resolve_power_kw(row),
                body_type=self._normalize_body(row.body_type),
                engine_type=self._normalize_engine(
                    row.envkv_engine_type or row.engine_type,
                    row.envkv_consumption_fuel or row.full_fuel_type,
                ),
                transmission=self._normalize_transmission(row.transmission),
                drive_type=drive,
                color=row.manufacturer_color or row.color,
                vin=None,
                source_url=row.url,
                thumbnail_url=thumb,
                images=images,
                source_payload=payload,
            )
