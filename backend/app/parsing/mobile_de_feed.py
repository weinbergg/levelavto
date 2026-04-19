from __future__ import annotations

import re
from typing import Iterable, Iterator, List, Optional, Any, Dict
from dataclasses import asdict
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
from .base import CarParsed
from .config import SiteConfig
from ..importing.mobilede_csv import MobileDeCsvRow
from ..services.cars_service import CarsService, normalize_brand, normalize_model_label


class MobileDeFeedParser:
    _POWER_KW_RE = re.compile(r"\b(\d{2,4})(?:[.,]\d+)?\s*k\s*w\b", re.IGNORECASE)
    _POWER_HP_RE = re.compile(r"\b(\d{2,4})(?:[.,]\d+)?\s*(?:cv|ps|hp)\b", re.IGNORECASE)
    _PLACEHOLDER_MODELS = {"other", "others"}
    _MODEL_RECOVERY_DONORS = {
        "MERCEDES-BENZ": [
            "A-Class",
            "B-Class",
            "C-Class",
            "CL-Class",
            "CLA",
            "CLC",
            "CLK",
            "CLS",
            "E-Class",
            "G-Class",
            "GLA",
            "GLB",
            "GLC",
            "GLE",
            "GLK",
            "GLS",
            "M-Class",
            "ML",
            "R-Class",
            "S-Class",
            "SL",
            "SLK",
            "SLC",
            "AMG GT",
            "Citan",
            "Sprinter",
            "V-Class",
            "Vito",
            "Viano",
            "X-Class",
            "EQA",
            "EQB",
            "EQC",
            "EQE",
            "EQS",
            "EQV",
            "Maybach",
        ],
    }

    def __init__(self, site_config: SiteConfig):
        self.config = site_config
        self._model_recovery_service = CarsService(None)  # type: ignore[arg-type]

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
        for raw in (row.sub_title, row.title, row.description):
            if not raw:
                continue
            match = self._POWER_KW_RE.search(str(raw))
            if match:
                try:
                    value = float(match.group(1))
                except Exception:
                    continue
                if 0 < value <= 2500:
                    return value
        return None

    def _resolve_power_hp(self, row: MobileDeCsvRow) -> Optional[float]:
        if row.horse_power:
            try:
                value = float(row.horse_power)
                if 0 < value <= 3500:
                    return value
            except Exception:
                pass
        if row.power_kw is not None:
            try:
                value = float(row.power_kw)
                if 0 < value <= 2500:
                    return round(value * 1.35962, 1)
            except Exception:
                pass
        for raw in (row.sub_title, row.title, row.description):
            if not raw:
                continue
            match_hp = self._POWER_HP_RE.search(str(raw))
            if match_hp:
                try:
                    value = float(match_hp.group(1))
                except Exception:
                    continue
                if 0 < value <= 3500:
                    return value
            match_kw = self._POWER_KW_RE.search(str(raw))
            if match_kw:
                try:
                    value_kw = float(match_kw.group(1))
                except Exception:
                    continue
                if 0 < value_kw <= 2500:
                    return round(value_kw * 1.35962, 1)
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

    def _parse_first_registration(self, raw: Optional[str]) -> tuple[Optional[int], Optional[int]]:
        if not raw:
            return None, None
        value = str(raw).strip()
        if not value:
            return None, None
        normalized = re.sub(r"\s+", "", value)
        if not normalized or normalized in {"-", "--", "n/a", "na"}:
            return None, None

        patterns = [
            r"^(?P<month>\d{1,2})[./-](?P<year>\d{4})$",
            r"^(?P<day>\d{1,2})[./-](?P<month>\d{1,2})[./-](?P<year>\d{4})$",
            r"^(?P<year>\d{4})[./-](?P<month>\d{1,2})$",
            r"^(?P<year>\d{4})$",
        ]
        for pattern in patterns:
            match = re.match(pattern, normalized)
            if not match:
                continue
            year_raw = match.groupdict().get("year")
            month_raw = match.groupdict().get("month")
            try:
                year = int(year_raw) if year_raw else None
            except Exception:
                year = None
            try:
                month = int(month_raw) if month_raw else None
            except Exception:
                month = None
            if year is None or year < 1950 or year > 2100:
                return None, None
            if month is not None and (month < 1 or month > 12):
                return None, None
            return year, month
        return None, None

    def _resolve_model(self, row: MobileDeCsvRow) -> Optional[str]:
        raw_model = normalize_model_label(row.model)
        if raw_model and raw_model.casefold() not in self._PLACEHOLDER_MODELS:
            return raw_model
        brand = normalize_brand(row.mark).strip() if row.mark else ""
        donors = self._MODEL_RECOVERY_DONORS.get(brand.upper(), [])
        if not donors:
            return raw_model or None
        for candidate in (row.sub_title, row.title):
            text = normalize_model_label(candidate)
            if not text:
                continue
            recovered = self._model_recovery_service._match_eu_model_donor(text, donors)
            if recovered:
                return recovered
        return raw_model or None

    def iter_parsed_from_csv(self, rows: Iterable[MobileDeCsvRow]) -> Iterator[CarParsed]:
        for row in rows:
            images: List[str] = list(row.image_urls) if row.image_urls else []
            thumb = images[0] if images else None
            option_values = list(row.options or [])
            if row.features:
                option_values.extend(row.features)
            drive = self._detect_drive(option_values)
            payload = self._payload_from_row(row)
            registration_year, registration_month = self._parse_first_registration(
                row.first_registration
            )
            # skip obviously broken rows
            if not row.mark and not row.model and not row.title:
                continue
            if not row.url:
                continue
            engine_type = self._normalize_engine(
                row.envkv_engine_type or row.engine_type,
                row.envkv_consumption_fuel or row.full_fuel_type,
            )
            power_hp = self._resolve_power_hp(row)
            power_kw = self._resolve_power_kw(row)
            engine_cc = self._resolve_engine_cc(row)
            if engine_type == "Electric":
                engine_cc = None
            yield CarParsed(
                source_key=self.config.key,
                external_id=str(row.inner_id),
                country=(row.seller_country or self.config.country or "DE").upper(),
                brand=(row.mark or None),
                model=self._resolve_model(row),
                variant=(row.sub_title or None),
                year=row.year,
                registration_year=registration_year,
                registration_month=registration_month,
                mileage=row.km_age,
                price=self._resolve_price_eur(row),
                currency="EUR",
                listing_date=row.created_at,
                engine_cc=engine_cc,
                power_hp=power_hp,
                power_kw=power_kw,
                body_type=self._normalize_body(row.body_type),
                engine_type=engine_type,
                transmission=self._normalize_transmission(row.transmission),
                drive_type=drive,
                color=row.manufacturer_color or row.color,
                description=row.description,
                vin=None,
                source_url=row.url,
                thumbnail_url=thumb,
                images=images,
                source_payload=payload,
            )
