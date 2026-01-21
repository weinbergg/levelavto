from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from pathlib import Path
from sqlalchemy.orm import Session
from sqlalchemy import select, func, and_, or_, case, cast, String, text
from sqlalchemy.dialects.postgresql import JSONB
import logging
from cachetools import TTLCache

logger = logging.getLogger(__name__)
import re
import os
import requests
import time
from ..models import Car, Source, FeaturedCar
from ..utils.localization import display_color
from ..utils.country_map import normalize_country_code
from ..utils.taxonomy import (
    normalize_color,
    normalize_fuel,
    ru_color,
    ru_fuel,
    ru_transmission,
    color_aliases,
    fuel_aliases,
    color_hex,
    is_color_base,
)
from ..utils.breakdown_labels import label_for
from .calculator_config_service import CalculatorConfigService
from .calculator_runtime import EstimateRequest, calculate

BRAND_ALIASES = {
    "alfa": "Alfa Romeo",
    "alfa romeo": "Alfa Romeo",
}


def normalize_brand(value: Optional[str]) -> str:
    if not value:
        return ""
    raw = str(value).strip()
    if not raw:
        return ""
    return BRAND_ALIASES.get(raw.lower(), raw)


def brand_variants(value: Optional[str]) -> List[str]:
    norm = normalize_brand(value)
    if not norm:
        return []
    variants = {norm}
    if norm == "Alfa Romeo":
        variants.add("Alfa")
    return sorted(variants, key=lambda v: v.lower())


class CarsService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.logger = logging.getLogger(__name__)

    EU_COUNTRIES = {
        "DE", "AT", "FR", "IT", "ES", "NL", "BE", "PL", "CZ", "SE", "FI",
        "NO", "DK", "PT", "GR", "CH", "LU", "IE", "GB", "HU", "SK", "SI",
        "HR", "RO", "BG", "EE", "LV", "LT", "MT", "CY", "IS", "LI", "MC",
        "SM", "AD",
    }
    KOREA_SOURCE_HINTS = ("emavto", "encar", "m-auto", "m_auto")
    EUROPE_SOURCE_PREFIX = "mobile"

    def _source_ids_for_hints(self, hints: tuple[str, ...]) -> List[int]:
        if not hints:
            return []
        key_expr = func.lower(Source.key)
        conds = [key_expr.like(f"%{hint}%") for hint in hints]
        return self.db.execute(select(Source.id).where(or_(*conds))).scalars().all()

    def _source_ids_for_europe(self) -> List[int]:
        key_expr = func.lower(Source.key)
        return self.db.execute(
            select(Source.id).where(
                or_(
                    key_expr.like(f"%{self.EUROPE_SOURCE_PREFIX}%"),
                    func.upper(Source.country).in_(self.EU_COUNTRIES),
                )
            )
        ).scalars().all()

    def available_eu_countries(self) -> List[str]:
        rows = self.db.execute(
            select(func.distinct(Car.country))
            .where(Car.is_available.is_(True), Car.country.is_not(None))
        ).scalars().all()
        countries: List[str] = []
        seen = set()
        for c in rows:
            code = normalize_country_code(c)
            if not code:
                continue
            if code in self.EU_COUNTRIES and code not in seen:
                countries.append(code)
                seen.add(code)
        return countries

    def has_korea(self) -> bool:
        rows = self.db.execute(
            select(func.distinct(Car.country))
            .where(Car.is_available.is_(True), Car.country.is_not(None))
        ).scalars().all()
        for c in rows:
            code = normalize_country_code(c)
            if code == "KR":
                return True
        return bool(self._source_ids_for_hints(self.KOREA_SOURCE_HINTS))

    def available_regions(self) -> List[str]:
        regions: List[str] = []
        if self.available_eu_countries() or self._source_ids_for_europe():
            regions.append("EU")
        if self.has_korea():
            regions.append("KR")
        return regions

    _fx_cache: dict | None = None
    _fx_cache_ts: float | None = None
    _count_cache: TTLCache = TTLCache(maxsize=1024, ttl=120)

    def _can_fast_count(
        self,
        *,
        region: Optional[str],
        country: Optional[str],
        brand: Optional[str],
        model: Optional[str],
        lines: Optional[List[str]],
        source_key: Optional[str | List[str]],
        q: Optional[str],
        generation: Optional[str],
        color: Optional[str],
        price_min: Optional[float],
        price_max: Optional[float],
        year_min: Optional[int],
        year_max: Optional[int],
        mileage_min: Optional[int],
        mileage_max: Optional[int],
        reg_year_min: Optional[int],
        reg_month_min: Optional[int],
        reg_year_max: Optional[int],
        reg_month_max: Optional[int],
        body_type: Optional[str],
        engine_type: Optional[str],
        transmission: Optional[str],
        drive_type: Optional[str],
        num_seats: Optional[str],
        doors_count: Optional[str],
        emission_class: Optional[str],
        efficiency_class: Optional[str],
        climatisation: Optional[str],
        airbags: Optional[str],
        interior_design: Optional[str],
        air_suspension: Optional[bool],
        price_rating_label: Optional[str],
        owners_count: Optional[str],
        power_hp_min: Optional[float],
        power_hp_max: Optional[float],
        engine_cc_min: Optional[int],
        engine_cc_max: Optional[int],
        condition: Optional[str],
        kr_type: Optional[str],
    ) -> bool:
        if any(
            [
                lines,
                source_key,
                q,
                generation,
                color,
                price_min,
                price_max,
                year_min,
                year_max,
                mileage_min,
                mileage_max,
                reg_year_min,
                reg_month_min,
                reg_year_max,
                reg_month_max,
                body_type,
                engine_type,
                transmission,
                drive_type,
                num_seats,
                doors_count,
                emission_class,
                efficiency_class,
                climatisation,
                airbags,
                interior_design,
                air_suspension,
                price_rating_label,
                owners_count,
                power_hp_min,
                power_hp_max,
                engine_cc_min,
                engine_cc_max,
                condition,
                kr_type,
            ]
        ):
            return False
        return True

    def _fast_count(
        self,
        *,
        region: str,
        country: Optional[str],
        brand: Optional[str],
        model: Optional[str],
    ) -> Optional[int]:
        region_norm = region.upper().strip() if region else None
        if region_norm not in ("EU", "KR", None):
            return None
        brand_norm = normalize_brand(brand).strip() if brand else None
        model_norm = model.strip() if model else None
        country_norm = normalize_country_code(country) if country else None
        if country_norm == "EU":
            country_norm = None
        if country_norm == "KR" and region_norm not in (None, "KR"):
            return None
        stmt = text(
            """
            SELECT COALESCE(SUM(total), 0) AS total
            FROM car_counts
            WHERE (:region IS NULL OR region = :region)
              AND (:country IS NULL OR country = :country)
              AND (:brand IS NULL OR brand = :brand)
              AND (:model IS NULL OR model = :model)
            """
        )
        try:
            row = self.db.execute(
                stmt,
                {
                    "region": region_norm,
                    "country": country_norm,
                    "brand": brand_norm or None,
                    "model": model_norm or None,
                },
            ).first()
        except Exception:
            self.logger.exception("fast_count_failed region=%s", region_norm)
            return None
        return int(row[0]) if row else None

    def _facet_where(self, filters: Dict[str, Any], params: Dict[str, Any]) -> List[str]:
        clauses = []
        for key, col in filters.items():
            val = params.get(key)
            if val is None or val == "":
                continue
            clauses.append(f"{col} = :{key}")
        return clauses

    def facet_counts(self, *, field: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        cols = {
            "region": "region",
            "country": "country",
            "brand": "brand",
            "model": "model",
            "color": "color",
            "engine_type": "engine_type",
            "transmission": "transmission",
            "body_type": "body_type",
            "drive_type": "drive_type",
            "price_bucket": "price_bucket",
            "mileage_bucket": "mileage_bucket",
            "reg_year": "reg_year",
        }
        if field not in cols:
            return []

        params: Dict[str, Any] = {}
        filters_norm = {
            "region": filters.get("region"),
            "country": normalize_country_code(filters.get("country")) if filters.get("country") else None,
            "brand": normalize_brand(filters.get("brand")).strip() if filters.get("brand") else None,
            "model": filters.get("model"),
            "color": filters.get("color"),
            "engine_type": filters.get("engine_type"),
            "transmission": filters.get("transmission"),
            "body_type": filters.get("body_type"),
            "drive_type": filters.get("drive_type"),
            "price_bucket": filters.get("price_bucket"),
            "mileage_bucket": filters.get("mileage_bucket"),
            "reg_year": filters.get("reg_year"),
        }
        for k, v in filters_norm.items():
            if v is not None and v != "":
                params[k] = v

        where_clauses = self._facet_where(cols, params)
        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        order_sql = "count DESC, value"
        if field == "reg_year":
            order_sql = "value DESC"

        query = text(
            f"""
            SELECT {cols[field]} AS value, SUM(total) AS count
            FROM car_counts
            {where_sql}
            GROUP BY {cols[field]}
            HAVING SUM(total) > 0
            ORDER BY {order_sql}
            """
        )
        rows = self.db.execute(query, params).all()
        out = []
        for value, count in rows:
            if value is None or value == "":
                continue
            out.append({"value": value, "count": int(count)})
        return out

    def get_fx_rates(self) -> dict | None:
        now = time.time()
        if self._fx_cache and self._fx_cache_ts and now - self._fx_cache_ts < 3600:
            return self._fx_cache
        eur_env = float(os.environ.get("EURO_RATE", "95.0"))
        usd_env = float(os.environ.get("USD_RATE", "85.0"))
        eur, usd = eur_env, usd_env
        try:
            res = requests.get(
                "https://www.cbr-xml-daily.ru/daily_json.js", timeout=1.5)
            data = res.json()
            eur = float(data["Valute"]["EUR"]["Value"])
            usd = float(data["Valute"]["USD"]["Value"])
        except Exception:
            pass
        self._fx_cache = {"EUR": eur, "USD": usd, "RUB": 1.0}
        self._fx_cache_ts = now
        return self._fx_cache

    def list_cars(
        self,
        *,
        region: Optional[str] = None,
        country: Optional[str] = None,
        kr_type: Optional[str] = None,
        brand: Optional[str] = None,
        lines: Optional[List[str]] = None,
        source_key: Optional[str | List[str]] = None,
        q: Optional[str] = None,
        model: Optional[str] = None,
        generation: Optional[str] = None,
        color: Optional[str] = None,
        price_min: Optional[float] = None,
        price_max: Optional[float] = None,
        year_min: Optional[int] = None,
        year_max: Optional[int] = None,
        mileage_min: Optional[int] = None,
        mileage_max: Optional[int] = None,
        reg_year_min: Optional[int] = None,
        reg_month_min: Optional[int] = None,
        reg_year_max: Optional[int] = None,
        reg_month_max: Optional[int] = None,
        body_type: Optional[str] = None,
        engine_type: Optional[str] = None,
        transmission: Optional[str] = None,
        drive_type: Optional[str] = None,
        num_seats: Optional[str] = None,
        doors_count: Optional[str] = None,
        emission_class: Optional[str] = None,
        efficiency_class: Optional[str] = None,
        climatisation: Optional[str] = None,
        airbags: Optional[str] = None,
        interior_design: Optional[str] = None,
        air_suspension: Optional[bool] = None,
        price_rating_label: Optional[str] = None,
        owners_count: Optional[str] = None,
        power_hp_min: Optional[float] = None,
        power_hp_max: Optional[float] = None,
        engine_cc_min: Optional[int] = None,
        engine_cc_max: Optional[int] = None,
        condition: Optional[str] = None,
        sort: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Tuple[List[Car], int]:
        conditions = [Car.is_available.is_(True)]
        if region and not country:
            if region.upper() == "KR":
                country = "KR"
        if lines:
            line_conditions = []
            brand_field = func.lower(func.trim(Car.brand))
            model_field = func.lower(func.trim(Car.model))
            variant_field = func.lower(func.trim(Car.variant))
            for line in lines:
                parts = [p.strip() for p in (line or "").split("|")]
                while len(parts) < 3:
                    parts.append("")
                b, m, v = parts[0], parts[1], parts[2]
                group = []
                if b:
                    norm_b = normalize_brand(b).strip().strip(".,;")
                    variants = brand_variants(norm_b) if norm_b else []
                    if variants:
                        group.append(or_(*[
                            brand_field.like(func.lower(f"%{v}%")) for v in variants
                        ]))
                    else:
                        group.append(brand_field.like(func.lower(f"%{b}%")))
                if m:
                    group.append(model_field.like(func.lower(f"%{m}%")))
                if v:
                    group.append(variant_field.like(func.lower(f"%{v}%")))
                if group:
                    line_conditions.append(and_(*group))
            if line_conditions:
                conditions.append(or_(*line_conditions))
        if country:
            c = normalize_country_code(country)
            if c == "EU":
                country = None
                if not region:
                    region = "EU"
            elif c == "KR":
                kr_sources = self._source_ids_for_hints(self.KOREA_SOURCE_HINTS)
                kr_conds = [func.upper(Car.country).like("KR%")]
                if kr_sources:
                    kr_conds.append(Car.source_id.in_(kr_sources))
                conditions.append(or_(*kr_conds))
            elif c:
                conditions.append(func.upper(Car.country) == c)
        if region:
            r = region.upper()
            if r == "KR":
                kr_sources = self._source_ids_for_hints(self.KOREA_SOURCE_HINTS)
                conds = [func.upper(Car.country).like("KR%")]
                if kr_sources:
                    conds.append(Car.source_id.in_(kr_sources))
                conditions.append(or_(*conds))
            elif r == "EU":
                eu_sources = self._source_ids_for_europe()
                if eu_sources:
                    conditions.append(Car.source_id.in_(eu_sources))
                else:
                    conditions.append(func.upper(Car.country).in_(self.EU_COUNTRIES))
        if kr_type:
            kt_raw = str(kr_type).upper()
            kt = None
            if kt_raw in ("KR_INTERNAL", "DOMESTIC"):
                kt = "domestic"
            elif kt_raw in ("KR_IMPORT", "IMPORT"):
                kt = "import"
            if kt:
                # allow KR by country or by source hints
                kr_sources = self._source_ids_for_hints(self.KOREA_SOURCE_HINTS)
                conds = [func.lower(Car.kr_market_type) == kt]
                conds.append(func.upper(Car.country).like("KR%"))
                if kr_sources:
                    conds.append(Car.source_id.in_(kr_sources))
                conditions.append(and_(or_(*conds)))
        if brand:
            # case-insensitive contains (brand may have variants/extra symbols)
            b = normalize_brand(brand).strip().strip(".,;")
            if b:
                variants = brand_variants(b)
                if variants:
                    brand_field = func.lower(func.trim(Car.brand))
                    brand_conds = [brand_field.like(func.lower(f"%{v}%")) for v in variants]
                    conditions.append(or_(*brand_conds))
        if q:
            tokens = [t for t in re.split(r"[\\s,]+", q.strip().lower()) if t]
            payload_text = func.lower(cast(Car.source_payload, String))
            token_groups = []
            fuel_map = {
                "дизель": ["diesel"],
                "дизельный": ["diesel"],
                "дизельные": ["diesel"],
                "дизельное": ["diesel"],
                "diesel": ["diesel"],
                "бензин": ["petrol", "gasoline", "benzin"],
                "бенз": ["petrol", "gasoline", "benzin"],
                "hybrid": ["hybrid"],
                "гибрид": ["hybrid"],
                "электро": ["electric", "ev"],
                "электр": ["electric", "ev"],
                "electric": ["electric", "ev"],
            }
            drive_tokens = {"4x4", "4х4", "4wd", "awd", "full", "полный", "полныйпривод"}
            for token in tokens:
                conds = []
                if token in fuel_map or token.startswith("дизел"):
                    mapped = fuel_map.get(token, fuel_map["дизель"])
                    for f in mapped:
                        conds.append(func.lower(Car.engine_type).like(f"%{f}%"))
                        conds.append(payload_text.like(f"%{f}%"))
                elif token in drive_tokens:
                    conds.append(func.lower(Car.drive_type).like("%awd%"))
                    conds.append(func.lower(Car.drive_type).like("%4wd%"))
                    conds.append(payload_text.like("%four-wheel%"))
                    conds.append(payload_text.like("%all wheel%"))
                    conds.append(payload_text.like("%4x4%"))
                elif token.startswith("панор") or token.startswith("panor"):
                    conds.append(payload_text.like("%panor%"))
                else:
                    like = f"%{token}%"
                    conds.extend(
                        [
                            func.lower(Car.brand).like(like),
                            func.lower(Car.model).like(like),
                            func.lower(Car.variant).like(like),
                            func.lower(Car.generation).like(like),
                            func.lower(Car.body_type).like(like),
                            func.lower(Car.engine_type).like(like),
                            func.lower(Car.transmission).like(like),
                            func.lower(Car.drive_type).like(like),
                            func.lower(Car.color).like(like),
                            payload_text.like(like),
                        ]
                    )
                if conds:
                    token_groups.append(or_(*conds))
            if token_groups:
                conditions.append(and_(*token_groups))
        if model:
            like = f"%{model.strip()}%"
            # match by model field; if brand is not specified, also try brand+model text
            model_expr = func.lower(Car.model).like(func.lower(like))
            if not brand:
                concat_expr = func.lower(func.concat(
                    Car.brand, " ", Car.model)).like(func.lower(like))
                conditions.append(or_(model_expr, concat_expr))
            else:
                conditions.append(model_expr)
        if num_seats:
            conditions.append(
                func.jsonb_extract_path_text(cast(Car.source_payload, JSONB), "num_seats")
                == str(num_seats)
            )
        if doors_count:
            conditions.append(
                func.jsonb_extract_path_text(cast(Car.source_payload, JSONB), "doors_count")
                == str(doors_count)
            )
        if emission_class:
            conditions.append(
                func.jsonb_extract_path_text(cast(Car.source_payload, JSONB), "emission_class")
                == emission_class
            )
        if efficiency_class:
            conditions.append(
                func.jsonb_extract_path_text(cast(Car.source_payload, JSONB), "efficiency_class")
                == efficiency_class
            )
        if climatisation:
            conditions.append(
                func.jsonb_extract_path_text(cast(Car.source_payload, JSONB), "climatisation")
                == climatisation
            )
        if airbags:
            conditions.append(
                func.jsonb_extract_path_text(cast(Car.source_payload, JSONB), "airbags")
                == airbags
            )
        if interior_design:
            conditions.append(
                func.jsonb_extract_path_text(cast(Car.source_payload, JSONB), "interior_design")
                == interior_design
            )
        if air_suspension:
            payload_text = func.lower(cast(Car.source_payload, String))
            conditions.append(
                or_(
                    payload_text.like("%air suspension%"),
                    payload_text.like("%air_suspension%"),
                    payload_text.like("%pneum%"),
                    payload_text.like("%пневмо%"),
                )
            )
        if price_rating_label:
            conditions.append(
                func.jsonb_extract_path_text(cast(Car.source_payload, JSONB), "price_rating_label")
                == price_rating_label
            )
        if owners_count:
            conditions.append(
                func.jsonb_extract_path_text(cast(Car.source_payload, JSONB), "owners_count")
                == str(owners_count)
            )
        if generation:
            conditions.append(func.lower(Car.generation).like(
                func.lower(f"%{generation.strip()}%")))
        if color:
            aliases = color_aliases(color)
            if aliases:
                conditions.append(or_(
                    *[func.lower(Car.color).like(f"%{a}%") for a in aliases]
                ))
            else:
                conditions.append(func.lower(Car.color) == color.lower())
        if source_key:
            keys: List[str] = []
            if isinstance(source_key, str):
                keys = [k.strip() for k in source_key.split(",") if k.strip()]
            else:
                keys = [k.strip() for k in source_key if k and k.strip()]
            if keys:
                src_ids = self.db.execute(select(Source.id).where(
                    Source.key.in_(keys))).scalars().all()
                if src_ids:
                    conditions.append(Car.source_id.in_(src_ids))
        if price_min is not None or price_max is not None:
            conditions.append(Car.total_price_rub_cached.is_not(None))
            if price_min is not None:
                conditions.append(Car.total_price_rub_cached >= price_min)
            if price_max is not None:
                conditions.append(Car.total_price_rub_cached <= price_max)
        if year_min is not None:
            conditions.append(Car.year >= year_min)
        if year_max is not None:
            conditions.append(Car.year <= year_max)
        if mileage_min is not None:
            conditions.append(Car.mileage >= mileage_min)
        if mileage_max is not None:
            conditions.append(Car.mileage <= mileage_max)
        if reg_year_min is not None:
            if reg_month_min is not None:
                conditions.append(
                    or_(
                        Car.registration_year > reg_year_min,
                        and_(
                            Car.registration_year == reg_year_min,
                            Car.registration_month.is_not(None),
                            Car.registration_month >= reg_month_min,
                        ),
                    )
                )
            else:
                conditions.append(Car.registration_year >= reg_year_min)
        if reg_year_max is not None:
            if reg_month_max is not None:
                conditions.append(
                    or_(
                        Car.registration_year < reg_year_max,
                        and_(
                            Car.registration_year == reg_year_max,
                            Car.registration_month.is_not(None),
                            Car.registration_month <= reg_month_max,
                        ),
                    )
                )
            else:
                conditions.append(Car.registration_year <= reg_year_max)
        if body_type:
            conditions.append(func.lower(Car.body_type) == body_type.lower())
        if engine_type:
            aliases = fuel_aliases(engine_type)
            if aliases:
                conditions.append(or_(
                    *[func.lower(Car.engine_type).like(f"%{a}%") for a in aliases]
                ))
            else:
                conditions.append(func.lower(Car.engine_type) == engine_type.lower())
        if transmission:
            conditions.append(func.lower(Car.transmission)
                              == transmission.lower())
        if drive_type:
            conditions.append(func.lower(Car.drive_type) == drive_type.lower())
        if power_hp_min is not None:
            conditions.append(Car.power_hp >= power_hp_min)
        if power_hp_max is not None:
            conditions.append(Car.power_hp <= power_hp_max)
        if engine_cc_min is not None:
            conditions.append(Car.engine_cc >= engine_cc_min)
        if engine_cc_max is not None:
            conditions.append(Car.engine_cc <= engine_cc_max)
        if condition:
            cond = condition.strip().lower()
            if cond == "new":
                conditions.append(Car.mileage.is_not(None))
                conditions.append(Car.mileage <= 100)
            elif cond == "used":
                conditions.append(Car.mileage.is_not(None))
                conditions.append(Car.mileage > 100)

        where_expr = and_(*conditions) if conditions else None

        # cached count for repeated requests
        count_key = (
            region,
            country,
            brand,
            tuple(lines) if lines else None,
            tuple(source_key) if isinstance(source_key, list) else source_key,
            q,
            model,
            generation,
            color,
            price_min,
            price_max,
            mileage_min,
            mileage_max,
            reg_year_min,
            reg_month_min,
            reg_year_max,
            reg_month_max,
            body_type,
            engine_type,
            transmission,
            drive_type,
            num_seats,
            doors_count,
            emission_class,
            efficiency_class,
            climatisation,
            airbags,
            interior_design,
            air_suspension,
            price_rating_label,
            owners_count,
            condition,
            kr_type,
        )
        total = self._count_cache.get(count_key)
        total_t0 = time.perf_counter()
        if total is None:
            total = None
            if self._can_fast_count(
                region=region,
                country=country,
                brand=brand,
                model=model,
                lines=lines,
                source_key=source_key,
                q=q,
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
                power_hp_min=power_hp_min,
                power_hp_max=power_hp_max,
                engine_cc_min=engine_cc_min,
                engine_cc_max=engine_cc_max,
                condition=condition,
                kr_type=kr_type,
            ):
                total = self._fast_count(
                    region=region or "",
                    country=country,
                    brand=brand,
                    model=model,
                )
            if total is None:
                total_stmt = select(func.count()).select_from(Car).where(where_expr)
                total = self.db.execute(total_stmt).scalar_one()
            self._count_cache[count_key] = total
            elapsed = time.perf_counter() - total_t0
            if elapsed > 2:
                self.logger.warning("count_slow total=%.3fs filters=%s", elapsed, count_key)
            elif elapsed > 1:
                self.logger.info("count_warn total=%.3fs filters=%s", elapsed, count_key)

        order_clause = []
        if sort == "price_asc":
            order_clause = [
                Car.total_price_rub_cached.asc().nullslast(),
                Car.price_rub_cached.asc().nullslast(),
                Car.id.desc(),
            ]
        elif sort == "price_desc":
            order_clause = [
                Car.total_price_rub_cached.desc().nullslast(),
                Car.price_rub_cached.desc().nullslast(),
                Car.id.desc(),
            ]
        elif sort == "year_desc":
            order_clause = [Car.year.desc().nullslast(), Car.id.desc()]
        elif sort == "year_asc":
            order_clause = [Car.year.asc().nullslast(), Car.id.desc()]
        elif sort == "mileage_asc":
            order_clause = [Car.mileage.asc().nullslast(), Car.id.desc()]
        elif sort == "mileage_desc":
            order_clause = [Car.mileage.desc().nullslast(), Car.id.desc()]
        elif sort == "reg_desc":
            order_clause = [Car.reg_sort_key.desc().nullslast(), Car.id.desc()]
        elif sort == "reg_asc":
            order_clause = [Car.reg_sort_key.asc().nullslast(), Car.id.desc()]
        elif sort == "listing_desc":
            order_clause = [Car.listing_sort_ts.desc().nullslast(), Car.id.desc()]
        elif sort == "listing_asc":
            order_clause = [Car.listing_sort_ts.asc().nullslast(), Car.id.desc()]
        else:
            # default: цена сначала дешевые
            order_clause = [Car.price_rub_cached.asc().nullslast(), Car.id.desc()]

        thumb_rank = case(
            (and_(Car.thumbnail_url.is_not(None), Car.thumbnail_url != ""), 1),
            else_=0,
        ).desc()
        stmt = (
            select(Car)
            .where(where_expr)
            .order_by(thumb_rank, *order_clause)
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        items_t0 = time.perf_counter()
        items = list(self.db.execute(stmt).scalars().all())
        elapsed_items = time.perf_counter() - items_t0
        if elapsed_items > 2:
            self.logger.warning(
                "list_slow total=%.3fs sort=%s page=%s size=%s filters=%s",
                elapsed_items,
                sort,
                page,
                page_size,
                count_key,
            )
        elif elapsed_items > 1:
            self.logger.info(
                "list_warn total=%.3fs sort=%s page=%s size=%s filters=%s",
                elapsed_items,
                sort,
                page,
                page_size,
                count_key,
            )
        return items, total

    def ensure_calc_cache(self, car: Car) -> dict | None:
        if not car:
            return None
        # базовые цены из source_payload
        payload = car.source_payload or {}
        price_gross = payload.get("price_eur")
        price_net = payload.get("price_eur_nt")
        vat_pct = payload.get("vat")
        used_price = None
        used_currency = "EUR"
        vat_reclaim = False
        if price_net and vat_pct and float(vat_pct) > 0:
            used_price = float(price_net)
            vat_reclaim = True
        elif price_gross:
            used_price = float(price_gross)
        else:
            used_price = car.price
            used_currency = car.currency or "EUR"
        if not used_price:
            self.logger.info("calc_skip_no_price car=%s src=%s", car.id, getattr(car.source, "key", None))
            return None
        reg_year = car.registration_year or car.year
        reg_month = car.registration_month or 1
        if reg_year is None:
            self.logger.info("calc_skip_no_reg_year car=%s src=%s", car.id, getattr(car.source, "key", None))
            return None
        # кеш
        if (
            car.total_price_rub_cached is not None
            and car.calc_breakdown_json is not None
            and car.calc_updated_at is not None
            and car.updated_at is not None
            and car.calc_updated_at >= car.updated_at
        ):
            return {
                "total_rub": float(car.total_price_rub_cached),
                "breakdown": car.calc_breakdown_json or [],
                "vat_reclaim": vat_reclaim,
                "used_price": used_price,
                "used_currency": used_currency,
            }
        cfg_svc = CalculatorConfigService(self.db)
        # try multiple known paths
        base_paths = [
            Path("/app/Калькулятор Авто под заказ.xlsx"),
            Path("/mnt/data/Калькулятор Авто под заказ.xlsx"),
            Path(__file__).resolve().parent.parent / "resources" / "Калькулятор Авто под заказ.xlsx",
        ]
        cfg = None
        for p in base_paths:
            cfg = cfg_svc.ensure_default_from_path(p)
            if cfg:
                break
        if not cfg:
            return None
        fx = self.get_fx_rates() or {}
        eur_rate = fx.get("EUR") or cfg.payload.get("meta", {}).get("eur_rate_default") or 95.0
        usd_rate = fx.get("USD") or cfg.payload.get("meta", {}).get("usd_rate_default") or 85.0
        cur = str(used_currency or "EUR").strip().upper()
        price_net_eur = None
        if cur == "EUR":
            price_net_eur = used_price
        elif cur in ("RUB", "₽"):
            if eur_rate:
                price_net_eur = float(used_price) / float(eur_rate)
        elif cur == "USD":
            if eur_rate and usd_rate:
                price_net_eur = float(used_price) * (float(usd_rate) / float(eur_rate))
        if price_net_eur is None:
            return None
        engine_type = (car.engine_type or "").lower()
        is_electric = "electric" in engine_type or "ev" in engine_type
        if is_electric and not (car.power_hp or car.power_kw):
            self.logger.info("calc_skip_no_power car=%s src=%s", car.id, getattr(car.source, "key", None))
            return None
        if not is_electric and not car.engine_cc:
            self.logger.info("calc_skip_no_cc car=%s src=%s", car.id, getattr(car.source, "key", None))
            return None
        scenario = None
        if is_electric:
            scenario = "electric"
        elif not (car.registration_year and car.registration_month):
            scenario = "3_5"
        req = EstimateRequest(
            scenario=scenario,
            price_net_eur=price_net_eur,
            eur_rate=eur_rate,
            engine_cc=car.engine_cc,
            power_hp=float(car.power_hp) if car.power_hp is not None else None,
            power_kw=float(car.power_kw) if car.power_kw is not None else None,
            is_electric=is_electric,
            reg_year=reg_year,
            reg_month=reg_month,
        )
        try:
            result = calculate(cfg.payload, req)
        except Exception:
            self.logger.exception("calc_failed car=%s src=%s", car.id, getattr(car.source, "key", None))
            return None
        display = []
        label_map = cfg.payload.get("label_map", {})
        for item in result.get("breakdown", []):
            title = item.get("title") or ""
            if "итого" in title.lower():
                continue
            cur = (item.get("currency") or "RUB").upper()
            amt = float(item.get("amount") or 0)
            rub = amt
            if cur == "EUR" and eur_rate:
                rub = amt * eur_rate
            display.append({
                "title": label_map.get(title, label_for(title)),
                "amount_rub": rub,
            })
        total_rub = float(result.get("total_rub") or 0)
        car.total_price_rub_cached = total_rub
        car.calc_breakdown_json = display
        car.calc_updated_at = datetime.utcnow()
        self.db.commit()
        return {"total_rub": total_rub, "breakdown": display, "vat_reclaim": vat_reclaim, "used_price": used_price, "used_currency": used_currency}

    def get_car(self, car_id: int) -> Optional[Car]:
        stmt = select(Car).where(Car.id == car_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def brands(self, country: Optional[str] = None) -> List[str]:
        conditions = [Car.is_available.is_(True)]
        if country:
            conditions.append(Car.country == country)
        stmt = select(func.distinct(Car.brand)).where(
            and_(*conditions)).order_by(Car.brand.asc())
        raw = [row[0] for row in self.db.execute(stmt).all() if row[0]]
        merged: Dict[str, None] = {}
        for val in raw:
            norm = normalize_brand(val)
            if not norm:
                continue
            merged[norm] = None
        return sorted(merged.keys(), key=lambda v: v.lower())

    def brand_stats(self) -> List[Dict[str, Any]]:
        stmt = (
            select(Car.brand, func.count().label("count"))
            .where(Car.is_available.is_(True), Car.brand.is_not(None))
            .group_by(Car.brand)
            .order_by(func.count().desc(), Car.brand.asc())
        )
        rows = self.db.execute(stmt).all()
        merged: Dict[str, int] = {}
        for brand, count in rows:
            if not brand:
                continue
            norm = normalize_brand(brand)
            if not norm:
                continue
            merged[norm] = merged.get(norm, 0) + int(count)
        out = [{"brand": b, "count": c} for b, c in merged.items()]
        return sorted(out, key=lambda x: (-x["count"], x["brand"].lower()))

    def body_type_stats(self) -> List[Dict[str, Any]]:
        stmt = (
            select(Car.body_type, func.count().label("count"))
            .where(Car.is_available.is_(True), Car.body_type.is_not(None))
            .group_by(Car.body_type)
            .order_by(func.count().desc(), Car.body_type.asc())
        )
        rows = self.db.execute(stmt).all()
        return [{"body_type": r[0], "count": int(r[1])} for r in rows if r[0]]

    def transmissions(self) -> List[str]:
        stmt = (
            select(func.distinct(Car.transmission))
            .where(Car.is_available.is_(True), Car.transmission.is_not(None))
            .order_by(Car.transmission.asc())
        )
        return [row[0] for row in self.db.execute(stmt).all() if row[0]]

    def transmission_options(self) -> List[Dict[str, Any]]:
        stmt = (
            select(Car.transmission, func.count().label("count"))
            .where(Car.is_available.is_(True), Car.transmission.is_not(None))
            .group_by(Car.transmission)
            .order_by(func.count().desc(), Car.transmission.asc())
        )
        rows = self.db.execute(stmt).all()
        out: List[Dict[str, Any]] = []
        for val, cnt in rows:
            if not val:
                continue
            label = ru_transmission(val) or val
            out.append({"value": val, "label": label, "count": int(cnt)})
        return out

    def engine_types(self) -> List[Dict[str, Any]]:
        stmt = (
            select(Car.engine_type, func.count().label("count"))
            .where(Car.is_available.is_(True), Car.engine_type.is_not(None))
            .group_by(Car.engine_type)
            .order_by(func.count().desc(), Car.engine_type.asc())
        )
        rows = self.db.execute(stmt).all()
        agg: Dict[str, Dict[str, Any]] = {}
        for val, cnt in rows:
            if not val:
                continue
            norm = normalize_fuel(val) or val.strip().lower()
            label = ru_fuel(val) or ru_fuel(norm) or val
            if norm not in agg:
                agg[norm] = {"value": norm, "label": label, "count": 0}
            agg[norm]["count"] += int(cnt)
        return sorted(agg.values(), key=lambda x: x["count"], reverse=True)

    def payload_values(
        self,
        key: str,
        limit: int = 120,
        source_ids: Optional[List[int]] = None,
    ) -> List[str]:
        if not key:
            return []
        stmt = (
            select(Car.source_payload)
            .where(Car.is_available.is_(True), Car.source_payload.is_not(None))
        )
        if source_ids is None:
            stmt = stmt.join(Source, Car.source_id == Source.id).where(Source.key == "mobile_de")
        else:
            if not source_ids:
                return []
            stmt = stmt.where(Car.source_id.in_(source_ids))
        stmt = stmt.execution_options(stream_results=True)
        seen: set[str] = set()
        results: List[str] = []
        scanned = 0
        max_scan = 50000
        for payload in self.db.execute(stmt).scalars().yield_per(1000):
            scanned += 1
            if not payload or key not in payload:
                if scanned >= max_scan:
                    break
                continue
            val = payload.get(key)
            items = val if isinstance(val, list) else [val]
            for item in items:
                if item is None:
                    continue
                text = str(item).strip()
                if not text or text in seen:
                    continue
                seen.add(text)
                results.append(text)
                if len(results) >= limit:
                    break
            if len(results) >= limit or scanned >= max_scan:
                break
        return sorted(results)

    def payload_values_bulk(
        self,
        keys: List[str],
        limit: int = 120,
        source_ids: Optional[List[int]] = None,
        max_scan: int = 50000,
    ) -> Dict[str, List[str]]:
        if not keys:
            return {}
        stmt = (
            select(Car.source_payload)
            .where(Car.is_available.is_(True), Car.source_payload.is_not(None))
        )
        if source_ids is None:
            stmt = stmt.join(Source, Car.source_id == Source.id).where(Source.key == "mobile_de")
        else:
            if not source_ids:
                return {k: [] for k in keys}
            stmt = stmt.where(Car.source_id.in_(source_ids))
        stmt = stmt.execution_options(stream_results=True)
        buckets: Dict[str, set[str]] = {k: set() for k in keys}
        scanned = 0
        for payload in self.db.execute(stmt).scalars().yield_per(1000):
            scanned += 1
            if not payload:
                if scanned >= max_scan:
                    break
                continue
            for key in keys:
                if key not in payload:
                    continue
                items = payload.get(key)
                values = items if isinstance(items, list) else [items]
                bucket = buckets[key]
                if len(bucket) >= limit:
                    continue
                for item in values:
                    if item is None:
                        continue
                    text = str(item).strip()
                    if not text:
                        continue
                    bucket.add(text)
                    if len(bucket) >= limit:
                        break
            if scanned >= max_scan:
                break
            if all(len(buckets[k]) >= limit for k in keys):
                break
        return {k: sorted(list(v)) for k, v in buckets.items()}

    def source_ids_for_region(self, region: str) -> List[int]:
        if not region:
            return []
        key = region.strip().upper()
        if key == "EU":
            return self._source_ids_for_europe()
        if key == "KR":
            return self._source_ids_for_hints(self.KOREA_SOURCE_HINTS)
        return []

    def has_air_suspension(self) -> bool:
        payload_text = func.lower(cast(Car.source_payload, String))
        stmt = (
            select(Car.id)
            .where(
                Car.is_available.is_(True),
                Car.source_payload.is_not(None),
                or_(
                    payload_text.like("%air suspension%"),
                    payload_text.like("%air_suspension%"),
                    payload_text.like("%pneum%"),
                    payload_text.like("%пневмо%"),
                ),
            )
            .limit(1)
        )
        return self.db.execute(stmt).first() is not None

    def models_for_brand(self, brand: str) -> List[Dict[str, Any]]:
        if not brand:
            return []
        cache = getattr(self, "_models_cache", {})
        key = brand.lower()
        entry = cache.get(key) if cache else None
        if entry and (datetime.utcnow().timestamp() - entry["ts"] < 300):
            return entry["data"]
        norm_brand = normalize_brand(brand).strip()
        if not norm_brand:
            return []
        stmt = text(
            """
            SELECT model, SUM(total) AS count
            FROM car_counts
            WHERE brand = :brand AND model IS NOT NULL AND model <> ''
            GROUP BY model
            ORDER BY count DESC, model ASC
            LIMIT 200
            """
        )
        rows = self.db.execute(stmt, {"brand": norm_brand}).all()
        models = [{"model": r[0], "count": int(r[1])} for r in rows if r[0]]

        def sort_key(item: Dict[str, Any]):
            val = str(item.get("model") or "").strip()
            m = re.match(r"^\\s*(\\d+)", val)
            if m:
                return (0, int(m.group(1)), val.lower())
            return (1, val.lower())

        data = sorted(models, key=sort_key)
        setattr(self, "_models_cache", {**cache, key: {"ts": datetime.utcnow().timestamp(), "data": data}})
        return data

    def drive_types(self) -> List[Dict[str, Any]]:
        stmt = (
            select(Car.drive_type, func.count().label("count"))
            .where(Car.is_available.is_(True), Car.drive_type.is_not(None))
            .group_by(Car.drive_type)
            .order_by(func.count().desc(), Car.drive_type.asc())
        )
        rows = self.db.execute(stmt).all()
        out: List[Dict[str, Any]] = []
        mapping = {"awd": "Полный", "4wd": "Полный", "fwd": "Передний", "rwd": "Задний"}
        for val, cnt in rows:
            if not val:
                continue
            key = val.strip().lower()
            label = mapping.get(key, val)
            out.append({"value": val, "label": label, "count": int(cnt)})
        return out

    def recommended_auto(
        self,
        *,
        max_age_years: int | None = 5,
        price_min: int | None = 1_000_000,
        price_max: int | None = 4_000_000,
        mileage_max: int | None = 80_000,
        limit: int = 12,
    ) -> List[Car]:
        """
        Подбор рекомендуемых без ручных списков: возраст, цена, пробег.
        Возраст считаем по registration_year/month, иначе по year.
        """
        conditions = [Car.is_available.is_(True)]
        now_year = func.extract("year", func.now())
        now_month = func.extract("month", func.now())

        if max_age_years is not None:
            # возраст в месяцах
            max_months = max_age_years * 12
            reg_year = func.coalesce(Car.registration_year, Car.year)
            reg_month = func.coalesce(Car.registration_month, 1)
            age_months = (now_year - reg_year) * 12 + (now_month - reg_month)
            conditions.append(age_months <= max_months)

        if price_min is not None:
            conditions.append(Car.price_rub_cached >= price_min)
        if price_max is not None:
            conditions.append(Car.price_rub_cached <= price_max)
        if mileage_max is not None:
            conditions.append(Car.mileage <= mileage_max)

        stmt = (
            select(Car)
            .where(and_(*conditions))
            .order_by(
                Car.price_rub_cached.asc().nullslast(),
                Car.mileage.asc().nullslast(),
                Car.year.desc().nullslast(),
                Car.created_at.desc(),
            )
            .limit(limit)
        )
        return list(self.db.execute(stmt).scalars().all())

    def top_models_by_brand(self, max_brands: int = 5, top_n: int = 6) -> Dict[str, List[Dict[str, Any]]]:
        brands = [b["brand"] for b in self.brand_stats()[:max_brands]]
        result: Dict[str, List[Dict[str, Any]]] = {}
        for brand in brands:
            result[brand] = self.models_for_brand(brand)[:top_n]
        return result

    def colors(self) -> List[Dict[str, Any]]:
        stmt = (
            select(Car.color, func.count().label("count"))
            .where(Car.is_available.is_(True), Car.color.is_not(None))
            .group_by(Car.color)
        )
        rows = self.db.execute(stmt).all()
        basic: Dict[str, Dict[str, Any]] = {}
        other: Dict[str, Dict[str, Any]] = {}
        for color, cnt in rows:
            if not color:
                continue
            raw = str(color).strip()
            if not raw:
                continue
            norm = normalize_color(raw) or raw.lower()
            if is_color_base(norm):
                entry = basic.get(norm)
                if not entry:
                    label = ru_color(norm) or display_color(norm) or raw
                    entry = {
                        "value": norm,
                        "label": label,
                        "count": 0,
                        "hex": color_hex(norm),
                    }
                    basic[norm] = entry
                entry["count"] += int(cnt)
            if raw.lower() != norm:
                key = raw.lower()
                entry = other.get(key)
                if not entry:
                    label = ru_color(raw) or display_color(raw) or raw
                    entry = {
                        "value": raw,
                        "label": label,
                        "count": 0,
                        "hex": color_hex(norm),
                    }
                    other[key] = entry
                entry["count"] += int(cnt)
        ordered = list(basic.values()) + sorted(other.values(), key=lambda x: x["count"], reverse=True)
        return ordered

    def highlighted_cars(self, limit: int = 8) -> List[Car]:
        # для обратной совместимости используем рекомендованные
        return self.featured_for("recommended", limit=limit, fallback_limit=limit)

    def featured_for(self, placement: str, limit: int = 8, fallback_limit: int | None = None) -> List[Car]:
        stmt = (
            select(Car)
            .join(FeaturedCar, FeaturedCar.car_id == Car.id)
            .where(
                FeaturedCar.placement == placement,
                FeaturedCar.is_active.is_(True),
                Car.is_available.is_(True),
            )
            .order_by(FeaturedCar.position.asc(), Car.created_at.desc(), Car.id.desc())
            .limit(limit)
        )
        items = list(self.db.execute(stmt).scalars().all())
        if items or fallback_limit is None:
            return items
        # Fallback to fresh cars when featured is empty
        fallback_stmt = (
            select(Car)
            .where(
                Car.is_available.is_(True),
                Car.thumbnail_url.is_not(None),
                Car.thumbnail_url != "",
            )
            .order_by(Car.created_at.desc(), Car.id.desc())
            .limit(fallback_limit)
        )
        return list(self.db.execute(fallback_stmt).scalars().all())

    def recent_with_thumbnails(self, limit: int = 50) -> List[Car]:
        stmt = (
            select(Car)
            .where(
                Car.is_available.is_(True),
                Car.thumbnail_url.is_not(None),
                Car.thumbnail_url != "",
            )
            .order_by(Car.created_at.desc(), Car.id.desc())
            .limit(limit)
        )
        return list(self.db.execute(stmt).scalars().all())

    def total_cars(self, source_keys: Optional[List[str]] = None) -> int:
        conditions = [Car.is_available.is_(True)]
        if source_keys:
            src_ids = self.db.execute(select(Source.id).where(
                Source.key.in_(source_keys))).scalars().all()
            if src_ids:
                conditions.append(Car.source_id.in_(src_ids))
        stmt = select(func.count()).select_from(Car).where(and_(*conditions))
        return self.db.execute(stmt).scalar_one()

    def price_info(self, car: Car) -> Dict[str, Any]:
        payload = car.source_payload or {}
        price_gross = payload.get("price_eur")
        price_net = payload.get("price_eur_nt")
        vat_pct = payload.get("vat")
        vat_reclaimable = bool(price_net and vat_pct and float(vat_pct) > 0)
        return {
            "gross_eur": float(price_gross) if price_gross is not None else None,
            "net_eur": float(price_net) if price_net is not None else None,
            "vat_percent": float(vat_pct) if vat_pct is not None else None,
            "vat_reclaimable": vat_reclaimable,
        }

    def upsert_cars(self, source: Source, parsed: List[Dict[str, Any]]) -> Tuple[int, int]:
        inserted = 0
        updated = 0
        for item in parsed:
            existing = self.db.execute(
                select(Car).where(
                    (Car.source_id == source.id) & (
                        Car.external_id == item["external_id"])
                )
            ).scalar_one_or_none()
            if existing:
                for key, value in item.items():
                    if hasattr(existing, key) and key not in ("id", "created_at"):
                        setattr(existing, key, value)
                existing.is_available = True
                updated += 1
            else:
                car = Car(source_id=source.id, **item)
                self.db.add(car)
                inserted += 1
        self.db.commit()
        return inserted, updated

    def mark_unavailable_except(self, source: Source, external_ids: List[str]) -> int:
        # Mark cars from this source that are not in the latest external_ids as unavailable
        stmt = select(Car).where((Car.source_id == source.id)
                                 & (Car.is_available.is_(True)))
        cars = self.db.execute(stmt).scalars().all()
        changed = 0
        external_set = set(external_ids)
        for car in cars:
            if car.external_id not in external_set:
                car.is_available = False
                changed += 1
        if changed:
            self.db.commit()
        return changed

    def search_featured_candidates(self, query: str, limit: int = 20) -> List[Car]:
        if not query:
            return []
        q = query.strip()
        stmt = select(Car).where(Car.is_available.is_(True))
        conds = []
        try:
            cid = int(q)
            conds.append(Car.id == cid)
        except ValueError:
            pass
        like = f"%{q}%"
        conds.append(func.lower(Car.brand).like(func.lower(like)))
        conds.append(func.lower(Car.model).like(func.lower(like)))
        conds.append(func.lower(func.concat(Car.brand, " ", Car.model)).like(func.lower(like)))
        stmt = stmt.where(or_(*conds)).order_by(Car.created_at.desc(), Car.id.desc()).limit(limit)
        return list(self.db.execute(stmt).scalars().all())
