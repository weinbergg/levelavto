from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import select, func, and_, or_
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
)


class CarsService:
    def __init__(self, db: Session) -> None:
        self.db = db

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
        return self.db.execute(
            select(Source.id).where(func.lower(Source.key).like(f"{self.EUROPE_SOURCE_PREFIX}%"))
        ).scalars().all()

    def available_countries(self) -> List[str]:
        rows = self.db.execute(
            select(func.distinct(Car.country))
            .where(Car.is_available.is_(True), Car.country.is_not(None))
        ).scalars().all()
        countries: List[str] = []
        seen = set()
        has_kr = False
        for c in rows:
            code = normalize_country_code(c)
            if not code:
                continue
            if code == "KR":
                has_kr = True
            if code in self.EU_COUNTRIES and code not in seen:
                countries.append(code)
                seen.add(code)
        kr_sources = self._source_ids_for_hints(self.KOREA_SOURCE_HINTS)
        if (has_kr or kr_sources) and "KR" not in seen:
            countries.append("KR")
        return countries

    _fx_cache: dict | None = None
    _fx_cache_ts: float | None = None

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
        country: Optional[str] = None,
        brand: Optional[str] = None,
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
        sort: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Tuple[List[Car], int]:
        conditions = [Car.is_available.is_(True)]
        if country:
            c = normalize_country_code(country)
            if c == "KR":
                kr_sources = self._source_ids_for_hints(self.KOREA_SOURCE_HINTS)
                kr_conds = [func.upper(Car.country).like("KR%")]
                if kr_sources:
                    kr_conds.append(Car.source_id.in_(kr_sources))
                conditions.append(or_(*kr_conds))
            elif c == "EU":
                eu_sources = self._source_ids_for_europe()
                eu_conds = [func.upper(Car.country).in_(self.EU_COUNTRIES)]
                if eu_sources:
                    eu_conds.append(Car.source_id.in_(eu_sources))
                conditions.append(or_(*eu_conds))
            elif c:
                conditions.append(func.upper(Car.country) == c)
        if brand:
            # case-insensitive contains (brand may have variants/extra symbols)
            b = brand.strip().strip(".,;")
            if b:
                conditions.append(func.lower(Car.brand).like(func.lower(f"%{b}%")))
        if q:
            like = f"%{q.strip()}%"
            conditions.append(
                or_(
                    func.lower(Car.brand).like(func.lower(like)),
                    func.lower(Car.model).like(func.lower(like)),
                    func.lower(func.concat(Car.brand, " ", Car.model)).like(func.lower(like)),
                )
            )
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
        if price_min is not None:
            conditions.append(Car.price_rub_cached >= price_min)
        if price_max is not None:
            conditions.append(Car.price_rub_cached <= price_max)
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

        where_expr = and_(*conditions) if conditions else None

        total_stmt = select(func.count()).select_from(Car).where(where_expr)
        total = self.db.execute(total_stmt).scalar_one()

        order_clause = []
        if sort == "price_asc":
            order_clause = [Car.price_rub_cached.asc().nullslast()]
        elif sort == "price_desc":
            order_clause = [Car.price_rub_cached.desc().nullslast()]
        elif sort == "year_desc":
            order_clause = [Car.year.desc().nullslast()]
        elif sort == "year_asc":
            order_clause = [Car.year.asc().nullslast()]
        elif sort == "mileage_asc":
            order_clause = [Car.mileage.asc().nullslast()]
        elif sort == "mileage_desc":
            order_clause = [Car.mileage.desc().nullslast()]
        else:
            # default: цена сначала дешевые
            order_clause = [Car.price_rub_cached.asc().nullslast()]
        # всегда поднимаем машины с фото выше
        order_clause.insert(0, Car.thumbnail_url.is_(None).asc())

        stmt = (
            select(Car)
            .where(where_expr)
            .order_by(*order_clause, Car.created_at.desc(), Car.updated_at.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        items = list(self.db.execute(stmt).scalars().all())
        return items, total

    def get_car(self, car_id: int) -> Optional[Car]:
        stmt = select(Car).where(Car.id == car_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def brands(self, country: Optional[str] = None) -> List[str]:
        conditions = [Car.is_available.is_(True)]
        if country:
            conditions.append(Car.country == country)
        stmt = select(func.distinct(Car.brand)).where(
            and_(*conditions)).order_by(Car.brand.asc())
        brands = [row[0] for row in self.db.execute(stmt).all() if row[0]]
        return brands

    def brand_stats(self) -> List[Dict[str, Any]]:
        stmt = (
            select(Car.brand, func.count().label("count"))
            .where(Car.is_available.is_(True), Car.brand.is_not(None))
            .group_by(Car.brand)
            .order_by(func.count().desc(), Car.brand.asc())
        )
        rows = self.db.execute(stmt).all()
        return [{"brand": r[0], "count": int(r[1])} for r in rows if r[0]]

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

    def models_for_brand(self, brand: str) -> List[Dict[str, Any]]:
        if not brand:
            return []
        stmt = (
            select(Car.model, func.count().label("count"))
            .where(
                Car.is_available.is_(True),
                func.lower(Car.brand) == brand.lower(),
                Car.model.is_not(None),
            )
            .group_by(Car.model)
            .order_by(func.count().desc(), Car.model.asc())
        )
        rows = self.db.execute(stmt).all()
        return [{"model": r[0], "count": int(r[1])} for r in rows if r[0]]

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
        agg: Dict[str, Dict[str, Any]] = {}
        for color, cnt in rows:
            if not color:
                continue
            norm = normalize_color(color) or color.strip().lower()
            label = ru_color(color) or ru_color(norm) or display_color(color) or color
            if norm not in agg:
                agg[norm] = {
                    "value": norm,
                    "label": label,
                    "count": 0,
                    "hex": color_hex(norm),
                }
            agg[norm]["count"] += int(cnt)
        return sorted(agg.values(), key=lambda x: x["count"], reverse=True)

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
            .where(Car.is_available.is_(True), Car.thumbnail_url.is_not(None))
            .order_by(Car.created_at.desc(), Car.id.desc())
            .limit(fallback_limit)
        )
        return list(self.db.execute(fallback_stmt).scalars().all())

    def recent_with_thumbnails(self, limit: int = 50) -> List[Car]:
        stmt = (
            select(Car)
            .where(Car.is_available.is_(True), Car.thumbnail_url.is_not(None))
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
