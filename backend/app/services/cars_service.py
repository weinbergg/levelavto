from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import select, func, and_, or_, literal_column, case
import os
import requests
import time
from ..models import Car, Source, FeaturedCar
from ..utils.localization import display_region, display_body, display_color
from ..utils.taxonomy import normalize_color, ru_color


class CarsService:
    def __init__(self, db: Session) -> None:
        self.db = db

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
        body_type: Optional[str] = None,
        engine_type: Optional[str] = None,
        transmission: Optional[str] = None,
        sort: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Tuple[List[Car], int]:
        conditions = [Car.is_available.is_(True)]
        region = None
        if country:
            c = country.upper()
            if c == "EU":
                region = "EU"
            elif c == "KR":
                region = "KR"

        if region:
            region_keys = []
            if region == "EU":
                region_keys = ["mobile", "mobile_de"]
            elif region == "KR":
                region_keys = ["emavto", "encar", "m-auto", "m_auto"]
            if region_keys:
                src_ids = self.db.execute(
                    select(Source.id).where(
                        or_(*[Source.key.ilike(f"{k}%") for k in region_keys])
                    )
                ).scalars().all()
                if src_ids:
                    conditions.append(Car.source_id.in_(src_ids))
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
            rates = self.get_fx_rates()
            if rates:
                rub_expr = case(
                    (func.lower(Car.currency) == "usd",
                     Car.price * rates.get("USD", 1.0)),
                    (func.lower(Car.currency) == "rub", Car.price),
                    else_=Car.price * rates.get("EUR", 1.0),
                )
                if price_min is not None:
                    conditions.append(rub_expr >= price_min)
                if price_max is not None:
                    conditions.append(rub_expr <= price_max)
            else:
                if price_min is not None:
                    conditions.append(Car.price >= price_min)
                if price_max is not None:
                    conditions.append(Car.price <= price_max)
        if year_min is not None:
            conditions.append(Car.year >= year_min)
        if year_max is not None:
            conditions.append(Car.year <= year_max)
        if mileage_min is not None:
            conditions.append(Car.mileage >= mileage_min)
        if mileage_max is not None:
            conditions.append(Car.mileage <= mileage_max)
        if body_type:
            conditions.append(func.lower(Car.body_type) == body_type.lower())
        if engine_type:
            conditions.append(func.lower(Car.engine_type)
                              == engine_type.lower())
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
            label = ru_color(color) or display_color(color) or color
            if norm not in agg:
                agg[norm] = {"value": norm, "label": label, "count": 0}
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

    def colors(self) -> List[str]:
        stmt = (
            select(func.distinct(Car.color))
            .where(Car.is_available.is_(True), Car.color.is_not(None))
            .order_by(Car.color.asc())
        )
        return [row[0] for row in self.db.execute(stmt).all() if row[0]]

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
