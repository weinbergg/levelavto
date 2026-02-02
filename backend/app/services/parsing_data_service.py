from __future__ import annotations

from typing import Iterable, Tuple, List, Dict, Any
from datetime import datetime
import hashlib
from sqlalchemy.orm import Session
from sqlalchemy import select, update, or_
from ..models import Car, Source, CarImage, ProgressKV
from ..services.cars_service import CarsService
from ..utils.pricing import to_rub
from ..utils.color_groups import normalize_color_group


def compute_car_hash(payload: Dict[str, Any]) -> str:
    # Keep only stable fields for change detection
    parts = [
        payload.get("brand") or "",
        payload.get("model") or "",
        payload.get("variant") or "",
        str(payload.get("year") or ""),
        str(payload.get("mileage") or ""),
        str(payload.get("price") or ""),
        payload.get("currency") or "",
        payload.get("vin") or "",
        payload.get("source_url") or "",
        str(payload.get("engine_cc") or ""),
        str(payload.get("power_hp") or ""),
        str(payload.get("power_kw") or ""),
        str(payload.get("registration_year") or ""),
        str(payload.get("registration_month") or ""),
    ]
    text = "|".join(parts)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:64]


class ParsingDataService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def ensure_source(self, *, key: str, name: str, country: str, base_url: str) -> Source:
        existing = self.db.execute(select(Source).where(
            Source.key == key)).scalar_one_or_none()
        if existing:
            return existing
        source = Source(key=key, name=name, base_url=base_url, country=country)
        self.db.add(source)
        self.db.commit()
        self.db.refresh(source)
        return source

    def upsert_parsed_items(self, source: Source, parsed_items: List[Dict[str, Any]]) -> Tuple[int, int, int]:
        """
        Returns (inserted, updated, seen).
        """
        now = datetime.utcnow()
        inserted = 0
        updated = 0
        rates = CarsService(self.db).get_fx_rates() or {}
        # Normalize and de-duplicate by external_id
        unique_items: Dict[str, Dict[str, Any]] = {}
        for p in parsed_items:
            payload = dict(p)
            payload.pop("source_key", None)
            payload.pop("listing_sort_ts", None)
            payload.pop("reg_sort_key", None)
            payload.setdefault("country", source.country)
            payload.setdefault("thumbnail_url", None)
            payload.setdefault("is_available", True)
            if "color" in payload:
                payload["color_group"] = normalize_color_group(payload.get("color"))
            # normalize KR market type
            if payload.get("country") == "KR":
                if not payload.get("kr_market_type"):
                    payload["kr_market_type"] = "domestic"
                listing_val = payload.get("listing_date")
                ts = None
                if isinstance(listing_val, str):
                    try:
                        ts = datetime.fromisoformat(listing_val)
                    except Exception:
                        ts = None
                elif isinstance(listing_val, datetime):
                    ts = listing_val
                if ts is None:
                    ts = now
                payload["listing_date"] = ts
            rub = to_rub(payload.get("price"), payload.get("currency"), rates)
            if rub is not None:
                payload["price_rub_cached"] = round(rub, 2)
            payload["hash"] = compute_car_hash(payload)
            unique_items[payload["external_id"]] = payload

        if not unique_items:
            return 0, 0, 0

        ext_ids = list(unique_items.keys())
        existing_rows = self.db.execute(
            select(Car).where((Car.source_id == source.id)
                              & (Car.external_id.in_(ext_ids)))
        ).scalars().all()
        existing_by_eid = {c.external_id: c for c in existing_rows}

        for eid, payload in unique_items.items():
            # Extract images out of payload; they are persisted separately
            images: List[str] = []
            raw_images = payload.pop("images", None)
            if isinstance(raw_images, list):
                images = [str(u) for u in raw_images if isinstance(
                    u, str) and u.strip()]
            # if new images exist, set thumbnail; otherwise keep existing images/thumbnail
            new_thumb = images[0] if images else None
            if new_thumb:
                payload["thumbnail_url"] = payload.get(
                    "thumbnail_url") or new_thumb
            existing = existing_by_eid.get(eid)
            if existing:
                if existing.hash != payload["hash"]:
                    for k, v in payload.items():
                        if hasattr(existing, k) and k not in ("id", "created_at", "first_seen_at"):
                            setattr(existing, k, v)
                    existing.last_seen_at = now
                    existing.is_available = True
                    updated += 1
                else:
                    if existing.source_payload is None and payload.get("source_payload") is not None:
                        existing.source_payload = payload["source_payload"]
                        updated += 1
                    if existing.price_rub_cached is None and payload.get("price_rub_cached") is not None:
                        existing.price_rub_cached = payload["price_rub_cached"]
                        updated += 1
                    if existing.listing_date is None and payload.get("listing_date") is not None:
                        existing.listing_date = payload["listing_date"]
                        updated += 1
                    existing.last_seen_at = now
                    existing.is_available = True
                car_row = existing
            else:
                car = Car(
                    source_id=source.id,
                    first_seen_at=now,
                    last_seen_at=now,
                    **payload,
                )
                self.db.add(car)
                inserted += 1
                car_row = car
            # Sync images for this car: if provided, use them; else fallback to thumbnail_url
            # Flush to get ids for newly created rows
            self.db.flush()
            if car_row and getattr(car_row, "id", None):
                old_imgs = self.db.execute(select(CarImage).where(
                    CarImage.car_id == car_row.id).order_by(CarImage.position.asc())).scalars().all()
                # decide images list
                candidate_images: List[str] = images[:]
                if not candidate_images and old_imgs:
                    candidate_images = [img.url for img in old_imgs]
                if not candidate_images and car_row.thumbnail_url:
                    candidate_images = [car_row.thumbnail_url]
                if candidate_images:
                    # replace old images only when we have something to set
                    for oi in old_imgs:
                        self.db.delete(oi)
                    for pos, url in enumerate(candidate_images):
                        self.db.add(CarImage(car_id=car_row.id, url=url,
                                    is_primary=(pos == 0), position=pos))

        self.db.commit()
        return inserted, updated, len(ext_ids)

    def deactivate_missing(self, source: Source, seen_external_ids: List[str]) -> int:
        # Mark cars from this source not seen in this run as unavailable
        external_set = set(seen_external_ids)
        cars = self.db.execute(select(Car).where(
            Car.source_id == source.id)).scalars().all()
        changed = 0
        now = datetime.utcnow()
        for car in cars:
            if car.external_id not in external_set and car.is_available:
                car.is_available = False
                car.last_seen_at = now
                changed += 1
        if changed:
            self.db.commit()
        return changed

    def deactivate_missing_by_last_seen(self, source: Source, run_started_at: datetime) -> int:
        """
        Deactivate cars not seen during the current run based on last_seen_at.
        This avoids keeping a full external_id list in memory for large feeds.
        """
        now = datetime.utcnow()
        stmt = (
            update(Car)
            .where(
                Car.source_id == source.id,
                Car.is_available.is_(True),
                or_(Car.last_seen_at.is_(None), Car.last_seen_at < run_started_at),
            )
            .values(is_available=False, last_seen_at=now)
        )
        result = self.db.execute(stmt)
        self.db.commit()
        return int(result.rowcount or 0)

    # --- progress helpers ---
    def get_progress(self, key: str) -> str | None:
        row = self.db.execute(select(ProgressKV).where(
            ProgressKV.key == key)).scalar_one_or_none()
        return row.value if row else None

    def set_progress(self, key: str, value: str) -> None:
        row = self.db.execute(select(ProgressKV).where(
            ProgressKV.key == key)).scalar_one_or_none()
        now = datetime.utcnow()
        if row:
            row.value = value
            row.updated_at = now
        else:
            self.db.add(ProgressKV(key=key, value=value, updated_at=now))
        self.db.commit()
