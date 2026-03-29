from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import Car, CarSpecReference, Source
from ..services.cars_service import normalize_brand
from ..utils.spec_inference import (
    build_reference_signature,
    choose_reference_consensus,
    has_complete_raw_specs,
    normalize_engine_type,
    normalize_spec_text,
    normalized_power_hp,
    normalized_power_kw,
)


class CarSpecInferenceService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def refresh_reference(
        self,
        *,
        region: Optional[str] = None,
        country: Optional[str] = None,
        source_key: Optional[str] = None,
        since_minutes: Optional[int] = None,
        batch: int = 2000,
        chunk: int = 50000,
        full_rebuild: bool = False,
    ) -> Dict[str, int]:
        if full_rebuild:
            self.db.query(CarSpecReference).delete(synchronize_session=False)
            self.db.commit()

        base = self.db.query(Car.id)
        region_norm = (region or "").strip().upper()
        if full_rebuild:
            base = base.filter(Car.is_available.is_(True))
        if region_norm == "EU":
            base = base.filter(~Car.country.like("KR%"))
        elif region_norm == "KR":
            base = base.filter(Car.country.like("KR%"))
        if country:
            base = base.filter(Car.country == country.strip().upper())
        if source_key:
            keys = [key.strip() for key in str(source_key).split(",") if key.strip()]
            if keys:
                source_ids = self.db.execute(select(Source.id).where(Source.key.in_(keys))).scalars().all()
                if not source_ids:
                    return {"total": 0, "processed": 0, "upserted": 0, "deleted": 0}
                base = base.filter(Car.source_id.in_(source_ids))
        if since_minutes and not full_rebuild:
            since_ts = datetime.utcnow() - timedelta(minutes=since_minutes)
            base = base.filter(Car.updated_at >= since_ts)

        total = base.count()
        if total == 0:
            return {"total": 0, "processed": 0, "upserted": 0, "deleted": 0}

        min_id = base.with_entities(Car.id).order_by(Car.id.asc()).limit(1).scalar()
        max_id = base.with_entities(Car.id).order_by(Car.id.desc()).limit(1).scalar()
        if min_id is None or max_id is None:
            return {"total": 0, "processed": 0, "upserted": 0, "deleted": 0}

        processed = 0
        upserted = 0
        deleted = 0
        start = int(min_id)
        started_at = time.time()
        window_no = 0
        while start <= int(max_id):
            end = min(start + chunk - 1, int(max_id))
            window_no += 1
            ids = [row[0] for row in base.filter(Car.id.between(start, end)).order_by(Car.id.asc()).all()]
            if ids:
                refs_by_car_id = {
                    ref.source_car_id: ref
                    for ref in self.db.query(CarSpecReference).filter(CarSpecReference.source_car_id.in_(ids)).all()
                }
                for i in range(0, len(ids), batch):
                    batch_ids = ids[i : i + batch]
                    cars = self.db.query(Car).filter(Car.id.in_(batch_ids)).all()
                    for car in cars:
                        payload = self._reference_payload_for_car(car)
                        existing = refs_by_car_id.get(car.id)
                        if payload:
                            if existing is None:
                                self.db.add(CarSpecReference(source_car_id=car.id, **payload))
                            else:
                                for key, value in payload.items():
                                    setattr(existing, key, value)
                            upserted += 1
                        elif existing is not None:
                            self.db.delete(existing)
                            deleted += 1
                        processed += 1
                    self.db.commit()
                elapsed = max(time.time() - started_at, 1.0)
                rate = processed / elapsed if processed else 0.0
                print(
                    f"[refresh_spec_reference] window={window_no} ids={start}-{end} "
                    f"processed={processed}/{total} upserted={upserted} deleted={deleted} rate={rate:.2f}/s",
                    flush=True,
                )
            start = end + 1
        return {"total": total, "processed": processed, "upserted": upserted, "deleted": deleted}

    def infer_missing_specs(
        self,
        *,
        region: Optional[str] = None,
        country: Optional[str] = None,
        source_key: Optional[str] = None,
        since_minutes: Optional[int] = None,
        batch: int = 2000,
        chunk: int = 50000,
        year_window: int = 2,
    ) -> Dict[str, int]:
        base = self.db.query(Car.id).filter(Car.is_available.is_(True))
        region_norm = (region or "").strip().upper()
        if region_norm == "EU":
            base = base.filter(~Car.country.like("KR%"))
        elif region_norm == "KR":
            base = base.filter(Car.country.like("KR%"))
        if country:
            base = base.filter(Car.country == country.strip().upper())
        if source_key:
            keys = [key.strip() for key in str(source_key).split(",") if key.strip()]
            if keys:
                source_ids = self.db.execute(select(Source.id).where(Source.key.in_(keys))).scalars().all()
                if not source_ids:
                    return {"total": 0, "processed": 0, "matched": 0, "cleared": 0, "unmatched": 0}
                base = base.filter(Car.source_id.in_(source_ids))
        if since_minutes:
            since_ts = datetime.utcnow() - timedelta(minutes=since_minutes)
            base = base.filter(
                (Car.updated_at >= since_ts)
                | (Car.spec_inferred_at.is_(None))
            )
        base = base.filter(
            (Car.engine_cc.is_(None))
            | ((Car.power_hp.is_(None)) & (Car.power_kw.is_(None)))
            | (Car.inferred_engine_cc.is_not(None))
            | (Car.inferred_power_hp.is_not(None))
            | (Car.inferred_power_kw.is_not(None))
        )

        total = base.count()
        if total == 0:
            return {"total": 0, "processed": 0, "matched": 0, "cleared": 0, "unmatched": 0}

        min_id = base.with_entities(Car.id).order_by(Car.id.asc()).limit(1).scalar()
        max_id = base.with_entities(Car.id).order_by(Car.id.desc()).limit(1).scalar()
        if min_id is None or max_id is None:
            return {"total": 0, "processed": 0, "matched": 0, "cleared": 0, "unmatched": 0}

        processed = 0
        matched = 0
        cleared = 0
        unmatched = 0
        start = int(min_id)
        started_at = time.time()
        window_no = 0
        while start <= int(max_id):
            end = min(start + chunk - 1, int(max_id))
            window_no += 1
            ids = [row[0] for row in base.filter(Car.id.between(start, end)).order_by(Car.id.asc()).all()]
            if ids:
                for i in range(0, len(ids), batch):
                    batch_ids = ids[i : i + batch]
                    cars = self.db.query(Car).filter(Car.id.in_(batch_ids)).all()
                    for car in cars:
                        processed += 1
                        if has_complete_raw_specs(car.engine_type, car.engine_cc, car.power_hp, car.power_kw):
                            if self._clear_inferred_specs(car):
                                cleared += 1
                            continue
                        inference = self.infer_specs_for_car(car, year_window=year_window)
                        if inference:
                            self._apply_inferred_specs(car, inference)
                            matched += 1
                        else:
                            if self._clear_inferred_specs(car):
                                cleared += 1
                            unmatched += 1
                    self.db.commit()
                elapsed = max(time.time() - started_at, 1.0)
                rate = processed / elapsed if processed else 0.0
                print(
                    f"[infer_missing_specs] window={window_no} ids={start}-{end} "
                    f"processed={processed}/{total} matched={matched} cleared={cleared} unmatched={unmatched} "
                    f"rate={rate:.2f}/s",
                    flush=True,
                )
            start = end + 1
        return {
            "total": total,
            "processed": processed,
            "matched": matched,
            "cleared": cleared,
            "unmatched": unmatched,
        }

    def infer_specs_for_car(self, car: Car, *, year_window: int = 2) -> Optional[Dict[str, Any]]:
        sig = build_reference_signature(
            brand=normalize_brand(car.brand),
            model=car.model,
            variant=car.variant,
            engine_type=car.engine_type,
            body_type=car.body_type,
            year=car.year,
            source_payload=car.source_payload or {},
        )
        if not sig["brand_norm"] or not sig["model_norm"] or not sig["year"]:
            return None

        query = self.db.query(CarSpecReference).filter(
            CarSpecReference.brand_norm == sig["brand_norm"],
            CarSpecReference.model_norm == sig["model_norm"],
            CarSpecReference.source_car_id != car.id,
        )
        if sig["engine_type_norm"]:
            query = query.filter(CarSpecReference.engine_type_norm == sig["engine_type_norm"])
        if sig["body_type_norm"]:
            query = query.filter(CarSpecReference.body_type_norm == sig["body_type_norm"])
        if sig["year"]:
            query = query.filter(
                CarSpecReference.year.between(sig["year"] - year_window, sig["year"] + year_window)
            )
        refs = query.all()
        if not refs:
            return None

        rows = [self._reference_row_to_dict(ref) for ref in refs]
        if sig["variant_key"]:
            exact_variant_rows = [row for row in rows if row.get("variant_key") == sig["variant_key"]]
            consensus = choose_reference_consensus(
                exact_variant_rows,
                target_year=sig["year"],
                has_variant_key=True,
            )
            if consensus:
                return consensus
        return choose_reference_consensus(
            rows,
            target_year=sig["year"],
            has_variant_key=False,
        )

    def _reference_payload_for_car(self, car: Car) -> Optional[Dict[str, Any]]:
        if not car.is_available:
            return None
        if not has_complete_raw_specs(car.engine_type, car.engine_cc, car.power_hp, car.power_kw):
            return None
        signature = build_reference_signature(
            brand=normalize_brand(car.brand),
            model=car.model,
            variant=car.variant,
            engine_type=car.engine_type,
            body_type=car.body_type,
            year=car.year,
            source_payload=car.source_payload or {},
        )
        if not signature["brand_norm"] or not signature["model_norm"] or not signature["year"]:
            return None
        return {
            "car_hash": car.hash,
            "brand_norm": signature["brand_norm"],
            "model_norm": signature["model_norm"],
            "variant_key": signature["variant_key"],
            "engine_type_norm": signature["engine_type_norm"] or None,
            "body_type_norm": signature["body_type_norm"] or None,
            "year": signature["year"],
            "engine_cc": int(car.engine_cc) if car.engine_cc is not None else None,
            "power_hp": normalized_power_hp(car.power_hp, car.power_kw),
            "power_kw": normalized_power_kw(car.power_hp, car.power_kw),
            "updated_at": datetime.utcnow(),
        }

    def _reference_row_to_dict(self, ref: CarSpecReference) -> Dict[str, Any]:
        return {
            "source_car_id": ref.source_car_id,
            "variant_key": ref.variant_key,
            "year": ref.year,
            "engine_cc": ref.engine_cc,
            "power_hp": ref.power_hp,
            "power_kw": ref.power_kw,
        }

    def _apply_inferred_specs(self, car: Car, inference: Dict[str, Any]) -> None:
        engine_type_norm = normalize_engine_type(car.engine_type)
        applied_any = False

        if car.engine_cc is None and engine_type_norm != "electric":
            car.inferred_engine_cc = inference.get("engine_cc")
            applied_any = applied_any or car.inferred_engine_cc is not None
        else:
            car.inferred_engine_cc = None

        if car.power_hp is None and car.power_kw is None:
            car.inferred_power_hp = inference.get("power_hp")
            car.inferred_power_kw = inference.get("power_kw")
            applied_any = applied_any or car.inferred_power_hp is not None or car.inferred_power_kw is not None
        else:
            car.inferred_power_hp = None
            car.inferred_power_kw = None

        if not applied_any:
            self._clear_inferred_specs(car)
            return

        car.inferred_source_car_id = inference.get("source_car_id")
        car.inferred_confidence = inference.get("confidence")
        car.inferred_rule = inference.get("rule")
        car.spec_inferred_at = datetime.utcnow()

    def _clear_inferred_specs(self, car: Car) -> bool:
        changed = any(
            value is not None
            for value in (
                car.inferred_engine_cc,
                car.inferred_power_hp,
                car.inferred_power_kw,
                car.inferred_source_car_id,
                car.inferred_confidence,
                car.inferred_rule,
                car.spec_inferred_at,
            )
        )
        if not changed:
            return False
        car.inferred_engine_cc = None
        car.inferred_power_hp = None
        car.inferred_power_kw = None
        car.inferred_source_car_id = None
        car.inferred_confidence = None
        car.inferred_rule = None
        car.spec_inferred_at = None
        return True
