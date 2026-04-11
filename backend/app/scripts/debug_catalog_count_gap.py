from __future__ import annotations

import argparse
import json
from collections import Counter
from typing import Any

from sqlalchemy import and_, func, or_, select

from backend.app.db import SessionLocal
from backend.app.models import Car, Source
from backend.app.services.cars_service import CarsService, normalize_brand


def _bool_flag(payload: dict[str, Any] | None, key: str) -> bool:
    if not isinstance(payload, dict):
        return False
    return payload.get(key) is True or str(payload.get(key)).lower() == "true"


def _sample_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    keys = [
        "registration_defaulted",
        "registration_year_defaulted",
        "registration_month_defaulted",
        "registration_default_year",
        "registration_default_month",
        "first_registration",
        "erstzulassung",
    ]
    return {key: payload.get(key) for key in keys if key in payload}


def main() -> None:
    parser = argparse.ArgumentParser(description="Diagnose catalog-vs-DB count gaps for a concrete case.")
    parser.add_argument("--source", default="mobile_de")
    parser.add_argument("--region", default="EU")
    parser.add_argument("--country", default=None)
    parser.add_argument("--brand", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--mileage-max", type=int, default=None)
    parser.add_argument("--mileage-min", type=int, default=None)
    parser.add_argument("--reg-year-min", type=int, default=None)
    parser.add_argument("--reg-year-max", type=int, default=None)
    parser.add_argument("--reg-month-min", type=int, default=None)
    parser.add_argument("--reg-month-max", type=int, default=None)
    parser.add_argument("--sample", type=int, default=20)
    args = parser.parse_args()

    db = SessionLocal()
    service = CarsService(db)

    source_row = db.execute(select(Source.id, Source.key).where(Source.key == args.source)).first()
    if not source_row:
        raise SystemExit(f"source_not_found: {args.source}")
    source_id = int(source_row[0])

    brand_norm = normalize_brand(args.brand).strip()
    model_norm = str(args.model or "").strip()

    raw_conditions = [
        Car.is_available.is_(True),
        Car.source_id == source_id,
        func.lower(func.trim(func.coalesce(Car.brand, ""))) == brand_norm.lower(),
        func.lower(func.trim(func.coalesce(Car.model, ""))) == model_norm.lower(),
    ]
    if args.mileage_min is not None:
        raw_conditions.extend([Car.mileage.is_not(None), Car.mileage >= args.mileage_min])
    if args.mileage_max is not None:
        raw_conditions.extend([Car.mileage.is_not(None), Car.mileage <= args.mileage_max])
    if args.reg_year_min is not None:
        raw_conditions.extend([Car.registration_year.is_not(None), Car.registration_year >= args.reg_year_min])
    if args.reg_year_max is not None:
        raw_conditions.extend([Car.registration_year.is_not(None), Car.registration_year <= args.reg_year_max])

    raw_ids = [
        int(row[0])
        for row in db.execute(select(Car.id).where(and_(*raw_conditions))).all()
    ]

    service_conditions, _ = service._build_list_conditions(
        region=args.region,
        country=args.country,
        brand=brand_norm,
        model=model_norm,
        mileage_min=args.mileage_min,
        mileage_max=args.mileage_max,
        reg_year_min=args.reg_year_min,
        reg_year_max=args.reg_year_max,
        reg_month_min=args.reg_month_min,
        reg_month_max=args.reg_month_max,
    )
    service_ids = [
        int(row[0])
        for row in db.execute(select(Car.id).where(and_(*service_conditions))).all()
    ]

    api_count = service.count_cars(
        region=args.region,
        country=args.country,
        brand=brand_norm,
        model=model_norm,
        mileage_min=args.mileage_min,
        mileage_max=args.mileage_max,
        reg_year_min=args.reg_year_min,
        reg_year_max=args.reg_year_max,
        reg_month_min=args.reg_month_min,
        reg_month_max=args.reg_month_max,
    )

    effective_reg_year_expr = service._effective_registration_year_expr()
    effective_conditions = [
        Car.is_available.is_(True),
        Car.source_id == source_id,
        func.lower(func.trim(func.coalesce(Car.brand, ""))) == brand_norm.lower(),
        func.lower(func.trim(func.coalesce(Car.model, ""))) == model_norm.lower(),
    ]
    if args.mileage_min is not None:
        effective_conditions.extend([Car.mileage.is_not(None), Car.mileage >= args.mileage_min])
    if args.mileage_max is not None:
        effective_conditions.extend([Car.mileage.is_not(None), Car.mileage <= args.mileage_max])
    if args.reg_year_min is not None:
        effective_conditions.append(effective_reg_year_expr >= args.reg_year_min)
    if args.reg_year_max is not None:
        effective_conditions.append(effective_reg_year_expr <= args.reg_year_max)

    effective_count = int(
        db.execute(select(func.count(Car.id)).where(and_(*effective_conditions))).scalar() or 0
    )

    boundary_summary = {
        "mileage_le": int(
            db.execute(
                select(func.count(Car.id)).where(
                    and_(
                        Car.is_available.is_(True),
                        Car.source_id == source_id,
                        func.lower(func.trim(func.coalesce(Car.brand, ""))) == brand_norm.lower(),
                        func.lower(func.trim(func.coalesce(Car.model, ""))) == model_norm.lower(),
                        Car.mileage.is_not(None),
                        Car.mileage <= (args.mileage_max if args.mileage_max is not None else 10**9),
                    )
                )
            ).scalar()
            or 0
        ),
        "mileage_lt": int(
            db.execute(
                select(func.count(Car.id)).where(
                    and_(
                        Car.is_available.is_(True),
                        Car.source_id == source_id,
                        func.lower(func.trim(func.coalesce(Car.brand, ""))) == brand_norm.lower(),
                        func.lower(func.trim(func.coalesce(Car.model, ""))) == model_norm.lower(),
                        Car.mileage.is_not(None),
                        Car.mileage < (args.mileage_max if args.mileage_max is not None else 10**9),
                    )
                )
            ).scalar()
            or 0
        ),
    }
    if args.reg_year_min is not None:
        boundary_summary["reg_year_ge"] = int(
            db.execute(
                select(func.count(Car.id)).where(
                    and_(
                        Car.is_available.is_(True),
                        Car.source_id == source_id,
                        func.lower(func.trim(func.coalesce(Car.brand, ""))) == brand_norm.lower(),
                        func.lower(func.trim(func.coalesce(Car.model, ""))) == model_norm.lower(),
                        Car.registration_year.is_not(None),
                        Car.registration_year >= args.reg_year_min,
                    )
                )
            ).scalar()
            or 0
        )
        boundary_summary["reg_year_gt"] = int(
            db.execute(
                select(func.count(Car.id)).where(
                    and_(
                        Car.is_available.is_(True),
                        Car.source_id == source_id,
                        func.lower(func.trim(func.coalesce(Car.brand, ""))) == brand_norm.lower(),
                        func.lower(func.trim(func.coalesce(Car.model, ""))) == model_norm.lower(),
                        Car.registration_year.is_not(None),
                        Car.registration_year > args.reg_year_min,
                    )
                )
            ).scalar()
            or 0
        )

    raw_only_ids = list(sorted(set(raw_ids) - set(service_ids)))
    service_only_ids = list(sorted(set(service_ids) - set(raw_ids)))

    raw_only_rows = db.execute(
        select(
            Car.id,
            Car.external_id,
            Car.country,
            Car.model,
            Car.year,
            Car.registration_year,
            Car.registration_month,
            Car.thumbnail_local_path,
            Car.source_payload,
        )
        .where(Car.id.in_(raw_only_ids[: max(args.sample * 3, 60)]))
        .order_by(Car.id.desc())
    ).all()

    service_only_rows = db.execute(
        select(
            Car.id,
            Car.external_id,
            Car.country,
            Car.model,
            Car.year,
            Car.registration_year,
            Car.registration_month,
            Car.thumbnail_local_path,
            Car.source_payload,
        )
        .where(Car.id.in_(service_only_ids[: max(args.sample * 2, 40)]))
        .order_by(Car.id.desc())
    ).all()

    raw_only_reasons: Counter[str] = Counter()
    raw_only_samples: list[dict[str, Any]] = []
    for row in raw_only_rows:
        payload = row.source_payload if isinstance(row.source_payload, dict) else {}
        reason_parts = []
        if _bool_flag(payload, "registration_defaulted"):
            reason_parts.append("registration_defaulted")
        if _bool_flag(payload, "registration_year_defaulted"):
            reason_parts.append("registration_year_defaulted")
        if _bool_flag(payload, "registration_month_defaulted"):
            reason_parts.append("registration_month_defaulted")
        if row.registration_year is None:
            reason_parts.append("registration_year_null")
        if row.registration_month in (None, 0):
            reason_parts.append("registration_month_null")
        if not row.thumbnail_local_path:
            reason_parts.append("no_local_thumb")
        if not reason_parts:
            reason_parts.append("unclassified")
        raw_only_reasons["+".join(reason_parts)] += 1
        if len(raw_only_samples) < args.sample:
            raw_only_samples.append(
                {
                    "id": row.id,
                    "external_id": row.external_id,
                    "country": row.country,
                    "model": row.model,
                    "year": row.year,
                    "registration_year": row.registration_year,
                    "registration_month": row.registration_month,
                    "thumbnail_local_path": row.thumbnail_local_path,
                    "payload": _sample_payload(payload),
                }
            )

    service_only_samples = [
        {
            "id": row.id,
            "external_id": row.external_id,
            "country": row.country,
            "model": row.model,
            "year": row.year,
            "registration_year": row.registration_year,
            "registration_month": row.registration_month,
            "thumbnail_local_path": row.thumbnail_local_path,
            "payload": _sample_payload(row.source_payload if isinstance(row.source_payload, dict) else {}),
        }
        for row in service_only_rows[: args.sample]
    ]

    report = {
        "filters": {
            "source": args.source,
            "region": args.region,
            "country": args.country,
            "brand": brand_norm,
            "model": model_norm,
            "mileage_min": args.mileage_min,
            "mileage_max": args.mileage_max,
            "reg_year_min": args.reg_year_min,
            "reg_year_max": args.reg_year_max,
            "reg_month_min": args.reg_month_min,
            "reg_month_max": args.reg_month_max,
        },
        "counts": {
            "raw_exact_db": len(raw_ids),
            "raw_effective_reg_expr": effective_count,
            "service_internal_conditions": len(service_ids),
            "api_count_cars": int(api_count),
        },
        "boundaries": boundary_summary,
        "diffs": {
            "raw_only_count": len(raw_only_ids),
            "service_only_count": len(service_only_ids),
            "raw_only_reason_buckets": dict(raw_only_reasons.most_common()),
            "raw_only_samples": raw_only_samples,
            "service_only_samples": service_only_samples,
        },
    }
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    db.close()


if __name__ == "__main__":
    main()
