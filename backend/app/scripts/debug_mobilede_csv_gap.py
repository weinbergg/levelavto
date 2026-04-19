from __future__ import annotations

import argparse
import json
import logging
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import func, select

from backend.app.db import SessionLocal
from backend.app.importing.mobilede_csv import iter_mobilede_csv_rows
from backend.app.models import Car, Source
from backend.app.parsing.config import load_sites_config
from backend.app.parsing.mobile_de_feed import MobileDeFeedParser
from backend.app.services.cars_service import CarsService, brand_variants, model_lookup_key, normalize_model_label


def _normalize(value: str | None) -> str:
    return (value or "").strip().lower()


def _resolve_csv_path(explicit_path: str | None) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if not path.is_file():
            raise FileNotFoundError(f"CSV file not found: {path}")
        return path
    candidates: list[Path] = []
    tmp_dir = Path("/app/tmp")
    current = tmp_dir / "mobilede_active_offers.csv"
    if current.is_file():
        candidates.append(current)
    candidates.extend(
        sorted(
            tmp_dir.glob("mobilede_active_offers_*.csv"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
    )
    if not candidates:
        raise FileNotFoundError("No mobile.de CSV found in /app/tmp")
    return candidates[0]


def _serialize_dt(value: datetime | None) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


def _passes_range_filters(*, mileage: Any, reg_year: Any, mileage_max: int | None, reg_year_min: int | None) -> bool:
    if mileage_max is not None:
        if mileage is None:
            return False
        try:
            if int(mileage) > int(mileage_max):
                return False
        except Exception:
            return False
    if reg_year_min is not None:
        if reg_year is None:
            return False
        try:
            if int(reg_year) < int(reg_year_min):
                return False
        except Exception:
            return False
    return True


def main() -> None:
    # This script is meant to emit a compact comparison report.
    # The CSV parser can log thousands of suspicious field warnings for noisy rows,
    # which makes the actual JSON result unusable in terminal sessions.
    logging.getLogger("backend.app.importing.mobilede_csv").setLevel(logging.ERROR)
    logging.getLogger("backend.app.imports.mobilede_csv").setLevel(logging.ERROR)

    ap = argparse.ArgumentParser(description="Compare current mobile.de CSV coverage with DB state")
    ap.add_argument("--csv", default=None, help="Optional path to CSV inside container")
    ap.add_argument("--brand", required=True)
    ap.add_argument("--model", required=True)
    ap.add_argument("--region", default="EU")
    ap.add_argument("--country", default=None)
    ap.add_argument("--kr-type", default=None)
    ap.add_argument("--mileage-max", type=int, default=None)
    ap.add_argument("--reg-year-min", type=int, default=None)
    ap.add_argument("--sample-limit", type=int, default=20)
    args = ap.parse_args()

    try:
        csv_path = _resolve_csv_path(args.csv)
    except FileNotFoundError as exc:
        print(
            json.dumps(
                {
                    "filters": {
                        "brand": args.brand,
                        "model": args.model,
                        "mileage_max": args.mileage_max,
                        "reg_year_min": args.reg_year_min,
                    },
                    "csv": {
                        "path": args.csv,
                        "status": "missing",
                        "error": str(exc),
                    },
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        raise SystemExit(0)

    parser = MobileDeFeedParser(load_sites_config().get("mobile_de"))
    brand_norm = _normalize(args.brand)
    model_norm = model_lookup_key(args.model)

    csv_matches: dict[str, dict[str, Any]] = {}
    bucket_csv_matches: dict[str, dict[str, Any]] = {}
    duplicate_match_ids = 0
    duplicate_bucket_match_ids = 0
    total_rows = 0
    parsed_rows = 0
    parse_status = Counter()
    model_counter = Counter()
    bucket_model_counter = Counter()

    with SessionLocal() as db:
        source_id = db.execute(select(Source.id).where(Source.key == "mobile_de")).scalar_one()
        service = CarsService(db)
        resolved_aliases = service._resolve_model_aliases(
            region=args.region,
            country=args.country,
            kr_type=args.kr_type,
            brand=args.brand,
            model=args.model,
        )
        normalized_target = normalize_model_label(args.model)
        if not resolved_aliases:
            resolved_aliases = [normalized_target]
        alias_keys = {
            model_lookup_key(alias)
            for alias in resolved_aliases
            if normalize_model_label(alias)
        }
        if not alias_keys:
            alias_keys = {model_lookup_key(normalized_target)}

        models = service.models_for_brand_filtered(
            region=args.region,
            country=args.country,
            kr_type=args.kr_type,
            brand=args.brand,
        )
        groups = service.build_model_groups(brand=args.brand, models=models)
        target_key = model_lookup_key(normalized_target)
        matched_group = next(
            (
                group
                for group in groups
                if model_lookup_key(group.get("label") or "") == target_key
            ),
            None,
        )

        brand_variants_lc = [value.lower() for value in brand_variants(args.brand) if value]
        brand_expr = func.lower(func.trim(func.coalesce(Car.brand, "")))
        model_expr = func.lower(func.trim(func.coalesce(Car.model, "")))

        db_filters = [
            Car.source_id == source_id,
            brand_expr == brand_norm,
            model_expr == model_norm,
        ]
        if args.mileage_max is not None:
            db_filters.extend([Car.mileage.is_not(None), Car.mileage <= int(args.mileage_max)])
        if args.reg_year_min is not None:
            db_filters.extend(
                [
                    Car.registration_year.is_not(None),
                    Car.registration_year >= int(args.reg_year_min),
                ]
            )

        db_rows = db.execute(
            select(
                Car.id,
                Car.external_id,
                Car.model,
                Car.is_available,
                Car.registration_year,
                Car.registration_month,
                Car.year,
                Car.mileage,
                Car.updated_at,
                Car.last_seen_at,
                Car.source_url,
                Car.country,
            ).where(*db_filters)
        ).all()

        bucket_filters = [
            Car.source_id == source_id,
            brand_expr.in_(brand_variants_lc),
        ]
        bucket_model_clause = service._model_filter_clause(
            region=args.region,
            country=args.country,
            kr_type=args.kr_type,
            brand=args.brand,
            model=args.model,
        )
        if bucket_model_clause is not None:
            bucket_filters.append(bucket_model_clause)
        if args.mileage_max is not None:
            bucket_filters.extend([Car.mileage.is_not(None), Car.mileage <= int(args.mileage_max)])
        if args.reg_year_min is not None:
            bucket_filters.extend(
                [
                    Car.registration_year.is_not(None),
                    Car.registration_year >= int(args.reg_year_min),
                ]
            )

        bucket_db_rows = db.execute(
            select(
                Car.id,
                Car.external_id,
                Car.model,
                Car.is_available,
                Car.registration_year,
                Car.registration_month,
                Car.year,
                Car.mileage,
                Car.updated_at,
                Car.last_seen_at,
                Car.source_url,
                Car.country,
            ).where(*bucket_filters)
        ).all()

    for row in iter_mobilede_csv_rows(str(csv_path)):
        total_rows += 1
        parsed_iter = parser.iter_parsed_from_csv([row])
        parsed = next(parsed_iter, None)
        if parsed is None:
            parse_status["skipped_by_parser"] += 1
            continue
        parsed_rows += 1
        payload = parsed.as_dict()
        if _normalize(payload.get("brand")) != brand_norm:
            continue
        if not _passes_range_filters(
            mileage=payload.get("mileage"),
            reg_year=payload.get("registration_year"),
            mileage_max=args.mileage_max,
            reg_year_min=args.reg_year_min,
        ):
            continue
        payload_model_key = model_lookup_key(payload.get("model"))
        if payload_model_key in alias_keys:
            ext_id = str(payload.get("external_id") or "").strip()
            if ext_id:
                if ext_id in bucket_csv_matches:
                    duplicate_bucket_match_ids += 1
                bucket_csv_matches[ext_id] = {
                    "external_id": ext_id,
                    "brand": payload.get("brand"),
                    "model": payload.get("model"),
                    "variant": payload.get("variant"),
                    "registration_year": payload.get("registration_year"),
                    "registration_month": payload.get("registration_month"),
                    "year": payload.get("year"),
                    "mileage": payload.get("mileage"),
                    "source_url": payload.get("source_url"),
                    "country": payload.get("country"),
                }
                bucket_model_counter[str(payload.get("model") or "")] += 1
        if payload_model_key != model_norm:
            continue
        ext_id = str(payload.get("external_id") or "").strip()
        if not ext_id:
            parse_status["missing_external_id"] += 1
            continue
        if ext_id in csv_matches:
            duplicate_match_ids += 1
        csv_matches[ext_id] = {
            "external_id": ext_id,
            "brand": payload.get("brand"),
            "model": payload.get("model"),
            "variant": payload.get("variant"),
            "registration_year": payload.get("registration_year"),
            "registration_month": payload.get("registration_month"),
            "year": payload.get("year"),
            "mileage": payload.get("mileage"),
            "source_url": payload.get("source_url"),
            "country": payload.get("country"),
        }
        model_counter[str(payload.get("model") or "")] += 1

    db_by_ext: dict[str, dict[str, Any]] = {}
    db_status_counter = Counter()
    unavailable_recent = []
    for row in db_rows:
        record = {
            "id": int(row.id),
            "external_id": str(row.external_id or ""),
            "is_available": bool(row.is_available),
            "registration_year": row.registration_year,
            "registration_month": row.registration_month,
            "year": row.year,
            "mileage": row.mileage,
            "updated_at": _serialize_dt(row.updated_at),
            "last_seen_at": _serialize_dt(row.last_seen_at),
            "source_url": row.source_url,
            "country": row.country,
        }
        db_by_ext[record["external_id"]] = record
        db_status_counter["available" if record["is_available"] else "unavailable"] += 1
        if not record["is_available"]:
            unavailable_recent.append(record)

    bucket_db_by_ext: dict[str, dict[str, Any]] = {}
    bucket_db_status_counter = Counter()
    bucket_unavailable_recent = []
    for row in bucket_db_rows:
        record = {
            "id": int(row.id),
            "external_id": str(row.external_id or ""),
            "model": row.model,
            "is_available": bool(row.is_available),
            "registration_year": row.registration_year,
            "registration_month": row.registration_month,
            "year": row.year,
            "mileage": row.mileage,
            "updated_at": _serialize_dt(row.updated_at),
            "last_seen_at": _serialize_dt(row.last_seen_at),
            "source_url": row.source_url,
            "country": row.country,
        }
        bucket_db_by_ext[record["external_id"]] = record
        bucket_db_status_counter["available" if record["is_available"] else "unavailable"] += 1
        if not record["is_available"]:
            bucket_unavailable_recent.append(record)

    csv_ids = set(csv_matches.keys())
    db_ids = set(db_by_ext.keys())
    csv_missing_in_db = sorted(csv_ids - db_ids)
    csv_present_unavailable = sorted(
        ext_id for ext_id in (csv_ids & db_ids) if not db_by_ext[ext_id]["is_available"]
    )
    csv_present_available = sorted(
        ext_id for ext_id in (csv_ids & db_ids) if db_by_ext[ext_id]["is_available"]
    )
    db_available_not_in_csv = sorted(
        ext_id for ext_id in db_ids if db_by_ext[ext_id]["is_available"] and ext_id not in csv_ids
    )

    bucket_csv_ids = set(bucket_csv_matches.keys())
    bucket_db_ids = set(bucket_db_by_ext.keys())
    bucket_csv_missing_in_db = sorted(bucket_csv_ids - bucket_db_ids)
    bucket_csv_present_unavailable = sorted(
        ext_id for ext_id in (bucket_csv_ids & bucket_db_ids) if not bucket_db_by_ext[ext_id]["is_available"]
    )
    bucket_csv_present_available = sorted(
        ext_id for ext_id in (bucket_csv_ids & bucket_db_ids) if bucket_db_by_ext[ext_id]["is_available"]
    )
    bucket_db_available_not_in_csv = sorted(
        ext_id
        for ext_id in bucket_db_ids
        if bucket_db_by_ext[ext_id]["is_available"] and ext_id not in bucket_csv_ids
    )

    sample_limit = max(1, int(args.sample_limit))
    unavailable_recent.sort(
        key=lambda item: (
            item.get("updated_at") or "",
            item.get("last_seen_at") or "",
            item.get("id") or 0,
        ),
        reverse=True,
    )
    bucket_unavailable_recent.sort(
        key=lambda item: (
            item.get("updated_at") or "",
            item.get("last_seen_at") or "",
            item.get("id") or 0,
        ),
        reverse=True,
    )

    bucket_db_models = Counter(str(item.get("model") or "") for item in bucket_db_by_ext.values())
    bucket_db_available_models = Counter(
        str(item.get("model") or "") for item in bucket_db_by_ext.values() if item.get("is_available")
    )

    result = {
        "filters": {
            "brand": args.brand,
            "model": args.model,
            "region": args.region,
            "country": args.country,
            "kr_type": args.kr_type,
            "mileage_max": args.mileage_max,
            "reg_year_min": args.reg_year_min,
        },
        "csv": {
            "path": str(csv_path),
            "total_rows": total_rows,
            "parsed_rows": parsed_rows,
            "matching_unique_external_ids": len(csv_ids),
            "matching_duplicate_rows": duplicate_match_ids,
            "matching_models": dict(model_counter.most_common(20)),
            "parse_status": dict(parse_status),
        },
        "db": {
            "matching_total": len(db_ids),
            "matching_available": int(db_status_counter.get("available", 0)),
            "matching_unavailable": int(db_status_counter.get("unavailable", 0)),
        },
        "diffs": {
            "csv_present_available": len(csv_present_available),
            "csv_present_unavailable": len(csv_present_unavailable),
            "csv_missing_in_db": len(csv_missing_in_db),
            "db_available_not_in_csv": len(db_available_not_in_csv),
            "csv_present_unavailable_samples": [
                {
                    **db_by_ext[ext_id],
                    "csv_source_url": csv_matches[ext_id]["source_url"],
                }
                for ext_id in csv_present_unavailable[:sample_limit]
            ],
            "csv_missing_in_db_samples": [
                csv_matches[ext_id] for ext_id in csv_missing_in_db[:sample_limit]
            ],
            "db_available_not_in_csv_samples": [
                db_by_ext[ext_id] for ext_id in db_available_not_in_csv[:sample_limit]
            ],
            "db_recent_unavailable_samples": unavailable_recent[:sample_limit],
        },
        "site_bucket": {
            "requested_model": args.model,
            "resolved_aliases": resolved_aliases,
            "resolved_alias_keys": sorted(alias_keys),
            "group": {
                "label": matched_group.get("label") if matched_group else None,
                "count": int(matched_group.get("count") or 0) if matched_group else None,
                "models": [str(item.get("value") or "") for item in (matched_group.get("models") or [])] if matched_group else [],
            },
            "csv": {
                "matching_unique_external_ids": len(bucket_csv_ids),
                "matching_duplicate_rows": duplicate_bucket_match_ids,
                "matching_models": dict(bucket_model_counter.most_common(30)),
            },
            "db": {
                "matching_total": len(bucket_db_ids),
                "matching_available": int(bucket_db_status_counter.get("available", 0)),
                "matching_unavailable": int(bucket_db_status_counter.get("unavailable", 0)),
                "matching_models": dict(bucket_db_models.most_common(30)),
                "matching_available_models": dict(bucket_db_available_models.most_common(30)),
            },
            "diffs": {
                "csv_present_available": len(bucket_csv_present_available),
                "csv_present_unavailable": len(bucket_csv_present_unavailable),
                "csv_missing_in_db": len(bucket_csv_missing_in_db),
                "db_available_not_in_csv": len(bucket_db_available_not_in_csv),
                "csv_present_unavailable_samples": [
                    {
                        **bucket_db_by_ext[ext_id],
                        "csv_source_url": bucket_csv_matches[ext_id]["source_url"],
                    }
                    for ext_id in bucket_csv_present_unavailable[:sample_limit]
                ],
                "csv_missing_in_db_samples": [
                    bucket_csv_matches[ext_id] for ext_id in bucket_csv_missing_in_db[:sample_limit]
                ],
                "db_available_not_in_csv_samples": [
                    bucket_db_by_ext[ext_id] for ext_id in bucket_db_available_not_in_csv[:sample_limit]
                ],
                "db_recent_unavailable_samples": bucket_unavailable_recent[:sample_limit],
            },
        },
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
