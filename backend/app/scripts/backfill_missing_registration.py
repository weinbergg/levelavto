from __future__ import annotations

import argparse
import time
from datetime import datetime

from sqlalchemy import and_, cast, func, or_, select
from sqlalchemy.dialects.postgresql import JSONB

from backend.app.db import SessionLocal
from backend.app.models import Car, Source
from backend.app.utils.registration_defaults import get_missing_registration_default


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--region", default=None, help="EU, KR or empty for all")
    ap.add_argument("--country", default=None)
    ap.add_argument("--source-key", default=None, help="comma-separated source keys")
    ap.add_argument("--batch", type=int, default=2000)
    ap.add_argument("--chunk", type=int, default=50000)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    fallback_year, fallback_month = get_missing_registration_default()

    with SessionLocal() as db:
        payload_json = cast(Car.source_payload, JSONB)
        defaulted_expr = (
            func.coalesce(
                func.jsonb_extract_path_text(payload_json, "registration_defaulted"),
                "false",
            )
            == "true"
        )
        default_year_expr = func.coalesce(
            func.jsonb_extract_path_text(payload_json, "registration_default_year"),
            "",
        )
        default_month_expr = func.coalesce(
            func.jsonb_extract_path_text(payload_json, "registration_default_month"),
            "",
        )
        missing_registration_expr = or_(
            Car.registration_year.is_(None),
            Car.registration_month.is_(None),
        )
        stale_default_expr = and_(
            defaulted_expr,
            or_(
                Car.registration_year != fallback_year,
                Car.registration_month != fallback_month,
                default_year_expr != str(fallback_year),
                default_month_expr != str(fallback_month),
            ),
        )
        base = db.query(Car.id).filter(
            or_(missing_registration_expr, stale_default_expr)
        )
        region = (args.region or "").strip().upper()
        if region == "EU":
            base = base.filter(~Car.country.like("KR%"))
        elif region == "KR":
            base = base.filter(Car.country.like("KR%"))
        if args.country:
            base = base.filter(Car.country == args.country.strip().upper())
        if args.source_key:
            keys = [k.strip() for k in args.source_key.split(",") if k.strip()]
            if keys:
                src_ids = db.execute(select(Source.id).where(Source.key.in_(keys))).scalars().all()
                if src_ids:
                    base = base.filter(Car.source_id.in_(src_ids))
                else:
                    print("[backfill_missing_registration] total=0 updated=0 source_match=0")
                    return

        total = base.count()
        print(
            f"[backfill_missing_registration] total_candidates={total} "
            f"default={fallback_year:04d}-{fallback_month:02d}",
            flush=True,
        )
        if args.dry_run or total == 0:
            return

        min_id = base.with_entities(func.min(Car.id)).scalar()
        max_id = base.with_entities(func.max(Car.id)).scalar()
        if min_id is None or max_id is None:
            print("[backfill_missing_registration] total=0 updated=0", flush=True)
            return

        updated = 0
        processed = 0
        started_at = time.time()
        start = int(min_id)
        window_no = 0
        while start <= int(max_id):
            end = min(start + args.chunk - 1, int(max_id))
            window_no += 1
            ids = [
                r[0]
                for r in base.filter(Car.id.between(start, end)).order_by(Car.id.asc()).all()
            ]
            if ids:
                for i in range(0, len(ids), args.batch):
                    batch_ids = ids[i : i + args.batch]
                    cars = db.query(Car).filter(Car.id.in_(batch_ids)).all()
                    for car in cars:
                        payload = dict(car.source_payload or {})
                        is_defaulted = payload.get("registration_defaulted") is True
                        needs_rewrite = is_defaulted and (
                            int(car.registration_year or 0) != fallback_year
                            or int(car.registration_month or 0) != fallback_month
                            or int(payload.get("registration_default_year") or 0) != fallback_year
                            or int(payload.get("registration_default_month") or 0) != fallback_month
                        )
                        needs_fill = (
                            car.registration_year is None
                            or car.registration_month is None
                        )
                        changed = False
                        if car.registration_year is None or needs_rewrite:
                            car.registration_year = fallback_year
                            changed = True
                        if car.registration_month is None or needs_rewrite:
                            car.registration_month = fallback_month
                            changed = True
                        if changed or needs_fill or needs_rewrite:
                            payload["registration_defaulted"] = True
                            payload["registration_default_year"] = int(
                                car.registration_year or fallback_year
                            )
                            payload["registration_default_month"] = int(
                                car.registration_month or fallback_month
                            )
                            car.source_payload = payload
                        if changed:
                            car.updated_at = datetime.utcnow()
                            updated += 1
                        processed += 1
                    db.commit()
                elapsed = max(time.time() - started_at, 1.0)
                rate = processed / elapsed if processed else 0.0
                print(
                    f"[backfill_missing_registration] window={window_no} ids={start}-{end} "
                    f"processed={processed}/{total} updated={updated} rate={rate:.2f}/s",
                    flush=True,
                )
            start = end + 1

        print(
            f"[backfill_missing_registration] done total={total} processed={processed} updated={updated}",
            flush=True,
        )


if __name__ == "__main__":
    main()
