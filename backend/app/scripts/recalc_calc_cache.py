import argparse
import os
import time
from datetime import datetime, timedelta

from backend.app.db import SessionLocal
from backend.app.models import Car
from backend.app.services.cars_service import CarsService
from backend.app.utils.filter_values import split_csv_values
from backend.app.utils.telegram import send_telegram_message
from sqlalchemy import or_, func, cast
from sqlalchemy.dialects.postgresql import JSONB


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--region", default="EU")
    ap.add_argument("--country", default=None)
    ap.add_argument("--engine-type", default=None, help="Optional normalized fuel filter, e.g. electric")
    ap.add_argument("--batch", type=int, default=2000)
    ap.add_argument("--only-missing", action="store_true")
    ap.add_argument(
        "--only-missing-registration",
        action="store_true",
        help="recalculate only cars with missing registration_year or registration_month",
    )
    ap.add_argument(
        "--only-defaulted-registration",
        action="store_true",
        help="recalculate only cars with auto-defaulted registration date",
    )
    ap.add_argument(
        "--only-inferred-specs",
        action="store_true",
        help="recalculate only cars with inferred engine/power specs",
    )
    ap.add_argument(
        "--only-recoverable-fallback",
        action="store_true",
        help="recalculate cars that still show fallback Europe/KR price markers and may now be recoverable",
    )
    ap.add_argument("--since-minutes", type=int, default=None)
    ap.add_argument("--chunk", type=int, default=50000, help="ids per window (avoid huge offsets)")
    ap.add_argument("--sleep", type=int, default=0, help="seconds to sleep between windows")
    ap.add_argument("--telegram", action="store_true", help="send progress to Telegram if env configured")
    ap.add_argument("--telegram-interval", type=int, default=900, help="seconds between telegram updates")
    ap.add_argument("--rate-shift", type=float, default=0.0, help="add rub to EUR/USD rates when recalculating")
    ap.add_argument("--shard-total", type=int, default=1, help="total number of parallel shards")
    ap.add_argument("--shard-index", type=int, default=0, help="zero-based shard index")
    ap.add_argument(
        "--brands",
        default="",
        help="comma-separated brand list (case-insensitive), e.g. BMW,Mercedes-Benz",
    )
    args = ap.parse_args()

    if args.shard_total < 1:
        raise SystemExit("--shard-total must be >= 1")
    if args.shard_index < 0 or args.shard_index >= args.shard_total:
        raise SystemExit("--shard-index must be in [0, --shard-total)")

    updated = skipped = errors = processed = 0
    last_notify = 0.0
    token = os.getenv("TELEGRAM_BOT_TOKEN") if args.telegram else None
    chat_id = os.getenv("TELEGRAM_CHAT_ID") if args.telegram else None

    def maybe_notify(stage: str) -> None:
        nonlocal last_notify
        if not token or not chat_id:
            return
        now = time.time()
        if stage != "done" and now - last_notify < args.telegram_interval:
            return
        msg = (
            f"recalc_calc_cache {stage}\\n"
            f"region={args.region} country={args.country or 'ALL'}\\n"
            f"shard={args.shard_index + 1}/{args.shard_total}\\n"
            f"processed={processed} updated={updated} skipped={skipped} errors={errors}"
        )
        ok = send_telegram_message(token, chat_id, msg)
        if ok:
            last_notify = now

    with SessionLocal() as db:
        svc = CarsService(db)
        brands = [b.strip().lower() for b in (args.brands or "").split(",") if b.strip()]
        if args.rate_shift:
            # warm cache with shifted rate so subsequent calls reuse it
            rates = svc.get_fx_rates(allow_fetch=True) or {}
            if rates:
                rates = {
                    "EUR": float(rates.get("EUR", 0.0)) + args.rate_shift,
                    "USD": float(rates.get("USD", 0.0)) + args.rate_shift,
                    "RUB": 1.0,
                }
                svc._fx_cache = rates
                svc._fx_cache_ts = time.time()
        base = db.query(Car.id).filter(Car.is_available.is_(True))
        if args.region.upper() == "EU":
            base = base.filter(~Car.country.like("KR%"))
        elif args.region.upper() == "KR":
            base = base.filter(Car.country.like("KR%"))
        if args.country:
            base = base.filter(Car.country == args.country.upper())
        if args.shard_total > 1:
            base = base.filter((Car.id % args.shard_total) == args.shard_index)
        if brands:
            base = base.filter(func.lower(func.trim(Car.brand)).in_(brands))
        if args.engine_type:
            engine_clauses = []
            for raw_engine in split_csv_values(args.engine_type):
                clause = svc._fuel_filter_clause(raw_engine)
                if clause is not None:
                    engine_clauses.append(clause)
            if engine_clauses:
                base = base.filter(or_(*engine_clauses))
        if args.only_missing:
            base = base.filter(Car.total_price_rub_cached.is_(None))
        if args.only_missing_registration:
            base = base.filter(
                or_(Car.registration_year.is_(None), Car.registration_month.is_(None))
            )
        if args.only_defaulted_registration:
            payload_json = cast(Car.source_payload, JSONB)
            base = base.filter(
                func.coalesce(
                    func.jsonb_extract_path_text(payload_json, "registration_defaulted"),
                    "false",
                ) == "true"
            )
        if args.only_inferred_specs:
            base = base.filter(
                or_(
                    Car.inferred_engine_cc.is_not(None),
                    Car.inferred_power_hp.is_not(None),
                    Car.inferred_power_kw.is_not(None),
                )
            )
        if args.only_recoverable_fallback:
            payload_json = cast(Car.calc_breakdown_json, JSONB)
            base = base.filter(
                payload_json.is_not(None),
                payload_json.contains([{"title": "__without_util_fee"}]),
            )
        if args.since_minutes:
            since_ts = datetime.utcnow() - timedelta(minutes=args.since_minutes)
            base = base.filter(Car.updated_at >= since_ts)

        min_id = base.with_entities(Car.id).order_by(Car.id.asc()).limit(1).scalar()
        max_id = base.with_entities(Car.id).order_by(Car.id.desc()).limit(1).scalar()
        if min_id is None or max_id is None:
            print("[recalc_calc_cache] total=0 updated=0 skipped=0 errors=0")
            return

        total = base.count()
        start = min_id
        started_at = time.time()
        window_no = 0
        while start <= max_id:
            end = min(start + args.chunk - 1, max_id)
            window_no += 1
            ids = [r[0] for r in base.filter(Car.id.between(start, end)).order_by(Car.id.asc()).all()]
            if ids:
                for i in range(0, len(ids), args.batch):
                    batch_ids = ids[i : i + args.batch]
                    cars = db.query(Car).filter(Car.id.in_(batch_ids)).all()
                    for car in cars:
                        processed += 1
                        try:
                            force_recalc = bool(
                                args.only_missing_registration
                                or args.only_defaulted_registration
                                or args.only_inferred_specs
                                or args.only_recoverable_fallback
                            )
                            res = svc.ensure_calc_cache(car, force=force_recalc)
                            if res is None:
                                skipped += 1
                                continue
                            updated += 1
                        except Exception:
                            errors += 1
                    db.commit()
                elapsed = max(time.time() - started_at, 1.0)
                rate = processed / elapsed if processed else 0.0
                print(
                    f"[recalc_calc_cache] progress shard={args.shard_index + 1}/{args.shard_total} "
                    f"window={window_no} ids={start}-{end} "
                    f"processed={processed}/{total} updated={updated} skipped={skipped} errors={errors} "
                    f"rate={rate:.2f}/s",
                    flush=True,
                )
            start = end + 1
            maybe_notify("progress")
            if args.sleep:
                time.sleep(args.sleep)

    maybe_notify("done")
    print(
        f"[recalc_calc_cache] shard={args.shard_index + 1}/{args.shard_total} "
        f"total={total} processed={processed} updated={updated} skipped={skipped} errors={errors}"
    )


if __name__ == "__main__":
    main()
