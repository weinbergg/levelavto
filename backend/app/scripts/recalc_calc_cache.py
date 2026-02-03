import argparse
import os
import time
from datetime import datetime, timedelta

from backend.app.db import SessionLocal
from backend.app.models import Car
from backend.app.services.cars_service import CarsService
from backend.app.utils.telegram import send_telegram_message


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--region", default="EU")
    ap.add_argument("--country", default=None)
    ap.add_argument("--batch", type=int, default=2000)
    ap.add_argument("--only-missing", action="store_true")
    ap.add_argument("--since-minutes", type=int, default=None)
    ap.add_argument("--chunk", type=int, default=50000, help="ids per window (avoid huge offsets)")
    ap.add_argument("--sleep", type=int, default=0, help="seconds to sleep between windows")
    ap.add_argument("--telegram", action="store_true", help="send progress to Telegram if env configured")
    ap.add_argument("--telegram-interval", type=int, default=900, help="seconds between telegram updates")
    ap.add_argument("--rate-shift", type=float, default=0.0, help="add rub to EUR/USD rates when recalculating")
    args = ap.parse_args()

    updated = skipped = errors = 0
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
            f"updated={updated} skipped={skipped} errors={errors}"
        )
        ok = send_telegram_message(token, chat_id, msg)
        if ok:
            last_notify = now

    with SessionLocal() as db:
        svc = CarsService(db)
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
        if args.country:
            base = base.filter(Car.country == args.country.upper())
        if args.only_missing:
            base = base.filter(Car.total_price_rub_cached.is_(None))
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
        while start <= max_id:
            end = min(start + args.chunk - 1, max_id)
            ids = [r[0] for r in base.filter(Car.id.between(start, end)).order_by(Car.id.asc()).all()]
            if ids:
                for i in range(0, len(ids), args.batch):
                    batch_ids = ids[i : i + args.batch]
                    cars = db.query(Car).filter(Car.id.in_(batch_ids)).all()
                    for car in cars:
                        try:
                            res = svc.ensure_calc_cache(car)
                            if res is None:
                                skipped += 1
                                continue
                            updated += 1
                        except Exception:
                            errors += 1
                    db.commit()
            start = end + 1
            maybe_notify("progress")
            if args.sleep:
                time.sleep(args.sleep)

    maybe_notify("done")
    print(
        f"[recalc_calc_cache] total={total} updated={updated} skipped={skipped} errors={errors}"
    )


if __name__ == "__main__":
    main()
