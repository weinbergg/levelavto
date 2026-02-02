import argparse
from datetime import datetime, timedelta

from sqlalchemy import and_

from backend.app.db import SessionLocal
from backend.app.models import Car
from backend.app.services.cars_service import CarsService


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--region", default="EU")
    ap.add_argument("--country", default=None)
    ap.add_argument("--batch", type=int, default=2000)
    ap.add_argument("--only-missing", action="store_true")
    ap.add_argument("--since-minutes", type=int, default=None)
    args = ap.parse_args()

    updated = skipped = errors = 0
    with SessionLocal() as db:
        svc = CarsService(db)
        q = db.query(Car.id).filter(Car.is_available.is_(True))
        if args.region.upper() == "EU":
            q = q.filter(~Car.country.like("KR%"))
        if args.country:
            q = q.filter(Car.country == args.country.upper())
        if args.only_missing:
            q = q.filter(Car.total_price_rub_cached.is_(None))
        if args.since_minutes:
            since_ts = datetime.utcnow() - timedelta(minutes=args.since_minutes)
            q = q.filter(Car.updated_at >= since_ts)

        total = q.count()
        offset = 0
        while True:
            ids = [r[0] for r in q.order_by(Car.id.asc()).offset(offset).limit(args.batch).all()]
            if not ids:
                break
            cars = db.query(Car).filter(Car.id.in_(ids)).all()
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
            offset += args.batch

    print(
        f"[recalc_calc_cache] total={total} updated={updated} skipped={skipped} errors={errors}"
    )


if __name__ == "__main__":
    main()
