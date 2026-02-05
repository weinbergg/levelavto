import argparse
from datetime import datetime

from backend.app.db import SessionLocal
from backend.app.models import Car
from backend.app.services.cars_service import CarsService
from backend.app.utils.price_utils import ceil_to_step


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--region", default="KR")
    ap.add_argument("--batch", type=int, default=2000)
    args = ap.parse_args()

    updated = skipped = errors = 0
    with SessionLocal() as db:
        svc = CarsService(db)
        rates = svc.get_fx_rates() or {}
        usd = float(rates.get("USD", 0) or 0)
        krw = float(rates.get("KRW", 0) or 0)

        q = db.query(Car).filter(Car.is_available.is_(True))
        if args.region.upper() == "KR":
            q = q.filter(Car.country.like("KR%"))
        total = q.count()

        offset = 0
        while True:
            batch = q.order_by(Car.id.asc()).offset(offset).limit(args.batch).all()
            if not batch:
                break
            for car in batch:
                try:
                    if car.price is None or not car.currency:
                        skipped += 1
                        continue
                    curr = car.currency.lower().strip()
                    if curr == "rub":
                        rub = float(car.price)
                    elif curr == "usd":
                        if not usd:
                            skipped += 1
                            continue
                        rub = float(car.price) * usd
                    elif curr == "krw":
                        if not krw:
                            skipped += 1
                            continue
                        rub = float(car.price) * krw
                    else:
                        skipped += 1
                        continue
                    rub = ceil_to_step(rub, 10000)
                    car.price_rub_cached = rub
                    car.total_price_rub_cached = rub
                    car.calc_updated_at = datetime.utcnow()
                    updated += 1
                except Exception:
                    errors += 1
            db.commit()
            offset += args.batch

    print(f"[recalc_cached_prices] total={total} updated={updated} skipped={skipped} errors={errors}")


if __name__ == "__main__":
    main()
