from __future__ import annotations

import argparse

from sqlalchemy import select

from backend.app.db import SessionLocal
from backend.app.models import Car
from backend.app.services.cars_service import CarsService
from backend.app.utils.pricing import to_rub


def backfill_price_rub(only_missing: bool = True, batch: int = 5000, dry_run: bool = False) -> int:
    db = SessionLocal()
    updated = 0
    try:
        rates = CarsService(db).get_fx_rates() or {}
        stmt = select(Car).where(Car.price.is_not(None))
        if only_missing:
            stmt = stmt.where(Car.price_rub_cached.is_(None))
        rows = db.execute(stmt).scalars().yield_per(batch)
        touched = 0
        for car in rows:
            rub = to_rub(car.price, car.currency, rates)
            if rub is None:
                continue
            if not dry_run:
                car.price_rub_cached = round(rub, 2)
            updated += 1
            touched += 1
            if touched >= batch:
                if not dry_run:
                    db.commit()
                touched = 0
        if not dry_run:
            db.commit()
    finally:
        db.close()
    return updated


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill price_rub_cached for existing cars.")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Update all rows, not only those with missing price_rub_cached.",
    )
    parser.add_argument("--batch", type=int, default=5000,
                        help="Commit batch size.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Do not write changes, only count updates.")
    args = parser.parse_args()

    updated = backfill_price_rub(
        only_missing=not args.all,
        batch=max(100, args.batch),
        dry_run=args.dry_run,
    )
    mode = "dry-run" if args.dry_run else "updated"
    print(f"{mode}: {updated} rows")


if __name__ == "__main__":
    main()
