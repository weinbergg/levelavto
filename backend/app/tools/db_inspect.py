from __future__ import annotations

import argparse
from typing import Optional
from sqlalchemy import select, func
from ..db import SessionLocal
from ..models import Car, Source, CarImage


def inspect_source(source_key: str, limit: int = 10) -> None:
    db = SessionLocal()
    try:
        src = db.execute(select(Source).where(Source.key == source_key)).scalar_one_or_none()
        if not src:
            print(f"Source not found: {source_key}")
            return
        total = db.execute(select(func.count()).select_from(Car).where(Car.source_id == src.id)).scalar_one()
        print(f"Source={source_key} cars total: {total}")
        if total == 0:
            print("No cars for this source yet.")
            return
        cars = (
            db.execute(
                select(Car).where(Car.source_id == src.id).order_by(Car.id.desc()).limit(limit)
            )
            .scalars()
            .all()
        )
        for i, c in enumerate(cars, 1):
            img_count = db.execute(select(func.count()).select_from(CarImage).where(CarImage.car_id == c.id)).scalar_one()
            print(
                f"[{i}] id={c.id} {c.brand or '—'} {c.model or ''} "
                f"year={c.year or '—'} km={c.mileage or '—'} "
                f"price={c.price or '—'} {c.currency or ''} "
                f"engine={c.engine_type or '—'} imgs={img_count} "
                f"url={c.source_url or '—'}"
            )
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect DB contents for a source")
    parser.add_argument("--source", type=str, required=True, help="Source key (e.g. mobile_de, emavto_klg)")
    parser.add_argument("--limit", type=int, default=10, help="Number of rows to show")
    args = parser.parse_args()
    inspect_source(args.source, args.limit)


if __name__ == "__main__":
    main()


