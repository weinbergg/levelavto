from __future__ import annotations

import argparse
from typing import Optional
from sqlalchemy import select, delete, update
from ..db import SessionLocal
from ..models import Car, CarImage, Source


def soft_disable(db, source_id: int) -> int:
    res = db.execute(
        update(Car).where(Car.source_id ==
                          source_id).values(is_available=False)
    )
    db.commit()
    return res.rowcount


def hard_delete(db, source_id: int) -> int:
    # delete images first (foreign key to cars)
    db.execute(
        delete(CarImage).where(CarImage.car_id.in_(
            select(Car.id).where(Car.source_id == source_id)))
    )
    res = db.execute(delete(Car).where(Car.source_id == source_id))
    db.commit()
    return res.rowcount


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Cleanup helper for Encar data. By default marks cars unavailable; use --hard to delete."
    )
    ap.add_argument("--hard", action="store_true",
                    help="Hard delete cars and images for source=encar")
    args = ap.parse_args()

    db = SessionLocal()
    try:
        src: Optional[Source] = db.execute(select(Source).where(
            Source.key == "encar")).scalar_one_or_none()
        if not src:
            print("Source 'encar' not found. Nothing to do.")
            return
        total = db.execute(select(Car).where(Car.source_id == src.id)).all()
        print(f"Found {len(total)} cars for source=encar")
        if args.hard:
            removed = hard_delete(db, src.id)
            print(f"Hard deleted {removed} cars (images removed as well).")
        else:
            updated = soft_disable(db, src.id)
            print(
                f"Marked {updated} cars as unavailable (soft). Use --hard to delete.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
