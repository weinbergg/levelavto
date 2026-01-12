from __future__ import annotations

import argparse
from sqlalchemy import select, func, and_, or_

from ..db import SessionLocal
from ..models import Car, CarImage, Source


def _parse_brand_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def _normalize_brand(brand: str) -> str:
    return brand.strip().lower()


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit DB counts for mobile.de source.")
    ap.add_argument(
        "--source",
        default="mobile_de",
        help="Source key to inspect (default: mobile_de)",
    )
    ap.add_argument(
        "--brands",
        default=None,
        help="Comma-separated brand list to verify presence in DB",
    )
    args = ap.parse_args()

    db = SessionLocal()
    try:
        src = db.execute(select(Source).where(Source.key == args.source)).scalar_one_or_none()
        if not src:
            print(f"Source not found: {args.source}")
            keys = db.execute(select(Source.key).order_by(Source.key.asc())).scalars().all()
            print("Available sources:", ", ".join(keys))
            return

        total = db.execute(
            select(func.count()).select_from(Car).where(Car.source_id == src.id)
        ).scalar_one()
        active = db.execute(
            select(func.count()).select_from(Car).where(
                and_(Car.source_id == src.id, Car.is_available.is_(True))
            )
        ).scalar_one()
        distinct_ext = db.execute(
            select(func.count(func.distinct(Car.external_id)))
            .select_from(Car)
            .where(Car.source_id == src.id)
        ).scalar_one()
        no_thumb = db.execute(
            select(func.count()).select_from(Car).where(
                and_(
                    Car.source_id == src.id,
                    or_(Car.thumbnail_url.is_(None), Car.thumbnail_url == ""),
                )
            )
        ).scalar_one()

        image_total = db.execute(
            select(func.count()).select_from(CarImage).join(Car, CarImage.car_id == Car.id).where(
                Car.source_id == src.id
            )
        ).scalar_one()
        cars_with_images = db.execute(
            select(func.count(func.distinct(CarImage.car_id)))
            .select_from(CarImage)
            .join(Car, CarImage.car_id == Car.id)
            .where(Car.source_id == src.id)
        ).scalar_one()

        print(f"source_key={src.key}")
        print(f"cars_total={total}")
        print(f"cars_active={active}")
        print(f"distinct_external_id={distinct_ext}")
        print(f"cars_without_thumbnail={no_thumb}")
        print(f"car_images_total={image_total}")
        print(f"cars_with_images={cars_with_images}")

        brands_check = _parse_brand_list(args.brands)
        if brands_check:
            db_brands = db.execute(
                select(func.distinct(Car.brand)).where(
                    and_(Car.source_id == src.id, Car.brand.is_not(None))
                )
            ).scalars().all()
            have = {_normalize_brand(b) for b in db_brands if b}
            want = {_normalize_brand(b): b for b in brands_check}
            missing = [want[key] for key in want if key not in have]
            print("\nBrand check:")
            if missing:
                print("  missing_in_db=" + ", ".join(missing))
            else:
                print("  all_present")
    finally:
        db.close()


if __name__ == "__main__":
    main()
