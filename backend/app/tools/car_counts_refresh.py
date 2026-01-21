from __future__ import annotations

from collections import defaultdict
from typing import Any
from datetime import datetime, timezone
import argparse
from sqlalchemy import case, func, select, text, or_, Integer
from sqlalchemy.orm import Session

from ..models import Car, Source
from ..services.cars_service import normalize_brand, CarsService
from ..utils.country_map import normalize_country_code
from ..db import SessionLocal


EU_COUNTRIES = CarsService.EU_COUNTRIES

KOREA_HINTS = ["emavto", "m-auto", "encar"]


def _source_ids_for_europe(db: Session) -> list[int]:
    stmt = select(Source.id).where(
        or_(
            func.lower(Source.key).like("%mobile%"),
            func.upper(Source.country).in_(EU_COUNTRIES),
        )
    )
    return [r[0] for r in db.execute(stmt).all()]


def _source_ids_for_korea(db: Session) -> list[int]:
    # Prefer explicit country=KR if set, fallback to key hints.
    stmt = select(Source.id).where(func.upper(Source.country) == "KR")
    ids = [r[0] for r in db.execute(stmt).all()]
    if ids:
        return ids
    conds = [func.lower(Source.key).like(f"%{hint}%") for hint in KOREA_HINTS]
    stmt = select(Source.id).where(or_(*conds))
    return [r[0] for r in db.execute(stmt).all()]


PRICE_BUCKETS = [
    (1_000_000, "lt_1m"),
    (3_000_000, "1_3m"),
    (5_000_000, "3_5m"),
    (10_000_000, "5_10m"),
    (20_000_000, "10_20m"),
]

MILEAGE_BUCKETS = [
    (50_000, "lt_50k"),
    (100_000, "50_100k"),
    (150_000, "100_150k"),
    (200_000, "150_200k"),
    (300_000, "200_300k"),
]


def _price_bucket_expr() -> Any:
    price_val = func.coalesce(Car.total_price_rub_cached, Car.price_rub_cached, Car.price)
    cases = []
    for limit, label in PRICE_BUCKETS:
        cases.append((price_val < limit, label))
    return case(*cases, else_="20m_plus")


def _mileage_bucket_expr() -> Any:
    mileage_val = func.coalesce(Car.mileage, 0)
    cases = []
    for limit, label in MILEAGE_BUCKETS:
        cases.append((mileage_val < limit, label))
    return case(*cases, else_="300k_plus")


def refresh_counts(db: Session) -> int:
    korea_key_conds = [func.lower(Source.key).like(f"%{hint}%") for hint in KOREA_HINTS]
    kr_cond = or_(
        func.upper(Source.country) == "KR",
        func.upper(Car.country).like("KR%"),
        or_(*korea_key_conds),
    )
    region_case = case(
        (kr_cond, "KR"),
        else_="EU",
    )
    country_case = case(
        (kr_cond, "KR"),
        else_=func.upper(Car.country),
    )

    reg_year_expr = func.coalesce(Car.registration_year, Car.year).cast(Integer)
    price_bucket = _price_bucket_expr()
    mileage_bucket = _mileage_bucket_expr()
    color_expr = func.lower(func.trim(Car.color))
    engine_expr = func.lower(func.trim(Car.engine_type))
    transmission_expr = func.lower(func.trim(Car.transmission))
    body_expr = func.lower(func.trim(Car.body_type))
    drive_expr = func.lower(func.trim(Car.drive_type))

    stmt = (
        select(
            region_case.label("region"),
            country_case.label("country"),
            Car.brand,
            Car.model,
            color_expr.label("color"),
            engine_expr.label("engine_type"),
            transmission_expr.label("transmission"),
            body_expr.label("body_type"),
            drive_expr.label("drive_type"),
            price_bucket.label("price_bucket"),
            mileage_bucket.label("mileage_bucket"),
            reg_year_expr.label("reg_year"),
            func.count().label("total"),
        )
        .select_from(Car)
        .join(Source, Source.id == Car.source_id)
        .where(Car.is_available.is_(True))
        .group_by(
            region_case,
            country_case,
            Car.brand,
            Car.model,
            color_expr,
            engine_expr,
            transmission_expr,
            body_expr,
            drive_expr,
            price_bucket,
            mileage_bucket,
            reg_year_expr,
        )
    )

    rows = db.execute(stmt).all()
    aggregated: dict[tuple[str, str | None, str | None, str | None], int] = defaultdict(int)
    for (
        region,
        country,
        brand,
        model,
        color,
        engine_type,
        transmission,
        body_type,
        drive_type,
        price_bucket,
        mileage_bucket,
        reg_year,
        total,
    ) in rows:
        region = (region or "EU").upper()
        country_norm = normalize_country_code(country) if country else None
        brand_norm = normalize_brand(brand).strip() if brand else None
        model_norm = model.strip() if model else None
        total = int(total)

        key = (
            region,
            country_norm,
            brand_norm,
            model_norm,
            (color or None),
            (engine_type or None),
            (transmission or None),
            (body_type or None),
            (drive_type or None),
            price_bucket or None,
            mileage_bucket or None,
            reg_year if reg_year else None,
        )
        aggregated[key] += total

    now = datetime.now(timezone.utc)
    db.execute(text("DELETE FROM car_counts"))
    if not aggregated:
        db.commit()
        return 0

    values = []
    for k, v in aggregated.items():
        values.append(
            {
                "region": k[0],
                "country": k[1],
                "brand": k[2],
                "model": k[3],
                "color": k[4],
                "engine_type": k[5],
                "transmission": k[6],
                "body_type": k[7],
                "drive_type": k[8],
                "price_bucket": k[9],
                "mileage_bucket": k[10],
                "reg_year": k[11],
                "total": v,
                "updated_at": now,
            }
        )
    db.execute(
        text(
            """
            INSERT INTO car_counts (
                region, country, brand, model, color, engine_type, transmission,
                body_type, drive_type, price_bucket, mileage_bucket, reg_year, total, updated_at
            )
            VALUES (
                :region, :country, :brand, :model, :color, :engine_type, :transmission,
                :body_type, :drive_type, :price_bucket, :mileage_bucket, :reg_year, :total, :updated_at
            )
            """
        ),
        values,
    )
    db.commit()
    return len(values)


def _report(db: Session) -> None:
    rows = db.execute(text("SELECT region, SUM(total) FROM car_counts GROUP BY region ORDER BY region")).all()
    print("car_counts by region:")
    for region, total in rows:
        print(f"  {region}: {int(total or 0)}")
    rows = db.execute(text("SELECT country, SUM(total) FROM car_counts GROUP BY country ORDER BY SUM(total) DESC LIMIT 10")).all()
    print("car_counts top countries:")
    for country, total in rows:
        print(f"  {country}: {int(total or 0)}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--report", action="store_true")
    args = parser.parse_args()

    with SessionLocal() as db:
        count = refresh_counts(db)
        print(f"car_counts rows={count}")
        if args.report:
            _report(db)


if __name__ == "__main__":
    main()
