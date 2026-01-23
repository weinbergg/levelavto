from __future__ import annotations

from typing import Any
from datetime import datetime, timezone
import argparse
from sqlalchemy import case, func, select, text, or_, Integer
from sqlalchemy.orm import Session

from ..models import Car, Source
from ..services.cars_service import CarsService
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

    now = datetime.now(timezone.utc)
    db.execute(
        text(
            """
            TRUNCATE car_counts_core,
                     car_counts_brand,
                     car_counts_model,
                     car_counts_color,
                     car_counts_engine_type,
                     car_counts_transmission,
                     car_counts_body_type,
                     car_counts_drive_type,
                     car_counts_price_bucket,
                     car_counts_mileage_bucket,
                     car_counts_reg_year
            """
        )
    )

    base_sql = """
        SELECT
            {region_case} AS region,
            {country_case} AS country,
            cars.brand AS brand,
            cars.model AS model,
            {color_expr} AS color,
            {engine_expr} AS engine_type,
            {transmission_expr} AS transmission,
            {body_expr} AS body_type,
            {drive_expr} AS drive_type,
            {price_bucket} AS price_bucket,
            {mileage_bucket} AS mileage_bucket,
            {reg_year_expr} AS reg_year
        FROM cars
        JOIN sources ON sources.id = cars.source_id
        WHERE COALESCE(cars.is_available, true)
    """.format(
        region_case=str(region_case.compile(compile_kwargs={"literal_binds": True})),
        country_case=str(country_case.compile(compile_kwargs={"literal_binds": True})),
        color_expr=str(color_expr.compile(compile_kwargs={"literal_binds": True})),
        engine_expr=str(engine_expr.compile(compile_kwargs={"literal_binds": True})),
        transmission_expr=str(transmission_expr.compile(compile_kwargs={"literal_binds": True})),
        body_expr=str(body_expr.compile(compile_kwargs={"literal_binds": True})),
        drive_expr=str(drive_expr.compile(compile_kwargs={"literal_binds": True})),
        price_bucket=str(price_bucket.compile(compile_kwargs={"literal_binds": True})),
        mileage_bucket=str(mileage_bucket.compile(compile_kwargs={"literal_binds": True})),
        reg_year_expr=str(reg_year_expr.compile(compile_kwargs={"literal_binds": True})),
    )

    def insert_group(table: str, cols: list[str], where: str = "") -> None:
        cols_sql = ", ".join(cols)
        where_sql = f"WHERE {where}" if where else ""
        db.execute(
            text(
                f"""
                INSERT INTO {table} ({cols_sql}, total, updated_at)
                SELECT {cols_sql}, COUNT(*), :now
                FROM ({base_sql}) AS base
                {where_sql}
                GROUP BY {cols_sql}
                """
            ),
            {"now": now},
        )

    insert_group("car_counts_core", ["region", "country"])
    insert_group("car_counts_brand", ["region", "country", "brand"], "brand IS NOT NULL AND brand <> ''")
    insert_group(
        "car_counts_model",
        ["region", "country", "brand", "model"],
        "brand IS NOT NULL AND brand <> '' AND model IS NOT NULL AND model <> ''",
    )
    insert_group("car_counts_color", ["region", "country", "brand", "color"], "brand IS NOT NULL AND brand <> '' AND color IS NOT NULL AND color <> ''")
    insert_group("car_counts_engine_type", ["region", "country", "brand", "engine_type"], "brand IS NOT NULL AND brand <> '' AND engine_type IS NOT NULL AND engine_type <> ''")
    insert_group("car_counts_transmission", ["region", "country", "brand", "transmission"], "brand IS NOT NULL AND brand <> '' AND transmission IS NOT NULL AND transmission <> ''")
    insert_group("car_counts_body_type", ["region", "country", "brand", "body_type"], "brand IS NOT NULL AND brand <> '' AND body_type IS NOT NULL AND body_type <> ''")
    insert_group("car_counts_drive_type", ["region", "country", "brand", "drive_type"], "brand IS NOT NULL AND brand <> '' AND drive_type IS NOT NULL AND drive_type <> ''")
    insert_group("car_counts_price_bucket", ["region", "country", "brand", "price_bucket"], "brand IS NOT NULL AND brand <> '' AND price_bucket IS NOT NULL AND price_bucket <> ''")
    insert_group("car_counts_mileage_bucket", ["region", "country", "brand", "mileage_bucket"], "brand IS NOT NULL AND brand <> '' AND mileage_bucket IS NOT NULL AND mileage_bucket <> ''")
    insert_group("car_counts_reg_year", ["region", "country", "reg_year"], "reg_year IS NOT NULL")

    db.commit()
    count = db.execute(text("SELECT COUNT(*) FROM car_counts_core")).scalar_one()
    return int(count or 0)


def _report(db: Session) -> None:
    rows = db.execute(text("SELECT region, SUM(total) FROM car_counts_core GROUP BY region ORDER BY region")).all()
    print("car_counts by region:")
    for region, total in rows:
        print(f"  {region}: {int(total or 0)}")
    rows = db.execute(text("SELECT country, SUM(total) FROM car_counts_core GROUP BY country ORDER BY SUM(total) DESC LIMIT 10")).all()
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
