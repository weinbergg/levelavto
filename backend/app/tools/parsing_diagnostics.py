from __future__ import annotations

import argparse
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict
from sqlalchemy import select, func, distinct
from ..db import SessionLocal
from ..models import Source, Car, CarImage
from ..services.parser_runner import ParserRunner


def pct(n: int, d: int) -> str:
    if d == 0:
        return "0.0%"
    return f"{(n / d) * 100:.1f}%"


def numeric_range(db, field, source_id: int) -> Tuple[Optional[Any], Optional[Any]]:
    q = select(func.min(field), func.max(field)).where(
        getattr(Car, "source_id") == source_id, field.is_not(None))
    min_v, max_v = db.execute(q).one()
    return min_v, max_v


def count_not_null(db, field, source_id: int) -> int:
    q = select(func.count()).where(
        getattr(Car, "source_id") == source_id, field.is_not(None))
    return db.execute(q).scalar_one()


def count_total(db, source_id: int) -> int:
    return db.execute(select(func.count()).where(Car.source_id == source_id)).scalar_one()


def distinct_values(db, field, source_id: int, limit: int = 20) -> List[Any]:
    q = (
        select(field, func.count())
        .where(Car.source_id == source_id, field.is_not(None))
        .group_by(field)
        .order_by(func.count().desc())
        .limit(limit)
    )
    return [f"{r[0]} ({r[1]})" for r in db.execute(q).all() if r[0] is not None]


def print_source_report(db, source_key: str, trigger: str, profiles: Optional[List[int]]) -> None:
    runner = ParserRunner()
    summary = runner.run_for_source(
        source_key, trigger=trigger, search_profile_ids=profiles)
    print(f"=== Diagnostics for source: {source_key} ===")
    print(
        f"Run #{summary.run_id} status={summary.status}, trigger={summary.trigger}")
    if summary.error_message:
        print(f"Note: {summary.error_message}")
    if source_key in summary.per_source:
        stats = summary.per_source[source_key]
        print(
            f"Last run totals: total_seen={stats['total_seen']}, inserted={stats['inserted']}, updated={stats['updated']}, deactivated={stats['deactivated']}")
    else:
        print("No per-source statistics available for this run.")

    # Load Source and totals
    src = db.execute(select(Source).where(
        Source.key == source_key)).scalar_one_or_none()
    if not src:
        print("Source not found in DB; nothing to inspect.")
        return
    total = count_total(db, src.id)
    print(f"Total cars in DB for this source: {total}")
    with_images = db.execute(
        select(func.count(func.distinct(Car.id)))
        .select_from(Car)
        .join(CarImage, CarImage.car_id == Car.id)
        .where(Car.source_id == src.id)
    ).scalar_one()
    print(
        f"With images (>0): {with_images} / {total} ({pct(with_images, total)})")

    # Field completeness overview
    F = Car  # alias
    fields_info = [
        ("brand", F.brand, False),
        ("model", F.model, False),
        ("year", F.year, True),
        ("mileage", F.mileage, True),
        ("price", F.price, True),
        ("currency", F.currency, False),
        ("country", F.country, False),
        ("source_url", F.source_url, False),
        ("thumbnail_url", F.thumbnail_url, False),
        ("body_type", F.body_type, False),
        ("engine_type", F.engine_type, False),
        ("transmission", F.transmission, False),
        ("drive_type", F.drive_type, False),
        ("color", F.color, False),
        ("vin", F.vin, False),
    ]
    print("Field completeness:")
    for name, col, is_numeric in fields_info:
        filled = count_not_null(db, col, src.id)
        line = f"  {name:<13} {filled} / {total} ({pct(filled, total)})"
        if is_numeric and filled > 0:
            mn, mx = numeric_range(db, col, src.id)
            line += f", min={mn}, max={mx}"
        vals = distinct_values(db, col, src.id)
        if vals:
            line += f", top distinct={vals}"
        print(line)
    if source_key == "emavto_klg":
        print(
            "\nNote: remaining missing fields may be due to data absence in some listings or unhandled text variants. See [EMAVTO DEBUG] logs for samples.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Parsing diagnostics tool")
    parser.add_argument("--source", type=str, default="all", choices=[
                        "mobile_de", "encar", "emavto_klg", "all"], help="Which source to diagnose")
    parser.add_argument("--trigger", type=str, default="manual",
                        help="Trigger label for the run")
    parser.add_argument("--profiles", type=str, default=None,
                        help="Comma-separated profile IDs (optional)")
    args = parser.parse_args()

    profile_ids = None
    if args.profiles:
        try:
            profile_ids = [int(x.strip())
                           for x in args.profiles.split(",") if x.strip()]
        except Exception:
            profile_ids = None

    keys = ["mobile_de", "encar",
            "emavto_klg"] if args.source == "all" else [args.source]
    db = SessionLocal()
    try:
        for key in keys:
            print_source_report(db, key, args.trigger, profile_ids)
            print()
    finally:
        db.close()


if __name__ == "__main__":
    main()
