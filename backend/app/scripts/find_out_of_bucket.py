from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from typing import Optional

from backend.app.db import SessionLocal
from backend.app.models import Car
from backend.app.services.calculator_runtime import _calc_age_months, is_bev
from backend.app.services.customs_config import get_customs_config


def _age_bucket(car: Car) -> str:
    reg_year = car.registration_year or car.year
    reg_month = car.registration_month or 1
    if is_bev(car.engine_cc, car.power_kw, car.power_hp, car.engine_type):
        return "electric"
    age_months = _calc_age_months(reg_year, reg_month) if reg_year else None
    if age_months is None:
        return "unknown"
    return "under_3" if age_months < 36 else "3_5"


def _pick_table(cfg, age_bucket: str, cc: int):
    buckets = cfg.util_cc_buckets
    min_cc = min(b.from_cc for b in buckets)
    max_cc = max(b.to_cc for b in buckets)
    bucket = None
    for b in buckets:
        if b.from_cc <= cc <= b.to_cc:
            bucket = b
            break
    tables = cfg.util_tables
    if age_bucket == "under_3" and cfg.util_tables_under3:
        tables = cfg.util_tables_under3
    elif age_bucket == "3_5" and cfg.util_tables_3_5:
        tables = cfg.util_tables_3_5
    elif age_bucket == "electric" and cfg.util_tables_electric:
        tables = cfg.util_tables_electric
    return bucket, tables, min_cc, max_cc


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--country", default=None)
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--out", default="/app/artifacts/out_of_bucket.csv")
    args = ap.parse_args()

    cfg = get_customs_config()
    total = flagged = 0

    with SessionLocal() as db:
        q = db.query(Car).filter(Car.is_available.is_(True))
        if args.country:
            q = q.filter(Car.country == args.country.upper())
        q = q.order_by(Car.id.asc())

        with open(args.out, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(
                [
                    "id",
                    "country",
                    "engine_cc",
                    "power_kw",
                    "power_hp",
                    "age_bucket",
                    "reason",
                ]
            )

            offset = 0
            while True:
                rows = q.offset(offset).limit(args.batch).all()
                if not rows:
                    break
                for car in rows:
                    total += 1
                    reasons = []
                    cc = car.engine_cc or 0
                    age_bucket = _age_bucket(car)
                    if age_bucket != "electric" and cc <= 0:
                        reasons.append("missing_engine_cc")
                    bucket, tables, min_cc, max_cc = _pick_table(cfg, age_bucket, max(cc, 0))
                    if cc < min_cc:
                        reasons.append(f"cc_below_min({cc}<{min_cc})")
                    if cc > max_cc:
                        reasons.append(f"cc_above_max({cc}>{max_cc})")
                    if bucket is None:
                        reasons.append("cc_bucket_not_found")
                    # power range check
                    use_kw = car.power_kw is not None and float(car.power_kw) > 0
                    val = float(car.power_kw) if use_kw else float(car.power_hp or 0)
                    table = tables.get(bucket.table) if bucket else None
                    if table:
                        rows_tbl = table.kw if use_kw else table.hp
                        min_val = min(r.from_ for r in rows_tbl)
                        max_val = max(r.to for r in rows_tbl)
                        if val < min_val:
                            reasons.append(f"power_below_min({val}<{min_val})")
                        if val > max_val:
                            reasons.append(f"power_above_max({val}>{max_val})")
                    else:
                        reasons.append("util_table_missing")

                    if reasons:
                        flagged += 1
                        writer.writerow(
                            [
                                car.id,
                                car.country,
                                car.engine_cc,
                                car.power_kw,
                                car.power_hp,
                                age_bucket,
                                ";".join(reasons),
                            ]
                        )
                offset += args.batch
                if args.limit and total >= args.limit:
                    break

    print(
        f"[out_of_bucket] total_checked={total} flagged={flagged} out={args.out} at={datetime.now(timezone.utc).isoformat()}"
    )


if __name__ == "__main__":
    main()
