from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from pathlib import Path

from sqlalchemy import and_, or_

from backend.app.db import SessionLocal
from backend.app.models.car import Car
from backend.app.services.cars_service import CarsService
from backend.app.utils.filter_values import split_csv_values


def _has_minimum_data_expr():
    # Minimal set so calculator can build a meaningful RUB total:
    # source price/currency + registration/year context.
    return and_(
        Car.price.is_not(None),
        Car.currency.is_not(None),
        or_(Car.registration_year.is_not(None), Car.year.is_not(None)),
    )


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Recalculate cars with missing cached total price where enough input data exists."
    )
    ap.add_argument("--region", default="EU", help="EU|KR|RU|ALL")
    ap.add_argument("--country", default=None, help="Country code, e.g. DE")
    ap.add_argument("--engine-type", default=None, help="Optional normalized fuel filter, e.g. electric")
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--sleep", type=float, default=0.0, help="Sleep between batches (sec)")
    ap.add_argument(
        "--only-missing-total",
        action="store_true",
        help="Compatibility flag: this script always recalculates only rows with missing total.",
    )
    ap.add_argument("--report-json", default=None, help="Optional report path")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    checked = 0
    updated = 0
    failed = 0
    started = time.time()

    with SessionLocal() as db:
        svc = CarsService(db)
        q = db.query(Car).filter(
            Car.is_available.is_(True),
            Car.total_price_rub_cached.is_(None),
            _has_minimum_data_expr(),
        )

        reg = (args.region or "").upper().strip()
        ctry = (args.country or "").upper().strip()
        if ctry:
            q = q.filter(Car.country == ctry)
        elif reg == "KR":
            q = q.filter(Car.country.like("KR%"))
        elif reg == "EU":
            q = q.filter(~Car.country.like("KR%"), Car.country != "RU")
        elif reg == "RU":
            q = q.filter(Car.country == "RU")
        if args.engine_type:
            engine_clauses = []
            for raw_engine in split_csv_values(args.engine_type):
                clause = svc._fuel_filter_clause(raw_engine)
                if clause is not None:
                    engine_clauses.append(clause)
            if engine_clauses:
                q = q.filter(or_(*engine_clauses))

        total_candidates = q.count()
        print(f"[recalc_missing_prices] candidates={total_candidates} dry_run={args.dry_run}", flush=True)

        last_id = 0
        while True:
            rows = (
                q.filter(Car.id > last_id)
                .order_by(Car.id.asc())
                .limit(max(1, int(args.batch)))
                .all()
            )
            if not rows:
                break

            for car in rows:
                last_id = max(last_id, int(car.id))
                checked += 1
                if args.limit and checked > args.limit:
                    break
                if args.dry_run:
                    updated += 1
                    continue
                try:
                    svc.ensure_calc_cache(car, force=True)
                    if car.total_price_rub_cached is not None:
                        updated += 1
                except Exception:
                    failed += 1

            if not args.dry_run:
                db.commit()

            if checked % 200 == 0:
                elapsed = time.time() - started
                print(
                    f"[recalc_missing_prices] checked={checked} updated={updated} failed={failed} "
                    f"elapsed={elapsed:.1f}s",
                    flush=True,
                )

            if args.limit and checked >= args.limit:
                break
            if args.sleep > 0:
                time.sleep(args.sleep)

        elapsed = time.time() - started
        print(
            f"[recalc_missing_prices] done checked={checked} updated={updated} failed={failed} "
            f"at={datetime.utcnow().isoformat()} elapsed={elapsed:.1f}s",
            flush=True,
        )

        if args.report_json:
            report = {
                "generated_at": datetime.utcnow().isoformat(),
                "region": args.region,
                "country": args.country,
                "batch": int(args.batch),
                "limit": args.limit,
                "dry_run": bool(args.dry_run),
                "checked": checked,
                "updated": updated,
                "failed": failed,
                "elapsed_sec": round(elapsed, 2),
            }
            path = Path(args.report_json)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
