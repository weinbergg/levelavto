from __future__ import annotations

import argparse
import time
from datetime import datetime

from sqlalchemy import cast, func, select
from sqlalchemy.dialects.postgresql import JSONB

from backend.app.db import SessionLocal
from backend.app.models import Car, Source
from backend.app.parsing.config import load_sites_config
from backend.app.parsing.mobile_de_feed import MobileDeFeedParser
from backend.app.services.parsing_data_service import compute_car_hash


def _build_hash_payload(car: Car) -> dict:
    return {
        "country": car.country,
        "brand": car.brand,
        "model": car.model,
        "variant": car.variant,
        "year": car.year,
        "mileage": car.mileage,
        "price": car.price,
        "currency": car.currency,
        "vin": car.vin,
        "source_url": car.source_url,
        "engine_cc": car.engine_cc,
        "power_hp": car.power_hp,
        "power_kw": car.power_kw,
        "registration_year": car.registration_year,
        "registration_month": car.registration_month,
        "description": car.description,
    }


def _clean_payload(payload: object) -> dict:
    return dict(payload) if isinstance(payload, dict) else {}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--car-id", type=int, action="append", default=[], help="Specific local car.id to refresh")
    ap.add_argument("--limit", type=int, default=50000, help="Maximum mobile.de cars to inspect")
    ap.add_argument("--batch", type=int, default=500, help="Update batch size")
    ap.add_argument("--chunk", type=int, default=50000, help="ID window size for scanning")
    ap.add_argument("--max-runtime-sec", type=int, default=3600, help="Overall runtime budget")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    cfg = load_sites_config().get("mobile_de")
    if not cfg:
        print("[refresh_mobilede_registration] mobile_de config not found", flush=True)
        return
    parser = MobileDeFeedParser(cfg)

    with SessionLocal() as db:
        source = db.execute(select(Source).where(Source.key == cfg.key)).scalar_one_or_none()
        if not source:
            print("[refresh_mobilede_registration] source mobile_de not found", flush=True)
            return

        payload_json = cast(Car.source_payload, JSONB)
        first_reg_expr = func.coalesce(
            func.jsonb_extract_path_text(payload_json, "first_registration"),
            "",
        )
        base = db.query(Car.id).filter(
            Car.source_id == source.id,
            first_reg_expr != "",
        )
        if args.car_id:
            base = base.filter(Car.id.in_(args.car_id))

        min_id = base.with_entities(func.min(Car.id)).scalar()
        max_id = base.with_entities(func.max(Car.id)).scalar()
        if min_id is None or max_id is None:
            print("[refresh_mobilede_registration] selected=0", flush=True)
            return

        print(
            f"[refresh_mobilede_registration] ids={int(min_id)}-{int(max_id)} "
            f"limit={args.limit} batch={args.batch} chunk={args.chunk} runtime={args.max_runtime_sec}s",
            flush=True,
        )
        if args.dry_run:
            return

        scanned = 0
        matched = 0
        updated = 0
        started = time.monotonic()
        window_no = 0
        start = int(min_id)
        stop_after = max(1, int(args.limit))
        batch_size = max(1, int(args.batch))
        chunk_size = max(batch_size, int(args.chunk))

        while start <= int(max_id) and scanned < stop_after:
            if time.monotonic() - started >= int(args.max_runtime_sec):
                print("[refresh_mobilede_registration] deadline reached", flush=True)
                break

            end = min(start + chunk_size - 1, int(max_id))
            window_no += 1
            ids = [
                row[0]
                for row in (
                    base.filter(Car.id.between(start, end))
                    .order_by(Car.id.asc())
                    .limit(stop_after - scanned)
                    .all()
                )
            ]
            if not ids:
                start = end + 1
                continue

            for offset in range(0, len(ids), batch_size):
                if time.monotonic() - started >= int(args.max_runtime_sec):
                    print("[refresh_mobilede_registration] deadline reached", flush=True)
                    break

                batch_ids = ids[offset : offset + batch_size]
                cars = db.query(Car).filter(Car.id.in_(batch_ids)).order_by(Car.id.asc()).all()
                for car in cars:
                    if scanned >= stop_after:
                        break
                    scanned += 1
                    payload = _clean_payload(car.source_payload)
                    raw_registration = payload.get("first_registration")
                    parsed_year, parsed_month = parser._parse_first_registration(raw_registration)
                    if parsed_year is None:
                        continue
                    matched += 1

                    changed = False
                    if car.registration_year != parsed_year:
                        car.registration_year = parsed_year
                        changed = True
                    if car.registration_month != parsed_month:
                        car.registration_month = parsed_month
                        changed = True

                    if payload.pop("registration_defaulted", None) is not None:
                        changed = True
                    if payload.pop("registration_default_year", None) is not None:
                        changed = True
                    if payload.pop("registration_default_month", None) is not None:
                        changed = True

                    if not changed:
                        continue

                    car.source_payload = payload
                    car.hash = compute_car_hash(_build_hash_payload(car))
                    car.updated_at = datetime.utcnow()
                    updated += 1

                db.commit()

            elapsed = max(time.monotonic() - started, 1.0)
            rate = scanned / elapsed if scanned else 0.0
            print(
                f"[refresh_mobilede_registration] window={window_no} ids={start}-{end} "
                f"scanned={scanned} matched={matched} updated={updated} rate={rate:.2f}/s",
                flush=True,
            )
            start = end + 1

        print(
            f"[refresh_mobilede_registration] done scanned={scanned} matched={matched} updated={updated}",
            flush=True,
        )


if __name__ == "__main__":
    main()
