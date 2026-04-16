from __future__ import annotations

import argparse
import time
from typing import Iterable

from sqlalchemy import cast, func, or_, select
from sqlalchemy.dialects.postgresql import JSONB

from backend.app.db import SessionLocal
from backend.app.models import Car, Source
from backend.app.parsing.config import load_sites_config
from backend.app.parsing.emavto_klg import EmAvtoKlgParser
from backend.app.services.parsing_data_service import ParsingDataService


def _chunked(items: list[Car], size: int) -> Iterable[list[Car]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _build_task(car: Car) -> dict:
    return {
        "external_id": car.external_id,
        "source_url": car.source_url,
        "brand": car.brand,
        "model": car.model,
        "year": car.year,
        "mileage": car.mileage,
        "price": car.price,
        "engine_type": car.engine_type,
        "thumbnail_url": car.thumbnail_url,
    }


def _merge_source_payload(existing_payload: object, parsed_payload: object) -> dict:
    merged = dict(existing_payload) if isinstance(existing_payload, dict) else {}
    if isinstance(parsed_payload, dict):
        merged.update(parsed_payload)
    merged.pop("registration_defaulted", None)
    merged.pop("registration_year_defaulted", None)
    merged.pop("registration_month_defaulted", None)
    merged.pop("registration_default_year", None)
    merged.pop("registration_default_month", None)
    return merged


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--car-id", type=int, action="append", default=[], help="Local car.id to refresh")
    ap.add_argument("--limit", type=int, default=200, help="How many KR cars to refresh")
    ap.add_argument("--batch", type=int, default=20, help="Detail fetch batch size")
    ap.add_argument("--max-runtime-sec", type=int, default=2400, help="Overall runtime budget")
    ap.add_argument(
        "--include-missing-engine-cc",
        action="store_true",
        help="also refresh ICE rows with missing engine_cc so KR util fee can be recalculated",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    sites = load_sites_config()
    cfg = sites.get("emavto_klg")
    parser = EmAvtoKlgParser(cfg)

    with SessionLocal() as db:
        ds = ParsingDataService(db)
        source = db.execute(select(Source).where(Source.key == cfg.key)).scalar_one_or_none()
        if not source:
            print("[refresh_emavto_registration] source emavto_klg not found", flush=True)
            return

        base = db.query(Car).filter(Car.source_id == source.id, Car.country.like("KR%"))
        if args.car_id:
            base = base.filter(Car.id.in_(args.car_id))
        else:
            payload_json = cast(Car.source_payload, JSONB)
            defaulted_expr = (
                func.coalesce(
                    func.jsonb_extract_path_text(payload_json, "registration_defaulted"),
                    "false",
                )
                == "true"
            )
            filters = [
                defaulted_expr,
                Car.registration_year.is_(None),
                Car.registration_month.is_(None),
            ]
            if args.include_missing_engine_cc:
                filters.append(
                    (Car.engine_cc.is_(None))
                    & (func.lower(func.coalesce(Car.engine_type, "")) != "electric")
                )
            base = base.filter(or_(*filters))

        cars = base.order_by(Car.id.asc()).limit(max(1, int(args.limit))).all()
        print(
            f"[refresh_emavto_registration] selected={len(cars)} batch={args.batch} runtime={args.max_runtime_sec}s",
            flush=True,
        )
        if args.dry_run or not cars:
            return

        started = time.monotonic()
        processed = 0
        refreshed = 0
        skipped = 0

        for batch_no, batch in enumerate(_chunked(cars, max(1, int(args.batch))), start=1):
            remaining = int(args.max_runtime_sec - (time.monotonic() - started))
            if remaining <= 0:
                print("[refresh_emavto_registration] deadline reached", flush=True)
                break

            tasks = [_build_task(car) for car in batch if car.source_url]
            if not tasks:
                skipped += len(batch)
                processed += len(batch)
                continue

            parsed_items = parser.fetch_missing_details(tasks, max_runtime_sec=remaining)
            parsed_by_external_id = {
                item.external_id: item
                for item in parsed_items
            }

            payloads = []
            for car in batch:
                processed += 1
                parsed = parsed_by_external_id.get(car.external_id)
                if not parsed:
                    skipped += 1
                    continue
                payload = parsed.as_dict()
                payload["source_payload"] = _merge_source_payload(
                    car.source_payload,
                    payload.get("source_payload"),
                )
                payloads.append(payload)

            if not payloads:
                print(
                    f"[refresh_emavto_registration] batch={batch_no} processed={processed} refreshed={refreshed} skipped={skipped}",
                    flush=True,
                )
                continue

            inserted, updated, _ = ds.upsert_parsed_items(source, payloads)
            refreshed += inserted + updated
            print(
                f"[refresh_emavto_registration] batch={batch_no} processed={processed} refreshed={refreshed} skipped={skipped}",
                flush=True,
            )

        print(
            f"[refresh_emavto_registration] done processed={processed} refreshed={refreshed} skipped={skipped}",
            flush=True,
        )


if __name__ == "__main__":
    main()
