from __future__ import annotations

import argparse
from typing import Dict, Any, List

from sqlalchemy import select

from ..db import SessionLocal
from ..importing.mobilede_csv import iter_mobilede_csv_rows
from ..models import Car, Source
from ..parsing.config import load_sites_config
from ..parsing.mobile_de_feed import MobileDeFeedParser


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Backfill mobile.de source_payload from CSV without reimporting all fields."
    )
    ap.add_argument(
        "--file",
        required=True,
        help="Path to mobilede_active_offers.csv (inside container)",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit of CSV rows to process",
    )
    ap.add_argument(
        "--all",
        action="store_true",
        help="Update payload even when it already exists",
    )
    args = ap.parse_args()

    cfg = load_sites_config().get("mobile_de")
    if not cfg:
        raise SystemExit("mobile_de config not found")
    parser = MobileDeFeedParser(cfg)

    db = SessionLocal()
    try:
        source = db.execute(select(Source).where(Source.key == cfg.key)).scalar_one_or_none()
        if not source:
            raise SystemExit("mobile_de source not found in DB")

        updated = 0
        seen = 0
        batch: List[Dict[str, Any]] = []
        BATCH_SIZE = 500

        def flush_batch() -> None:
            nonlocal updated, batch
            if not batch:
                return
            ext_ids = [item["external_id"] for item in batch]
            cars = db.execute(
                select(Car)
                .where(Car.source_id == source.id, Car.external_id.in_(ext_ids))
            ).scalars().all()
            car_by_id = {c.external_id: c for c in cars}
            for item in batch:
                car = car_by_id.get(item["external_id"])
                if not car:
                    continue
                if not args.all and car.source_payload:
                    continue
                car.source_payload = item["payload"]
                updated += 1
            db.commit()
            batch = []

        for row in iter_mobilede_csv_rows(args.file):
            seen += 1
            payload = parser._payload_from_row(row)
            batch.append({"external_id": str(row.inner_id), "payload": payload})
            if len(batch) >= BATCH_SIZE:
                flush_batch()
            if args.limit and seen >= args.limit:
                break

        flush_batch()
        print(f"Backfill complete: seen={seen}, updated={updated}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
