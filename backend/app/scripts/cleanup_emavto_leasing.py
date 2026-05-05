from __future__ import annotations

import argparse
import time
from datetime import datetime
from typing import Iterable

import httpx
from sqlalchemy import select

from backend.app.db import SessionLocal
from backend.app.models import Car, Source
from backend.app.parsing.config import load_sites_config
from backend.app.parsing.emavto_klg import EmAvtoKlgParser
from backend.app.utils.rate_limiter import TokenBucket
from backend.app.utils.redis_cache import bump_dataset_version


def _chunked(items: list[Car], size: int) -> Iterable[list[Car]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _merge_leasing_payload(existing_payload: object) -> dict:
    merged = dict(existing_payload) if isinstance(existing_payload, dict) else {}
    merged["emavto_is_leasing"] = True
    merged["emavto_skip_reason"] = "leasing"
    merged["emavto_leasing_checked_at"] = datetime.utcnow().isoformat()
    return merged


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Detect EMAVTO listings marked as leasing and deactivate or delete them"
    )
    ap.add_argument("--car-id", type=int, action="append", default=[], help="Local car.id to inspect")
    ap.add_argument("--limit", type=int, default=500, help="How many EMAVTO rows to inspect")
    ap.add_argument("--batch", type=int, default=20, help="Commit batch size")
    ap.add_argument("--max-runtime-sec", type=int, default=2400, help="Overall runtime budget")
    ap.add_argument(
        "--include-inactive",
        action="store_true",
        help="Inspect inactive rows too; default is active-only",
    )
    ap.add_argument(
        "--delete",
        action="store_true",
        help="Hard-delete matched rows instead of deactivating them",
    )
    ap.add_argument("--apply", action="store_true", help="Write changes; default is dry-run")
    args = ap.parse_args()

    sites = load_sites_config()
    cfg = sites.get("emavto_klg")
    parser = EmAvtoKlgParser(cfg)

    with SessionLocal() as db:
        source = db.execute(select(Source).where(Source.key == cfg.key)).scalar_one_or_none()
        if not source:
            print("[cleanup_emavto_leasing] source emavto_klg not found", flush=True)
            return

        base = db.query(Car).filter(Car.source_id == source.id)
        if args.car_id:
            base = base.filter(Car.id.in_(args.car_id))
        elif not args.include_inactive:
            base = base.filter(Car.is_available.is_(True))
        cars = base.order_by(Car.id.asc()).limit(max(1, int(args.limit))).all()
        print(
            f"[cleanup_emavto_leasing] selected={len(cars)} batch={args.batch} apply={int(args.apply)} delete={int(args.delete)}",
            flush=True,
        )
        if not cars:
            return

        bucket = TokenBucket(rate_per_sec=parser.detail_rps)
        deadline = time.monotonic() + max(1, int(args.max_runtime_sec))
        client = httpx.Client(
            headers={"User-Agent": parser.client.headers.get("User-Agent")},
            timeout=httpx.Timeout(10.0, read=20.0),
            follow_redirects=True,
        )

        processed = 0
        matched = 0
        deactivated = 0
        deleted = 0
        checked_batches = 0

        try:
            for batch_no, batch in enumerate(_chunked(cars, max(1, int(args.batch))), start=1):
                if time.monotonic() > deadline:
                    print("[cleanup_emavto_leasing] deadline reached", flush=True)
                    break
                checked_batches += 1
                batch_mutated = False
                for car in batch:
                    processed += 1
                    if not car.source_url:
                        continue
                    detail = parser._fetch_detail(car.source_url, bucket=bucket, client=client, deadline=deadline)
                    is_leasing = detail.get("skip_reason") == "leasing" or bool(
                        dict(detail.get("source_payload") or {}).get("emavto_is_leasing")
                    )
                    if not is_leasing:
                        continue
                    matched += 1
                    print(
                        f"[cleanup_emavto_leasing] matched car_id={car.id} external_id={car.external_id} url={car.source_url}",
                        flush=True,
                    )
                    if not args.apply:
                        continue
                    car.source_payload = _merge_leasing_payload(car.source_payload)
                    if args.delete:
                        db.delete(car)
                        deleted += 1
                    else:
                        if car.is_available:
                            car.is_available = False
                        deactivated += 1
                    batch_mutated = True

                if batch_mutated:
                    db.commit()
                print(
                    f"[cleanup_emavto_leasing] batch={batch_no} processed={processed} matched={matched} deactivated={deactivated} deleted={deleted}",
                    flush=True,
                )
        finally:
            client.close()

        if args.apply and (deactivated or deleted):
            bump_dataset_version(db)
        print(
            f"[cleanup_emavto_leasing] done batches={checked_batches} processed={processed} matched={matched} deactivated={deactivated} deleted={deleted}",
            flush=True,
        )


if __name__ == "__main__":
    main()
