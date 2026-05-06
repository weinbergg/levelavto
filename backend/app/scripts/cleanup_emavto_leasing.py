from __future__ import annotations

import argparse
import time
from datetime import datetime
from typing import Iterable, Iterator

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


def _iter_target_chunks(
    db,
    *,
    source_id: int,
    car_ids: list[int],
    include_inactive: bool,
    select_batch: int,
    limit: int,
    start_after_id: int,
) -> Iterator[list[Car]]:
    if car_ids:
        base = db.query(Car).filter(Car.source_id == source_id, Car.id.in_(car_ids)).order_by(Car.id.asc())
        rows = base.all()
        for chunk in _chunked(rows, max(1, int(select_batch))):
            yield chunk
        return

    remaining = max(0, int(limit)) if int(limit) > 0 else None
    last_id = max(0, int(start_after_id))
    while True:
        batch_limit = max(1, int(select_batch))
        if remaining is not None:
            batch_limit = min(batch_limit, remaining)
            if batch_limit <= 0:
                return
        query = db.query(Car).filter(Car.source_id == source_id, Car.id > last_id)
        if not include_inactive:
            query = query.filter(Car.is_available.is_(True))
        rows = query.order_by(Car.id.asc()).limit(batch_limit).all()
        if not rows:
            return
        yield rows
        last_id = int(rows[-1].id or last_id)
        if remaining is not None:
            remaining -= len(rows)
            if remaining <= 0:
                return


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
    ap.add_argument("--limit", type=int, default=0, help="How many EMAVTO rows to inspect; 0 means all rows")
    ap.add_argument("--select-batch", type=int, default=200, help="How many rows to fetch from DB per cursor step")
    ap.add_argument("--batch", type=int, default=20, help="How many processed rows to group per DB commit batch")
    ap.add_argument("--start-after-id", type=int, default=0, help="Resume scan strictly after this local car.id")
    ap.add_argument("--max-runtime-sec", type=int, default=0, help="Optional runtime budget; 0 disables the limit")
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

        print(
            "[cleanup_emavto_leasing] "
            f"limit={int(args.limit)} select_batch={int(args.select_batch)} batch={int(args.batch)} "
            f"start_after_id={int(args.start_after_id)} include_inactive={int(args.include_inactive)} "
            f"apply={int(args.apply)} delete={int(args.delete)} max_runtime_sec={int(args.max_runtime_sec)}",
            flush=True,
        )

        bucket = TokenBucket(rate_per_sec=parser.detail_rps)
        deadline = None
        if int(args.max_runtime_sec) > 0:
            deadline = time.monotonic() + int(args.max_runtime_sec)
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
            for batch_no, batch in enumerate(
                _iter_target_chunks(
                    db,
                    source_id=source.id,
                    car_ids=list(args.car_id or []),
                    include_inactive=bool(args.include_inactive),
                    select_batch=max(1, int(args.select_batch)),
                    limit=int(args.limit),
                    start_after_id=int(args.start_after_id),
                ),
                start=1,
            ):
                if deadline is not None and time.monotonic() > deadline:
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
                    db.expire_all()
                print(
                    f"[cleanup_emavto_leasing] batch={batch_no} last_id={int(batch[-1].id or 0)} processed={processed} matched={matched} deactivated={deactivated} deleted={deleted}",
                    flush=True,
                )
        finally:
            client.close()

        if args.apply and (deactivated or deleted):
            bump_dataset_version()
        print(
            f"[cleanup_emavto_leasing] done batches={checked_batches} processed={processed} matched={matched} deactivated={deactivated} deleted={deleted}",
            flush=True,
        )


if __name__ == "__main__":
    main()
