from __future__ import annotations

import argparse
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Iterable, Iterator

import httpx
from sqlalchemy import select

from backend.app.db import SessionLocal
from backend.app.models import Car, Source
from backend.app.parsing.config import load_sites_config
from backend.app.parsing.emavto_klg import EmAvtoKlgParser
from backend.app.utils.rate_limiter import TokenBucket
from backend.app.utils.redis_cache import bump_dataset_version


def _chunked_rows(items: list[tuple[Any, ...]], size: int) -> Iterable[list[tuple[Any, ...]]]:
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
) -> Iterator[list[tuple[Any, ...]]]:
    base_select = select(
        Car.id,
        Car.external_id,
        Car.source_url,
        Car.source_payload,
        Car.is_available,
    )
    if car_ids:
        rows = db.execute(
            base_select
            .where(Car.source_id == source_id, Car.id.in_(car_ids))
            .order_by(Car.id.asc())
        ).all()
        db.rollback()
        for chunk in _chunked_rows(list(rows), max(1, int(select_batch))):
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
        stmt = (
            base_select
            .where(Car.source_id == source_id, Car.id > last_id)
            .order_by(Car.id.asc())
            .limit(batch_limit)
        )
        if not include_inactive:
            stmt = stmt.where(Car.is_available.is_(True))
        rows = list(db.execute(stmt).all())
        db.rollback()
        if not rows:
            return
        yield rows
        last_id = int(rows[-1][0] or last_id)
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
    ap.add_argument("--workers", type=int, default=1, help="Parallel detail fetch workers per DB chunk")
    ap.add_argument("--detail-rps", type=float, default=0.0, help="Override parser detail_rps; 0 keeps parser default")
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
    if float(args.detail_rps or 0) > 0:
        parser.detail_rps = float(args.detail_rps)

    with SessionLocal() as db:
        source = db.execute(select(Source).where(Source.key == cfg.key)).scalar_one_or_none()
        if not source:
            print("[cleanup_emavto_leasing] source emavto_klg not found", flush=True)
            return

        print(
            "[cleanup_emavto_leasing] "
            f"limit={int(args.limit)} select_batch={int(args.select_batch)} batch={int(args.batch)} "
            f"start_after_id={int(args.start_after_id)} include_inactive={int(args.include_inactive)} "
            f"apply={int(args.apply)} delete={int(args.delete)} workers={max(1, int(args.workers))} "
            f"detail_rps={float(parser.detail_rps):.2f} max_runtime_sec={int(args.max_runtime_sec)}",
            flush=True,
        )

        bucket = TokenBucket(rate_per_sec=parser.detail_rps)
        deadline = None
        if int(args.max_runtime_sec) > 0:
            deadline = time.monotonic() + int(args.max_runtime_sec)

        client_tls = threading.local()

        def _get_client() -> httpx.Client:
            existing = getattr(client_tls, "client", None)
            if existing is not None:
                return existing
            created = httpx.Client(
                headers={"User-Agent": parser.client.headers.get("User-Agent")},
                timeout=httpx.Timeout(10.0, read=20.0),
                follow_redirects=True,
            )
            client_tls.client = created
            return created

        def _inspect_row(row: tuple[Any, ...]) -> tuple[int, str | None, str | None, dict] | None:
            car_id = int(row[0] or 0)
            external_id = str(row[1] or "") or None
            source_url = str(row[2] or "") or None
            existing_payload = row[3]
            if not source_url:
                return None
            detail = parser._fetch_detail(
                source_url,
                bucket=bucket,
                client=_get_client(),
                deadline=deadline,
            )
            is_leasing = detail.get("skip_reason") == "leasing" or bool(
                dict(detail.get("source_payload") or {}).get("emavto_is_leasing")
            )
            if not is_leasing:
                return None
            return car_id, external_id, source_url, _merge_leasing_payload(existing_payload)

        processed = 0
        matched = 0
        deactivated = 0
        deleted = 0
        checked_batches = 0

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
            processed += len(batch)

            matched_rows: list[tuple[int, str | None, str | None, dict]] = []
            workers = max(1, int(args.workers))
            if workers == 1:
                for row in batch:
                    result = _inspect_row(row)
                    if result is not None:
                        matched_rows.append(result)
            else:
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    futures = [pool.submit(_inspect_row, row) for row in batch]
                    for fut in as_completed(futures):
                        result = fut.result()
                        if result is not None:
                            matched_rows.append(result)

            for car_id, external_id, source_url, _ in matched_rows:
                matched += 1
                print(
                    f"[cleanup_emavto_leasing] matched car_id={car_id} external_id={external_id} url={source_url}",
                    flush=True,
                )

            if args.apply and matched_rows:
                for write_chunk in _chunked_rows(matched_rows, max(1, int(args.batch))):
                    payloads = {car_id: payload for car_id, _, _, payload in write_chunk}
                    match_ids = list(payloads.keys())
                    with SessionLocal() as write_db:
                        cars = write_db.execute(select(Car).where(Car.id.in_(match_ids))).scalars().all()
                        for car in cars:
                            car.source_payload = payloads.get(int(car.id), car.source_payload)
                            if args.delete:
                                write_db.delete(car)
                                deleted += 1
                            else:
                                if car.is_available:
                                    car.is_available = False
                                deactivated += 1
                        write_db.commit()

            print(
                f"[cleanup_emavto_leasing] batch={batch_no} last_id={int(batch[-1][0] or 0)} processed={processed} matched={matched} deactivated={deactivated} deleted={deleted}",
                flush=True,
            )

        if args.apply and (deactivated or deleted):
            bump_dataset_version()
        print(
            f"[cleanup_emavto_leasing] done batches={checked_batches} processed={processed} matched={matched} deactivated={deactivated} deleted={deleted}",
            flush=True,
        )


if __name__ == "__main__":
    main()
