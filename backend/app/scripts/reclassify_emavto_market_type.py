from __future__ import annotations

import argparse
import time
from datetime import datetime

from sqlalchemy import select, update

from backend.app.db import SessionLocal
from backend.app.models import Car
from backend.app.parsing.config import load_sites_config
from backend.app.parsing.emavto_klg import EmAvtoKlgParser
from backend.app.schema_bootstrap import ensure_runtime_schema
from backend.app.services.parsing_data_service import ParsingDataService


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Fast list-only reclassification of existing emavto KR cars into domestic/import"
    )
    ap.add_argument("--chunk-pages", type=int, default=100, help="List pages per chunk")
    ap.add_argument("--pause-sec", type=int, default=5, help="Pause between chunks")
    ap.add_argument("--start-page", type=int, default=1, help="Start page number")
    ap.add_argument("--total-pages", type=int, default=0, help="Optional hard page limit, 0 = until end")
    ap.add_argument("--max-runtime-sec", type=int, default=7200, help="Max runtime per chunk")
    ap.add_argument("--min-price-usd", type=int, default=5000, help="Filter too-cheap ads consistently with runner")
    ap.add_argument(
        "--reset-first",
        action="store_true",
        help="Clear current kr_market_type for emavto rows before reclassification",
    )
    args = ap.parse_args()

    ensure_runtime_schema()
    sites = load_sites_config()
    cfg = sites["emavto_klg"]
    parser = EmAvtoKlgParser(cfg)

    with SessionLocal() as db:
        ds = ParsingDataService(db)
        source = ds.ensure_source(
            key=cfg.key,
            name=cfg.name,
            country=cfg.country,
            base_url=cfg.base_search_url,
        )

        if args.reset_first:
            reset_stmt = (
                update(Car)
                .where(Car.source_id == source.id)
                .values(kr_market_type=None)
            )
            result = db.execute(reset_stmt)
            db.commit()
            print(
                f"[reclassify_emavto_market_type] reset kr_market_type rows={int(result.rowcount or 0)}",
                flush=True,
            )

        total_matched_existing = 0
        total_updated = 0
        total_unmatched_live = 0
        total_seen_live = 0
        chunk_no = 0
        current_page = max(1, int(args.start_page))
        pages_left = int(args.total_pages or 0)
        started_at = time.time()

        while True:
            if pages_left > 0 and pages_left <= 0:
                break
            pages_this = int(args.chunk_pages)
            if pages_left > 0:
                pages_this = min(pages_this, pages_left)
            if pages_this <= 0:
                break

            chunk_no += 1
            profile = {
                "mode": "incremental",
                "resume_page_full": current_page,
                "max_pages": pages_this,
                "max_runtime_sec": int(args.max_runtime_sec),
                "min_price_usd": int(args.min_price_usd),
                "skip_details": True,
            }
            parser.fetch_items(profile)
            tasks = list(parser.last_list_tasks or [])
            pages_done = int(getattr(parser, "last_pages_processed", 0) or 0)
            reached_end = bool(getattr(parser, "last_reached_end", False))
            stop_reason = str(getattr(parser, "last_stop_reason", "") or "")

            live_map = {
                str(task.get("external_id")): str(task.get("kr_market_type") or "").strip() or None
                for task in tasks
                if task.get("external_id")
            }
            ext_ids = list(live_map.keys())
            total_seen_live += len(ext_ids)

            matched_existing = 0
            updated = 0
            if ext_ids:
                rows = db.execute(
                    select(Car).where(Car.source_id == source.id, Car.external_id.in_(ext_ids))
                ).scalars().all()
                matched_existing = len(rows)
                total_unmatched_live += max(len(ext_ids) - matched_existing, 0)
                now = datetime.utcnow()
                for car in rows:
                    market_type = live_map.get(car.external_id)
                    payload = car.source_payload if isinstance(car.source_payload, dict) else {}
                    payload = dict(payload)
                    payload_changed = False
                    if car.kr_market_type != market_type:
                        car.kr_market_type = market_type
                        updated += 1
                    if payload.get("kr_market_type") != market_type:
                        payload["kr_market_type"] = market_type
                        payload_changed = True
                    if payload.get("kr_market_type_source") != "emavto_tab":
                        payload["kr_market_type_source"] = "emavto_tab"
                        payload_changed = True
                    if payload_changed:
                        car.source_payload = payload
                        updated += 1
                    car.last_seen_at = now
                db.commit()

            total_matched_existing += matched_existing
            total_updated += updated
            elapsed = max(time.time() - started_at, 1.0)
            rate = total_seen_live / elapsed if total_seen_live else 0.0
            print(
                f"[reclassify_emavto_market_type] chunk={chunk_no} page={current_page} pages={pages_this} "
                f"pages_done={pages_done} live_seen={len(ext_ids)} matched_existing={matched_existing} "
                f"updated={updated} total_live_seen={total_seen_live} rate={rate:.2f}/s "
                f"stop_reason={stop_reason or '-'} reached_end={int(reached_end)}",
                flush=True,
            )

            if reached_end:
                break
            if stop_reason in {"error", "deadline"}:
                break
            if pages_done <= 0:
                break

            current_page += pages_done
            if pages_left > 0:
                pages_left -= pages_done
                if pages_left <= 0:
                    break
            if args.pause_sec > 0:
                time.sleep(args.pause_sec)

        domestic = (
            db.execute(
                select(Car.id).where(
                    Car.source_id == source.id,
                    Car.is_available.is_(True),
                    Car.kr_market_type == "domestic",
                )
            )
            .scalars()
            .all()
        )
        imported = (
            db.execute(
                select(Car.id).where(
                    Car.source_id == source.id,
                    Car.is_available.is_(True),
                    Car.kr_market_type == "import",
                )
            )
            .scalars()
            .all()
        )
        nulls = (
            db.execute(
                select(Car.id).where(
                    Car.source_id == source.id,
                    Car.is_available.is_(True),
                    Car.kr_market_type.is_(None),
                )
            )
            .scalars()
            .all()
        )
        print(
            f"[reclassify_emavto_market_type] done total_live_seen={total_seen_live} "
            f"matched_existing={total_matched_existing} updated={total_updated} unmatched_live={total_unmatched_live} "
            f"db_available_domestic={len(domestic)} db_available_import={len(imported)} db_available_null={len(nulls)}",
            flush=True,
        )


if __name__ == "__main__":
    main()
