from __future__ import annotations

import argparse
import os
import time
from typing import Optional

from ..db import SessionLocal
from ..parsing.config import load_sites_config
from ..parsing.emavto_klg import EmAvtoKlgParser
from ..services.parsing_data_service import ParsingDataService


def run_chunk(
    parser: EmAvtoKlgParser,
    ds: ParsingDataService,
    source,
    start_page: int,
    pages: int,
    max_runtime_sec: int,
    backfill_missing: bool = False,
    mode: str = "full",
) -> tuple[int, int, int, int, int, int]:
    profile = {
        "mode": mode,
        "resume_page_full": start_page,
        "pages": pages,
        "max_pages": pages,  # ensure fetch_items limits list pages to this chunk
        "max_runtime_sec": max_runtime_sec,
        "details": None,
    }
    # mode override for incremental
    if hasattr(parser, "config") and getattr(parser, "config", None):
        profile["mode"] = profile.get("mode", "full")
    items = parser.fetch_items(profile)
    missing = len(parser.missing_tasks or [])
    initial_missing = max(0, getattr(parser, "last_tasks_total", 0) - getattr(parser, "last_details_done", 0))
    if backfill_missing and missing:
        backfill_items = parser.fetch_missing_details(
            parser.missing_tasks, max_runtime_sec=max_runtime_sec // 2
        )
        items.extend(backfill_items)
        missing = len(parser.missing_tasks or [])

    inserted, updated, _ = ds.upsert_parsed_items(source, [c.as_dict() for c in items])
    if getattr(parser, "progress", None) and "last_page_full" in parser.progress:
        last_page = int(parser.progress["last_page_full"])
    else:
        last_page = start_page + pages - 1
    ds.set_progress(f"{parser.config.key}.last_page_full", str(last_page))
    return len(items), inserted, updated, last_page, missing, parser.last_tasks_total


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Chunked runner for emavto with pause and resume"
    )
    ap.add_argument("--chunk-pages", type=int, default=10,
                    help="How many list pages per chunk (default: 10)")
    ap.add_argument("--pause-sec", type=int, default=60,
                    help="Pause between chunks in seconds (default: 60)")
    ap.add_argument("--total-pages", type=int, default=None,
                    help="Total pages to process (optional, 0 = until stop)")
    ap.add_argument("--start-page", type=int, default=None,
                    help="Start page; if not set, uses progress kv (last_page_full+1)")
    ap.add_argument("--max-runtime-sec", type=int, default=1800,
                    help="Max runtime per chunk in seconds (default: 1800)")
    ap.add_argument("--stop-file", type=str, default="/tmp/emavto_stop",
                    help="Path to stop file; if exists before a chunk, runner stops")
    ap.add_argument("--mode", type=str, default="full",
                    choices=["full", "incremental"], help="Run mode: full or incremental")
    ap.add_argument("--backfill-missing", action="store_true",
                    help="After main loop, try to доfetch missing details within half runtime")
    args = ap.parse_args()

    sites = load_sites_config()
    cfg = sites.get("emavto_klg")
    parser = EmAvtoKlgParser(cfg)

    db = SessionLocal()
    try:
        ds = ParsingDataService(db)
        source = ds.ensure_source(
            key=cfg.key, name=cfg.name, country=cfg.country, base_url=cfg.base_search_url
        )
        total_pages_left: Optional[int] = (
            args.total_pages if args.total_pages and args.total_pages > 0 else None
        )

        start_page = args.start_page
        if start_page is None:
            last = ds.get_progress(f"{cfg.key}.last_page_full")
            try:
                start_page = int(last) + \
                    1 if last else cfg.pagination.start_page
            except ValueError:
                start_page = cfg.pagination.start_page

        current_page = start_page
        while True:
            if os.path.exists(args.stop_file):
                print(
                    f"[runner] stop file present ({args.stop_file}), exiting")
                break
            if total_pages_left is not None and total_pages_left <= 0:
                print("[runner] total pages limit reached, exiting")
                break

            pages_this = args.chunk_pages
            if total_pages_left is not None:
                pages_this = min(pages_this, total_pages_left)

            print(
                f"[runner] chunk start page={current_page} pages={pages_this}")
            chunk_items, inserted, updated, last_page, missing, tasks_total = run_chunk(
                parser, ds, source, current_page, pages_this, args.max_runtime_sec, args.backfill_missing, args.mode
            )
            expected = pages_this * 50
            print(
                f"[runner] chunk done pages={pages_this} items={chunk_items} "
                f"(expected~{expected}) inserted={inserted} updated={updated} missing_after_backfill={missing} "
                f"last_page={last_page} tasks_total={tasks_total}"
            )
            if missing:
                try:
                    missing_ids = [t.get("external_id") for t in parser.missing_tasks or [] if t.get("external_id")]
                    if missing_ids:
                        with open("/tmp/emavto_missing_ids.txt", "a", encoding="utf-8") as f:
                            for mid in missing_ids:
                                f.write(str(mid) + "\n")
                        print(f"[runner] missing ids appended to /tmp/emavto_missing_ids.txt count={len(missing_ids)}")
                except Exception as e:  # noqa: BLE001
                    print(f"[runner] failed to dump missing ids: {e}")

            current_page = last_page + 1
            if total_pages_left is not None:
                total_pages_left -= pages_this

            if total_pages_left is not None and total_pages_left <= 0:
                break

            print(f"[runner] sleeping {args.pause_sec}s before next chunk...")
            time.sleep(args.pause_sec)
    except KeyboardInterrupt:
        print("[runner] interrupted, progress saved up to last completed chunk")
    finally:
        db.close()


if __name__ == "__main__":
    main()
