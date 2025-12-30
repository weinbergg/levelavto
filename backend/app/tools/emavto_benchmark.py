from __future__ import annotations

import argparse
import time
from typing import List

from ..db import SessionLocal
from ..services.parsing_data_service import ParsingDataService
from ..parsing.config import load_sites_config
from ..parsing.emavto_klg import EmAvtoKlgParser


def fmt_latency(arr: List[float]) -> str:
    if not arr:
        return "n/a"
    return f"{(sum(arr) / len(arr)) * 1000:.1f} ms"


def main() -> None:
    ap = argparse.ArgumentParser(description="Benchmark emavto parser locally")
    ap.add_argument("--pages", type=int, default=10,
                    help="Number of list pages to fetch")
    ap.add_argument("--details", type=int, default=200,
                    help="Max detail items to fetch (0 = no limit)")
    ap.add_argument("--max-runtime-sec", type=int, default=1800,
                    help="Overall run deadline in seconds (default: 1800)")
    ap.add_argument("--write-db", action="store_true",
                    help="Upsert into DB (default: no DB writes)")
    ap.add_argument("--mode", type=str,
                    choices=["full", "incremental"], default="incremental")
    ap.add_argument("--skip-details", action="store_true",
                    help="Skip detail requests (list-only)")
    ap.add_argument("--resume-page-full", type=int, default=None,
                    help="Start page for full mode (for resuming/ranges)")
    args = ap.parse_args()

    sites = load_sites_config()
    cfg = sites.get("emavto_klg")
    parser = EmAvtoKlgParser(cfg)

    profile = {
        "mode": args.mode,
        "max_pages": args.pages,
        "max_items": args.details if args.details > 0 else None,
        "skip_details": args.skip_details,
        "max_runtime_sec": args.max_runtime_sec,
    }
    if args.resume_page_full:
        profile["resume_page_full"] = args.resume_page_full

    t0 = time.monotonic()
    items = parser.fetch_items(profile)
    elapsed = time.monotonic() - t0

    inserted = updated = 0
    if args.write_db:
        db = SessionLocal()
        try:
            ds = ParsingDataService(db)
            src = ds.ensure_source(
                key=cfg.key, name=cfg.name, country=cfg.country, base_url=cfg.base_search_url
            )
            inserted, updated, _ = ds.upsert_parsed_items(
                src, [c.as_dict() for c in items])
        finally:
            db.close()

    total = len(items)
    cars_per_min = total / elapsed * 60 if elapsed > 0 else 0
    print("=== emavto benchmark ===")
    print(f"mode={args.mode}, pages={args.pages}, max_items={args.details}")
    print(
        f"fetched cars: {total}, time: {elapsed:.1f}s, rate: {cars_per_min:.1f} cars/min")
    print(
        f"list reqs={parser.metrics['list_requests']} detail reqs={parser.metrics['detail_requests']}")
    print(
        f"list 429={parser.metrics['list_429']} detail 429={parser.metrics['detail_429']}")
    print(f"avg list latency={fmt_latency(parser.metrics['list_latency'])}")
    print(
        f"avg detail latency={fmt_latency(parser.metrics['detail_latency'])}")
    if args.write_db:
        print(f"upsert: inserted={inserted}, updated={updated}")


if __name__ == "__main__":
    main()
