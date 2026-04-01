from __future__ import annotations

import argparse
import json
import time
from typing import List
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from datetime import datetime
import os
import itertools
from ..db import SessionLocal
from ..parsing.config import load_sites_config
from ..parsing.mobile_de_feed import MobileDeFeedParser
from ..importing.mobilede_csv import iter_mobilede_csv_rows
from ..services.parsing_data_service import ParsingDataService
from ..models import Source, ParserRun, ParserRunSource
from ..utils.feed_deactivation import should_deactivate_feed


def _latest_previous_seen(db, source_id: int, current_run_id: int) -> int | None:
    row = db.execute(
        select(ParserRunSource.total_seen)
        .join(ParserRun, ParserRun.id == ParserRunSource.parser_run_id)
        .where(
            ParserRunSource.source_id == source_id,
            ParserRunSource.parser_run_id != current_run_id,
            ParserRun.status == "success",
            ParserRunSource.total_seen > 0,
        )
        .order_by(ParserRun.started_at.desc())
        .limit(1)
    ).first()
    if not row:
        return None
    return int(row[0] or 0) or None


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Import mobile.de CSV feed into DB")
    ap.add_argument(
        "--file",
        required=True,
        help="Path to mobilede_active_offers.csv (e.g. /app/backend/app/imports/mobilede_active_offers.csv inside container)",
    )
    ap.add_argument("--trigger", default="manual",
                    help="Trigger string, e.g., manual")
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit of rows to import (for testing/partial load)",
    )
    ap.add_argument(
        "--skip-deactivate",
        action="store_true",
        help="Do not deactivate missing cars (useful for partial/short-lived feeds)",
    )
    ap.add_argument(
        "--deactivate-mode",
        choices=("auto", "force", "skip"),
        default=os.getenv("MOBILEDE_DEACTIVATE_MODE", "auto"),
        help="Deactivation mode: auto compares feed size with previous successful import; force always deactivates; skip disables deactivation.",
    )
    ap.add_argument(
        "--deactivate-min-ratio",
        type=float,
        default=float(os.getenv("MOBILEDE_DEACTIVATE_MIN_RATIO", "0.93")),
        help="Minimum current/previous total_seen ratio required for auto deactivation.",
    )
    ap.add_argument(
        "--deactivate-min-seen",
        type=int,
        default=int(os.getenv("MOBILEDE_DEACTIVATE_MIN_SEEN", "100000")),
        help="Minimum rows seen before auto deactivation can run.",
    )
    ap.add_argument(
        "--stats-file",
        help="Path to write JSON stats (processed/inserted/updated/deactivated/skipped/no_photos)",
        default=None,
    )
    args = ap.parse_args()

    if not os.path.isfile(args.file):
        raise FileNotFoundError(
            f"CSV file not found or not a file: {args.file}")

    cfg = load_sites_config().get("mobile_de")
    feed_parser = MobileDeFeedParser(cfg)

    db = SessionLocal()
    try:
        service = ParsingDataService(db)
        # Ensure source row
        source = service.ensure_source(
            key=cfg.key, name="mobile.de CSV feed", country=cfg.country, base_url="csv://mobile_de"
        )
        # Start run
        run = ParserRun(started_at=datetime.utcnow(),
                        trigger=args.trigger, status="partial")
        db.add(run)
        db.commit()
        db.refresh(run)

        inserted_total = updated_total = seen_total = skipped_total = 0
        batch: List[dict] = []
        BATCH_SIZE = 500
        MAX_BATCH_RETRIES = 5

        def apply_batch(items: List[dict]) -> tuple[int, int, int]:
            attempt = 0
            while True:
                try:
                    return service.upsert_parsed_items(source, items)
                except OperationalError as exc:
                    db.rollback()
                    attempt += 1
                    message = str(exc).lower()
                    if "deadlock detected" not in message or attempt > MAX_BATCH_RETRIES:
                        raise
                    delay = min(5.0, 0.5 * attempt)
                    print(
                        f"Deadlock on batch retry {attempt}/{MAX_BATCH_RETRIES}; sleeping {delay:.1f}s"
                    )
                    time.sleep(delay)

        row_iter = feed_parser.iter_parsed_from_csv(
            iter_mobilede_csv_rows(args.file))
        if args.limit:
            row_iter = itertools.islice(row_iter, args.limit)

        for parsed in row_iter:
            batch.append(parsed.as_dict())
            if len(batch) >= BATCH_SIZE:
                ins, upd, seen = apply_batch(batch)
                inserted_total += ins
                updated_total += upd
                seen_total += seen
                batch.clear()
        if batch:
            ins, upd, seen = apply_batch(batch)
            inserted_total += ins
            updated_total += upd
            seen_total += seen

        deactivated = 0
        deactivate_mode = "skip" if args.skip_deactivate or os.getenv("MOBILEDE_SKIP_DEACTIVATE") == "1" else args.deactivate_mode
        previous_seen = _latest_previous_seen(db, source.id, run.id)
        allow_deactivate, deactivate_reason = should_deactivate_feed(
            mode=deactivate_mode,
            current_seen=seen_total,
            previous_seen=previous_seen,
            min_ratio=max(0.0, min(float(args.deactivate_min_ratio), 1.0)),
            min_seen=max(0, int(args.deactivate_min_seen)),
        )
        ratio_text = "n/a"
        if previous_seen:
            ratio_text = f"{(float(seen_total) / float(previous_seen)):.4f}"
        print(
            "[mobilede_import] deactivation gate "
            f"mode={deactivate_mode} current_seen={seen_total} previous_seen={previous_seen or 'n/a'} "
            f"ratio={ratio_text} min_ratio={float(args.deactivate_min_ratio):.4f} min_seen={int(args.deactivate_min_seen)} "
            f"decision={'allow' if allow_deactivate else 'skip'} reason={deactivate_reason}",
            flush=True,
        )
        if allow_deactivate:
            deactivated = service.deactivate_missing_by_last_seen(source, run.started_at)

        # Record per-source stats
        prs = ParserRunSource(
            parser_run_id=run.id,
            source_id=source.id,
            total_seen=seen_total,
            inserted=inserted_total,
            updated=updated_total,
            deactivated=deactivated,
        )
        db.add(prs)
        # Update run
        run.status = "success"
        run.finished_at = datetime.utcnow()
        run.total_seen = seen_total
        run.inserted = inserted_total
        run.updated = updated_total
        run.deactivated = deactivated
        db.commit()

        print(
            f"Import finished: seen={seen_total}, inserted={inserted_total}, updated={updated_total}, deactivated={deactivated}"
        )
        if args.stats_file:
            stats = {
                "job": "mobilede",
                "seen": seen_total,
                "inserted": inserted_total,
                "updated": updated_total,
                "deactivated": deactivated,
                "skipped": skipped_total,
                "deactivation_allowed": allow_deactivate,
                "deactivate_mode": deactivate_mode,
                "deactivate_previous_seen": previous_seen,
                "deactivate_reason": deactivate_reason,
                "timestamp": datetime.utcnow().isoformat(),
            }
            os.makedirs(os.path.dirname(args.stats_file), exist_ok=True)
            with open(args.stats_file, "w", encoding="utf-8") as sf:
                json.dump(stats, sf, ensure_ascii=False, indent=2)
    except Exception as exc:
        db.rollback()
        print(f"Import failed: {type(exc).__name__}: {exc}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
