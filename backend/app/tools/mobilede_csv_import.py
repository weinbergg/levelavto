from __future__ import annotations

import argparse
from typing import List
from sqlalchemy import select
from datetime import datetime
import os
from ..db import SessionLocal
from ..parsing.config import load_sites_config
from ..parsing.mobile_de_feed import MobileDeFeedParser
from ..importing.mobilede_csv import iter_mobilede_csv_rows
from ..services.parsing_data_service import ParsingDataService
from ..models import Source, ParserRun, ParserRunSource


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

        inserted_total = updated_total = seen_total = 0
        seen_external_ids: List[str] = []
        batch: List[dict] = []
        BATCH_SIZE = 500

        for parsed in feed_parser.iter_parsed_from_csv(iter_mobilede_csv_rows(args.file)):
            seen_external_ids.append(parsed.external_id)
            batch.append(parsed.as_dict())
            if len(batch) >= BATCH_SIZE:
                ins, upd, seen = service.upsert_parsed_items(source, batch)
                inserted_total += ins
                updated_total += upd
                seen_total += seen
                batch.clear()
        if batch:
            ins, upd, seen = service.upsert_parsed_items(source, batch)
            inserted_total += ins
            updated_total += upd
            seen_total += seen

        deactivated = service.deactivate_missing(source, seen_external_ids)

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
    except Exception as exc:
        db.rollback()
        print(f"Import failed: {type(exc).__name__}: {exc}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
