from __future__ import annotations

import argparse
from pathlib import Path
from ..parsing.config import load_sites_config
from ..parsing.mobile_de import MobileDeParser
from ..db import SessionLocal
from sqlalchemy import select, func
from ..models import Car
from ..services.parsing_data_service import ParsingDataService


def main() -> None:
    parser = argparse.ArgumentParser(description="Offline parser for mobile.de HTML snapshots")
    parser.add_argument("--file", type=str, default="/app/tmp/mobile_de_debug.html", help="Path to HTML file")
    parser.add_argument("--insert", action="store_true", help="Insert parsed items into DB")
    args = parser.parse_args()

    path = Path(args.file)
    if not path.exists():
        print(f"File not found: {path}")
        return
    html = path.read_text(encoding="utf-8", errors="ignore")
    cfg = load_sites_config().get("mobile_de")
    p = MobileDeParser(cfg)
    items = p.parse_html(html)
    if not items:
        print(f"No items parsed — check selectors or HTML snapshot: {path}")
        if args.insert:
            print("Nothing to insert.")
        return
    print(f"Parsed {len(items)} items from offline HTML: {path}")
    # Choose the first reasonably filled item to display as a sample
    it = next((x for x in items if (x.price is not None) or (x.brand or x.model)), items[0])
    filled = sum(1 for x in items if (x.price is not None) and (x.brand is not None) and (x.model is not None))
    print(f"Filled items (brand+model+price): {filled}/{len(items)}")
    print("Sample item:")
    print(f" brand={it.brand!r}, model={it.model!r}, price={it.price} EUR, year={it.year}, mileage={it.mileage}, image={it.thumbnail_url}, link={it.source_url}")

    if args.insert:
        print("Inserting into DB...")
        db = SessionLocal()
        try:
            service = ParsingDataService(db)
            source = service.ensure_source(
                key=cfg.key,
                name=cfg.name,
                country=cfg.country,
                base_url=cfg.base_search_url,
            )
            inserted, updated, seen = service.upsert_parsed_items(source, [c.as_dict() for c in items])
            total = db.execute(
                select(func.count()).select_from(Car).where(Car.source_id == source.id)
            ).scalar_one()
            print(f"Inserted {inserted}, updated {updated}, seen {seen}, total in DB for {cfg.key}: {total}")
        finally:
            db.close()


if __name__ == "__main__":
    main()


