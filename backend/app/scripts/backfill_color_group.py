import argparse
from datetime import datetime

from backend.app.db import SessionLocal
from backend.app.models import Car
from backend.app.utils.color_groups import normalize_color_group


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", type=int, default=1000)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--only-missing", action="store_true", default=False)
    args = parser.parse_args()

    updated = 0
    scanned = 0
    with SessionLocal() as db:
        q = db.query(Car).order_by(Car.id)
        if args.only_missing:
            q = q.filter(Car.color_group.is_(None))
        if args.limit:
            q = q.limit(args.limit)

        batch = []
        for car in q.yield_per(args.batch):
            scanned += 1
            group = normalize_color_group(car.color, car.color_hex)
            if car.color_group != group:
                car.color_group = group
                batch.append(car)
                updated += 1
            if len(batch) >= args.batch:
                db.commit()
                batch.clear()
        if batch:
            db.commit()

    print(
        "[backfill_color_group] scanned=%d updated=%d at=%s"
        % (scanned, updated, datetime.utcnow().isoformat())
    )


if __name__ == "__main__":
    main()
