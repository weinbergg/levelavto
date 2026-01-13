from __future__ import annotations

from datetime import datetime
from sqlalchemy import select

from backend.app.db import SessionLocal
from backend.app.models import Car


def main() -> None:
    db = SessionLocal()
    try:
        now = datetime.utcnow()
        rows = db.execute(select(Car).where(Car.listing_date.is_(None))).scalars().all()
        updated = 0
        for car in rows:
            ts = None
            payload = car.source_payload or {}
            val = payload.get("listing_date") or payload.get("created_at")
            if isinstance(val, str):
                try:
                    ts = datetime.fromisoformat(val)
                except Exception:
                    ts = None
            elif isinstance(val, datetime):
                ts = val
            car.listing_date = ts or now
            updated += 1
        if updated:
            db.commit()
        print(f"backfill listing_date done: updated={updated}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
from __future__ import annotations

from datetime import datetime
from sqlalchemy import select
from backend.app.db import SessionLocal
from backend.app.models import Car


def main() -> None:
    db = SessionLocal()
    try:
        now = datetime.utcnow()
        rows = db.execute(select(Car).where(Car.listing_date.is_(None))).scalars().all()
        updated = 0
        for car in rows:
            # try payload.created_at
            payload = car.source_payload or {}
            val = payload.get("created_at")
            ts = None
            if isinstance(val, str):
                try:
                    ts = datetime.fromisoformat(val)
                except Exception:
                    ts = None
            car.listing_date = ts or now
            updated += 1
        if updated:
            db.commit()
        print(f"backfill done: updated={updated}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
