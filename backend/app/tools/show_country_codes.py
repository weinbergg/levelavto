from __future__ import annotations

from sqlalchemy import select, func
from ..db import SessionLocal
from ..models import Car
from ..utils.country_map import normalize_country_code


def main() -> None:
    db = SessionLocal()
    try:
        rows = db.execute(
            select(func.upper(Car.country), func.count())
            .where(Car.country.is_not(None))
            .group_by(func.upper(Car.country))
            .order_by(func.count().desc())
            .limit(10)
        ).all()
        print("Top country codes (raw -> normalized):")
        for raw, count in rows:
            code = normalize_country_code(raw)
            print(f"  {raw} -> {code} ({count})")
    finally:
        db.close()


if __name__ == "__main__":
    main()
