from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, List, Optional

from sqlalchemy import select

from ..db import SessionLocal
from ..models.car import Car
from ..services.calc_debug import build_calc_debug


@dataclass
class CarRow:
    id: int
    registration_year: Optional[int]
    registration_month: Optional[int]
    year: Optional[int]
    engine_type: Optional[str]
    engine_cc: Optional[int]
    power_hp: Optional[float]
    power_kw: Optional[float]
    source_url: Optional[str]


def _age_months(row: CarRow) -> Optional[int]:
    year = row.registration_year or row.year
    month = row.registration_month or 1
    if not year:
        return None
    try:
        d = date(year, month, 1)
        today = date.today().replace(day=1)
        return max((today.year - d.year) * 12 + (today.month - d.month), 0)
    except Exception:
        return None


def _is_electric(row: CarRow) -> bool:
    if row.engine_type and "electric" in row.engine_type.lower():
        return True
    if row.engine_type and "ev" in row.engine_type.lower():
        return True
    return False


def _fetch_candidates(db, limit: int = 5000) -> List[CarRow]:
    stmt = (
        select(
            Car.id,
            Car.registration_year,
            Car.registration_month,
            Car.year,
            Car.engine_type,
            Car.engine_cc,
            Car.power_hp,
            Car.power_kw,
            Car.source_url,
        )
        .where(Car.is_available.is_(True))
        .order_by(Car.updated_at.desc(), Car.id.desc())
        .limit(limit)
    )
    rows = db.execute(stmt).all()
    out: List[CarRow] = []
    for r in rows:
        out.append(
            CarRow(
                id=r[0],
                registration_year=r[1],
                registration_month=r[2],
                year=r[3],
                engine_type=r[4],
                engine_cc=r[5],
                power_hp=r[6],
                power_kw=r[7],
                source_url=r[8],
            )
        )
    return out


def _pick(rows: Iterable[CarRow], predicate, n: int) -> List[CarRow]:
    out: List[CarRow] = []
    for r in rows:
        if predicate(r):
            out.append(r)
        if len(out) >= n:
            break
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--eur-rate", type=float, default=None)
    parser.add_argument("--usd-rate", type=float, default=None)
    parser.add_argument("--per-bucket", type=int, default=2)
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--out-dir", type=str, default="artifacts/calc_debug")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    with SessionLocal() as db:
        rows = _fetch_candidates(db, limit=args.limit)

        def is_under3(r: CarRow) -> bool:
            age = _age_months(r)
            return age is not None and age < 36 and not _is_electric(r) and r.engine_cc is not None

        def is_3_5(r: CarRow) -> bool:
            age = _age_months(r)
            return age is not None and 36 <= age <= 60 and not _is_electric(r) and r.engine_cc is not None

        def is_over5(r: CarRow) -> bool:
            age = _age_months(r)
            return age is not None and age > 60 and not _is_electric(r) and r.engine_cc is not None

        def is_electric(r: CarRow) -> bool:
            return _is_electric(r) and (r.power_hp is not None or r.power_kw is not None)

        buckets = {
            "under_3": _pick(rows, is_under3, args.per_bucket),
            "3_5": _pick(rows, is_3_5, args.per_bucket),
            "over_5": _pick(rows, is_over5, args.per_bucket),
            "electric": _pick(rows, is_electric, args.per_bucket),
        }

        print("[calc_debug_batch] selected:")
        for k, items in buckets.items():
            ids = [str(i.id) for i in items]
            print(f"  {k}: {', '.join(ids) if ids else 'NONE'}")

        for k, items in buckets.items():
            for r in items:
                payload = build_calc_debug(
                    db,
                    car_id=r.id,
                    eur_rate=args.eur_rate,
                    usd_rate=args.usd_rate,
                )
                path = out_dir / f"{k}_{r.id}.json"
                path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))

    print(f"[calc_debug_batch] done -> {out_dir}")


if __name__ == "__main__":
    main()
