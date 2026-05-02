"""Compare what's in the daily mobile.de CSV with what's in the DB.

Answers the operator's question "do we have right values in the DB?"
by sampling N rows from the CSV, looking each one up in cars by
external_id, and checking field-by-field consistency for the columns
that drive public-facing filters (engine_type, body_type,
transmission, mileage, registration_year, price_eur).

For each field we count:
  * match     — value in DB equals what canonicalisation would produce
                from the CSV row. This is the success metric.
  * mismatch  — both sides have a value but they disagree. These are
                the cars that show wrong filter values to users.
  * csv_only  — CSV has a value, DB has NULL (parser drop, fixable).
  * db_only   — DB has a value the CSV does not (DB-side enrichment
                like manual edits, normally fine).
  * both_null — neither side has it.

Coverage section also reports how many CSV rows we could not find in
the DB at all (= broken import) and how many we found but they're
flagged inactive (= deactivation-bug fallout).

Usage::

    docker compose exec -T web python -m backend.app.scripts.audit_csv_vs_db \\
        --file /app/tmp/mobilede_active_offers_2026-05-02.csv \\
        --sample 5000
"""

from __future__ import annotations

import argparse
import random
import re
from collections import Counter
from typing import Any, Optional

from sqlalchemy import select

from ..db import SessionLocal
from ..importing.mobilede_csv import MobileDeCsvRow, iter_mobilede_csv_rows
from ..models import Car, Source
from ..utils.engine_type import canonicalize_engine_type


_BODY_MAPPING = {
    "estatecar": "wagon",
    "van": "van",
    "limousine": "sedan",
    "smallcar": "hatchback",
    "offroad": "suv",
    "cabrio": "cabrio",
}


def _norm_body(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    r = raw.strip().lower()
    if not r:
        return None
    return _BODY_MAPPING.get(r, r)


def _norm_transmission(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    t = raw.strip().lower()
    if not t:
        return None
    if "auto" in t:
        return "automatic"
    if "schalt" in t or "manual" in t:
        return "manual"
    return t


_REG_YEAR_RE = re.compile(r"(19|20)\d{2}")


def _norm_year_from_csv(row: MobileDeCsvRow) -> Optional[int]:
    if row.first_registration:
        m = _REG_YEAR_RE.search(str(row.first_registration))
        if m:
            try:
                return int(m.group(0))
            except ValueError:
                pass
    return row.year


def _norm_engine(row: MobileDeCsvRow) -> Optional[str]:
    """Apply the same fuel canonicalisation that the parser uses."""

    for candidate in (row.envkv_engine_type, row.envkv_consumption_fuel,
                      row.engine_type, row.full_fuel_type):
        result = canonicalize_engine_type(candidate)
        if result:
            return result
    return None


def _str_eq(a: Any, b: Any) -> bool:
    """Case-insensitive equality on stripped lowercased strings."""

    sa = (str(a).strip().lower() if a is not None and str(a).strip() else None)
    sb = (str(b).strip().lower() if b is not None and str(b).strip() else None)
    return sa == sb


def _classify(csv_val: Any, db_val: Any) -> str:
    csv_present = csv_val is not None and csv_val != "" and not (
        isinstance(csv_val, str) and not csv_val.strip())
    db_present = db_val is not None and db_val != "" and not (
        isinstance(db_val, str) and not db_val.strip())
    if csv_present and db_present:
        return "match" if _str_eq(csv_val, db_val) else "mismatch"
    if csv_present and not db_present:
        return "csv_only"
    if db_present and not csv_present:
        return "db_only"
    return "both_null"


def _classify_numeric(csv_val: Optional[float], db_val: Optional[float],
                      tolerance: float = 0.01) -> str:
    """Numeric comparison with relative tolerance for floats."""

    if csv_val is None and db_val is None:
        return "both_null"
    if csv_val is not None and db_val is None:
        return "csv_only"
    if csv_val is None and db_val is not None:
        return "db_only"
    try:
        a = float(csv_val)
        b = float(db_val)
    except (TypeError, ValueError):
        return "mismatch"
    denom = max(abs(a), abs(b), 1.0)
    return "match" if abs(a - b) / denom <= tolerance else "mismatch"


def _scan_csv_for_sample(file_path: str, sample_size: int,
                         seed: int = 42) -> tuple[int, list[MobileDeCsvRow]]:
    """Reservoir-sample N rows from the CSV in a single pass.

    We do reservoir sampling because the CSV has 1.5M+ rows and we
    only want a small representative sample, but we cannot know the
    total in advance without a separate scan.
    """

    rng = random.Random(seed)
    reservoir: list[MobileDeCsvRow] = []
    total = 0
    for row in iter_mobilede_csv_rows(file_path):
        total += 1
        if len(reservoir) < sample_size:
            reservoir.append(row)
        else:
            j = rng.randint(0, total - 1)
            if j < sample_size:
                reservoir[j] = row
    return total, reservoir


def _lookup_cars(db, source_key: str, inner_ids: list[str]) -> dict[str, Car]:
    src_id = db.execute(
        select(Source.id).where(Source.key == source_key)
    ).scalar_one_or_none()
    if src_id is None:
        raise SystemExit(f"Source '{source_key}' not found in DB")
    found: dict[str, Car] = {}
    chunk = 1000
    for i in range(0, len(inner_ids), chunk):
        batch = inner_ids[i:i + chunk]
        rows = db.execute(
            select(Car).where(
                Car.source_id == src_id,
                Car.external_id.in_(batch),
            )
        ).scalars().all()
        for car in rows:
            found[car.external_id] = car
    return found


def _examples(samples: list[tuple[Any, Any, str]], n: int = 5) -> list[str]:
    """Pick up to n representative mismatch lines."""

    out: list[str] = []
    for csv_val, db_val, ext_id in samples[:n]:
        out.append(f"    CSV={csv_val!r:<20}  DB={db_val!r:<20}  inner_id={ext_id}")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare daily mobile.de CSV with what's in the DB."
    )
    parser.add_argument("--file", required=True, help="Path to mobilede_active_offers.csv")
    parser.add_argument("--sample", type=int, default=5000,
                        help="How many random rows to compare (default: 5000)")
    parser.add_argument("--source-key", default="mobile_de",
                        help="Source.key value (default: mobile_de)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility")
    args = parser.parse_args()

    print(f">>> Sampling {args.sample} rows from {args.file} ...", flush=True)
    total_csv, sample = _scan_csv_for_sample(args.file, args.sample, args.seed)
    print(f">>> CSV total rows: {total_csv}, sampled: {len(sample)}", flush=True)

    inner_ids = [r.inner_id for r in sample if r.inner_id]
    with SessionLocal() as db:
        found = _lookup_cars(db, args.source_key, inner_ids)

    in_db = len(found)
    not_in_db = len(inner_ids) - in_db
    inactive = sum(1 for c in found.values() if not c.is_available)
    active = in_db - inactive

    print()
    print("─" * 72)
    print("Coverage — насколько CSV-данные представлены в БД")
    print("─" * 72)
    print(f"  CSV rows total            {total_csv:>10}")
    print(f"  Sampled                   {len(inner_ids):>10}")
    print(f"  Found in DB (active)      {active:>10}  ({active / max(len(inner_ids), 1):.1%})")
    print(f"  Found in DB (inactive)    {inactive:>10}  ({inactive / max(len(inner_ids), 1):.1%})")
    print(f"  NOT in DB at all          {not_in_db:>10}  ({not_in_db / max(len(inner_ids), 1):.1%})")
    if not_in_db:
        print("    → эти inner_id есть в свежем CSV, но импорт их не подхватил.")
        print("      Если число > 0 — это баг в pipeline (битая строка / DB не успела).")
    if inactive:
        print("    → эти машины есть и в CSV, и в БД, но помечены is_available=false.")
        print("      Это и есть deactivation-bug fallout — после фикса должно стремиться к 0.")

    fields = ("engine_type", "body_type", "transmission", "mileage",
              "registration_year", "price_eur")
    counters = {f: Counter() for f in fields}
    examples: dict[str, list[tuple[Any, Any, str]]] = {f: [] for f in fields}

    for row in sample:
        car = found.get(row.inner_id)
        if not car:
            continue

        csv_engine = _norm_engine(row)
        kind = _classify(csv_engine, car.engine_type)
        counters["engine_type"][kind] += 1
        if kind == "mismatch" and len(examples["engine_type"]) < 50:
            examples["engine_type"].append((csv_engine, car.engine_type, row.inner_id))

        csv_body = _norm_body(row.body_type)
        kind = _classify(csv_body, car.body_type)
        counters["body_type"][kind] += 1
        if kind == "mismatch" and len(examples["body_type"]) < 50:
            examples["body_type"].append((csv_body, car.body_type, row.inner_id))

        csv_trans = _norm_transmission(row.transmission)
        db_trans = (str(car.transmission).strip().lower()
                    if car.transmission else None)
        kind = _classify(csv_trans, db_trans)
        counters["transmission"][kind] += 1
        if kind == "mismatch" and len(examples["transmission"]) < 50:
            examples["transmission"].append((csv_trans, car.transmission, row.inner_id))

        kind = _classify_numeric(row.km_age, car.mileage, tolerance=0.05)
        counters["mileage"][kind] += 1
        if kind == "mismatch" and len(examples["mileage"]) < 50:
            examples["mileage"].append((row.km_age, car.mileage, row.inner_id))

        csv_year = _norm_year_from_csv(row)
        kind = _classify_numeric(csv_year, car.registration_year)
        counters["registration_year"][kind] += 1
        if kind == "mismatch" and len(examples["registration_year"]) < 50:
            examples["registration_year"].append(
                (csv_year, car.registration_year, row.inner_id))

        csv_price = (float(row.price_eur) if row.price_eur is not None else None)
        db_price = (float(car.price_eur) if getattr(car, "price_eur", None) is not None
                    else float(car.price) if car.price and (car.currency or "").upper() == "EUR"
                    else None)
        kind = _classify_numeric(csv_price, db_price, tolerance=0.02)
        counters["price_eur"][kind] += 1
        if kind == "mismatch" and len(examples["price_eur"]) < 50:
            examples["price_eur"].append((csv_price, db_price, row.inner_id))

    print()
    print("─" * 72)
    print("Field consistency — для машин, которые есть в обоих местах")
    print("─" * 72)
    print(f"  {'field':<20} {'match':>10} {'mismatch':>10} {'csv_only':>10} "
          f"{'db_only':>10} {'both_null':>10}")
    for f in fields:
        c = counters[f]
        with_data = sum(c.values()) - c["both_null"]
        match_ratio = c["match"] / with_data if with_data else 0.0
        print(f"  {f:<20} {c['match']:>10} {c['mismatch']:>10} "
              f"{c['csv_only']:>10} {c['db_only']:>10} {c['both_null']:>10}    "
              f"({match_ratio:.1%} match среди заполненных)")

    print()
    print("─" * 72)
    print("Топ-5 примеров расхождений по каждому полю")
    print("─" * 72)
    for f in fields:
        if examples[f]:
            print(f"\n  {f}:")
            for line in _examples(examples[f], n=5):
                print(line)
        else:
            print(f"\n  {f}: расхождений нет")


if __name__ == "__main__":
    main()
