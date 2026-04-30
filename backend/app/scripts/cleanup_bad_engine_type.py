"""One-off cleanup for ``cars.engine_type`` rows polluted by mobile.de disclaimer text.

Background: until the parser fix in ``mobile_de_feed.py``, the CSV importer's
``_normalize_engine`` function passed unknown values through verbatim. mobile.de
sometimes serves regulatory disclaimer text (``"Based on CO₂ emissions
(combined)"``) instead of an actual fuel type, and that text ended up in the
``engine_type`` column on hundreds of cars. The disclaimer is not a valid fuel
type, so every car with that value was invisible to the public fuel filter
(hybrid / diesel / petrol / electric).

This script:

1. Counts how many rows look polluted (matches a hand-crafted blacklist
   plus the canonical normaliser).
2. Re-derives a canonical ``engine_type`` from the JSONB payload
   (``full_fuel_type`` / ``envkv_consumption_fuel`` / ``envkv_engine_type``)
   when possible — same logic as the patched parser.
3. Optionally writes the changes (dry-run by default).

Usage:

    docker compose exec -T web python -m backend.app.scripts.cleanup_bad_engine_type --report
    docker compose exec -T web python -m backend.app.scripts.cleanup_bad_engine_type --apply
"""

from __future__ import annotations

import argparse
import re
from collections import Counter
from typing import Optional

from sqlalchemy import or_, select, func

from ..db import SessionLocal
from ..models import Car
from ..utils.spec_inference import normalize_engine_type


_DISCLAIMER_FRAGMENTS = (
    "based on",
    "co2",
    "co₂",
    "emission",
    "consumption",
    "combined",
)


def _looks_polluted(value: Optional[str]) -> bool:
    if not value:
        return False
    val = value.strip().lower()
    if not val:
        return False
    if any(frag in val for frag in _DISCLAIMER_FRAGMENTS):
        return True
    if re.fullmatch(r"[0-9]+(?:[.,][0-9]+)?", val):
        return True
    canonical = normalize_engine_type(val)
    return canonical == "" and val not in {"hybrid", "diesel", "petrol", "electric", "lpg", "cng"}


def _derive_from_payload(payload: dict | None) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    for key in (
        "envkv_consumption_fuel",
        "full_fuel_type",
        "envkv_engine_type",
        "fuel_raw",
        "engine_raw",
    ):
        raw = payload.get(key)
        if not isinstance(raw, str):
            continue
        val = raw.strip().lower()
        if not val:
            continue
        if "diesel" in val:
            return "Diesel"
        if "hybrid" in val or "plug-in" in val or "plug in" in val or "phev" in val:
            return "Hybrid"
        if "electric" in val or re.search(r"\bev\b", val) or "elektro" in val:
            return "Electric"
        if "petrol" in val or "benzin" in val or "gasoline" in val:
            return "Petrol"
        if "lpg" in val or re.search(r"\bgpl\b", val) or "autogas" in val:
            return "LPG"
        if "cng" in val or "natural gas" in val or "erdgas" in val:
            return "CNG"
        canonical = normalize_engine_type(val)
        if canonical:
            return canonical.capitalize()
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Write changes (default: dry-run)")
    parser.add_argument(
        "--report",
        action="store_true",
        help="Print a per-value breakdown before doing anything",
    )
    parser.add_argument("--limit", type=int, default=0, help="Stop after N rows (0 = all)")
    args = parser.parse_args()

    with SessionLocal() as db:
        # Cheap pre-filter: every disclaimer fragment ends up containing
        # "based on", "co2", "co₂", "emission", "consumption", or "combined".
        # We collect candidates with an OR of LIKE patterns so the DB does
        # not have to scan the full table — engine_type is a small column
        # but the table can be large.
        like_clauses = [func.lower(Car.engine_type).like(f"%{frag}%") for frag in _DISCLAIMER_FRAGMENTS]
        # Also catch pure-numeric noise.
        like_clauses.append(Car.engine_type.op("~")(r"^[ \t]*[0-9]+([.,][0-9]+)?[ \t]*$"))
        candidates_stmt = select(Car).where(or_(*like_clauses))

        total_seen = 0
        total_polluted = 0
        total_recovered = 0
        total_blanked = 0
        per_value: Counter[str] = Counter()
        recovered_into: Counter[str] = Counter()

        for car in db.execute(candidates_stmt).scalars():
            total_seen += 1
            if not _looks_polluted(car.engine_type):
                continue
            total_polluted += 1
            per_value[(car.engine_type or "").strip().lower()] += 1
            new_value = _derive_from_payload(car.source_payload)
            if new_value:
                total_recovered += 1
                recovered_into[new_value] += 1
            else:
                total_blanked += 1
            if args.apply:
                car.engine_type = new_value
            if args.limit and total_seen >= args.limit:
                break

        if args.report or not args.apply:
            print(f"Кандидатов осмотрено: {total_seen}")
            print(f"Признаны загрязнёнными: {total_polluted}")
            print(f"Восстановлено из payload: {total_recovered}")
            print(f"Сброшено в NULL: {total_blanked}")
            print()
            print("Топ загрязнённых значений:")
            for value, n in per_value.most_common(20):
                print(f"  {n:>6}  '{value}'")
            print()
            print("Распределение восстановленных:")
            for value, n in recovered_into.most_common():
                print(f"  {n:>6}  {value}")

        if args.apply:
            db.commit()
            print(f"\n✅ Применено: {total_polluted} строк обновлено "
                  f"({total_recovered} восстановлено, {total_blanked} -> NULL).")
        else:
            print("\n⚠ Это был dry-run. Запустите с --apply, чтобы сохранить изменения.")


if __name__ == "__main__":
    main()
