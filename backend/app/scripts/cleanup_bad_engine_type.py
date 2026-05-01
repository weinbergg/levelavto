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


def _classify_text(text: Optional[str]) -> Optional[str]:
    """Same canonical-label mapping the patched parser uses."""

    val = (text or "").strip().lower()
    if not val:
        return None
    if any(noise in val for noise in _DISCLAIMER_FRAGMENTS):
        return None
    if "diesel" in val or re.search(r"\btdi\b", val):
        return "Diesel"
    if (
        "e-hybrid" in val
        or "e-hyb" in val
        or "e-hibri" in val
        or "phev" in val
        or "plug-in" in val
        or "plug in" in val
        or "hybrid" in val
    ):
        return "Hybrid"
    if (
        "electric" in val
        or "elektro" in val
        or re.search(r"\bev\b", val)
        or re.search(r"\beq[a-z]\b", val)
    ):
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


def _derive_from_car(car: Car) -> Optional[str]:
    """Try every available text field on the car to find a real fuel hint.

    Looks first at the structured payload columns (the same ones the parser
    consults), then at the human-readable variant / model / URL slug —
    Porsche's Cayenne 2026 listings carry the e-hybrid hint in the URL
    even when ``envkv.consumption_fuel`` is the disclaimer.
    """

    payload = car.source_payload if isinstance(car.source_payload, dict) else {}
    for key in (
        "envkv_consumption_fuel",
        "full_fuel_type",
        "envkv_engine_type",
        "fuel_raw",
        "engine_raw",
    ):
        label = _classify_text(payload.get(key))
        if label:
            return label

    hint_sources = [
        car.variant,
        payload.get("sub_title"),
        car.model,
        payload.get("title"),
        car.source_url,
        car.description,
    ]
    for source in hint_sources:
        label = _classify_text(source)
        if label:
            return label
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
    parser.add_argument(
        "--chunk",
        type=int,
        default=2000,
        help="Stream candidates in chunks of this size (default 2000)",
    )
    parser.add_argument(
        "--commit-every",
        type=int,
        default=2000,
        help="Commit and clear the SQLAlchemy identity map every N processed rows",
    )
    parser.add_argument(
        "--include-numeric",
        action="store_true",
        help="Also consider numeric-only engine_type (e.g. '2.0') as polluted. Off by default — too noisy.",
    )
    args = parser.parse_args()

    mode = "APPLY (writes will be committed)" if args.apply else "DRY-RUN (no writes)"
    print(f">>> Cleanup engine_type — {mode}", flush=True)

    with SessionLocal() as db:
        # Cheap, narrow pre-filter — only disclaimer fragments, so the
        # candidate set is small (a few hundred / thousand) instead of
        # the whole 700k+ table. The previous version added a numeric-only
        # regex which matched every car with an integer engine_type and
        # blew the candidate count up to 728k, choking memory before any
        # progress was made.
        like_clauses = [func.lower(Car.engine_type).like(f"%{frag}%") for frag in _DISCLAIMER_FRAGMENTS]
        if args.include_numeric:
            like_clauses.append(
                Car.engine_type.op("~")(r"^[ \t]*[0-9]+([.,][0-9]+)?[ \t]*$")
            )
        where_clause = or_(*like_clauses)
        candidates_stmt = (
            select(Car)
            .where(where_clause)
            .order_by(Car.id)
            .execution_options(yield_per=args.chunk)
        )

        total_count = db.execute(
            select(func.count()).select_from(Car).where(where_clause)
        ).scalar_one()
        print(f">>> Кандидатов в БД: {total_count}", flush=True)

        total_seen = 0
        total_polluted = 0
        total_recovered = 0
        total_blanked = 0
        pending_writes = 0
        per_value: Counter[str] = Counter()
        recovered_into: Counter[str] = Counter()

        # Stream rows so we don't load the entire candidate set into memory.
        # ``yield_per`` requires execution within ``stream_results=True``
        # context, which the execution option above already enables.
        for car in db.execute(candidates_stmt).yield_per(args.chunk).scalars():
            total_seen += 1
            if not _looks_polluted(car.engine_type):
                continue
            total_polluted += 1
            per_value[(car.engine_type or "").strip().lower()] += 1
            new_value = _derive_from_car(car)
            if new_value:
                total_recovered += 1
                recovered_into[new_value] += 1
            else:
                total_blanked += 1
            if args.apply:
                car.engine_type = new_value
                pending_writes += 1
                if pending_writes >= args.commit_every:
                    db.commit()
                    db.expire_all()
                    pending_writes = 0
            if total_seen % 500 == 0:
                print(
                    f"   ... осмотрено {total_seen}, найдено загрязнённых {total_polluted} "
                    f"(восстановлено {total_recovered}, в NULL {total_blanked})",
                    flush=True,
                )
            if args.limit and total_seen >= args.limit:
                break

        if args.apply and pending_writes:
            db.commit()
            db.expire_all()
            pending_writes = 0

        print(f"\nКандидатов осмотрено: {total_seen}")
        print(f"Признаны загрязнёнными: {total_polluted}")
        print(f"Восстановлено из payload/variant: {total_recovered}")
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
            print(
                f"\n✅ Применено: {total_polluted} строк обновлено "
                f"({total_recovered} восстановлено, {total_blanked} -> NULL).",
                flush=True,
            )
        else:
            print(
                "\n⚠ Это был dry-run. Запустите с --apply, чтобы сохранить изменения.",
                flush=True,
            )


if __name__ == "__main__":
    main()
