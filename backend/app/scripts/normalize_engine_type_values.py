"""Collapse ``cars.engine_type`` to canonical lowercase English values.

Why: long-running data accumulated three different conventions in the
column — Title-case English (``Diesel``, ``Hybrid``, ``Petrol`` —
written by the recent backfill), legacy Russian labels from the early
parser (``Бензин``, ``Дизель``, ``Бензин + электро``), and lowercase
English from the current parser. SQL is case-sensitive, so a query
like ``WHERE engine_type='hybrid'`` silently misses every ``Hybrid``
row, which is confusing for the operator and prone to mistakes in
ad-hoc admin scripts. The public catalog filter doesn't care (it uses
``lower(trim(engine_type))``), but the data hygiene matters.

This script is idempotent: each canonical form maps to its lowercase
English equivalent, every other value gets the same treatment via the
shared classifier. Empty / unrecognised values are left as-is.

Usage::

    docker compose exec -T web python -m backend.app.scripts.normalize_engine_type_values --report
    docker compose exec -T web python -m backend.app.scripts.normalize_engine_type_values --apply
"""

from __future__ import annotations

import argparse
from typing import Dict, List

from sqlalchemy import func, select, update

from ..db import SessionLocal
from ..models import Car
from ..utils.redis_cache import bump_dataset_version
from .cleanup_bad_engine_type import _classify_text


_CANONICAL_FORMS = {
    "diesel",
    "petrol",
    "hybrid",
    "electric",
    "lpg",
    "cng",
    "hydrogen",
    "ethanol",
}


def _build_mapping(distinct_values: List[str]) -> Dict[str, str]:
    """For each raw value present in the column, decide what to write."""

    mapping: Dict[str, str] = {}
    for raw in distinct_values:
        if raw is None:
            continue
        original = str(raw)
        stripped = original.strip()
        if not stripped:
            continue
        # Already canonical? Only fast-skip when the *exact stored bytes*
        # match — leading/trailing spaces or capital letters still need
        # to be rewritten so the column is uniform.
        canonical = _classify_text(stripped)
        if not canonical:
            continue
        if canonical not in _CANONICAL_FORMS:
            # Defensive: classifier can return values outside the strict
            # canonical set if normalize_engine_type evolves. Skip — better
            # to keep the original than to lossily rewrite to a string the
            # filter doesn't know.
            continue
        if original == canonical:
            continue
        mapping[original] = canonical
    return mapping


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Write changes (default: dry-run)")
    parser.add_argument(
        "--report",
        action="store_true",
        help="Show the per-value breakdown and what each would map to",
    )
    args = parser.parse_args()

    mode = "APPLY (writes will be committed)" if args.apply else "DRY-RUN (no writes)"
    print(f">>> Normalize engine_type — {mode}", flush=True)

    with SessionLocal() as db:
        rows = db.execute(
            select(Car.engine_type, func.count())
            .where(Car.engine_type.is_not(None))
            .where(func.length(func.trim(Car.engine_type)) > 0)
            .group_by(Car.engine_type)
            .order_by(func.count().desc())
        ).all()

        distinct_values = [row[0] for row in rows]
        counts: Dict[str, int] = {row[0]: int(row[1]) for row in rows}
        mapping = _build_mapping(distinct_values)

        if args.report or not args.apply:
            print()
            print("Текущее распределение engine_type (DISTINCT):")
            for value, n in rows:
                target = mapping.get(value)
                arrow = f" -> {target}" if target else "  (без изменений)"
                print(f"  {n:>6}  '{value}'{arrow}")
            print()

        if not mapping:
            print("Нечего нормализовать. Все значения уже каноничны.", flush=True)
            return

        affected_total = sum(counts.get(src, 0) for src in mapping)
        print(f"Будет затронуто строк: {affected_total} "
              f"(уникальных значений: {len(mapping)})", flush=True)

        if not args.apply:
            print(
                "\nЭто был dry-run. Запустите с --apply, чтобы сохранить изменения.",
                flush=True,
            )
            return

        for src, dst in mapping.items():
            res = db.execute(
                update(Car)
                .where(Car.engine_type == src)
                .values(engine_type=dst)
            )
            print(
                f"  '{src}' -> '{dst}': обновлено {res.rowcount} строк",
                flush=True,
            )
        db.commit()
        print(f"\nГотово. Всего обновлено строк: {affected_total}", flush=True)

    try:
        new_ver = bump_dataset_version()
        print(
            f"Версия датасета поднята до {new_ver} — все версионированные "
            "кэши автоматически инвалидируются.",
            flush=True,
        )
    except Exception as exc:
        print(
            "ВНИМАНИЕ: не удалось поднять dataset_version "
            f"({exc!r}). Сделайте redis-cli FLUSHDB вручную.",
            flush=True,
        )


if __name__ == "__main__":
    main()
