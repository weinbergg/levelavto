"""Backfill ``cars.engine_type`` from JSONB payload / variant / URL hints.

Background: the public catalog uses the *fast* fuel filter by default
(``FUEL_FILTER_DEEP_SCAN=0``) which only inspects the indexed
``cars.engine_type`` column. Listings imported with a missing or empty
``engine_type`` are therefore invisible to the hybrid / diesel / petrol /
electric chips even though the underlying mobile.de payload often has the
full fuel info in ``envkv_consumption_fuel`` / ``full_fuel_type`` and the
URL slug carries a hint (``e-hybrid``, ``-tdi-``, etc.).

This complements ``cleanup_bad_engine_type`` (which fixes *polluted*
disclaimer text) by populating *empty* values. Together they bring
``cars.engine_type`` close to a 99 % coverage of what mobile.de shows.

Usage::

    docker compose exec -T web python -m backend.app.scripts.backfill_engine_type --report
    docker compose exec -T web python -m backend.app.scripts.backfill_engine_type --apply

Filters (optional, all combinable):

    --brand BMW              # only this brand
    --model X5               # only this model
    --year-min 2025          # only registration_year (or year) >= 2025
    --mileage-max 1000       # only mileage <= 1000
    --country DE             # only this country
"""

from __future__ import annotations

import argparse
from collections import Counter
from typing import List

from sqlalchemy import and_, func, or_, select

from ..db import SessionLocal
from ..models import Car
from ..utils.redis_cache import bump_dataset_version
from .cleanup_bad_engine_type import _derive_from_car


def _build_where(args: argparse.Namespace):
    clauses = [
        Car.is_available.is_(True),
        or_(Car.engine_type.is_(None), func.trim(Car.engine_type) == ""),
    ]
    if args.brand:
        clauses.append(func.lower(Car.brand) == args.brand.strip().lower())
    if args.model:
        clauses.append(func.lower(Car.model) == args.model.strip().lower())
    if args.year_min is not None:
        year_expr = func.coalesce(Car.registration_year, Car.year)
        clauses.append(year_expr >= args.year_min)
    if args.year_max is not None:
        year_expr = func.coalesce(Car.registration_year, Car.year)
        clauses.append(year_expr <= args.year_max)
    if args.mileage_max is not None:
        clauses.append(Car.mileage.isnot(None))
        clauses.append(Car.mileage <= args.mileage_max)
    if args.country:
        clauses.append(func.upper(Car.country) == args.country.strip().upper())
    return and_(*clauses)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Write changes (default: dry-run)")
    parser.add_argument("--report", action="store_true", help="Show distribution of recovered fuels")
    parser.add_argument("--limit", type=int, default=0, help="Stop after N rows (0 = all)")
    parser.add_argument(
        "--chunk",
        type=int,
        default=2000,
        help="Process candidates in chunks of this size (default 2000)",
    )
    parser.add_argument("--brand", default=None)
    parser.add_argument("--model", default=None)
    parser.add_argument("--year-min", type=int, default=None)
    parser.add_argument("--year-max", type=int, default=None)
    parser.add_argument("--mileage-max", type=int, default=None)
    parser.add_argument("--country", default=None)
    args = parser.parse_args()

    mode = "APPLY (writes will be committed)" if args.apply else "DRY-RUN (no writes)"
    print(f">>> Backfill engine_type — {mode}", flush=True)

    where_clause = _build_where(args)

    with SessionLocal() as db:
        total_count = db.execute(
            select(func.count()).select_from(Car).where(where_clause)
        ).scalar_one()
        print(f">>> Кандидатов в БД (engine_type IS NULL/''): {total_count}", flush=True)

        candidate_ids: List[int] = list(
            db.execute(
                select(Car.id).where(where_clause).order_by(Car.id)
            ).scalars()
        )

    if args.limit:
        candidate_ids = candidate_ids[: args.limit]

    total_seen = 0
    total_recovered = 0
    recovered_into: Counter[str] = Counter()

    chunk_size = max(1, int(args.chunk))
    next_progress_at = chunk_size

    for offset in range(0, len(candidate_ids), chunk_size):
        batch_ids = candidate_ids[offset : offset + chunk_size]
        if not batch_ids:
            break
        with SessionLocal() as db:
            cars = list(
                db.execute(select(Car).where(Car.id.in_(batch_ids))).scalars()
            )
            for car in cars:
                total_seen += 1
                new_value = _derive_from_car(car)
                if not new_value:
                    continue
                total_recovered += 1
                recovered_into[new_value] += 1
                if args.apply:
                    car.engine_type = new_value
            if args.apply:
                db.commit()
        if total_seen >= next_progress_at:
            print(
                f"   ... осмотрено {total_seen}/{len(candidate_ids)}, "
                f"восстановлено {total_recovered}",
                flush=True,
            )
            next_progress_at += chunk_size

    print(f"\nКандидатов осмотрено: {total_seen}")
    print(f"Восстановлено: {total_recovered}")
    if args.report or args.apply:
        print("\nРаспределение восстановленных значений:")
        for value, n in recovered_into.most_common():
            print(f"  {n:>6}  {value}")

    if args.apply:
        print(
            f"\nПрименено: {total_recovered} строк обновлено.",
            flush=True,
        )
        if total_recovered > 0:
            try:
                new_ver = bump_dataset_version()
                print(
                    f"Версия датасета поднята до {new_ver} — все версионированные "
                    "кэши (Redis + in-process) автоматически инвалидируются.",
                    flush=True,
                )
            except Exception as exc:
                print(
                    "ВНИМАНИЕ: не удалось поднять dataset_version в Redis "
                    f"({exc!r}). Сделайте `redis-cli FLUSHDB` или рестарт web "
                    "вручную, чтобы пользователи увидели изменения.",
                    flush=True,
                )
    else:
        print(
            "\nЭто был dry-run. Запустите с --apply, чтобы сохранить изменения.",
            flush=True,
        )


if __name__ == "__main__":
    main()
