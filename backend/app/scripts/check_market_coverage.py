"""Compare a slice of our DB against an external listing source.

Practical use: the operator suspects we are missing inventory (e.g.
"mobile.de shows 680 Porsche Cayenne 2025+ ≤1000 km hybrid+diesel, but
our catalog shows 28 — is that real?"). This script answers that with
hard numbers: it counts cars matching the same brand/model/year/mileage
filter under several different combinations of source-of-truth fields
(``engine_type`` only, with the JSONB payload fallback, deeper title /
description matches, alternative spellings, country breakdown, etc.).

It does NOT call mobile.de — comparing to their displayed total is the
operator's job. The script's purpose is to surface every reason why we
might be undercounting and tell the operator which lever to pull.

Examples:

  # Default: Porsche Cayenne, 2025+, mileage ≤ 1000, hybrid|diesel
  docker compose exec -T web python -m backend.app.scripts.check_market_coverage

  # Custom slice
  docker compose exec -T web python -m backend.app.scripts.check_market_coverage \
      --brand Porsche --model Cayenne --year-min 2025 --mileage-max 1000 \
      --fuels hybrid diesel
"""

from __future__ import annotations

import argparse
import os

from sqlalchemy import and_, cast, func, or_, select
from sqlalchemy.dialects.postgresql import JSONB

from ..db import SessionLocal
from ..models import Car, Source
from ..services.cars_service import CarsService


def _print_section(title: str) -> None:
    print()
    print("─" * 72)
    print(title)
    print("─" * 72)


def _row_table(title: str, rows: list[tuple]) -> None:
    if not rows:
        print(f"{title}: (пусто)")
        return
    print(f"{title}:")
    width = max((len(str(r[0])) for r in rows), default=0) + 2
    for label, value in rows:
        print(f"  {str(label):<{width}}{value}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--brand", default="Porsche")
    parser.add_argument("--model", default="Cayenne")
    parser.add_argument("--year-min", type=int, default=2025)
    parser.add_argument("--year-max", type=int, default=None)
    parser.add_argument("--mileage-max", type=int, default=1000)
    parser.add_argument("--mileage-min", type=int, default=None)
    parser.add_argument(
        "--fuels",
        nargs="+",
        default=["hybrid", "diesel"],
        help="Канонические значения engine_type (по нашей нормализации)",
    )
    parser.add_argument("--country", default=None, help="Если указан — ограничить (например DE)")
    parser.add_argument("--show-samples", type=int, default=10)
    args = parser.parse_args()

    fuels = [f.lower() for f in args.fuels]

    with SessionLocal() as db:
        svc = CarsService(db)
        brand_lc = args.brand.strip()
        model_lc = args.model.strip()

        base_filters = [
            Car.is_available.is_(True),
            func.lower(Car.brand) == brand_lc.lower(),
            func.lower(Car.model) == model_lc.lower(),
        ]

        if args.year_min is not None:
            year_expr = func.coalesce(Car.registration_year, Car.year)
            base_filters.append(year_expr >= args.year_min)
        if args.year_max is not None:
            year_expr = func.coalesce(Car.registration_year, Car.year)
            base_filters.append(year_expr <= args.year_max)
        if args.mileage_max is not None:
            base_filters.append(Car.mileage.isnot(None))
            base_filters.append(Car.mileage <= args.mileage_max)
        if args.mileage_min is not None:
            base_filters.append(Car.mileage >= args.mileage_min)
        if args.country:
            base_filters.append(func.upper(Car.country) == args.country.upper())

        # ─── 1. Брэнд+модель без фильтра топлива (для контекста)
        no_fuel_total = db.execute(
            select(func.count(Car.id)).where(and_(*base_filters))
        ).scalar_one()

        _print_section(
            f"Срез: {brand_lc} {model_lc}, год≥{args.year_min}, "
            f"пробег≤{args.mileage_max}, страна={args.country or 'любая'}"
        )
        print(f"Всего без фильтра по топливу: {no_fuel_total}")

        # ─── 2. С фильтром топлива (быстрый режим: только stored engine_type)
        fuel_clauses_fast = [svc._fuel_filter_clause(f) for f in fuels]  # type: ignore[attr-defined]
        fast_total = db.execute(
            select(func.count(Car.id)).where(and_(*base_filters, or_(*fuel_clauses_fast)))
        ).scalar_one()
        print(f"С фильтром топлива (FUEL_FILTER_DEEP_SCAN=0, по умолчанию): {fast_total}")

        # ─── 3. С deep-режимом (включаем JSONB payload + BEV-hint)
        os.environ["FUEL_FILTER_DEEP_SCAN"] = "1"
        try:
            fuel_clauses_deep = [svc._fuel_filter_clause(f) for f in fuels]  # type: ignore[attr-defined]
            deep_total = db.execute(
                select(func.count(Car.id)).where(and_(*base_filters, or_(*fuel_clauses_deep)))
            ).scalar_one()
        finally:
            os.environ.pop("FUEL_FILTER_DEEP_SCAN", None)
        print(f"С фильтром топлива (FUEL_FILTER_DEEP_SCAN=1, JSONB+regex): {deep_total}")

        diff = deep_total - fast_total
        if diff:
            print(f"  → разница в {diff} машин — это машины без stored engine_type, "
                  f"топливо у них только в JSONB payload")

        # ─── 4. Сколько вообще машин с пустым engine_type в этом срезе
        empty_engine = db.execute(
            select(func.count(Car.id)).where(
                and_(*base_filters, or_(Car.engine_type.is_(None), func.trim(Car.engine_type) == ""))
            )
        ).scalar_one()
        print(f"С пустым cars.engine_type в этом срезе: {empty_engine}")

        # ─── 5. Распределение по engine_type (что именно у нас лежит)
        rows = db.execute(
            select(func.lower(func.trim(Car.engine_type)), func.count(Car.id))
            .where(and_(*base_filters))
            .group_by(func.lower(func.trim(Car.engine_type)))
            .order_by(func.count(Car.id).desc())
        ).all()
        _row_table("Распределение engine_type (без фильтра)", rows)

        # ─── 6. Распределение по странам
        rows = db.execute(
            select(func.upper(Car.country), func.count(Car.id))
            .where(and_(*base_filters))
            .group_by(func.upper(Car.country))
            .order_by(func.count(Car.id).desc())
        ).all()
        _row_table("Распределение по странам (без фильтра)", rows)

        # ─── 7. Распределение по источникам
        rows = db.execute(
            select(Source.key, func.count(Car.id))
            .join(Source, Source.id == Car.source_id)
            .where(and_(*base_filters))
            .group_by(Source.key)
            .order_by(func.count(Car.id).desc())
        ).all()
        _row_table("Распределение по парсерам", rows)

        # ─── 8. Распределение по году+мощности
        rows = db.execute(
            select(
                func.coalesce(Car.registration_year, Car.year),
                func.count(Car.id),
            )
            .where(and_(*base_filters))
            .group_by(func.coalesce(Car.registration_year, Car.year))
            .order_by(func.coalesce(Car.registration_year, Car.year))
        ).all()
        _row_table("Распределение по годам выпуска/регистрации", rows)

        # ─── 9. Сколько подсвечивает payload-LIKE по hybrid|diesel
        payload_json = cast(Car.source_payload, JSONB)
        payload_text = func.lower(
            func.coalesce(
                func.nullif(func.jsonb_extract_path_text(payload_json, "full_fuel_type"), ""),
                func.nullif(func.jsonb_extract_path_text(payload_json, "envkv_engine_type"), ""),
                func.nullif(func.jsonb_extract_path_text(payload_json, "envkv_consumption_fuel"), ""),
                func.nullif(func.jsonb_extract_path_text(payload_json, "fuel_raw"), ""),
                func.nullif(func.jsonb_extract_path_text(payload_json, "engine_raw"), ""),
                "",
            )
        )
        terms = []
        for f in fuels:
            if f == "hybrid":
                terms.extend(["hybrid", "гибрид"])
            elif f == "diesel":
                terms.extend(["diesel", "дизель"])
            elif f == "phev":
                terms.extend(["plug-in", "phev"])
            elif f == "electric":
                terms.extend(["electric", "elektro", "электро"])
            else:
                terms.append(f)
        payload_match = or_(*[payload_text.like(f"%{t}%") for t in terms])
        payload_only = db.execute(
            select(func.count(Car.id))
            .where(and_(*base_filters, or_(Car.engine_type.is_(None), func.trim(Car.engine_type) == "")))
            .where(payload_match)
        ).scalar_one()
        print(f"\nДополнительно ловится по JSONB payload (среди тех, у кого engine_type пуст): {payload_only}")

        # ─── 10. Примеры записей
        if args.show_samples:
            sample_rows = db.execute(
                select(Car.id, Car.country, Car.year, Car.registration_year, Car.mileage,
                       Car.engine_type, Car.source_url)
                .where(and_(*base_filters, or_(*fuel_clauses_fast)))
                .limit(args.show_samples)
            ).all()
            _print_section(f"Первые {args.show_samples} машин в срезе (быстрый режим)")
            for r in sample_rows:
                cid, country, year, reg_year, mileage, fuel, url = r
                print(f"  #{cid:>7}  {country}  reg={reg_year or year}  km={mileage}  fuel='{fuel}'  {url}")

        # ─── 11. Детектор «мусорных» значений engine_type
        polluted_fragments = ("based on", "co2", "co₂", "emission", "consumption", "combined")
        polluted_clauses = [
            func.lower(Car.engine_type).like(f"%{frag}%") for frag in polluted_fragments
        ]
        polluted_in_slice = db.execute(
            select(func.count(Car.id)).where(and_(*base_filters, or_(*polluted_clauses)))
        ).scalar_one()

        # ─── 12. Возможные причины расхождения
        _print_section("Возможные причины расхождения с внешним источником")
        causes = []
        if polluted_in_slice > 0:
            causes.append(
                f"{polluted_in_slice} машин в срезе имеют 'мусорный' engine_type "
                f"(disclaimer-текст 'Based on CO₂ emissions...' и т.п.) — они невидимы "
                f"для фильтра топлива. Запустите чистку: "
                f"`python -m backend.app.scripts.cleanup_bad_engine_type --report` "
                f"и затем `--apply`."
            )
        if empty_engine > 0:
            causes.append(
                f"{empty_engine} машин в срезе вообще без stored engine_type — попадают "
                f"только в deep-mode (FUEL_FILTER_DEEP_SCAN=1), который дороже."
            )
        kr_in_slice = db.execute(
            select(func.count(Car.id)).where(
                and_(*base_filters, func.upper(Car.country).like("KR%"))
            )
        ).scalar_one()
        if kr_in_slice:
            causes.append(f"{kr_in_slice} машин в срезе из Кореи — они не показываются в EU-каталоге")
        if not causes:
            causes.append("по нашим данным разрыв реально большой — проверьте свежесть парсера")
        for c in causes:
            print(f"  • {c}")


if __name__ == "__main__":
    main()
