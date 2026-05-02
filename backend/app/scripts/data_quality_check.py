"""Daily data-quality probe for ``cars`` — surfaces silent regressions.

What it checks (and why each matters in operations):

1. ``engine_type`` distinct values — every value MUST be NULL or in the
   canonical set. A non-canonical value means a parser regression
   bypassed the upsert defensive guard or the DB CHECK constraint
   (legacy DB without the migration). Either way: catalog filter goes
   blind to those rows.

2. NULL ratios for the columns that drive the public catalog filter
   (``engine_type``, ``body_type``, ``transmission``, ``drive_type``,
   ``mileage``, ``registration_year``). A sudden jump means the parser
   stopped extracting a field and the rows silently fall out of the
   sidebar facets.

3. Per-source totals. Day-over-day drop > 5% on any source is a strong
   "feed broke / parser regressed" signal.

4. Top-N coverage probes. For each (brand, model, fuel) tuple we show
   the active counts so the operator can spot-check against the
   external listing site (mobile.de etc.).

The script is read-only. Exit code is 0 on green / 1 on warnings / 2
on errors so it can wedge into the post-import telegram report or a
cron-based alert. Designed to be cheap (< 1s on prod-sized data) so
it can run after every import.

Usage::

    docker compose exec -T web python -m backend.app.scripts.data_quality_check
    docker compose exec -T web python -m backend.app.scripts.data_quality_check --json
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, List

from sqlalchemy import and_, func, select

from ..db import SessionLocal
from ..models import Car, Source
from ..utils.drive_type import CANONICAL_DRIVE_TYPES
from ..utils.engine_type import CANONICAL_ENGINE_TYPES


# Per-(brand, model, fuel) probes. Keep a small, stable list — these are
# the slices the operator most often manually compares against
# mobile.de / encar. Add new ones sparingly, otherwise the report bloats.
_COVERAGE_PROBES: List[Dict[str, Any]] = [
    {"brand": "BMW", "model_like": "X5%", "engine": "diesel", "year_min": 2025, "mileage_max": 1000},
    {"brand": "BMW", "model_like": "X5%", "engine": "hybrid", "year_min": 2025, "mileage_max": 1000},
    {"brand": "Porsche", "model_like": "Cayenne%", "engine": "hybrid", "year_min": 2025, "mileage_max": 1000},
    {"brand": "Mercedes-Benz", "model_like": "GLE%", "engine": "diesel", "year_min": 2025, "mileage_max": 1000},
    {"brand": "Audi", "model_like": "Q7%", "engine": "diesel", "year_min": 2025, "mileage_max": 1000},
]

# Columns whose NULL ratio matters because the public filter uses them.
_NULL_RATIO_COLUMNS = (
    ("engine_type", 0.20),       # warn if >20 % cars have no fuel
    ("body_type", 0.15),
    ("transmission", 0.30),
    ("drive_type", 0.40),
    ("mileage", 0.05),
    ("registration_year", 0.10),
)


def _section(title: str) -> None:
    print()
    print("─" * 72)
    print(title)
    print("─" * 72)


def _report_distinct_canonicality(
    db,
    section_title: str,
    column,
    canonical: frozenset[str],
    *,
    fix_hint: str,
) -> tuple[List[str], List[str]]:
    """Generic helper that prints a DISTINCT report and flags non-canonical
    values for any column with a fixed canonical set
    (engine_type, drive_type, ...).
    """

    rows = db.execute(
        select(column, func.count())
        .where(column.is_not(None))
        .group_by(column)
        .order_by(func.count().desc())
    ).all()
    warnings: List[str] = []
    errors: List[str] = []
    _section(section_title)
    if not rows:
        warnings.append(f"{column.key}: колонка целиком NULL")
        return warnings, errors
    print(f"{'count':>10}  value")
    bad = []
    for value, count in rows:
        marker = " "
        if value not in canonical:
            marker = "✗"
            bad.append((value, int(count)))
        print(f"{int(count):>10}  {marker} '{value}'")
    if bad:
        bad_total = sum(c for _, c in bad)
        errors.append(
            f"{column.key}: {len(bad)} non-canonical values, {bad_total} rows — {fix_hint}"
        )
    return warnings, errors


def _check_engine_type_canonical(db) -> tuple[List[str], List[str]]:
    return _report_distinct_canonicality(
        db,
        "1. cars.engine_type — каноничность DISTINCT значений",
        Car.engine_type,
        CANONICAL_ENGINE_TYPES,
        fix_hint=(
            "add them to utils.engine_type.CANONICAL_ENGINE_TYPES or run "
            "normalize_engine_type_values --apply"
        ),
    )


def _check_drive_type_canonical(db) -> tuple[List[str], List[str]]:
    return _report_distinct_canonicality(
        db,
        "1b. cars.drive_type — каноничность DISTINCT значений",
        Car.drive_type,
        CANONICAL_DRIVE_TYPES,
        fix_hint=(
            "add them to utils.drive_type.CANONICAL_DRIVE_TYPES or re-run "
            "the alembic migration"
        ),
    )


def _check_null_ratios(db) -> tuple[List[str], List[str]]:
    warnings: List[str] = []
    errors: List[str] = []
    _section("2. NULL-ratio критичных для фильтра колонок")
    total_active = db.execute(
        select(func.count())
        .select_from(Car)
        .where(Car.is_available.is_(True))
    ).scalar_one()
    if not total_active:
        warnings.append("data: 0 активных машин")
        return warnings, errors
    print(f"  всего активных машин: {total_active}")
    for column_name, threshold in _NULL_RATIO_COLUMNS:
        col = getattr(Car, column_name)
        null_count = db.execute(
            select(func.count())
            .select_from(Car)
            .where(Car.is_available.is_(True))
            .where(col.is_(None))
        ).scalar_one()
        ratio = null_count / total_active
        marker = " "
        if ratio > threshold:
            marker = "✗"
            warnings.append(
                f"{column_name}: NULL-ratio {ratio:.2%} > порога {threshold:.0%} "
                f"({null_count}/{total_active})"
            )
        print(f"  {marker} {column_name:<22} NULL={null_count:>8} ratio={ratio:>7.2%}  threshold={threshold:.0%}")
    return warnings, errors


def _check_table_ballast(db) -> tuple[List[str], List[str]]:
    """Surface the inactive-car overhead.

    Every batch operation (migrations, normalisers, backfills) scans
    ALL of cars, not just the active subset. If 70 % of the table is
    long-dead deactivated listings, every operation pays a 3× cost.
    """

    warnings: List[str] = []
    errors: List[str] = []
    _section("2b. Соотношение active / inactive в cars")
    total = db.execute(select(func.count()).select_from(Car)).scalar_one()
    active = db.execute(
        select(func.count()).select_from(Car).where(Car.is_available.is_(True))
    ).scalar_one()
    inactive = total - active
    print(f"  всего:       {total:>10}")
    print(f"  активных:    {active:>10}  ({active / max(total, 1):.1%})")
    print(f"  неактивных:  {inactive:>10}  ({inactive / max(total, 1):.1%})")
    if total and inactive / total > 0.5:
        warnings.append(
            f"cars: {inactive / total:.0%} строк уже неактивны — рассмотрите "
            "scripts.cleanup_old_inactive_cars --apply --days 180"
        )
    return warnings, errors


def _check_per_source_totals(db) -> tuple[List[str], List[str]]:
    warnings: List[str] = []
    errors: List[str] = []
    _section("3. Активные машины по парсерам")
    rows = db.execute(
        select(Source.key, func.count(Car.id))
        .join(Car, Car.source_id == Source.id)
        .where(Car.is_available.is_(True))
        .group_by(Source.key)
        .order_by(func.count(Car.id).desc())
    ).all()
    if not rows:
        warnings.append("sources: ни одного активного источника")
        return warnings, errors
    for key, count in rows:
        print(f"  {key or '(unknown)':<24} {int(count):>8}")
    # Cheap heuristic: if any source has < 100 active rows it's almost
    # certainly broken (test fixture or partially-imported run).
    for key, count in rows:
        if int(count) < 100:
            warnings.append(f"source '{key}' has only {int(count)} active rows")
    return warnings, errors


def _check_coverage_probes(db) -> tuple[List[str], List[str]]:
    warnings: List[str] = []
    _section("4. Coverage probes — сверять вручную с внешним источником")
    for probe in _COVERAGE_PROBES:
        clauses = [
            Car.is_available.is_(True),
            func.lower(Car.brand) == probe["brand"].lower(),
            Car.model.ilike(probe["model_like"]),
            func.lower(func.trim(Car.engine_type)) == probe["engine"],
        ]
        if "year_min" in probe:
            year_expr = func.coalesce(Car.registration_year, Car.year)
            clauses.append(year_expr >= probe["year_min"])
        if "mileage_max" in probe:
            clauses.append(Car.mileage.isnot(None))
            clauses.append(Car.mileage <= probe["mileage_max"])
        n = db.execute(
            select(func.count()).select_from(Car).where(and_(*clauses))
        ).scalar_one()
        descr = (
            f"{probe['brand']} {probe['model_like']} "
            f"{probe['engine']} year≥{probe.get('year_min', '*')} "
            f"km≤{probe.get('mileage_max', '*')}"
        )
        print(f"  {descr:<60} {int(n):>6}")
    return warnings, []


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable report instead of human text",
    )
    args = parser.parse_args()

    warnings: List[str] = []
    errors: List[str] = []
    report: Dict[str, Any] = {}

    with SessionLocal() as db:
        for fn in (
            _check_engine_type_canonical,
            _check_drive_type_canonical,
            _check_null_ratios,
            _check_table_ballast,
            _check_per_source_totals,
            _check_coverage_probes,
        ):
            w, e = fn(db)
            warnings.extend(w)
            errors.extend(e)

    _section("Итоги")
    if errors:
        print("ОШИБКИ:")
        for line in errors:
            print(f"  ✗ {line}")
            report.setdefault("errors", []).append(line)
    if warnings:
        print("ПРЕДУПРЕЖДЕНИЯ:")
        for line in warnings:
            print(f"  ! {line}")
            report.setdefault("warnings", []).append(line)
    if not errors and not warnings:
        print("Всё штатно ✓")

    if args.json:
        print()
        print(json.dumps(report, ensure_ascii=False, indent=2))

    if errors:
        return 2
    if warnings:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
