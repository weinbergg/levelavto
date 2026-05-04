"""Diagnose why our count differs from mobile.de for a specific model.

Real example that prompted this script:
    Land Rover Range Rover Sport, registration year >= 2025
        our DB:    2042 cars
        mobile.de: 727 cars

Possible causes for the gap, all worth checking:
  1) Stale rows the cleanup script missed. ``is_available=true`` should
     mean the car was in the latest CSV; if not — there's residue.
  2) Duplicate ``inner_id`` (same listing imported twice). Rare but
     happens when the CSV is corrupted or the deduper is off.
  3) Non-mobile.de sources (che168, manual imports, etc.) being
     counted alongside mobile.de.
  4) Region scoping: our "от 2025" (year_min=2025) includes 2026 too;
     mobile.de's exact-match firstRegistrationDate=2025 means just 2025.
  5) Brand/model normalisation: "Range Rover Sport" might also catch
     "Range Rover Sport SVR" / "Range Rover Sport DLE" in our tables
     while mobile.de treats them as separate models.
  6) Country split: mobile.de defaults to DE-only; we count all EU
     countries we scrape.

This script breaks the count down across all six dimensions so the
operator (or a developer reading the output) can pin the gap to a
specific cause and fix it. Read-only — no writes.

Usage::

    docker compose exec -T web python -m backend.app.scripts.diagnose_count_mismatch \\
        --brand "Land Rover" --model "Range Rover Sport" --reg-year-min 2025

    # Compare with a country=DE slice (closer to default mobile.de view):
    docker compose exec -T web python -m backend.app.scripts.diagnose_count_mismatch \\
        --brand "Land Rover" --model "Range Rover Sport" \\
        --reg-year-min 2025 --country DE
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta

from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from ..db import SessionLocal
from ..models import Car, Source


def _print_section(title: str) -> None:
    print()
    print("─" * 72)
    print(f" {title}")
    print("─" * 72)


def _row(label: str, value: object, *, dim: bool = False) -> None:
    val = f"{value:,}".replace(",", " ") if isinstance(value, int) else str(value)
    if dim:
        print(f"  {label:<46} {val}")
    else:
        print(f"  {label:<46} \033[1m{val}\033[0m")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--brand", required=True, help='e.g. "Land Rover"')
    ap.add_argument("--model", required=True, help='e.g. "Range Rover Sport"')
    ap.add_argument("--reg-year-min", type=int, default=None, help="inclusive, e.g. 2025")
    ap.add_argument("--reg-year-max", type=int, default=None, help="inclusive, e.g. 2025")
    ap.add_argument("--country", default="", help="ISO country code, e.g. DE")
    args = ap.parse_args()

    brand = args.brand.strip()
    model = args.model.strip()
    reg_year_min = args.reg_year_min
    reg_year_max = args.reg_year_max
    country = (args.country or "").strip().upper()

    db: Session = SessionLocal()
    try:
        # ── 1) Headline number — what the public catalog returns. ──
        # Same predicates as CarsService.list_filtered for brand/model/
        # registration_year/country. is_available drives availability.
        base_conds = [
            Car.is_available.is_(True),
            func.lower(func.trim(Car.brand)) == brand.lower(),
            func.lower(func.trim(Car.model)) == model.lower(),
        ]
        if reg_year_min is not None:
            base_conds.append(
                func.coalesce(Car.registration_year, Car.year) >= reg_year_min
            )
        if reg_year_max is not None:
            base_conds.append(
                func.coalesce(Car.registration_year, Car.year) <= reg_year_max
            )
        if country:
            base_conds.append(Car.country == country)

        total = int(
            db.execute(select(func.count(Car.id)).where(and_(*base_conds))).scalar()
            or 0
        )

        _print_section(
            f"BRAND={brand!r} MODEL={model!r} "
            f"reg_year=[{reg_year_min or '*'}..{reg_year_max or '*'}] "
            f"country={country or '*'}"
        )
        _row("Всего активных машин (is_available=true)", total)
        if total == 0:
            print("\n(пусто — проверьте написание бренда/модели)")
            return

        # ── 2) Source breakdown (mobile.de vs others). ──
        # mobile.de is the only source we expect to match the mobile.de
        # site count. Anything else (che168, manual) should be subtracted
        # before comparing.
        _print_section("Разбивка по источнику (только mobile.de сравним с сайтом)")
        src_rows = db.execute(
            select(Source.key, func.count(Car.id))
            .select_from(Car)
            .join(Source, Car.source_id == Source.id)
            .where(and_(*base_conds))
            .group_by(Source.key)
            .order_by(func.count(Car.id).desc())
        ).all()
        for src_key, cnt in src_rows:
            _row(f"  {src_key}", int(cnt))

        # ── 3) Region / country split. ──
        # mobile.de defaults to "Germany only" — operators searching DE
        # will see far fewer cars than our cross-EU catalog by design.
        _print_section("Распределение по странам")
        country_rows = db.execute(
            select(Car.country, func.count(Car.id))
            .where(and_(*base_conds))
            .group_by(Car.country)
            .order_by(func.count(Car.id).desc())
            .limit(15)
        ).all()
        for c, cnt in country_rows:
            _row(f"  {c or '—'}", int(cnt))

        # ── 4) Year-by-year (if reg_year_min was set, this exposes
        # whether "от 2025" is leaking 2026 into the count). ──
        _print_section("Распределение по году регистрации")
        reg_year = func.coalesce(Car.registration_year, Car.year).label("reg_year")
        year_rows = db.execute(
            select(reg_year, func.count(Car.id))
            .where(and_(*base_conds))
            .group_by(reg_year)
            .order_by(reg_year.desc())
            .limit(20)
        ).all()
        for y, cnt in year_rows:
            _row(f"  {y or '—'}", int(cnt))

        # ── 5) Duplicate ``inner_id`` — same listing counted twice? ──
        _print_section("Поиск дубликатов inner_id")
        dup_rows = db.execute(
            select(Car.inner_id, func.count(Car.id).label("c"))
            .where(and_(*base_conds))
            .group_by(Car.inner_id)
            .having(func.count(Car.id) > 1)
            .order_by(func.count(Car.id).desc())
            .limit(10)
        ).all()
        if dup_rows:
            _row("Найдено дубликатов inner_id", len(dup_rows))
            for inner, cnt in dup_rows:
                _row(f"  inner_id={inner}", int(cnt), dim=True)
        else:
            _row("Дубликатов нет", "OK", dim=True)

        # ── 6) Freshness — how many cars haven't been seen for a while?
        # In a healthy import, last_seen_at should be within 1-2 days;
        # cars older than 7 days but still is_available=true are stuck. ──
        _print_section("Свежесть (last_seen_at)")
        now = datetime.utcnow()
        for days, label in [(1, "Виден за последние 24 ч"),
                            (3, "Виден за последние 3 дня"),
                            (7, "Виден за последние 7 дней"),
                            (30, "Виден за последние 30 дней")]:
            cutoff = now - timedelta(days=days)
            cnt = int(
                db.execute(
                    select(func.count(Car.id)).where(
                        and_(*base_conds, Car.last_seen_at >= cutoff)
                    )
                ).scalar() or 0
            )
            _row(label, cnt)
        nullc = int(
            db.execute(
                select(func.count(Car.id)).where(
                    and_(*base_conds, Car.last_seen_at.is_(None))
                )
            ).scalar() or 0
        )
        _row("last_seen_at = NULL (старая запись?)", nullc)

        # ── 7) Variant / generation breakdown — mobile.de may treat
        # "Range Rover Sport SVR" as a separate model. We don't, so the
        # count includes all trims. ──
        _print_section("Топ вариантов (variant / generation)")
        variant_rows = db.execute(
            select(
                func.coalesce(Car.variant, Car.generation, "—").label("v"),
                func.count(Car.id),
            )
            .where(and_(*base_conds))
            .group_by(func.coalesce(Car.variant, Car.generation, "—"))
            .order_by(func.count(Car.id).desc())
            .limit(15)
        ).all()
        for v, cnt in variant_rows:
            _row(f"  {v}", int(cnt))

        # ── 8) Sample inner_ids the operator can verify on mobile.de. ──
        _print_section("Случайные 10 inner_id для ручной проверки")
        sample = db.execute(
            select(Car.id, Car.inner_id, Car.country, Car.registration_year, Car.last_seen_at)
            .where(and_(*base_conds))
            .order_by(func.random())
            .limit(10)
        ).all()
        for car_id, inner, c, ry, ls in sample:
            print(f"  id={car_id:<10} inner_id={inner!r:<14} {c} {ry} {ls}")

        # ── 9) Comparison hint. ──
        _print_section("Сравнение")
        # Ru-EU mobile.de count = mobile.de + active EU + reg_year exact + DE-only.
        if any(s == "mobile_de" for s, _ in src_rows):
            mob = next((cnt for s, cnt in src_rows if s == "mobile_de"), 0)
            print(
                f"  Чисто mobile.de активных в нашем срезе: {int(mob):,}".replace(",", " ")
            )
        if not country:
            de_count = next((cnt for c, cnt in country_rows if c == "DE"), None)
            if de_count is not None:
                print(
                    f"  Только Германия (DE): {int(de_count):,}".replace(",", " ")
                    + " — обычно ближе к default-показателю mobile.de"
                )

    finally:
        db.close()


if __name__ == "__main__":
    main()
