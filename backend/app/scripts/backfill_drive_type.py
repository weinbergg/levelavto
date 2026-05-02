"""Recover ``cars.drive_type`` from variant + payload — pure SQL, fast.

Why pure SQL: the previous Python-loop implementation round-tripped
800 000 rows through SQLAlchemy, which on prod meant ~30 minutes of
"is it still running?" anxiety. The same work as three SQL UPDATEs
finishes in seconds even without a dedicated index, because the
PostgreSQL optimiser does a single sequential scan per UPDATE and
the regex fires per row in C.

Order matters: AWD first so OEM AWD badges (xDrive, quattro,
4MATIC, 4MOTION, Allrad) win over a stray "FWD" / "front-wheel"
mention in a marketing blurb. RWD is checked next (BMW sDrive,
Porsche RWD 911 variants), FWD last. After AWD writes its rows,
the next UPDATE only scans the still-NULL slice, so the work
shrinks at every step.

The regex set MUST mirror
:func:`backend.app.utils.drive_type.canonicalize_drive_type`. The
matching test suite (``test_drive_type_canonicalization``) covers
every OEM badge listed below.

Usage::

    docker compose exec -T web python -m backend.app.scripts.backfill_drive_type --report
    docker compose exec -T web python -m backend.app.scripts.backfill_drive_type --apply
"""

from __future__ import annotations

import argparse
from typing import Tuple

from sqlalchemy import text

from ..db import SessionLocal
from ..utils.redis_cache import bump_dataset_version


# Each rule is matched against ``variant || ' ' || coalesce(model, '')``
# (lowercased) so things like the BMW "X5 xDrive40d" format are caught
# even when the parser stored the badge in the model column. We use
# ``\y`` (PostgreSQL "any word boundary") rather than ``\m`` / ``\M``
# (start/end of word). ``\y`` is unambiguous across PG locales — on
# some locale configurations ``\m`` / ``\M`` were silently failing to
# match anything in our data, hence the previous backfill recovering
# 0 rows over 3.2M candidates.
_AWD_REGEX = (
    r"\yxdrive[a-z0-9]*\y|\y4matic\+?\y|\yquattro\y|\y4motion\y"
    r"|\yallrad\y|\y4wd\y|\yawd\y|\y4x4\y"
    r"|all[- ]wheel[- ]drive|four[- ]wheel[- ]drive"
)
_RWD_REGEX = (
    r"\ysdrive[a-z0-9]*\y|\yrwd\y|rear[- ]wheel[- ]drive|\yhinterrad\y"
)
_FWD_REGEX = (
    r"\yfwd\y|front[- ]wheel[- ]drive|\yvorderrad\y"
)


def _count_null(db, active_only: bool = False) -> int:
    """Always grab a fresh connection — between batched commits the
    previous one may have been auto-released, and reusing the cached
    handle yields ResourceClosedError on the next ``conn.execute``.
    """

    base = "SELECT count(*) FROM cars WHERE (drive_type IS NULL OR drive_type = '')"
    if active_only:
        base += " AND is_available IS true"
    return int(db.execute(text(base)).scalar_one())


def _max_id(db) -> int:
    return int(db.execute(text("SELECT coalesce(max(id), 0) FROM cars")).scalar_one())


def _apply_rule(
    db,
    target: str,
    regex: str,
    *,
    active_only: bool,
    batch_size: int = 50_000,
) -> int:
    """Update every NULL row whose variant or model matches ``regex``.

    Loops in id-range batches with explicit COMMITs between them so a
    monolithic 800k-row transaction does not bloat WAL on prod. Each
    batch is small enough to finish in a few seconds; a mid-run kill
    loses at most one batch instead of the entire job. We use the
    Session's own ``execute`` / ``commit`` rather than a cached
    connection handle, otherwise SQLAlchemy 2.x closes the connection
    between commits and the next call yields ResourceClosedError.
    """

    max_id = _max_id(db)
    if not max_id:
        return 0
    extra = "AND is_available IS true" if active_only else ""
    cur_id = 0
    total = 0
    print(f"   {target}: max_id={max_id}, batch={batch_size}", flush=True)
    while cur_id <= max_id:
        res = db.execute(
            text(
                f"""
                UPDATE cars
                SET drive_type = :tgt
                WHERE id >= :lo AND id < :hi
                  {extra}
                  AND (drive_type IS NULL OR drive_type = '')
                  AND lower(coalesce(variant, '') || ' ' || coalesce(model, '')) ~ :rx
                """
            ),
            {"tgt": target, "lo": cur_id, "hi": cur_id + batch_size, "rx": regex},
        )
        n = int(res.rowcount or 0)
        db.commit()
        total += n
        if n:
            print(
                f"   {target}: id [{cur_id}, {cur_id + batch_size}) — updated {n}, total {total}",
                flush=True,
            )
        cur_id += batch_size
    return total


def _diagnose_regex_match(db, regex: str) -> None:
    """Print evidence of whether the regex actually matches anything.

    Useful when --report says 0 rows would be recovered: this surfaces
    whether the variant column is empty (data problem), whether the
    word-boundary syntax (\\m / \\M) is rejected by Postgres on this
    server's locale (regex problem) or whether the matching slice is
    just outside the active subset (filter problem).
    """

    print()
    print("Диагностика: сколько NULL drive_type строк содержат AWD-токены в variant/model:")
    queries = [
        ("LIKE '%xdrive%' (любая строка)",
         "SELECT count(*) FROM cars WHERE drive_type IS NULL "
         "AND lower(coalesce(variant,'')||' '||coalesce(model,'')) LIKE '%xdrive%'"),
        ("regex 'xdrive' (без word-boundary)",
         "SELECT count(*) FROM cars WHERE drive_type IS NULL "
         "AND lower(coalesce(variant,'')||' '||coalesce(model,'')) ~ 'xdrive'"),
        ("regex '\\mxdrive\\M' (word-boundary)",
         "SELECT count(*) FROM cars WHERE drive_type IS NULL "
         "AND lower(coalesce(variant,'')||' '||coalesce(model,'')) ~ '\\mxdrive\\M'"),
        ("regex '\\yxdrive\\y' (PG synonym)",
         "SELECT count(*) FROM cars WHERE drive_type IS NULL "
         "AND lower(coalesce(variant,'')||' '||coalesce(model,'')) ~ '\\yxdrive\\y'"),
        ("LIKE '%xdrive%' AND is_available=true",
         "SELECT count(*) FROM cars WHERE drive_type IS NULL AND is_available IS true "
         "AND lower(coalesce(variant,'')||' '||coalesce(model,'')) LIKE '%xdrive%'"),
        ("полный наш regex AWD (active only)",
         "SELECT count(*) FROM cars WHERE drive_type IS NULL AND is_available IS true "
         "AND lower(coalesce(variant,'')||' '||coalesce(model,'')) ~ :rx"),
    ]
    for label, sql in queries:
        try:
            params = {"rx": regex} if ":rx" in sql else {}
            n = int(db.execute(text(sql), params).scalar_one())
            print(f"  {n:>10}  {label}")
        except Exception as exc:  # noqa: BLE001
            print(f"  ERROR     {label} -> {exc!r}")
    print()
    print("Sample variants для NULL drive_type AND is_available=true:")
    samples = db.execute(
        text(
            "SELECT brand, model, variant FROM cars "
            "WHERE drive_type IS NULL AND is_available IS true "
            "ORDER BY id DESC LIMIT 10"
        )
    ).all()
    for brand, model, variant in samples:
        print(f"  {brand!s:<15} {model!s:<25} {variant!s}")


def _report_distribution(db) -> Tuple[int, int]:
    rows = db.execute(
        text(
            "SELECT drive_type, count(*) "
            "FROM cars GROUP BY drive_type ORDER BY count(*) DESC"
        )
    ).all()
    total = sum(int(c) for _, c in rows)
    null_count = sum(int(c) for v, c in rows if v is None or v == "")
    print()
    print("Распределение drive_type сейчас:")
    for value, count in rows:
        label = "(NULL)" if value is None else f"'{value}'"
        print(f"  {int(count):>10}  {label}")
    print(f"\nИтого: {total} (NULL: {null_count}, {null_count / max(total, 1):.1%})")
    return total, null_count


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Write recoveries (default: dry-run)")
    parser.add_argument(
        "--report", action="store_true", help="Show before/after distribution"
    )
    parser.add_argument(
        "--active-only",
        action="store_true",
        default=True,
        help=(
            "Only update active cars. Default ON: 78%% of NULL drive_type "
            "rows are inactive and never appear in the catalog, so writing "
            "to them just bloats WAL for nothing."
        ),
    )
    parser.add_argument(
        "--no-active-only",
        dest="active_only",
        action="store_false",
        help="Also touch inactive cars (slow, rarely useful).",
    )
    parser.add_argument(
        "--diagnose",
        action="store_true",
        help=(
            "Run regex/coverage probes against the AWD pattern and print "
            "10 sample variants. Use when --report says 0 recoveries to "
            "tell apart 'regex broken' from 'data really empty'."
        ),
    )
    args = parser.parse_args()

    mode = "APPLY (writes will be committed)" if args.apply else "DRY-RUN (no writes)"
    scope = "active only" if args.active_only else "all rows"
    print(f">>> Backfill drive_type — {mode} ({scope})", flush=True)

    with SessionLocal() as db:
        before_null_total = _count_null(db, active_only=False)
        before_null_active = _count_null(db, active_only=True)
        before_null = before_null_active if args.active_only else before_null_total
        print(
            f">>> Кандидатов в БД (drive_type IS NULL/''): "
            f"всего {before_null_total}, активных {before_null_active}",
            flush=True,
        )
        if not before_null:
            print("Нечего восстанавливать в выбранной области.", flush=True)
            return

        if args.report or not args.apply:
            _report_distribution(db)

        if args.diagnose:
            _diagnose_regex_match(db, _AWD_REGEX)

        if not args.apply:
            est = {}
            extra = "AND is_available IS true" if args.active_only else ""
            for label, regex in (("awd", _AWD_REGEX), ("rwd", _RWD_REGEX), ("fwd", _FWD_REGEX)):
                est[label] = int(
                    db.execute(
                        text(
                            f"""
                            SELECT count(*) FROM cars
                            WHERE (drive_type IS NULL OR drive_type = '')
                              {extra}
                              AND lower(coalesce(variant, '') || ' '
                                  || coalesce(model, '')) ~ :rx
                            """
                        ),
                        {"rx": regex},
                    ).scalar_one()
                )
            print()
            print("Что было бы восстановлено (--apply):")
            for label, n in est.items():
                print(f"  {n:>8}  -> {label}")
            total_est = sum(est.values())
            print(f"\nИтого: {total_est} строк ({total_est / max(before_null, 1):.1%} от NULL в выбранной области).")
            print(
                "\nЭто был dry-run. Запустите с --apply, чтобы сохранить изменения.",
                flush=True,
            )
            return

        recovered = {
            "awd": _apply_rule(db, "awd", _AWD_REGEX, active_only=args.active_only),
            "rwd": _apply_rule(db, "rwd", _RWD_REGEX, active_only=args.active_only),
            "fwd": _apply_rule(db, "fwd", _FWD_REGEX, active_only=args.active_only),
        }

        after_null = _count_null(db, active_only=args.active_only)
        scope_label = "среди активных" if args.active_only else "всего"
        print()
        print("Восстановлено:")
        for value, n in recovered.items():
            print(f"  {n:>8}  -> {value}")
        print(
            f"\nNULL до    ({scope_label}): {before_null}\n"
            f"NULL после ({scope_label}): {after_null}\n"
            f"Прирост заполненных: {before_null - after_null}",
            flush=True,
        )

    try:
        new_ver = bump_dataset_version()
        print(
            f"\nВерсия датасета поднята до {new_ver} — все версионированные "
            "кэши автоматически инвалидируются.",
            flush=True,
        )
    except Exception as exc:  # noqa: BLE001 — defensive log only
        print(
            "\nВНИМАНИЕ: не удалось поднять dataset_version "
            f"({exc!r}). Сделайте redis-cli FLUSHDB вручную.",
            flush=True,
        )


if __name__ == "__main__":
    main()
