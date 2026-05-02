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
# even when the parser stored the badge in the model column. Use the
# POSIX regex alternative ``~*`` (case-insensitive) and word-boundary
# anchors ``\m`` / ``\M`` to avoid matching ``"4wdiver"`` style false
# positives.
_AWD_REGEX = (
    r"\mxdrive[a-z0-9]*\M|\m4matic\+?\M|\mquattro\M|\m4motion\M"
    r"|\mallrad\M|\m4wd\M|\mawd\M|\m4x4\M"
    r"|all[- ]wheel[- ]drive|four[- ]wheel[- ]drive"
)
_RWD_REGEX = (
    r"\msdrive[a-z0-9]*\M|\mrwd\M|rear[- ]wheel[- ]drive|\mhinterrad\M"
)
_FWD_REGEX = (
    r"\mfwd\M|front[- ]wheel[- ]drive|\mvorderrad\M"
)


def _count_null(conn) -> int:
    return int(
        conn.execute(
            text(
                "SELECT count(*) FROM cars "
                "WHERE drive_type IS NULL OR drive_type = ''"
            )
        ).scalar_one()
    )


def _apply_rule(conn, target: str, regex: str) -> int:
    """Update every NULL row whose variant or model matches ``regex``."""

    res = conn.execute(
        text(
            """
            UPDATE cars
            SET drive_type = :tgt
            WHERE (drive_type IS NULL OR drive_type = '')
              AND lower(coalesce(variant, '') || ' ' || coalesce(model, '')) ~ :rx
            """
        ),
        {"tgt": target, "rx": regex},
    )
    return int(res.rowcount or 0)


def _report_distribution(conn) -> Tuple[int, int]:
    rows = conn.execute(
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
    args = parser.parse_args()

    mode = "APPLY (writes will be committed)" if args.apply else "DRY-RUN (no writes)"
    print(f">>> Backfill drive_type — {mode}", flush=True)

    with SessionLocal() as db:
        conn = db.connection()

        before_null = _count_null(conn)
        print(f">>> Кандидатов в БД (drive_type IS NULL/''): {before_null}", flush=True)
        if not before_null:
            print("Нечего восстанавливать.", flush=True)
            return

        if args.report or not args.apply:
            _report_distribution(conn)

        if not args.apply:
            # Dry-run estimate — count rows that WOULD match each rule
            # without actually updating anything. Run inside the same
            # connection so the rollback is cheap.
            est = {}
            for label, regex in (("awd", _AWD_REGEX), ("rwd", _RWD_REGEX), ("fwd", _FWD_REGEX)):
                est[label] = int(
                    conn.execute(
                        text(
                            """
                            SELECT count(*) FROM cars
                            WHERE (drive_type IS NULL OR drive_type = '')
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
            print(f"\nИтого: {total_est} строк ({total_est / max(before_null, 1):.1%} от NULL).")
            print(
                "\nЭто был dry-run. Запустите с --apply, чтобы сохранить изменения.",
                flush=True,
            )
            return

        recovered = {
            "awd": _apply_rule(conn, "awd", _AWD_REGEX),
            "rwd": _apply_rule(conn, "rwd", _RWD_REGEX),
            "fwd": _apply_rule(conn, "fwd", _FWD_REGEX),
        }
        db.commit()

        after_null = _count_null(conn)
        print()
        print("Восстановлено:")
        for value, n in recovered.items():
            print(f"  {n:>8}  -> {value}")
        print(
            f"\nNULL до:    {before_null}\n"
            f"NULL после: {after_null}\n"
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
