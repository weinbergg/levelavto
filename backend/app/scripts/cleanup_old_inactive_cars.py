"""Delete cars that have been inactive in source feeds for too long.

Why this exists: ``cars`` accumulated 5 550 315 rows of which only
1 777 097 (32 %) are currently active. The other 3 773 218 are
listings that mobile.de / emavto / encar stopped sending and our
deactivation logic flipped to ``is_available=false``. They were never
hard-deleted because the original design wanted to keep old detail
URLs alive and preserve a notional analytics history.

Operational impact of carrying that ballast:
* Every batch operation (migrations, backfills, normalisers) scans
  3× more rows than necessary — what should take 1 minute takes 3.
* Indexes are 3× bigger → slower planner, slower autovacuum, slower
  backups.
* WAL pressure on bulk UPDATEs scales linearly with row count.
* Disk usage on the VPS keeps climbing (currently ~72 % full).

What we keep / what we drop: deletion is keyed off ``last_seen_at``
(the timestamp the parser last saw the listing alive in the source
feed). Cars that are still ``is_available=true`` are NEVER touched
under any threshold. Cars whose ``last_seen_at`` is NULL fall back to
``updated_at`` so legacy rows from before the column existed are
covered.

All foreign keys to ``cars.id`` (car_images, favorites,
featured_cars, car_spec_reference) have ``ON DELETE CASCADE``, so
DELETE walks them automatically — no orphans left behind.

Usage::

    # Always start with --report. Shows the age histogram so you
    # can pick a sensible threshold for your VPS budget.
    docker compose run --rm web python -m \\
        backend.app.scripts.cleanup_old_inactive_cars --report

    # Deletion (writes!) — defaults to 180 days inactive.
    docker compose run --rm web python -m \\
        backend.app.scripts.cleanup_old_inactive_cars --apply --days 180

    # Be conservative on the first pass; you can always rerun lower.
    docker compose run --rm web python -m \\
        backend.app.scripts.cleanup_old_inactive_cars --apply --days 365
"""

from __future__ import annotations

import argparse
from typing import List, Tuple

from sqlalchemy import text

from ..db import SessionLocal
from ..utils.redis_cache import bump_dataset_version


# Age buckets reported in --report mode. Buckets are based on
# last_seen_at exclusively — see _age_expr() for why we deliberately
# do NOT fall back to updated_at any more.
_BUCKETS: List[Tuple[str, int, int | None]] = [
    ("активные (никогда не удаляются)",      -1, 0),
    ("неактивные, last_seen_at < 30 дн",       0, 30),
    ("неактивные, last_seen_at 30-90 дн",     30, 90),
    ("неактивные, last_seen_at 90-180 дн",    90, 180),
    ("неактивные, last_seen_at 180-365 дн",   180, 365),
    ("неактивные, last_seen_at 365-730 дн",   365, 730),
    ("неактивные, last_seen_at ≥ 730 дн",     730, None),
    ("неактивные, last_seen_at IS NULL (легаси)", -2, -2),
]

BATCH_SIZE = 5_000


def _age_expr() -> str:
    """SQL fragment yielding "days since this car was last seen alive".

    Uses ``last_seen_at`` only. The earlier
    ``coalesce(last_seen_at, updated_at)`` was a mistake: ``updated_at``
    has ``onupdate=now()``, so every routine data migration
    (engine_type normalisation, drive_type normalisation,
    price_rub_cached refresh, …) bumps it on millions of rows. After
    those migrations, the entire inactive set looked "freshly updated"
    and the cleanup script could not find a single old row.

    Returns NULL for cars whose ``last_seen_at`` is NULL — these are
    legacy rows from before the column was introduced, treated
    separately as the "ancient legacy" bucket.
    """

    return "extract(epoch from now() - last_seen_at) / 86400.0"


def _report(db) -> None:
    age = _age_expr()
    print()
    print("Распределение машин по возрасту неактивности (по last_seen_at):")
    print(f"  {'bucket':<48} {'count':>12}")
    for label, lo, hi in _BUCKETS:
        if label.startswith("активные"):
            sql = "SELECT count(*) FROM cars WHERE is_available IS true"
            params: dict[str, float] = {}
        elif "IS NULL" in label:
            sql = (
                "SELECT count(*) FROM cars "
                "WHERE is_available IS NOT true AND last_seen_at IS NULL"
            )
            params = {}
        else:
            clauses = ["is_available IS NOT true", "last_seen_at IS NOT NULL", f"{age} >= :lo"]
            params = {"lo": float(lo)}
            if hi is not None:
                clauses.append(f"{age} < :hi")
                params["hi"] = float(hi)
            sql = "SELECT count(*) FROM cars WHERE " + " AND ".join(clauses)
        n = db.execute(text(sql), params).scalar_one()
        print(f"  {label:<48} {int(n):>12}")
    total = db.execute(text("SELECT count(*) FROM cars")).scalar_one()
    print(f"  {'─' * 48}")
    print(f"  {'всего строк':<48} {int(total):>12}")


def _max_id(db) -> int:
    return int(db.execute(text("SELECT coalesce(max(id), 0) FROM cars")).scalar_one())


def _delete_chunked(db, days: int, include_legacy_null: bool) -> int:
    """Delete every inactive car older than ``days``, in id-range chunks.

    The predicate covers two cases:
      * ``last_seen_at IS NOT NULL AND age >= :days`` — a normal
        inactive listing whose parser saw it last more than N days ago.
      * (when ``include_legacy_null`` is set) ``last_seen_at IS NULL``
        — pre-2024 legacy rows with no last_seen_at populated; treated
        as ancient.

    Equality on ``is_available`` + id-range narrow each chunk; the
    last_seen_at check is then a fast per-row comparison on the
    5 000-row slice.
    """

    max_id = _max_id(db)
    if not max_id:
        return 0
    age_clause = (
        "(last_seen_at IS NOT NULL AND extract(epoch from now() - last_seen_at) / 86400.0 >= :days"
    )
    if include_legacy_null:
        age_clause += " OR last_seen_at IS NULL"
    age_clause += ")"
    cur_id = 0
    total = 0
    legacy_note = " (включая последние NULL last_seen_at)" if include_legacy_null else ""
    print(
        f">>> Удаляем cars where is_available=false AND age >= {days} дн{legacy_note}, "
        f"батчами по {BATCH_SIZE} строк (max_id={max_id})",
        flush=True,
    )
    while cur_id <= max_id:
        res = db.execute(
            text(
                f"""
                DELETE FROM cars
                WHERE id >= :lo AND id < :hi
                  AND is_available IS NOT true
                  AND {age_clause}
                """
            ),
            {"lo": cur_id, "hi": cur_id + BATCH_SIZE, "days": float(days)},
        )
        n = int(res.rowcount or 0)
        db.commit()
        total += n
        if n:
            print(
                f"   id [{cur_id}, {cur_id + BATCH_SIZE}) — deleted {n}, "
                f"running_total={total}",
                flush=True,
            )
        cur_id += BATCH_SIZE
    return total


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Actually delete (default: dry-run)")
    parser.add_argument(
        "--report", action="store_true", help="Print age-bucket distribution and exit"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=180,
        help="Delete inactive cars whose last_seen_at is older than this (default: 180)",
    )
    parser.add_argument(
        "--include-legacy-null",
        action="store_true",
        default=True,
        help=(
            "Also delete inactive cars whose last_seen_at IS NULL (pre-2024 "
            "legacy rows that never had the column populated; default: on)"
        ),
    )
    parser.add_argument(
        "--no-include-legacy-null",
        dest="include_legacy_null",
        action="store_false",
        help="Disable the legacy-NULL inclusion explicitly.",
    )
    args = parser.parse_args()

    if args.days < 30:
        raise SystemExit(
            "FATAL: --days < 30 is too aggressive — recently deactivated cars "
            "may be re-added by tomorrow's import. Use at least 30."
        )

    mode = "APPLY (deletes will be committed)" if args.apply else "DRY-RUN (no deletes)"
    legacy_note = " + legacy NULL" if args.include_legacy_null else ""
    print(
        f">>> Cleanup old inactive cars — {mode} "
        f"(threshold: {args.days} дн{legacy_note})",
        flush=True,
    )

    with SessionLocal() as db:
        if args.report or not args.apply:
            _report(db)

        age = _age_expr()
        legacy_clause = " OR last_seen_at IS NULL" if args.include_legacy_null else ""
        n_target = int(
            db.execute(
                text(
                    f"""
                    SELECT count(*) FROM cars
                    WHERE is_available IS NOT true
                      AND ((last_seen_at IS NOT NULL AND {age} >= :days){legacy_clause})
                    """
                ),
                {"days": float(args.days)},
            ).scalar_one()
        )
        print()
        print(
            f"Под удаление при threshold={args.days} дн{legacy_note}: {n_target} строк"
        )

        if not args.apply:
            print(
                "\nЭто был dry-run. Запустите с --apply, чтобы удалить.",
                flush=True,
            )
            return

        deleted = _delete_chunked(db, args.days, args.include_legacy_null)
        print(f"\nГотово. Удалено строк: {deleted}", flush=True)

    try:
        new_ver = bump_dataset_version()
        print(
            f"Версия датасета поднята до {new_ver} — кэши инвалидированы.",
            flush=True,
        )
    except Exception as exc:  # noqa: BLE001 — defensive log only
        print(
            "ВНИМАНИЕ: не удалось поднять dataset_version "
            f"({exc!r}). Сделайте redis-cli FLUSHDB вручную.",
            flush=True,
        )

    print()
    print(
        "Совет: после большого DELETE прогоните VACUUM (ANALYZE) cars; "
        "иначе место на диске не освободится сразу.",
        flush=True,
    )


if __name__ == "__main__":
    main()
