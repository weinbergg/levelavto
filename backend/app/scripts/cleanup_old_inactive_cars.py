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


# Age buckets reported in --report mode. Granularity is intentionally
# fine in the 30-180 day range because that is where most ballast
# accumulates on a young deployment (DB ~6 months old): a used-car
# listing on mobile.de typically lives 2-8 weeks, so cars first seen
# 60+ days ago that are now inactive are dead weight regardless of
# overall DB age.
_BUCKETS: List[Tuple[str, int, int | None]] = [
    ("активные (никогда не удаляются)",            -1, 0),
    ("неактивные, first_seen_at < 30 дн",           0, 30),
    ("неактивные, first_seen_at 30-60 дн",         30, 60),
    ("неактивные, first_seen_at 60-90 дн",         60, 90),
    ("неактивные, first_seen_at 90-180 дн",        90, 180),
    ("неактивные, first_seen_at 180-365 дн",      180, 365),
    ("неактивные, first_seen_at 365-730 дн",      365, 730),
    ("неактивные, first_seen_at ≥ 730 дн",        730, None),
    ("неактивные, first_seen_at IS NULL (легаси)", -2, -2),
]

BATCH_SIZE = 5_000


def _age_expr() -> str:
    """SQL fragment yielding "days since this car first appeared in our DB".

    Uses ``coalesce(first_seen_at, created_at)``. Both columns are
    set once on insert and never updated afterwards, so they are the
    only timestamps we can trust as "how long has this listing
    existed in our system":

      * ``updated_at`` has ``onupdate=now()`` and is bumped by every
        routine maintenance migration — useless as an age signal.
      * ``last_seen_at`` USED to be bumped on deactivation
        (parsing_data_service: deactivate_missing_*), which silently
        destroyed the "last time we saw it alive" semantics for
        every inactive car. That bug is now fixed, but the 3.77M
        inactive rows already in the DB have contaminated
        ``last_seen_at`` values that we cannot recover.

    Rationale for using first_seen_at as the cleanup signal: a
    used-car listing on mobile.de or emavto.ru rarely stays alive
    longer than ~6 months. If we first saw a row a year ago and it
    is currently ``is_available=false``, the listing has been gone
    from the feed for an unknown but bounded interval that, combined
    with "the listing existed in the source for >1 year", makes it
    safely deletable.
    """

    return "extract(epoch from now() - coalesce(first_seen_at, created_at)) / 86400.0"


def _report(db) -> None:
    age = _age_expr()
    print()
    print("Распределение машин по возрасту строки (по first_seen_at, fallback created_at):")
    print(f"  {'bucket':<52} {'count':>12}")
    for label, lo, hi in _BUCKETS:
        if label.startswith("активные"):
            sql = "SELECT count(*) FROM cars WHERE is_available IS true"
            params: dict[str, float] = {}
        elif "IS NULL" in label:
            sql = (
                "SELECT count(*) FROM cars "
                "WHERE is_available IS NOT true "
                "AND first_seen_at IS NULL AND created_at IS NULL"
            )
            params = {}
        else:
            clauses = [
                "is_available IS NOT true",
                "(first_seen_at IS NOT NULL OR created_at IS NOT NULL)",
                f"{age} >= :lo",
            ]
            params = {"lo": float(lo)}
            if hi is not None:
                clauses.append(f"{age} < :hi")
                params["hi"] = float(hi)
            sql = "SELECT count(*) FROM cars WHERE " + " AND ".join(clauses)
        n = db.execute(text(sql), params).scalar_one()
        print(f"  {label:<52} {int(n):>12}")
    total = db.execute(text("SELECT count(*) FROM cars")).scalar_one()
    print(f"  {'─' * 52}")
    print(f"  {'всего строк':<52} {int(total):>12}")


def _max_id(db) -> int:
    return int(db.execute(text("SELECT coalesce(max(id), 0) FROM cars")).scalar_one())


def _delete_chunked(db, days: int, include_legacy_null: bool) -> int:
    """Delete every inactive car older than ``days``, in id-range chunks.

    The predicate covers two cases:
      * ``coalesce(first_seen_at, created_at) < now - :days`` — the
        listing has existed in our DB for at least N days and is
        currently inactive.
      * (when ``include_legacy_null`` is set) both timestamps NULL —
        ancient pre-schema rows with no provenance, safe to drop.

    Equality on ``is_available`` + id-range narrow each chunk; the
    timestamp comparison is then a fast per-row check on the
    5 000-row slice. No new indexes required.
    """

    max_id = _max_id(db)
    if not max_id:
        return 0
    age_clause = (
        "((first_seen_at IS NOT NULL OR created_at IS NOT NULL) "
        "AND extract(epoch from now() - coalesce(first_seen_at, created_at)) "
        "/ 86400.0 >= :days"
    )
    if include_legacy_null:
        age_clause += " OR (first_seen_at IS NULL AND created_at IS NULL)"
    age_clause += ")"
    cur_id = 0
    total = 0
    legacy_note = " (включая легаси-строки без timestamp'ов)" if include_legacy_null else ""
    print(
        f">>> Удаляем cars where is_available=false AND first_seen_at >= {days} дн назад"
        f"{legacy_note}, батчами по {BATCH_SIZE} строк (max_id={max_id})",
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
        help=(
            "Delete inactive cars whose first_seen_at is older than this "
            "(default: 180). Recommend 180 for routine cleanup, 90 for "
            "aggressive ballast reduction."
        ),
    )
    parser.add_argument(
        "--include-legacy-null",
        action="store_true",
        default=True,
        help=(
            "Also delete inactive cars where BOTH first_seen_at AND "
            "created_at are NULL (very old pre-schema rows; default: on)"
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
        f"(threshold: first_seen_at >= {args.days} дн назад{legacy_note})",
        flush=True,
    )

    with SessionLocal() as db:
        if args.report or not args.apply:
            _report(db)

        age = _age_expr()
        legacy_clause = (
            " OR (first_seen_at IS NULL AND created_at IS NULL)"
            if args.include_legacy_null
            else ""
        )
        n_target = int(
            db.execute(
                text(
                    f"""
                    SELECT count(*) FROM cars
                    WHERE is_available IS NOT true
                      AND (
                        ((first_seen_at IS NOT NULL OR created_at IS NOT NULL)
                         AND {age} >= :days)
                        {legacy_clause}
                      )
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
