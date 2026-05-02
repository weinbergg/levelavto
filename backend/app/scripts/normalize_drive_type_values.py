"""Collapse ``cars.drive_type`` to canonical lowercase values.

Why a separate script (as opposed to doing it inside the Alembic
migration): the migration's transactional DDL model fights with our
need to commit between batches — the previous attempt to use
``op.execute("COMMIT")`` to escape the wrapping transaction broke the
DO block silently and left 2 052 204 rows in uppercase. Splitting the
data step into its own script lets us:

  * Use ordinary ``Session.commit()`` per batch — no DDL-vs-DML
    transactional gymnastics.
  * Watch progress in real time (one line per batch).
  * Resume from any point after a kill: every batch is its own
    transaction, the inner predicate skips rows already in the
    canonical set.

Operator workflow::

  1. python -m backend.app.scripts.normalize_drive_type_values --report
  2. python -m backend.app.scripts.normalize_drive_type_values --apply
  3. alembic -c migrations/alembic.ini upgrade head     (puts CHECK on)
  4. python -m backend.app.scripts.backfill_drive_type --apply
"""

from __future__ import annotations

import argparse
from typing import Dict

from sqlalchemy import text

from ..db import SessionLocal
from ..utils.drive_type import CANONICAL_DRIVE_TYPES, canonicalize_drive_type
from ..utils.redis_cache import bump_dataset_version


_CANONICAL_LIST_SQL = ", ".join(f"'{v}'" for v in sorted(CANONICAL_DRIVE_TYPES))
BATCH_SIZE = 50_000


def _build_mapping(db) -> Dict[str, str]:
    """Inspect distinct non-canonical values and decide each one's target.

    We rebuild the mapping from the live DB rather than hardcoding it
    so an unexpected legacy form (Cyrillic ``"Полный"``, German
    ``"Allrad"``) is handled by the canonicaliser without code changes.
    """

    rows = db.execute(
        text(
            f"""
            SELECT drive_type, count(*)
            FROM cars
            WHERE drive_type IS NOT NULL
              AND drive_type NOT IN ({_CANONICAL_LIST_SQL})
            GROUP BY drive_type
            ORDER BY count(*) DESC
            """
        )
    ).all()
    mapping: Dict[str, str] = {}
    print()
    print("Текущее распределение non-canonical drive_type:")
    for raw, n in rows:
        target = canonicalize_drive_type(raw)
        arrow = f" -> {target}" if target else "  (не распознано — будет в NULL)"
        print(f"  {int(n):>10}  '{raw}'{arrow}")
        if target:
            mapping[raw] = target
    return mapping


def _apply_one(db, raw: str, target: str) -> int:
    """Update every row where ``drive_type = raw`` in id-range chunks.

    Equality predicate is the fastest possible filter — PostgreSQL
    streams matching rows, no LIKE / regex evaluation. Chunking by
    primary key bounds keeps each transaction small (= bounded WAL,
    visible progress, killable mid-run).
    """

    max_id = int(db.execute(text("SELECT coalesce(max(id), 0) FROM cars")).scalar_one())
    if not max_id:
        return 0
    cur_id = 0
    total = 0
    while cur_id <= max_id:
        res = db.execute(
            text(
                """
                UPDATE cars SET drive_type = :tgt
                WHERE id >= :lo AND id < :hi
                  AND drive_type = :src
                """
            ),
            {"tgt": target, "src": raw, "lo": cur_id, "hi": cur_id + BATCH_SIZE},
        )
        n = int(res.rowcount or 0)
        db.commit()
        total += n
        if n:
            print(
                f"   '{raw}' -> '{target}': id [{cur_id}, {cur_id + BATCH_SIZE}) "
                f"updated {n}, running_total={total}",
                flush=True,
            )
        cur_id += BATCH_SIZE
    return total


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Write changes (default: dry-run)")
    parser.add_argument(
        "--report",
        action="store_true",
        help="Show per-value breakdown and what each maps to",
    )
    args = parser.parse_args()

    mode = "APPLY (writes will be committed)" if args.apply else "DRY-RUN (no writes)"
    print(f">>> Normalize drive_type — {mode}", flush=True)

    with SessionLocal() as db:
        mapping = _build_mapping(db)
        if not mapping:
            print("\nНечего нормализовать. Все значения каноничны или нераспознаваемы.", flush=True)
            return

        affected_total_estimate = sum(
            int(
                db.execute(
                    text("SELECT count(*) FROM cars WHERE drive_type = :src"),
                    {"src": src},
                ).scalar_one()
            )
            for src in mapping
        )
        print(
            f"\nБудет затронуто строк: {affected_total_estimate} "
            f"(уникальных raw-значений: {len(mapping)})",
            flush=True,
        )

        if not args.apply:
            print(
                "\nЭто был dry-run. Запустите с --apply, чтобы сохранить изменения.",
                flush=True,
            )
            return

        actually_updated = 0
        for raw, target in mapping.items():
            actually_updated += _apply_one(db, raw, target)
        print(f"\nГотово. Всего обновлено строк: {actually_updated}", flush=True)

    try:
        new_ver = bump_dataset_version()
        print(
            f"Версия датасета поднята до {new_ver} — все версионированные "
            "кэши автоматически инвалидируются.",
            flush=True,
        )
    except Exception as exc:  # noqa: BLE001 — defensive log only
        print(
            "ВНИМАНИЕ: не удалось поднять dataset_version "
            f"({exc!r}). Сделайте redis-cli FLUSHDB вручную.",
            flush=True,
        )


if __name__ == "__main__":
    main()
