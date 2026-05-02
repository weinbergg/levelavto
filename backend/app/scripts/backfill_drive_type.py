"""Recover ``cars.drive_type`` from variant / payload for existing rows.

Why: 45 % of active cars currently have ``drive_type IS NULL`` because
historical parsers either didn't have a dedicated drive-type column
in their source format (mobile.de CSV does, but only sometimes), or
extracted it only from the structured ``options`` field. The variant
string itself ("xDrive40d", "Q5 50 TDI quattro S line", "GLE 350 d
4MATIC") almost always contains the OEM AWD badge, but the parser
ignored it.

This script walks the NULL slice and re-runs
:func:`backend.app.utils.drive_type.canonicalize_drive_type` against
``variant`` first and ``source_payload`` JSONB fields second. Idempotent
— re-running on an already-canonical column does nothing. After ``--apply``
it bumps ``dataset_version`` so versioned caches re-render with the new
facet counts.

Usage::

    docker compose exec -T web python -m backend.app.scripts.backfill_drive_type --report
    docker compose exec -T web python -m backend.app.scripts.backfill_drive_type --apply
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from typing import Iterable, Optional

from sqlalchemy import or_, select, update

from ..db import SessionLocal
from ..models import Car
from ..utils.drive_type import canonicalize_drive_type
from ..utils.redis_cache import bump_dataset_version


# JSONB keys mobile.de's feed historically used for drive metadata.
# emavto stores its own free-text values in similarly-named keys, so
# the same probe set covers both sources.
_PAYLOAD_KEYS = (
    "drive",
    "drive_type",
    "driveType",
    "driveTrain",
    "drive_train",
    "drivetrain",
    "wheel_drive",
    "wheelDrive",
    "antrieb",
)

BATCH = 5_000


def _from_payload(payload) -> Optional[str]:
    if not payload:
        return None
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (ValueError, TypeError):
            return None
    if not isinstance(payload, dict):
        return None
    for key in _PAYLOAD_KEYS:
        candidate = payload.get(key)
        if not candidate:
            continue
        canonical = canonicalize_drive_type(str(candidate))
        if canonical:
            return canonical
    return None


def _count(db) -> int:
    from sqlalchemy import func
    return int(
        db.execute(
            select(func.count(Car.id)).where(
                or_(Car.drive_type.is_(None), Car.drive_type == "")
            )
        ).scalar_one()
    )


def _iter_candidates(db, limit: Optional[int]) -> Iterable[Car]:
    q = (
        select(Car.id, Car.variant, Car.source_payload)
        .where(or_(Car.drive_type.is_(None), Car.drive_type == ""))
        .order_by(Car.id.asc())
        .execution_options(yield_per=BATCH)
    )
    if limit:
        q = q.limit(limit)
    return db.execute(q).yield_per(BATCH)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Write recoveries (default: dry-run)")
    parser.add_argument("--report", action="store_true", help="Show distribution and sample sources")
    parser.add_argument("--limit", type=int, default=None, help="Limit candidates (debug)")
    args = parser.parse_args()

    mode = "APPLY (writes will be committed)" if args.apply else "DRY-RUN (no writes)"
    print(f">>> Backfill drive_type — {mode}", flush=True)

    with SessionLocal() as db:
        total = _count(db)
        print(f">>> Кандидатов в БД (drive_type IS NULL/''): {total}", flush=True)
        if not total:
            print("Нечего восстанавливать.", flush=True)
            return

        recovered_counter: Counter[str] = Counter()
        scanned = 0
        recovered = 0
        for cid, variant, payload in _iter_candidates(db, args.limit):
            scanned += 1
            value = canonicalize_drive_type(variant) or _from_payload(payload)
            if not value:
                continue
            recovered += 1
            recovered_counter[value] += 1
            if args.apply:
                db.execute(
                    update(Car).where(Car.id == cid).values(drive_type=value)
                )
                if recovered % BATCH == 0:
                    db.commit()
            if scanned % BATCH == 0:
                print(
                    f"   ... осмотрено {scanned}/{total}, восстановлено {recovered}",
                    flush=True,
                )
        if args.apply:
            db.commit()

        print()
        print(f"Кандидатов осмотрено: {scanned}")
        print(f"Восстановлено: {recovered}")
        print()
        print("Распределение восстановленных значений:")
        for value, n in recovered_counter.most_common():
            print(f"  {n:>6}  {value}")
        if not args.apply:
            print(
                "\nЭто был dry-run. Запустите с --apply, чтобы сохранить изменения.",
                flush=True,
            )
            return
        try:
            new_ver = bump_dataset_version()
            print(
                f"\nВерсия датасета поднята до {new_ver} — все версионированные "
                "кэши (Redis + in-process) автоматически инвалидируются.",
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001 — only log, never block backfill
            print(
                "\nВНИМАНИЕ: не удалось поднять dataset_version "
                f"({exc!r}). Сделайте redis-cli FLUSHDB вручную.",
                flush=True,
            )


if __name__ == "__main__":
    main()
