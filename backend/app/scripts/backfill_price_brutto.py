"""Repair Car.price for mobile.de rows that stored the net (VAT-free) price.

Background
----------
The previous mobile.de feed parser
(:func:`backend.app.parsing.mobile_de_feed.MobileDeFeedAdapter._resolve_price_eur`)
preferred ``price_eur_nt`` (net, VAT-free) over ``price_eur`` (gross,
VAT-inclusive) when both were present in the CSV. About 40 % of
mobile.de listings carry both — these are dealer-listed cars where
the VAT is refundable to commercial export buyers — so the public
catalogue silently showed ~16 % below-market prices for those cars
(scripts.audit_csv_vs_db measured 58 % field-match for ``price_eur``,
with every mismatch following the exact 1/1.19 net/gross ratio).

The parser is now fixed (gross first, net only as fallback) so all
new imports write the right value. This script repairs the existing
~600k mobile.de rows in place by reading both prices from
``source_payload``, picking the gross one, and recomputing
``price_rub_cached`` from the current FX snapshot.

Strategy
--------
* Walk only ``source_id = mobile_de`` rows where source_payload has
  both ``price_eur`` and ``price_eur_nt`` set and they differ. That
  is exactly the affected slice — no other rows can have the bug.
* Update in 5 000-row id-range batches with explicit COMMITs.
* For each updated row, rebuild ``price_rub_cached`` from the new
  gross EUR price using the current FX rate snapshot. Without this
  step the catalogue would still show the wrong RUB price even after
  ``price`` is fixed.
* Bump dataset_version at the end so all versioned caches drop the
  stale price values.

Usage::

    docker compose exec -T web python -m backend.app.scripts.backfill_price_brutto --report
    docker compose exec -T web python -m backend.app.scripts.backfill_price_brutto --apply
"""

from __future__ import annotations

import argparse
from typing import Optional

from sqlalchemy import text

from ..db import SessionLocal
from ..utils.redis_cache import bump_dataset_version


BATCH_SIZE = 5_000


def _max_id(db) -> int:
    return int(db.execute(text("SELECT coalesce(max(id), 0) FROM cars")).scalar_one())


def _source_id(db, key: str = "mobile_de") -> int:
    sid = db.execute(
        text("SELECT id FROM sources WHERE key = :k"), {"k": key}
    ).scalar_one_or_none()
    if sid is None:
        raise SystemExit(f"Source '{key}' not found")
    return int(sid)


def _count_affected(db, src_id: int) -> int:
    """Rows where the gross/net difference is real and > 0.5 %.

    NOTE: ``source_payload`` is declared as plain ``JSON`` in the
    model (not ``JSONB``), so the ``?`` key-existence operator does
    NOT exist for it. We use ``->>'key' IS NOT NULL`` instead, which
    works for both JSON and JSONB and returns NULL for missing keys.
    """

    return int(
        db.execute(
            text(
                """
                SELECT count(*) FROM cars
                WHERE source_id = :src
                  AND (source_payload->>'price_eur') IS NOT NULL
                  AND (source_payload->>'price_eur_nt') IS NOT NULL
                  AND (source_payload->>'price_eur')   ~ '^[0-9.]+$'
                  AND (source_payload->>'price_eur_nt') ~ '^[0-9.]+$'
                  AND (source_payload->>'price_eur')::numeric > 0
                  AND (source_payload->>'price_eur_nt')::numeric > 0
                  AND (source_payload->>'price_eur')::numeric
                      <> (source_payload->>'price_eur_nt')::numeric
                  AND price IS NOT NULL
                  AND abs(price - (source_payload->>'price_eur')::numeric)
                      / greatest((source_payload->>'price_eur')::numeric, 1) > 0.005
                """
            ),
            {"src": src_id},
        ).scalar_one()
    )


def _get_eur_rate(db) -> Optional[float]:
    """Pull the EUR -> RUB rate from CarsService.

    We import lazily to avoid hauling SQLAlchemy at module import
    time and to share the same FX snapshot strategy with the live
    catalogue (CBR + ENV fallback).
    """

    from ..services.cars_service import CarsService

    rates = CarsService(db).get_fx_rates() or {}
    eur = rates.get("EUR")
    if not eur:
        return None
    try:
        return float(eur)
    except (TypeError, ValueError):
        return None


def _apply(db, src_id: int, eur_rate: Optional[float]) -> tuple[int, int]:
    max_id = _max_id(db)
    if not max_id:
        return 0, 0
    cur_id = 0
    total = 0
    rub_updated = 0
    print(
        f">>> UPDATE cars в батчах по {BATCH_SIZE} (max_id={max_id}, "
        f"EUR rate = {eur_rate or 'n/a'})",
        flush=True,
    )
    while cur_id <= max_id:
        if eur_rate:
            res = db.execute(
                text(
                    """
                    UPDATE cars
                    SET price = (source_payload->>'price_eur')::numeric,
                        price_rub_cached = round(
                            (source_payload->>'price_eur')::numeric * :eur, 2
                        )
                    WHERE id >= :lo AND id < :hi
                      AND source_id = :src
                      AND (source_payload->>'price_eur') IS NOT NULL
                      AND (source_payload->>'price_eur_nt') IS NOT NULL
                      AND (source_payload->>'price_eur')   ~ '^[0-9.]+$'
                      AND (source_payload->>'price_eur_nt') ~ '^[0-9.]+$'
                      AND (source_payload->>'price_eur')::numeric > 0
                      AND (source_payload->>'price_eur_nt')::numeric > 0
                      AND price IS NOT NULL
                      AND abs(price - (source_payload->>'price_eur')::numeric)
                          / greatest((source_payload->>'price_eur')::numeric, 1) > 0.005
                    """
                ),
                {"lo": cur_id, "hi": cur_id + BATCH_SIZE, "src": src_id, "eur": float(eur_rate)},
            )
            rub_updated += int(res.rowcount or 0)
        else:
            res = db.execute(
                text(
                    """
                    UPDATE cars
                    SET price = (source_payload->>'price_eur')::numeric
                    WHERE id >= :lo AND id < :hi
                      AND source_id = :src
                      AND (source_payload->>'price_eur') IS NOT NULL
                      AND (source_payload->>'price_eur_nt') IS NOT NULL
                      AND (source_payload->>'price_eur')   ~ '^[0-9.]+$'
                      AND (source_payload->>'price_eur_nt') ~ '^[0-9.]+$'
                      AND (source_payload->>'price_eur')::numeric > 0
                      AND (source_payload->>'price_eur_nt')::numeric > 0
                      AND price IS NOT NULL
                      AND abs(price - (source_payload->>'price_eur')::numeric)
                          / greatest((source_payload->>'price_eur')::numeric, 1) > 0.005
                    """
                ),
                {"lo": cur_id, "hi": cur_id + BATCH_SIZE, "src": src_id},
            )
        n = int(res.rowcount or 0)
        db.commit()
        total += n
        if n:
            print(
                f"   id [{cur_id}, {cur_id + BATCH_SIZE}) — updated {n}, "
                f"running_total={total}",
                flush=True,
            )
        cur_id += BATCH_SIZE
    return total, rub_updated


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true",
                        help="Persist changes (default: dry-run report only)")
    parser.add_argument("--report", action="store_true",
                        help="Print before-state distribution and exit")
    parser.add_argument("--source-key", default="mobile_de",
                        help="Source.key to repair (default: mobile_de)")
    args = parser.parse_args()

    mode = "APPLY (writes will be committed)" if args.apply else "DRY-RUN"
    print(f">>> Backfill price_brutto — {mode}", flush=True)

    with SessionLocal() as db:
        src_id = _source_id(db, args.source_key)
        affected = _count_affected(db, src_id)
        print(f">>> Под исправление в '{args.source_key}': {affected} строк",
              flush=True)
        if not affected:
            print("Нечего чинить — все цены уже брутто.", flush=True)
            return

        if args.report or not args.apply:
            sample = db.execute(
                text(
                    """
                    SELECT
                        external_id,
                        price AS db_price,
                        (source_payload->>'price_eur')::numeric AS gross,
                        (source_payload->>'price_eur_nt')::numeric AS net
                    FROM cars
                    WHERE source_id = :src
                      AND (source_payload->>'price_eur') IS NOT NULL
                      AND (source_payload->>'price_eur_nt') IS NOT NULL
                      AND (source_payload->>'price_eur')   ~ '^[0-9.]+$'
                      AND (source_payload->>'price_eur_nt') ~ '^[0-9.]+$'
                      AND (source_payload->>'price_eur')::numeric
                          <> (source_payload->>'price_eur_nt')::numeric
                      AND price IS NOT NULL
                      AND abs(price - (source_payload->>'price_eur')::numeric)
                          / greatest((source_payload->>'price_eur')::numeric, 1) > 0.005
                    LIMIT 10
                    """
                ),
                {"src": src_id},
            ).all()
            print("\nПримеры (10 строк):")
            print(f"  {'external_id':<14} {'DB.price':>10} -> {'gross':>10}  ({'net':>10})")
            for ext, dbp, g, n in sample:
                print(f"  {ext!s:<14} {float(dbp):>10.2f} -> {float(g):>10.2f}  ({float(n):>10.2f})")

        if not args.apply:
            print("\nЭто был dry-run. Запустите с --apply, чтобы сохранить.",
                  flush=True)
            return

        eur_rate = _get_eur_rate(db)
        if not eur_rate:
            print(
                "ВНИМАНИЕ: курс EUR недоступен — обновим только price, "
                "price_rub_cached останется без изменений (его пересчитает "
                "следующий daily import).",
                flush=True,
            )
        updated, with_rub = _apply(db, src_id, eur_rate)
        print(f"\nГотово. price обновлён: {updated} строк", flush=True)
        if eur_rate:
            print(f"price_rub_cached одновременно пересчитан для тех же {with_rub} строк.",
                  flush=True)

    try:
        new_ver = bump_dataset_version()
        print(
            f"Версия датасета поднята до {new_ver} — все версионированные "
            "кэши автоматически инвалидируются.",
            flush=True,
        )
    except Exception as exc:  # noqa: BLE001 — defensive log only
        print(
            f"ВНИМАНИЕ: не удалось поднять dataset_version ({exc!r}). "
            "Сделайте redis-cli FLUSHDB вручную.",
            flush=True,
        )


if __name__ == "__main__":
    main()
