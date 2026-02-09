from __future__ import annotations

import argparse
import os
import time
from typing import Any, Iterable

from backend.app.db import SessionLocal
from backend.app.models import Car
from backend.app.services.cars_service import CarsService
from backend.app.utils.telegram import send_telegram_message
from backend.app.utils.price_utils import ceil_to_step, get_round_step_rub


def _iter_steps(raw: Any) -> list[dict]:
    if raw is None:
        return []
    if isinstance(raw, dict) and "steps" in raw and isinstance(raw["steps"], list):
        return raw["steps"]
    if isinstance(raw, list):
        return raw
    return []


def _step_name(step: dict) -> str:
    return str(step.get("title") or step.get("name") or "")


def _step_currency(step: dict) -> str | None:
    cur = step.get("currency")
    return str(cur) if cur else None


def _step_value(step: dict) -> float | None:
    try:
        return float(step.get("value"))
    except Exception:
        return None


def _update_rate_steps(steps: list[dict], eur_rate: float, usd_rate: float) -> None:
    for step in steps:
        name = _step_name(step).lower()
        if name == "eur_rate":
            step["value"] = eur_rate
        elif name == "usd_rate":
            step["value"] = usd_rate


def _update_total_step(steps: list[dict], total_rub: float) -> None:
    for step in steps:
        name = _step_name(step).lower()
        if "итого" in name:
            step["value"] = float(total_rub)


def recompute_total_from_breakdown(steps: list[dict], eur_rate: float, usd_rate: float) -> float | None:
    sum_eur = 0.0
    sum_usd = 0.0
    sum_rub = 0.0
    has_any = False
    for step in steps:
        name = _step_name(step).lower()
        if "итого" in name:
            continue
        cur = _step_currency(step)
        val = _step_value(step)
        if val is None:
            continue
        if cur == "EUR":
            sum_eur += val
            has_any = True
        elif cur == "USD":
            sum_usd += val
            has_any = True
        elif cur == "RUB":
            sum_rub += val
            has_any = True
    if not has_any:
        return None
    return float(sum_rub + sum_eur * eur_rate + sum_usd * usd_rate)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", type=int, default=2000)
    parser.add_argument("--sleep", type=float, default=0.0)
    parser.add_argument("--country", type=str, default="")
    parser.add_argument("--only-ids", type=str, default="")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--telegram", action="store_true")
    args = parser.parse_args()

    with SessionLocal() as db:
        svc = CarsService(db)
        rates = svc.get_fx_rates(allow_fetch=True) or {}
        eur_rate = float(rates.get("EUR") or 0)
        usd_rate = float(rates.get("USD") or 0)

        if eur_rate <= 0 or usd_rate <= 0:
            raise SystemExit("EUR/USD rates missing")

        query = db.query(Car).filter(Car.is_available.is_(True))
        if args.country:
            query = query.filter(Car.country == args.country)

        only_ids = []
        if args.only_ids:
            with open(args.only_ids, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.isdigit():
                        only_ids.append(int(line))
            if only_ids:
                query = query.filter(Car.id.in_(only_ids))

        last_id = 0
        total_checked = 0
        total_updated = 0
        while True:
            batch = (
                query.filter(Car.id > last_id)
                .order_by(Car.id.asc())
                .limit(args.batch)
                .all()
            )
            if not batch:
                break
            updates = []
            for car in batch:
                last_id = car.id
                total_checked += 1
                steps = _iter_steps(car.calc_breakdown_json)
                total_rub = recompute_total_from_breakdown(steps, eur_rate, usd_rate)

                price_rub = None
                if car.currency == "EUR" and car.price is not None:
                    price_rub = float(car.price) * eur_rate
                elif car.currency == "USD" and car.price is not None:
                    price_rub = float(car.price) * usd_rate

                if total_rub is None and price_rub is None:
                    continue

                if total_rub is not None:
                    total_rub = ceil_to_step(total_rub, get_round_step_rub())
                    _update_rate_steps(steps, eur_rate, usd_rate)
                    _update_total_step(steps, total_rub)

                updates.append(
                    {
                        "id": car.id,
                        "price_rub_cached": price_rub if price_rub is not None else car.price_rub_cached,
                        "total_price_rub_cached": total_rub if total_rub is not None else car.total_price_rub_cached,
                        "calc_breakdown_json": steps if steps else car.calc_breakdown_json,
                    }
                )
            if updates and not args.dry_run:
                db.bulk_update_mappings(Car, updates)
                db.commit()
                total_updated += len(updates)
            if args.sleep:
                time.sleep(args.sleep)
        summary = f"fx_update checked={total_checked} updated={total_updated} eur={eur_rate} usd={usd_rate}"
        print(summary)
        if args.telegram:
            token = os.getenv("TELEGRAM_BOT_TOKEN")
            chat_id = os.getenv("TELEGRAM_CHAT_ID")
            if token and chat_id:
                send_telegram_message(token, chat_id, summary)


if __name__ == "__main__":
    main()
