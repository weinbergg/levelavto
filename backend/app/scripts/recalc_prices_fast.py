from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta

from backend.app.db import SessionLocal
from backend.app.models import Car
from backend.app.services.cars_service import CarsService
from backend.app.services.calculator_runtime import is_bev, _calc_age_months
from backend.app.services.customs_config import get_customs_config, calc_util_fee_rub, calc_duty_eur


def _parse_ids(value: str | None) -> list[int]:
    if not value:
        return []
    ids = []
    for part in value.replace(" ", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.append(int(part))
        except ValueError:
            continue
    return ids


def _extract_amount(breakdown, title: str) -> float | None:
    for row in breakdown or []:
        if row.get("title") == title:
            return float(row.get("amount_rub") or 0)
    return None


def _compute_scenario(car: Car) -> tuple[str | None, str | None]:
    bev = is_bev(
        car.engine_cc,
        float(car.power_kw) if car.power_kw is not None else None,
        float(car.power_hp) if car.power_hp is not None else None,
        car.engine_type,
    )
    if bev:
        return "electric", "electric"
    reg_year = car.registration_year or car.year
    reg_month = car.registration_month or 1
    if not reg_year:
        return None, None
    age_months = _calc_age_months(reg_year, reg_month)
    if age_months is None:
        return None, None
    if age_months < 36:
        return "under_3", "under_3"
    return "3_5", "3_5"


def _compute_util_and_duty(car: Car, eur_rate: float) -> tuple[int | None, float | None, str | None]:
    scenario_key, age_bucket = _compute_scenario(car)
    if not scenario_key:
        return None, None, None
    customs_cfg = get_customs_config()
    util_rub = calc_util_fee_rub(
        engine_cc=car.engine_cc or 0,
        kw=float(car.power_kw) if car.power_kw is not None else None,
        hp=int(car.power_hp) if car.power_hp is not None else None,
        cfg=customs_cfg,
        age_bucket=age_bucket,
    )
    duty_rub = 0.0
    if scenario_key not in ("under_3", "electric"):
        if car.engine_cc:
            duty_eur = calc_duty_eur(car.engine_cc, customs_cfg)
            duty_rub = float(duty_eur) * float(eur_rate)
    return util_rub, duty_rub, scenario_key


def main() -> None:
    ap = argparse.ArgumentParser(description="Fast recalc prices for util/duty changes")
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--country", type=str, default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only-ids", type=str, default=None)
    ap.add_argument("--since-minutes", type=int, default=None)
    args = ap.parse_args()

    only_ids = _parse_ids(args.only_ids)
    updated = skipped = checked = 0

    with SessionLocal() as db:
        svc = CarsService(db)
        fx = svc.get_fx_rates() or {}
        eur_rate = fx.get("EUR") or 95.0
        q = db.query(Car).filter(Car.is_available.is_(True))
        if args.country:
            q = q.filter(Car.country == args.country)
        if only_ids:
            q = q.filter(Car.id.in_(only_ids))
        if args.since_minutes:
            since_ts = datetime.utcnow() - timedelta(minutes=args.since_minutes)
            q = q.filter(Car.updated_at >= since_ts)
        q = q.order_by(Car.id.asc())

        last_id = 0
        while True:
            batch_q = q.filter(Car.id > last_id).limit(args.batch)
            rows = batch_q.all()
            if not rows:
                break
            for car in rows:
                last_id = max(last_id, car.id)
                checked += 1
                util_new, duty_new_rub, scenario = _compute_util_and_duty(car, eur_rate)
                if util_new is None or scenario is None:
                    skipped += 1
                    continue
                util_old = _extract_amount(car.calc_breakdown_json, "Утилизационный сбор")
                duty_old = _extract_amount(car.calc_breakdown_json, "Пошлина РФ")
                # Compare util directly. Duty is EUR-based in calc; we only skip if both unchanged.
                if util_old is not None and int(util_old) == int(util_new):
                    if scenario in ("under_3", "electric") or (
                        duty_old is not None and abs(float(duty_old) - float(duty_new_rub)) < 1.0
                    ):
                        skipped += 1
                        continue
                if args.dry_run:
                    updated += 1
                else:
                    svc.ensure_calc_cache(car, force=True)
                    updated += 1

                if checked % 500 == 0:
                    print(f"[fast_recalc] checked={checked} updated={updated} skipped={skipped}", flush=True)

            if args.limit and checked >= args.limit:
                break

    print(f"[fast_recalc] done checked={checked} updated={updated} skipped={skipped}", flush=True)


if __name__ == "__main__":
    main()
