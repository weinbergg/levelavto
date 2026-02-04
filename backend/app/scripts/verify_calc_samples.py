from __future__ import annotations

import os
from typing import Iterable

from backend.app.db import SessionLocal
from backend.app.models import Car
from backend.app.services.calculator_runtime import is_bev, _calc_age_months
from backend.app.services.customs_config import get_customs_config, calc_util_fee_rub
from backend.app.services.cars_service import CarsService
from backend.app.services.calc_debug import build_calc_debug


DEFAULT_IDS = [
    881135,  # diesel 3_5
    238372,  # PHEV (engine_type electric, cc>0)
    312852,  # under_3 diesel
    578900,  # under_3 diesel
    1691908,  # 3_5 diesel
]


def _parse_ids(env_val: str | None) -> list[int]:
    if not env_val:
        return list(DEFAULT_IDS)
    out = []
    for part in env_val.replace(" ", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            continue
    return out or list(DEFAULT_IDS)


def _util_bucket_info(engine_cc: int, kw: float | None, hp: int | None, age_bucket: str | None) -> dict:
    cfg = get_customs_config()
    bucket = None
    for b in cfg.util_cc_buckets:
        if b.from_cc <= engine_cc <= b.to_cc:
            bucket = b
            break
    if bucket is None:
        bucket = sorted(cfg.util_cc_buckets, key=lambda x: x.from_cc)[-1]
    tables = cfg.util_tables
    if age_bucket == "under_3" and cfg.util_tables_under3:
        tables = cfg.util_tables_under3
    elif age_bucket == "3_5" and cfg.util_tables_3_5:
        tables = cfg.util_tables_3_5
    elif age_bucket == "electric" and cfg.util_tables_electric:
        tables = cfg.util_tables_electric
    table = tables.get(bucket.table)
    use_kw = kw is not None and float(kw) > 0
    rows = table.kw if use_kw else table.hp
    val = float(kw) if use_kw else float(hp or 0)
    picked = None
    for r in rows:
        if r.from_ <= val <= r.to:
            picked = r
            break
    return {
        "cc_bucket": bucket.table,
        "use_kw": use_kw,
        "range_from": getattr(picked, "from_", None),
        "range_to": getattr(picked, "to", None),
        "price_rub": getattr(picked, "price_rub", None),
    }


def _print_car_report(car: Car, svc: CarsService) -> None:
    reg_year = car.registration_year or car.year
    reg_month = car.registration_month or 1
    age_months = _calc_age_months(reg_year, reg_month) if reg_year else None
    is_electric = is_bev(
        car.engine_cc,
        float(car.power_kw) if car.power_kw is not None else None,
        float(car.power_hp) if car.power_hp is not None else None,
        car.engine_type,
    )
    age_bucket = "electric" if is_electric else "under_3" if (age_months is not None and age_months < 36) else "3_5"
    payload = car.source_payload or {}
    payload_engine = payload.get("engine_type") or payload.get("full_fuel_type")
    warn = ""
    if (car.engine_type or "").lower().find("electric") >= 0 and car.engine_cc and car.engine_cc > 0:
        warn = "WARN fuel_conflict: engine_type electric but engine_cc>0"

    util_info = _util_bucket_info(
        engine_cc=car.engine_cc or 0,
        kw=float(car.power_kw) if car.power_kw is not None else None,
        hp=int(car.power_hp) if car.power_hp is not None else None,
        age_bucket=age_bucket,
    )
    util_rub = calc_util_fee_rub(
        engine_cc=car.engine_cc or 0,
        kw=float(car.power_kw) if car.power_kw is not None else None,
        hp=int(car.power_hp) if car.power_hp is not None else None,
        cfg=get_customs_config(),
        age_bucket=age_bucket,
    )

    fx = svc.get_fx_rates() or {}
    eur_rate = fx.get("EUR") or 95.0
    try:
        result = build_calc_debug(svc.db, car.id, eur_rate=eur_rate)
    except Exception as exc:
        result = {"error": str(exc)}

    print("=" * 60)
    print(f"id={car.id} {car.brand} {car.model} {car.variant}")
    print(f"reg={reg_month}/{reg_year} age_months={age_months} age_bucket={age_bucket}")
    print(f"cc={car.engine_cc} hp={car.power_hp} kw={car.power_kw} engine_type={car.engine_type}")
    print(f"payload_engine={payload_engine}")
    if warn:
        print(warn)
    print(f"util_path: table={util_info['cc_bucket']} use_kw={util_info['use_kw']} range={util_info['range_from']}-{util_info['range_to']}")
    print(f"util_rub={util_rub}")
    if isinstance(result, dict) and result.get("result") and isinstance(result["result"], dict):
        print(f"total_rub={result['result'].get('total_rub')}")
    else:
        print(f"calc_error={result}")


def main() -> None:
    ids = _parse_ids(os.getenv("IDS"))
    with SessionLocal() as db:
        svc = CarsService(db)
        cars = list(db.query(Car).filter(Car.id.in_(ids)).all())
        for car in cars:
            _print_car_report(car, svc)

        # Optional BEV simulation if none found
        if not any(is_bev(c.engine_cc, c.power_kw, c.power_hp, c.engine_type) for c in cars):
            print("=" * 60)
            print("BEV sample skipped (no BEV in ids)")


if __name__ == "__main__":
    main()
