from __future__ import annotations

import argparse
import json
from typing import Any

from backend.app.db import SessionLocal
from backend.app.models import Car
from backend.app.services.calc_debug import build_calc_debug
from backend.app.services.cars_service import (
    CarsService,
    electric_vehicle_hint_text,
    effective_engine_cc_value,
    effective_power_hp_value,
    effective_power_kw_value,
)
from backend.app.services.calculator_runtime import is_bev
from backend.app.utils.price_utils import display_price_rub, price_without_util_note


def _parse_ids(raw: str | None) -> list[int]:
    if not raw:
        return []
    out: list[int] = []
    for part in raw.replace(" ", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            continue
    return sorted(set(out))


def _has_fallback_marker(car: Car) -> bool:
    return any(
        isinstance(row, dict) and row.get("title") == "__without_util_fee"
        for row in (car.calc_breakdown_json or [])
    )


def _classify(car: Car, debug_payload: dict[str, Any] | None, debug_error: str | None) -> tuple[str, list[str]]:
    reasons: list[str] = []
    effective_engine_cc = effective_engine_cc_value(car)
    effective_power_hp = effective_power_hp_value(car)
    effective_power_kw = effective_power_kw_value(car)
    electric = is_bev(
        effective_engine_cc,
        float(effective_power_kw) if effective_power_kw is not None else None,
        float(effective_power_hp) if effective_power_hp is not None else None,
        car.engine_type,
        brand=car.brand,
        model=car.model,
        variant=car.variant,
        text_hint=electric_vehicle_hint_text(car),
    )
    fallback_marker = _has_fallback_marker(car)
    if car.total_price_rub_cached is None:
        reasons.append("missing_cached_total")
    if fallback_marker:
        reasons.append("fallback_price_marker")
    if not electric and effective_engine_cc is None:
        reasons.append("missing_engine_cc")
    if electric and effective_power_hp is None and effective_power_kw is None:
        reasons.append("missing_power")
    if debug_error:
        reasons.append(f"calc_debug_error:{debug_error}")
    if fallback_marker and effective_engine_cc is not None and (electric or effective_power_hp is not None or effective_power_kw is not None):
        reasons.append("recoverable_fallback")
    if car.engine_cc is None and car.inferred_engine_cc is not None:
        reasons.append("uses_inferred_engine_cc")
    if car.power_hp is None and car.inferred_power_hp is not None:
        reasons.append("uses_inferred_power_hp")
    if car.power_kw is None and car.inferred_power_kw is not None:
        reasons.append("uses_inferred_power_kw")

    if debug_payload and not debug_error and car.total_price_rub_cached is not None and not fallback_marker:
        return "calculated", reasons
    if fallback_marker:
        return "fallback_price", reasons
    if car.total_price_rub_cached is None:
        return "missing_price_cache", reasons
    return "needs_review", reasons


def _row_for_car(service: CarsService, car: Car) -> dict[str, Any]:
    display_price = display_price_rub(car.total_price_rub_cached, car.price_rub_cached)
    price_note = price_without_util_note(
        display_price=display_price,
        total_price_rub_cached=car.total_price_rub_cached,
        calc_breakdown=car.calc_breakdown_json,
        country=car.country,
    )
    debug_payload: dict[str, Any] | None = None
    debug_error: str | None = None
    try:
        debug_payload = build_calc_debug(service.db, car.id)
    except Exception as exc:
        debug_error = str(exc)

    status, reasons = _classify(car, debug_payload, debug_error)
    return {
        "id": car.id,
        "brand": car.brand,
        "model": car.model,
        "variant": car.variant,
        "year": car.year,
        "country": car.country,
        "status": status,
        "reasons": reasons,
        "display_price_rub": display_price,
        "price_note": price_note,
        "price_rub_cached": float(car.price_rub_cached) if car.price_rub_cached is not None else None,
        "total_price_rub_cached": float(car.total_price_rub_cached) if car.total_price_rub_cached is not None else None,
        "has_fallback_marker": _has_fallback_marker(car),
        "engine_cc": car.engine_cc,
        "inferred_engine_cc": car.inferred_engine_cc,
        "effective_engine_cc": effective_engine_cc_value(car),
        "power_hp": float(car.power_hp) if car.power_hp is not None else None,
        "inferred_power_hp": float(car.inferred_power_hp) if car.inferred_power_hp is not None else None,
        "effective_power_hp": float(effective_power_hp_value(car)) if effective_power_hp_value(car) is not None else None,
        "power_kw": float(car.power_kw) if car.power_kw is not None else None,
        "inferred_power_kw": float(car.inferred_power_kw) if car.inferred_power_kw is not None else None,
        "effective_power_kw": float(effective_power_kw_value(car)) if effective_power_kw_value(car) is not None else None,
        "inferred_source_car_id": car.inferred_source_car_id,
        "inferred_confidence": car.inferred_confidence,
        "inferred_rule": car.inferred_rule,
        "calc_updated_at": car.calc_updated_at.isoformat() if car.calc_updated_at else None,
        "updated_at": car.updated_at.isoformat() if car.updated_at else None,
        "spec_inferred_at": car.spec_inferred_at.isoformat() if car.spec_inferred_at else None,
        "calc_debug_total_rub": (
            debug_payload.get("result", {}).get("total_rub")
            if isinstance(debug_payload, dict)
            else None
        ),
        "calc_debug_error": debug_error,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Explain why specific cars are calculated or still fallback-priced")
    ap.add_argument("--ids", required=True, help="comma-separated car ids")
    ap.add_argument("--fix", action="store_true", help="force ensure_calc_cache() for supplied ids before reporting")
    args = ap.parse_args()

    ids = _parse_ids(args.ids)
    if not ids:
        raise SystemExit("No valid ids passed")

    rows: list[dict[str, Any]] = []
    with SessionLocal() as db:
        service = CarsService(db)
        cars = {car.id: car for car in db.query(Car).filter(Car.id.in_(ids)).all()}
        for car_id in ids:
            car = cars.get(car_id)
            if not car:
                rows.append({"id": car_id, "status": "not_found", "reasons": ["not_found"]})
                continue
            if args.fix:
                try:
                    service.ensure_calc_cache(car, force=True)
                    db.refresh(car)
                except Exception as exc:
                    rows.append(
                        {
                            "id": car_id,
                            "brand": car.brand,
                            "model": car.model,
                            "status": "fix_failed",
                            "reasons": [str(exc)],
                        }
                    )
                    continue
            rows.append(_row_for_car(service, car))

    print(json.dumps({"ids": ids, "rows": rows}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
