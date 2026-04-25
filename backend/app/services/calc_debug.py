from __future__ import annotations

from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from ..models.car import Car
from ..services.cars_service import CarsService, electric_vehicle_hint_text
from ..services.calculator_config_service import CalculatorConfigService
from ..services.calculator_runtime import EstimateRequest, calculate, is_bev
from ..utils.registration_defaults import get_missing_registration_default


def build_calc_debug(
    db: Session,
    car_id: int,
    eur_rate: Optional[float] = None,
    usd_rate: Optional[float] = None,
    scenario: Optional[str] = None,
) -> Dict[str, Any]:
    car = db.query(Car).filter(Car.id == car_id).first()
    if not car:
        raise ValueError("car not found")

    service = CarsService(db)
    pricing = service.price_info(car)
    cfg_svc = CalculatorConfigService(db)
    cfg = cfg_svc.ensure_default_from_yaml("/app/backend/app/config/calculator.yml")
    if not cfg:
        cfg = cfg_svc.ensure_default_from_path("/app/Калькулятор Авто под заказ.xlsx")
    if not cfg:
        raise ValueError("calculator config not found")

    fx = service.get_fx_rates() or {}
    eur_rate_used = eur_rate or fx.get("EUR") or cfg.payload.get("meta", {}).get("eur_rate_default") or 95.0
    usd_rate_used = usd_rate or fx.get("USD") or cfg.payload.get("meta", {}).get("usd_rate_default") or 85.0

    price_net_eur = None
    price_source = None
    if pricing.get("net_eur") is not None:
        candidate = float(pricing["net_eur"])
        if candidate > 0:
            price_net_eur = candidate
            price_source = "payload.net_eur"
    elif pricing.get("gross_eur") is not None:
        candidate = float(pricing["gross_eur"])
        if candidate > 0:
            price_net_eur = candidate
            price_source = "payload.gross_eur"
    elif car.price is not None and car.currency:
        cur = car.currency.lower()
        if float(car.price) > 0:
            if cur == "eur":
                price_net_eur = float(car.price)
                price_source = "car.price_eur"
            elif cur == "usd":
                price_net_eur = float(car.price) * (usd_rate_used / eur_rate_used)
                price_source = "car.price_usd"
            elif cur == "rub":
                price_net_eur = float(car.price) / eur_rate_used
                price_source = "car.price_rub"

    notes = []
    effective_engine_cc = car.engine_cc if car.engine_cc is not None else car.inferred_engine_cc
    effective_power_hp = car.power_hp if car.power_hp is not None else car.inferred_power_hp
    effective_power_kw = car.power_kw if car.power_kw is not None else car.inferred_power_kw
    is_electric = is_bev(
        effective_engine_cc,
        float(effective_power_kw) if effective_power_kw is not None else None,
        float(effective_power_hp) if effective_power_hp is not None else None,
        car.engine_type,
        brand=car.brand,
        model=car.model,
        variant=car.variant,
        text_hint=electric_vehicle_hint_text(car),
    )
    if car.engine_type and "electric" in car.engine_type.lower() and car.engine_cc and car.engine_cc > 0:
        if is_electric:
            notes.append("fuel_conflict: engine_type electric but raw engine_cc>0, ignoring engine_cc for EV classification")
        else:
            notes.append("fuel_conflict: engine_type electric but engine_cc>0, treating as ICE/PHEV")

    fallback_reg_year, fallback_reg_month = get_missing_registration_default()

    result: Dict[str, Any] | None = None
    if price_net_eur is not None and price_net_eur > 0:
        req = EstimateRequest(
            scenario=scenario,
            price_net_eur=price_net_eur,
            eur_rate=eur_rate_used,
            engine_cc=effective_engine_cc,
            power_hp=float(effective_power_hp) if effective_power_hp is not None else None,
            power_kw=float(effective_power_kw) if effective_power_kw is not None else None,
            is_electric=is_electric,
            reg_year=car.registration_year or fallback_reg_year,
            reg_month=car.registration_month or fallback_reg_month,
        )
        result = calculate(cfg.payload, req)
    else:
        notes.append("missing_or_nonpositive_price: calc skipped")
        req = EstimateRequest(
            scenario=scenario,
            price_net_eur=0,
            eur_rate=eur_rate_used,
            engine_cc=effective_engine_cc,
            power_hp=float(effective_power_hp) if effective_power_hp is not None else None,
            power_kw=float(effective_power_kw) if effective_power_kw is not None else None,
            is_electric=is_electric,
            reg_year=car.registration_year or fallback_reg_year,
            reg_month=car.registration_month or fallback_reg_month,
        )
        result = {"scenario": scenario, "total_rub": None, "euro_rate_used": eur_rate_used, "breakdown": []}

    steps = [
        {"name": "price_net_eur", "value": price_net_eur, "note": price_source},
        {"name": "eur_rate", "value": eur_rate_used},
    ]
    if price_net_eur is not None:
        steps.append(
            {
                "name": "eur_to_rub",
                "formula": "price_net_eur * eur_rate",
                "value": price_net_eur * eur_rate_used,
            }
        )
    for item in result.get("breakdown", []):
        steps.append(
            {
                "name": item.get("title"),
                "value": item.get("amount"),
                "currency": item.get("currency"),
            }
        )

    return {
        "input": {
            "car_id": car_id,
            "scenario": scenario,
            "eur_rate": eur_rate_used,
            "usd_rate": usd_rate_used,
            "price_net_eur": price_net_eur,
            "effective_reg_year": req.reg_year,
            "effective_reg_month": req.reg_month,
        },
        "car": {
            "id": car.id,
            "brand": car.brand,
            "model": car.model,
            "variant": car.variant,
            "price": float(car.price) if car.price is not None else None,
            "currency": car.currency,
            "engine_cc": car.engine_cc,
            "power_hp": float(car.power_hp) if car.power_hp is not None else None,
            "power_kw": float(car.power_kw) if car.power_kw is not None else None,
            "inferred_engine_cc": car.inferred_engine_cc,
            "inferred_power_hp": float(car.inferred_power_hp) if car.inferred_power_hp is not None else None,
            "inferred_power_kw": float(car.inferred_power_kw) if car.inferred_power_kw is not None else None,
            "effective_engine_cc": effective_engine_cc,
            "effective_power_hp": float(effective_power_hp) if effective_power_hp is not None else None,
            "effective_power_kw": float(effective_power_kw) if effective_power_kw is not None else None,
            "inferred_source_car_id": car.inferred_source_car_id,
            "inferred_confidence": car.inferred_confidence,
            "inferred_rule": car.inferred_rule,
            "registration_year": car.registration_year,
            "registration_month": car.registration_month,
            "registration_fallback_applied": bool(not car.registration_year or not car.registration_month),
            "engine_type": car.engine_type,
            "country": car.country,
            "source_url": car.source_url,
        },
        "pricing": pricing,
        "steps": steps,
        "result": {
            "scenario": result.get("scenario"),
            "total_rub": result.get("total_rub"),
            "euro_rate_used": result.get("euro_rate_used"),
            "config_version": cfg.payload.get("meta", {}).get("version"),
        },
        "notes": notes,
        "config": {
            "version": cfg.payload.get("meta", {}).get("version"),
            "source": cfg.source,
        },
    }


def build_calc_compare(
    db: Session,
    car_id: int,
    eur_rate: Optional[float] = None,
    usd_rate: Optional[float] = None,
    scenario: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Compare current calc steps with Excel-aligned steps (same formula pipeline).
    """
    base = build_calc_debug(db, car_id=car_id, eur_rate=eur_rate, usd_rate=usd_rate, scenario=scenario)
    cfg_svc = CalculatorConfigService(db)
    cfg = cfg_svc.ensure_default_from_yaml("/app/backend/app/config/calculator.yml")
    if not cfg:
        cfg = cfg_svc.ensure_default_from_path("/app/Калькулятор Авто под заказ.xlsx")
    if not cfg:
        raise ValueError("calculator config not found")

    inp = base["input"]
    car = base["car"]
    req = EstimateRequest(
        scenario=scenario,
        price_net_eur=inp.get("price_net_eur") or 0,
        eur_rate=inp.get("eur_rate"),
        engine_cc=car.get("effective_engine_cc"),
        power_hp=car.get("effective_power_hp"),
        power_kw=car.get("effective_power_kw"),
        is_electric=is_bev(
            car.get("effective_engine_cc"),
            car.get("effective_power_kw"),
            car.get("effective_power_hp"),
            car.get("engine_type"),
            brand=car.get("brand"),
            model=car.get("model"),
            variant=car.get("variant"),
            text_hint=" ".join(
                str(part or "").strip()
                for part in (
                    car.get("brand"),
                    car.get("model"),
                    car.get("variant"),
                    car.get("source_url"),
                )
                if str(part or "").strip()
            ),
        ),
        reg_year=car.get("registration_year"),
        reg_month=car.get("registration_month"),
    )
    excel_result = calculate(cfg.payload, req)
    excel_steps = [
        {"name": "price_net_eur", "value": inp.get("price_net_eur"), "note": "input"},
        {"name": "eur_rate", "value": inp.get("eur_rate")},
    ]
    if inp.get("price_net_eur") is not None and inp.get("eur_rate") is not None:
        excel_steps.append(
            {
                "name": "eur_to_rub",
                "formula": "price_net_eur * eur_rate",
                "value": float(inp.get("price_net_eur")) * float(inp.get("eur_rate")),
            }
        )
    for item in excel_result.get("breakdown", []):
        excel_steps.append(
            {
                "name": item.get("title"),
                "value": item.get("amount"),
                "currency": item.get("currency"),
            }
        )

    calc_steps = base.get("steps", [])
    by_name_calc = {s.get("name"): s for s in calc_steps}
    by_name_excel = {s.get("name"): s for s in excel_steps}
    diff = []
    for name, ex in by_name_excel.items():
        if name not in by_name_calc:
            diff.append({"name": name, "calc": None, "excel": ex.get("value"), "delta": None})
            continue
        cv = by_name_calc[name].get("value")
        ev = ex.get("value")
        try:
            delta = float(cv) - float(ev)
        except Exception:
            delta = None
        diff.append({"name": name, "calc": cv, "excel": ev, "delta": delta})

    return {
        "car": base.get("car"),
        "input": base.get("input"),
        "calc_steps": calc_steps,
        "excel_steps": excel_steps,
        "diff": diff,
        "result": {
            "calc_total_rub": base.get("result", {}).get("total_rub"),
            "excel_total_rub": excel_result.get("total_rub"),
            "scenario": excel_result.get("scenario"),
        },
    }
