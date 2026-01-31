from __future__ import annotations

from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from ..models.car import Car
from ..services.cars_service import CarsService
from ..services.calculator_config_service import CalculatorConfigService
from ..services.calculator_runtime import EstimateRequest, calculate, is_bev


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
    if pricing.get("net_eur"):
        price_net_eur = float(pricing["net_eur"])
        price_source = "payload.net_eur"
    elif pricing.get("gross_eur"):
        price_net_eur = float(pricing["gross_eur"])
        price_source = "payload.gross_eur"
    elif car.price is not None and car.currency:
        cur = car.currency.lower()
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
    is_electric = is_bev(
        car.engine_cc,
        float(car.power_kw) if car.power_kw is not None else None,
        float(car.power_hp) if car.power_hp is not None else None,
        car.engine_type,
    )
    if car.engine_type and "electric" in car.engine_type.lower() and car.engine_cc and car.engine_cc > 0:
        notes.append("fuel_conflict: engine_type electric but engine_cc>0, treating as ICE/PHEV")

    req = EstimateRequest(
        scenario=scenario,
        price_net_eur=price_net_eur or 0,
        eur_rate=eur_rate_used,
        engine_cc=car.engine_cc,
        power_hp=float(car.power_hp) if car.power_hp is not None else None,
        power_kw=float(car.power_kw) if car.power_kw is not None else None,
        is_electric=is_electric,
        reg_year=car.registration_year,
        reg_month=car.registration_month,
    )
    result = calculate(cfg.payload, req)

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
            "registration_year": car.registration_year,
            "registration_month": car.registration_month,
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
        engine_cc=car.get("engine_cc"),
        power_hp=car.get("power_hp"),
        power_kw=car.get("power_kw"),
        is_electric=is_bev(
            car.get("engine_cc"),
            car.get("power_kw"),
            car.get("power_hp"),
            car.get("engine_type"),
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
