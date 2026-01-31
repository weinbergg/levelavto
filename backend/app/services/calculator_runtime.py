from __future__ import annotations

from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
import math

from .customs_config import get_customs_config, calc_duty_eur, calc_util_fee_rub


@dataclass
class EstimateRequest:
    scenario: Optional[str]
    price_net_eur: float
    eur_rate: Optional[float]
    engine_cc: Optional[int]
    power_hp: Optional[float]
    power_kw: Optional[float]
    is_electric: bool
    reg_year: Optional[int]
    reg_month: Optional[int]


def _find_range(table: List[Dict[str, Any]], val: float, key_from: str, key_to: str, val_key: str, default=None):
    for row in table:
        if row[key_from] <= val <= row[key_to]:
            return row[val_key]
    return default


def _calc_age_months(reg_year: Optional[int], reg_month: Optional[int]) -> Optional[int]:
    if not reg_year or not reg_month:
        return None
    import datetime as dt
    try:
        d = dt.date(reg_year, reg_month, 1)
        today = dt.date.today().replace(day=1)
        months = (today.year - d.year) * 12 + (today.month - d.month)
        return max(months, 0)
    except Exception:
        return None


def is_bev(engine_cc: Optional[int], power_kw: Optional[float], power_hp: Optional[float], engine_type: Optional[str]) -> bool:
    if engine_cc and engine_cc > 0:
        return False
    if not ((power_kw and power_kw > 0) or (power_hp and power_hp > 0)):
        return False
    if not engine_type:
        return False
    fuel = engine_type.lower()
    return ("electric" in fuel) or ("ev" in fuel)


def choose_scenario(req: EstimateRequest, payload: Dict[str, Any]) -> str:
    if req.is_electric:
        return "electric"
    age = _calc_age_months(req.reg_year, req.reg_month)
    if req.scenario:
        return req.scenario
    if age is None:
        raise ValueError("cannot determine scenario: no registration date")
    if age < 36:
        return "under_3"
    if 36 <= age <= 60:
        return "3_5"
    # >60 месяцев считаем по сценарию 3_5 (по требованию)
    if payload.get("rules", {}).get("age_bucket_over_5y_as_3_5", True):
        return "3_5"
    return "over_5"


def _ceil_rub(value: float) -> float:
    if value is None:
        return value
    # округляем только финальный итог: ceil до рубля
    if float(value).is_integer():
        return float(value)
    return float(math.ceil(value))


def _percent_fixed(amount: float, cfg: Dict[str, Any]) -> float:
    percent = float(cfg.get("percent", 0.0) or 0.0)
    fixed = float(cfg.get("fixed", 0.0) or 0.0)
    return amount * percent + fixed


def _calc_excise_rub(power_kw: Optional[float], power_hp: Optional[float], cfg: Dict[str, Any]) -> float:
    if power_kw is not None and float(power_kw) > 0:
        rate = _find_range(cfg.get("excise_by_kw", []), float(power_kw), "from_kw", "to_kw", "rub_per_kw", 0)
        return float(rate) * float(power_kw) if rate else 0.0
    if power_hp is None:
        return 0.0
    rate = _find_range(cfg.get("excise_by_hp", []), float(power_hp), "from_hp", "to_hp", "rub_per_hp", 0)
    return float(rate) * float(power_hp) if rate else 0.0


def calculate(payload: Dict[str, Any], req: EstimateRequest) -> Dict[str, Any]:
    def f(x) -> float:
        if x is None:
            return 0.0
        try:
            return float(x)
        except Exception:
            return 0.0

    scenarios = payload["scenarios"]
    scenario_key = choose_scenario(req, payload)
    if scenario_key not in scenarios:
        raise ValueError(f"scenario {scenario_key} not found")
    cfg = scenarios[scenario_key]
    eur_rate = f(req.eur_rate or payload["meta"].get("eur_rate_default") or 95.0)

    breakdown = []

    if scenario_key in ("under_3", "3_5"):
        if not req.engine_cc:
            raise ValueError("engine_cc is required for ICE scenarios")

        net_eur = f(req.price_net_eur)
        # EUR расходы по формулам
        bank_transfer_eu = _percent_fixed(net_eur, cfg["bank_transfer_eu"])
        purchase_netto = _percent_fixed(net_eur, cfg["purchase_netto"])
        inspection = f(cfg.get("inspection"))

        if scenario_key == "under_3":
            delivery_eu_minsk = f(cfg.get("delivery_eu_minsk"))
            customs_by = net_eur * f(cfg.get("customs_by_percent"))
            delivery_minsk_moscow = f(cfg.get("delivery_minsk_moscow"))
            customs_transfer_fee = (
                (delivery_eu_minsk + delivery_minsk_moscow + customs_by)
                * f(cfg.get("customs_transfer_fee_percent"))
            )
            elpts = f(cfg.get("elpts"))
            insurance_broker_commission = net_eur * f(cfg.get("insurance_broker_commission_percent"))
            investor_fee = f(cfg.get("investor_fee"))
            eur_fields = {
                "bank_transfer_eu": bank_transfer_eu,
                "purchase_netto": purchase_netto,
                "inspection": inspection,
                "delivery_eu_minsk": delivery_eu_minsk,
                "customs_by": customs_by,
                "customs_transfer_fee": customs_transfer_fee,
                "delivery_minsk_moscow": delivery_minsk_moscow,
                "elpts": elpts,
                "insurance_broker_commission": insurance_broker_commission,
                "investor_fee": investor_fee,
            }
        else:
            delivery_eu_moscow = f(cfg.get("delivery_eu_moscow"))
            insurance_broker_commission = net_eur * f(cfg.get("insurance_broker_commission_percent"))
            eur_fields = {
                "bank_transfer_eu": bank_transfer_eu,
                "purchase_netto": purchase_netto,
                "inspection": inspection,
                "delivery_eu_moscow": delivery_eu_moscow,
                "insurance_broker_commission": insurance_broker_commission,
            }

        rub_fields = {
            "broker_elpts": f(cfg.get("broker_elpts_rub")),
            "customs_fee": f(cfg.get("customs_fee_rub")),
        }

        sum_eur = net_eur + sum(eur_fields.values())
        subtotal_rub = sum_eur * eur_rate

        for k, v in eur_fields.items():
            breakdown.append({"title": k, "amount": v, "currency": "EUR"})
        for k, v in rub_fields.items():
            breakdown.append({"title": k, "amount": v, "currency": "RUB"})

        customs_cfg = get_customs_config()
        duty_rub = 0.0
        if cfg.get("duty_enabled", True):
            duty_eur = calc_duty_eur(req.engine_cc, customs_cfg)
            duty_rub = duty_eur * eur_rate

        util_rub = calc_util_fee_rub(
            engine_cc=req.engine_cc,
            kw=req.power_kw,
            hp=int(req.power_hp) if req.power_hp is not None else None,
            cfg=customs_cfg,
        )

        breakdown.append({"title": "Пошлина", "amount": duty_rub, "currency": "RUB"})
        breakdown.append({"title": "Утилизационный сбор", "amount": util_rub, "currency": "RUB"})

        total_rub = subtotal_rub + duty_rub + util_rub + sum(rub_fields.values())
        total_rub = _ceil_rub(total_rub)
        breakdown.append({"title": "Итого (RUB)", "amount": total_rub, "currency": "RUB"})
        return {
            "scenario": scenario_key,
            "total_rub": total_rub,
            "breakdown": breakdown,
            "euro_rate_used": eur_rate,
        }

    # electric (BEV only)
    power_kw = f(req.power_kw) if req.power_kw is not None else None
    power_hp = f(req.power_hp) if req.power_hp is not None else None
    if not ((power_kw and power_kw > 0) or (power_hp and power_hp > 0)):
        raise ValueError("power_kw or power_hp required for electric")

    net_eur = f(req.price_net_eur)
    bank_transfer_eu = _percent_fixed(net_eur, cfg["bank_transfer_eu"])
    purchase_netto = _percent_fixed(net_eur, cfg["purchase_netto"])
    inspection = f(cfg.get("inspection"))
    delivery_eu_moscow = f(cfg.get("delivery_eu_moscow"))
    insurance_broker_commission = net_eur * f(cfg.get("insurance_broker_commission_percent"))

    eur_fields = {
        "bank_transfer_eu": bank_transfer_eu,
        "purchase_netto": purchase_netto,
        "inspection": inspection,
        "delivery_eu_moscow": delivery_eu_moscow,
        "insurance_broker_commission": insurance_broker_commission,
    }
    rub_fields = {
        "broker_elpts": f(cfg.get("broker_elpts_rub")),
        "customs_fee": f(cfg.get("customs_fee_rub")),
    }

    subtotal_eur = net_eur + sum(eur_fields.values())
    subtotal_rub = subtotal_eur * eur_rate
    import_duty_rub = net_eur * f(cfg.get("import_duty_percent")) * eur_rate
    excise_rub = _calc_excise_rub(power_kw, power_hp, cfg)
    vat_base = (net_eur * eur_rate) + excise_rub
    vat_rub = vat_base * f(cfg.get("vat_percent"))

    customs_cfg = get_customs_config()
    util_rub = calc_util_fee_rub(
        engine_cc=req.engine_cc or 0,
        kw=power_kw,
        hp=int(power_hp) if power_hp is not None else None,
        cfg=customs_cfg,
    )

    breakdown.extend([{"title": k, "amount": v, "currency": "EUR"} for k, v in eur_fields.items()])
    breakdown.extend([{"title": k, "amount": v, "currency": "RUB"} for k, v in rub_fields.items()])
    breakdown.append({"title": "import_duty", "amount": import_duty_rub, "currency": "RUB"})
    breakdown.append({"title": "excise", "amount": excise_rub, "currency": "RUB"})
    breakdown.append({"title": "vat", "amount": vat_rub, "currency": "RUB"})
    breakdown.append({"title": "Утилизационный сбор", "amount": util_rub, "currency": "RUB"})

    total_rub = subtotal_rub + import_duty_rub + excise_rub + vat_rub + util_rub + sum(rub_fields.values())
    total_rub = _ceil_rub(total_rub)
    breakdown.append({"title": "Итого (RUB)", "amount": total_rub, "currency": "RUB"})
    return {
        "scenario": "electric",
        "total_rub": total_rub,
        "breakdown": breakdown,
        "euro_rate_used": eur_rate,
    }
