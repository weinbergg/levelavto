from __future__ import annotations

from typing import Dict, Any, List, Optional
from dataclasses import dataclass

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
    return "3_5"


def calculate(payload: Dict[str, Any], req: EstimateRequest) -> Dict[str, Any]:
    scenarios = payload["scenarios"]
    scenario_key = choose_scenario(req, payload)
    if scenario_key not in scenarios:
        raise ValueError(f"scenario {scenario_key} not found")
    cfg = scenarios[scenario_key]
    eur_rate = req.eur_rate or payload["meta"].get("eur_rate_default") or 95.0

    breakdown = []

    if scenario_key in ("under_3", "3_5"):
        if not req.engine_cc:
            raise ValueError("engine_cc is required for ICE scenarios")
        expenses = cfg.get("expenses", {})
        eur_fields = {k: v for k, v in expenses.items() if k not in ("customs_fee", "broker_elpts", "customs_by")}
        rub_fields = {}
        # explicit rub for 3_5
        if "broker_elpts" in expenses:
            rub_fields["broker_elpts"] = expenses["broker_elpts"]
        if "customs_fee" in expenses:
            rub_fields["customs_fee"] = expenses["customs_fee"]
        if "customs_by" in expenses and scenario_key == "under_3":
            # Таможня РБ в EUR
            eur_fields["customs_by"] = expenses["customs_by"]

        sum_eur = req.price_net_eur + sum(eur_fields.values())
        eur_part_rub = sum_eur * eur_rate
        for k, v in eur_fields.items():
            breakdown.append({"title": k, "amount": v, "currency": "EUR"})
        for k, v in rub_fields.items():
            breakdown.append({"title": k, "amount": v, "currency": "RUB"})

        customs_cfg = get_customs_config()
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

        total_rub = eur_part_rub + duty_rub + util_rub + sum(rub_fields.values())
        breakdown.append({"title": "Итого (RUB)", "amount": total_rub, "currency": "RUB"})
        return {
            "scenario": scenario_key,
            "total_rub": total_rub,
            "breakdown": breakdown,
            "euro_rate_used": eur_rate,
        }

    # electric
    hp = req.power_hp or (req.power_kw * 1.35962 if req.power_kw else None)
    if not hp:
        raise ValueError("power_hp or power_kw required for electric")
    age = _calc_age_months(req.reg_year, req.reg_month) or 0
    age_bucket = "under_3" if age < 36 else "3_5"
    expenses = cfg.get("expenses", {})
    eur_expenses = {k: v for k, v in expenses.items() if k not in ("customs_fee", "broker_elpts")}
    rub_expenses = {}
    if "customs_fee" in expenses:
        rub_expenses["customs_fee"] = expenses["customs_fee"]
    if "broker_elpts" in expenses:
        rub_expenses["broker_elpts"] = expenses["broker_elpts"]

    customs_value_rub = req.price_net_eur * eur_rate
    duty_rub = customs_value_rub * cfg.get("duty_percent", 0.15)
    excise_rate = _find_range(cfg.get("excise_by_hp", []), hp, "from_hp", "to_hp", "rub_per_hp", 0)
    excise_rub = hp * excise_rate if excise_rate else 0
    power_fee = None
    for row in cfg.get("power_fee", []):
        if row["age_bucket"] == age_bucket and row["from_hp"] <= hp <= row["to_hp"]:
            power_fee = row["rub"]
            break
    util_rub = cfg.get("util_rub", 0)
    vat_base = customs_value_rub + duty_rub + excise_rub + (power_fee or 0)
    vat_rub = vat_base * cfg.get("vat_percent", 0.22)

    eur_part_rub = (req.price_net_eur + sum(eur_expenses.values())) * eur_rate
    breakdown.extend([{"title": k, "amount": v, "currency": "EUR"} for k, v in eur_expenses.items()])
    breakdown.extend([{"title": k, "amount": v, "currency": "RUB"} for k, v in rub_expenses.items()])
    breakdown.append({"title": "Ввозная пошлина", "amount": duty_rub, "currency": "RUB"})
    breakdown.append({"title": "Акциз (л.с.)", "amount": excise_rub, "currency": "RUB"})
    if power_fee is not None:
        breakdown.append({"title": "Платёж по мощности/возрасту", "amount": power_fee, "currency": "RUB"})
    breakdown.append({"title": "Утилизационный сбор", "amount": util_rub, "currency": "RUB"})
    breakdown.append({"title": "НДС", "amount": vat_rub, "currency": "RUB"})

    total_rub = eur_part_rub + duty_rub + excise_rub + (power_fee or 0) + util_rub + vat_rub + sum(rub_expenses.values())
    breakdown.append({"title": "Итого (RUB)", "amount": total_rub, "currency": "RUB"})
    return {
        "scenario": "electric",
        "total_rub": total_rub,
        "breakdown": breakdown,
        "euro_rate_used": eur_rate,
    }
