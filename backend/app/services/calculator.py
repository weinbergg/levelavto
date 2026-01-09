from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional
import os
import math


EUR_RATE_FALLBACK = float(os.environ.get("EURO_RATE", "95.0"))


@dataclass
class CalcItem:
    key: str
    title: str
    amount: float
    currency: str
    notes: Optional[str] = None


CC_DUTY_TABLE = [
    (0, 1000, 1.5),
    (1001, 1500, 1.7),
    (1501, 1800, 2.5),
    (1801, 2300, 2.7),
    (2301, 3000, 3.0),
    (3001, 8000, 3.6),
]

# Утилизационный сбор для ДВС (RUB) по объёму
UTIL_FEE_UNDER_3 = [
    (100, 2000, 3_201_600),
    (2000, 3000, 3_448_800),
    (3000, 3500, 5_485_800),
    (3500, 10000, 6_879_000),
]
UTIL_FEE_3_5 = [
    (100, 2000, 5_200),
    (2000, 3000, 8_100),
    (3000, 3500, 12_700),
    (3500, 10000, 16_200),
]

# Утиль для электро (RUB) по kW
UTIL_FEE_ELECTRIC_KW = [
    (0, 30, 0),
    (30, 60, 0),
    (60, 90, 0),
    (90, 120, 0),
    (120, 150, 0),
    (150, 200, 0),
    (200, 250, 0),
    (250, 300, 0),
    (300, 1000, 3_648_000),
]

# Акциз электро: ставка по диапазону hp (из листа, ставки перенесены как есть)
EXCISE_RATE_ELECTRIC_HP = [
    (0, 80, 90),
    (81, 100, 151),
    (101, 130, 201),
    (131, 160, 301),
    (161, 190, 401),
    (191, 220, 501),
    (221, 250, 601),
    (251, 280, 701),
    (281, 408, 901),
    (409, 544, 1401),
    (545, 680, 1601),
    (681, 3000, 1829),  # пример из Excel: 750 hp -> ставка 1829
]


def pick_range(table, value: float, default: float = 0) -> float:
    for lo, hi, v in table:
        if lo <= value <= hi:
            return v
    return default


def get_eur_rate(explicit: Optional[float]) -> float:
    try:
        if explicit and explicit > 0:
            return float(explicit)
    except Exception:
        pass
    return EUR_RATE_FALLBACK


def get_util_fee_rub(engine_cc: int, scenario: str) -> float:
    table = UTIL_FEE_UNDER_3 if scenario == "under_3" else UTIL_FEE_3_5
    return pick_range(table, engine_cc, 0)


def calc_duty_rub(engine_cc: int, eur_rate: float) -> float:
    rate = pick_range(CC_DUTY_TABLE, engine_cc, 0)
    return engine_cc * rate * eur_rate


def calc_excise_electric(power_hp: int) -> float:
    rate = pick_range(EXCISE_RATE_ELECTRIC_HP, power_hp, 0)
    # Особенность Excel: ставка возводится в квадрат
    return float(rate) * float(rate)


def calc_under_3y(price_net_eur: float, eur_rate: float, engine_cc: int) -> Dict:
    fees_eur = [
        ("bank", "Банк, перевод", 1500),
        ("purchase", "Покупка по НЕТТО", 2500),
        ("inspection", "Осмотр подборщиком", 400),
        ("delivery_eu_minsk", "Доставка Европа–Минск", 6500),
        ("customs_by", "Таможня РБ", 29524),
        ("money_transfer_fee", "Комиссия за перевод таможни", 182.62),
        ("delivery_minsk_moscow", "Доставка Минск–Москва", 500),
        ("elpts", "ЭЛПТС", 500),
        ("insurance", "Страхование/брокер/транзит/комиссия", 8000),
    ]
    sum_eur = price_net_eur + sum(v for _, _, v in fees_eur)
    eur_part_rub = sum_eur * eur_rate
    duty_rub = calc_duty_rub(engine_cc, eur_rate)
    util_rub = get_util_fee_rub(engine_cc, "under_3")
    rub_part = duty_rub + util_rub
    total_rub = eur_part_rub + rub_part

    breakdown: List[CalcItem] = []
    breakdown.append(CalcItem("price_net", "Цена нетто", price_net_eur, "EUR"))
    for key, title, val in fees_eur:
        breakdown.append(CalcItem(key, title, val, "EUR"))
    breakdown.append(CalcItem("duty", "Пошлина", duty_rub, "RUB"))
    breakdown.append(CalcItem("util", "Утилизационный сбор", util_rub, "RUB"))
    breakdown.append(CalcItem("total_rub", "Итого (RUB)", total_rub, "RUB"))

    return {
        "total_rub": total_rub,
        "total_eur": None,
        "breakdown": [item.__dict__ for item in breakdown],
        "meta": {
            "euro_rate_used": eur_rate,
            "scenario": "under_3",
            "inputs_echo": {
                "price_net_eur": price_net_eur,
                "engine_cc": engine_cc,
            },
        },
    }


def calc_3_to_5y(price_net_eur: float, eur_rate: float, engine_cc: int) -> Dict:
    fees_eur = [
        ("bank", "Банк, перевод", 380),
        ("purchase", "Покупка по НЕТТО", 900),
        ("inspection", "Осмотр подборщиком", 400),
        ("delivery_eu_moscow", "Доставка Европа–МСК", 6000),
        ("insurance", "Страхование/брокер/транзит/комиссия", 1600),
    ]
    sum_eur = price_net_eur + sum(v for _, _, v in fees_eur)
    eur_part_rub = sum_eur * eur_rate
    duty_rub = calc_duty_rub(engine_cc, eur_rate)
    util_rub = get_util_fee_rub(engine_cc, "3_5")
    rub_only = [
        ("customs_fee", "Таможенный сбор", 30000),
        ("broker_elpts", "Брокер и ЭЛПТС", 115000),
    ]
    rub_part = duty_rub + util_rub + sum(v for _, _, v in rub_only)
    total_rub = eur_part_rub + rub_part

    breakdown: List[CalcItem] = []
    breakdown.append(CalcItem("price_net", "Цена нетто", price_net_eur, "EUR"))
    for key, title, val in fees_eur:
        breakdown.append(CalcItem(key, title, val, "EUR"))
    for key, title, val in rub_only:
        breakdown.append(CalcItem(key, title, val, "RUB"))
    breakdown.append(CalcItem("duty", "Пошлина", duty_rub, "RUB"))
    breakdown.append(CalcItem("util", "Утилизационный сбор", util_rub, "RUB"))
    breakdown.append(CalcItem("total_rub", "Итого (RUB)", total_rub, "RUB"))

    return {
        "total_rub": total_rub,
        "total_eur": None,
        "breakdown": [item.__dict__ for item in breakdown],
        "meta": {
            "euro_rate_used": eur_rate,
            "scenario": "3_5",
            "inputs_echo": {
                "price_net_eur": price_net_eur,
                "engine_cc": engine_cc,
            },
        },
    }


def calc_electric(price_net_eur: float, eur_rate: float, power_hp: int, power_kw: Optional[int] = None) -> Dict:
    fees_eur = [
        ("bank", "Банк, перевод", 2200),
        ("purchase", "Покупка по НЕТТО", 3500),
        ("inspection", "Осмотр подборщиком", 400),
        ("delivery_eu_moscow", "Доставка Европа–МСК", 6000),
        ("insurance", "Страхование/брокер/транзит/комиссия", 12000),
    ]
    customs_value_rub = price_net_eur * eur_rate
    duty_rub = customs_value_rub * 0.15
    excise_rub = calc_excise_electric(power_hp)
    vat_base = customs_value_rub + duty_rub + excise_rub
    vat_rub = vat_base * 0.20
    customs_fee_rub = 30000
    broker_rub = 115000
    util_rub = pick_range(UTIL_FEE_ELECTRIC_KW, power_kw or power_hp, 0)
    eur_part_rub = (price_net_eur + sum(v for _, _, v in fees_eur)) * eur_rate
    total_rub = eur_part_rub + duty_rub + excise_rub + \
        vat_rub + customs_fee_rub + broker_rub + util_rub

    breakdown: List[CalcItem] = []
    breakdown.append(CalcItem("price_net", "Цена нетто", price_net_eur, "EUR"))
    for key, title, val in fees_eur:
        breakdown.append(CalcItem(key, title, val, "EUR"))
    breakdown.append(CalcItem("duty", "Ввозная пошлина 15%", duty_rub, "RUB"))
    breakdown.append(
        CalcItem("excise", "Акциз (Excel формула)", excise_rub, "RUB"))
    breakdown.append(CalcItem("vat", "НДС 20%", vat_rub, "RUB"))
    breakdown.append(
        CalcItem("customs_fee", "Таможенный сбор", customs_fee_rub, "RUB"))
    breakdown.append(
        CalcItem("broker_elpts", "Брокер и ЭЛПТС", broker_rub, "RUB"))
    breakdown.append(CalcItem("util", "Утилизационный сбор", util_rub, "RUB"))
    breakdown.append(CalcItem("total_rub", "Итого (RUB)", total_rub, "RUB"))

    return {
        "total_rub": total_rub,
        "total_eur": None,
        "breakdown": [item.__dict__ for item in breakdown],
        "meta": {
            "euro_rate_used": eur_rate,
            "scenario": "electric",
            "inputs_echo": {
                "price_net_eur": price_net_eur,
                "power_hp": power_hp,
                "power_kw": power_kw,
            },
        },
    }


def calculate_import_cost(payload: Dict) -> Dict:
    scenario = payload.get("scenario")
    price_net_eur = float(payload.get("price_net_eur")
                          or payload.get("price_net") or 0)
    eur_rate = get_eur_rate(payload.get("eur_rate"))
    engine_cc = int(payload.get("engine_cc") or 0)
    power_hp = int(payload.get("power_hp") or 0)
    power_kw = payload.get("power_kw")
    if power_kw is not None:
        power_kw = float(power_kw)

    if scenario == "under_3":
        if not engine_cc:
            raise ValueError("missing engine_cc for under_3")
        return calc_under_3y(price_net_eur, eur_rate, engine_cc)
    if scenario == "3_5":
        if not engine_cc:
            raise ValueError("missing engine_cc for 3_5")
        return calc_3_to_5y(price_net_eur, eur_rate, engine_cc)
    if scenario == "electric":
        if not power_hp:
            raise ValueError("missing power_hp for electric")
        return calc_electric(price_net_eur, eur_rate, power_hp, power_kw)
    raise ValueError("invalid scenario")
