import math
from pathlib import Path

from pytest import approx

from backend.app.services.calculator_config_loader import load_runtime_payload
from backend.app.services.calculator_runtime import EstimateRequest, calculate
from backend.app.services.customs_config import get_customs_config, calc_duty_eur, calc_util_fee_rub


CFG_PATH = Path(__file__).resolve().parents[2] / "backend" / "app" / "config" / "calculator.yml"
payload = load_runtime_payload(CFG_PATH)


def _ceil_rub(x: float) -> float:
    return float(math.ceil(x)) if not float(x).is_integer() else float(x)


def _calc_under3_expected(net_eur: float, eur_rate: float, engine_cc: int, hp: float) -> float:
    bank = net_eur * 0.014 + 100
    purchase = net_eur * 0.03
    inspection = 400
    delivery_eu_minsk = 5500
    customs_by = net_eur * 0.24
    delivery_minsk_moscow = 400
    customs_transfer_fee = (delivery_eu_minsk + delivery_minsk_moscow + customs_by) * 0.005
    elpts = 600
    insurance = net_eur * 0.08
    investor = 0
    subtotal_eur = net_eur + bank + purchase + inspection + delivery_eu_minsk + customs_by + customs_transfer_fee + delivery_minsk_moscow + elpts + insurance + investor
    subtotal_rub = subtotal_eur * eur_rate
    duty_rub = calc_duty_eur(engine_cc, get_customs_config()) * eur_rate
    util_rub = calc_util_fee_rub(engine_cc=engine_cc, kw=None, hp=int(hp), cfg=get_customs_config())
    total = subtotal_rub + duty_rub + util_rub
    return _ceil_rub(total)


def _calc_3_5_expected(net_eur: float, eur_rate: float, engine_cc: int, hp: float) -> float:
    bank = net_eur * 0.014 + 100
    purchase = net_eur * 0.02
    inspection = 400
    delivery_eu_moscow = 5000
    insurance = net_eur * 0.08
    subtotal_eur = net_eur + bank + purchase + inspection + delivery_eu_moscow + insurance
    subtotal_rub = subtotal_eur * eur_rate
    duty_rub = calc_duty_eur(engine_cc, get_customs_config()) * eur_rate
    util_rub = calc_util_fee_rub(engine_cc=engine_cc, kw=None, hp=int(hp), cfg=get_customs_config())
    total = subtotal_rub + 65000 + 30000 + duty_rub + util_rub
    return _ceil_rub(total)


def _calc_electric_expected(net_eur: float, eur_rate: float, power_kw: float, power_hp: float) -> float:
    bank = net_eur * 0.014 + 100
    purchase = net_eur * 0.02 + 500
    inspection = 400
    delivery_eu_moscow = 6000
    insurance = net_eur * 0.08
    subtotal_eur = net_eur + bank + purchase + inspection + delivery_eu_moscow + insurance
    subtotal_rub = subtotal_eur * eur_rate
    import_duty = net_eur * 0.15 * eur_rate
    excise = 0.0
    excise_by_kw = payload["scenarios"]["electric"].get("excise_by_kw", [])
    excise_by_hp = payload["scenarios"]["electric"].get("excise_by_hp", [])
    if power_kw and power_kw > 0:
        for row in excise_by_kw:
            if row["from_kw"] <= power_kw <= row["to_kw"]:
                excise = power_kw * row["rub_per_kw"]
                break
    else:
        for row in excise_by_hp:
            if row["from_hp"] <= power_hp <= row["to_hp"]:
                excise = power_hp * row["rub_per_hp"]
                break
    vat = (net_eur * eur_rate + excise) * 0.22
    util_rub = calc_util_fee_rub(engine_cc=0, kw=power_kw, hp=int(power_hp), cfg=get_customs_config())
    total = subtotal_rub + 115000 + 30000 + import_duty + excise + vat + util_rub
    return _ceil_rub(total)


def test_ice_under3_formula():
    req = EstimateRequest(
        scenario=None,
        price_net_eur=100_000,
        eur_rate=94.96,
        engine_cc=2992,
        power_hp=200,
        power_kw=None,
        is_electric=False,
        reg_year=2024,
        reg_month=1,
    )
    res = calculate(payload, req)
    expected = _calc_under3_expected(100_000, 94.96, 2992, 200)
    assert res["total_rub"] == approx(expected, abs=1)


def test_ice_3_5_formula():
    req = EstimateRequest(
        scenario=None,
        price_net_eur=15_000,
        eur_rate=94.96,
        engine_cc=1290,
        power_hp=150,
        power_kw=None,
        is_electric=False,
        reg_year=2021,
        reg_month=1,
    )
    res = calculate(payload, req)
    expected = _calc_3_5_expected(15_000, 94.96, 1290, 150)
    assert res["total_rub"] == approx(expected, abs=1)


def test_electric_formula():
    req = EstimateRequest(
        scenario=None,
        price_net_eur=50_000,
        eur_rate=94.96,
        engine_cc=0,
        power_hp=250,
        power_kw=None,
        is_electric=True,
        reg_year=2023,
        reg_month=1,
    )
    res = calculate(payload, req)
    expected = _calc_electric_expected(50_000, 94.96, 0, 250)
    assert res["total_rub"] == approx(expected, abs=1)
