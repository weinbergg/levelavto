from pathlib import Path

from pytest import approx

from backend.app.services.calculator_extractor import CalculatorExtractor
from backend.app.services.calculator_runtime import EstimateRequest, calculate


EXCEL_PATH = Path(__file__).resolve().parents[2] / "Калькулятор Авто под заказ.xlsx"
payload = CalculatorExtractor(EXCEL_PATH).extract()


def _util_from_breakdown(res):
    for item in res.get("breakdown", []):
        if item.get("title") == "Утилизационный сбор":
            return float(item.get("amount", 0))
    return None


def test_util_under3_matches_excel():
    req = EstimateRequest(
        scenario=None,
        price_net_eur=20_000,
        eur_rate=95.0,
        engine_cc=2000,
        power_hp=None,
        power_kw=None,
        is_electric=False,
        reg_year=2024,
        reg_month=1,
    )
    res = calculate(payload, req)
    util = _util_from_breakdown(res)
    assert util == approx(3_448_800.0, rel=0, abs=1)


def test_util_3_5_matches_excel():
    req = EstimateRequest(
        scenario=None,
        price_net_eur=15_000,
        eur_rate=95.0,
        engine_cc=1600,
        power_hp=None,
        power_kw=None,
        is_electric=False,
        reg_year=2021,
        reg_month=2,
    )
    res = calculate(payload, req)
    util = _util_from_breakdown(res)
    assert util == approx(6_885_600.0, rel=0, abs=1)


def test_util_over_5y_uses_3_5_scenario():
    req = EstimateRequest(
        scenario=None,
        price_net_eur=10_000,
        eur_rate=95.0,
        engine_cc=1400,
        power_hp=None,
        power_kw=None,
        is_electric=False,
        reg_year=2016,
        reg_month=5,
    )
    res = calculate(payload, req)
    util = _util_from_breakdown(res)
    assert util == approx(6_885_600.0, rel=0, abs=1)


def test_util_electric_under3():
    req = EstimateRequest(
        scenario=None,
        price_net_eur=50_000,
        eur_rate=95.0,
        engine_cc=None,
        power_hp=250,
        power_kw=None,
        is_electric=True,
        reg_year=2024,
        reg_month=3,
    )
    res = calculate(payload, req)
    util = _util_from_breakdown(res)
    assert util == approx(2_599_200.0, rel=0, abs=1)


def test_util_electric_over3():
    req = EstimateRequest(
        scenario=None,
        price_net_eur=30_000,
        eur_rate=95.0,
        engine_cc=None,
        power_hp=200,
        power_kw=None,
        is_electric=True,
        reg_year=2020,
        reg_month=6,
    )
    res = calculate(payload, req)
    util = _util_from_breakdown(res)
    assert util == approx(2_599_200.0, rel=0, abs=1)
