from pathlib import Path

from backend.app.services.calculator_extractor import CalculatorExtractor
from backend.app.services.calculator_runtime import EstimateRequest, calculate
from pytest import approx


EXCEL_PATH = Path(__file__).resolve().parents[2] / "Калькулятор Авто под заказ.xlsx"
payload = CalculatorExtractor(EXCEL_PATH).extract()


def total(req: EstimateRequest) -> float:
    res = calculate(payload, req)
    return res["total_rub"]


def test_ice_under3_from_excel():
    req = EstimateRequest(
        scenario=None,
        price_net_eur=100_000,
        eur_rate=94.96,
        engine_cc=2992,
        power_hp=None,
        power_kw=None,
        is_electric=False,
        reg_year=2024,
        reg_month=1,
    )
    assert total(req) == approx(17_733_423.76, abs=1)


def test_ice_3_5_from_excel():
    req = EstimateRequest(
        scenario=None,
        price_net_eur=15_000,
        eur_rate=94.96,
        engine_cc=1290,
        power_hp=None,
        power_kw=None,
        is_electric=False,
        reg_year=2021,
        reg_month=1,
    )
    assert total(req) == approx(8_318_170.48, abs=1)


def test_electric_from_excel():
    req = EstimateRequest(
        scenario=None,
        price_net_eur=50_000,
        eur_rate=94.96,
        engine_cc=None,
        power_hp=250,
        power_kw=None,
        is_electric=True,
        reg_year=2023,
        reg_month=1,
    )
    assert total(req) == approx(8_059_739.0, abs=1)
