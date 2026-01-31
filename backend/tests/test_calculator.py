from pathlib import Path

from pytest import approx

from backend.app.services.calculator_config_loader import load_runtime_payload
from backend.app.services.calculator_runtime import EstimateRequest, calculate
from backend.app.services.customs_config import calc_duty_eur, get_customs_config


def test_duty_3_5_example():
    cfg = get_customs_config()
    duty_eur = calc_duty_eur(1490, cfg)
    duty_rub = duty_eur * 95.0796
    assert duty_rub == approx(240_836.6, abs=5)
    payload = load_runtime_payload(Path(__file__).resolve().parents[2] / "backend" / "app" / "config" / "calculator.yml")
    req = EstimateRequest(
        scenario=None,
        price_net_eur=20000,
        eur_rate=95.0796,
        engine_cc=1490,
        power_hp=150,
        power_kw=None,
        is_electric=False,
        reg_year=2021,
        reg_month=1,
    )
    result = calculate(payload, req)
    assert result["total_rub"] > 0


def test_electric_example():
    payload = load_runtime_payload(Path(__file__).resolve().parents[2] / "backend" / "app" / "config" / "calculator.yml")
    req = EstimateRequest(
        scenario=None,
        price_net_eur=150000,
        eur_rate=95.0796,
        engine_cc=0,
        power_hp=750,
        power_kw=None,
        is_electric=True,
        reg_year=2024,
        reg_month=1,
    )
    result = calculate(payload, req)
    assert result["total_rub"] > 0
