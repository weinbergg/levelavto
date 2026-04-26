from pathlib import Path

from pytest import approx

from backend.app.services.calculator_config_loader import load_runtime_payload
from backend.app.services.calculator_runtime import EstimateRequest, calculate, is_bev
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


def test_is_bev_ignores_dirty_engine_cc_for_real_evs_with_power():
    assert is_bev(10, 35.3, 48, "Electric") is True


def test_is_bev_uses_bmw_ix_hint_when_fuel_string_is_garbage():
    assert is_bev(
        None,
        239.77,
        326,
        "based on co₂ emissions (combined)",
        brand="BMW",
        model="iX",
        variant="xDrive40",
    ) is True


def test_is_bev_does_not_treat_hyundai_ix35_as_electric():
    assert is_bev(
        None,
        100.0,
        136,
        "",
        brand="Hyundai",
        model="ix35",
        variant="2.0 CRDi",
    ) is False


def test_is_bev_uses_mokka_e_hint_when_fuel_string_is_garbage():
    assert is_bev(
        None,
        114.74,
        156,
        "based on co₂ emissions (combined)",
        brand="Opel",
        model="Mokka-e",
        variant="Ultimate Long Range",
    ) is True
