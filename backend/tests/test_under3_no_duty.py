from backend.app.services.calculator_runtime import EstimateRequest, calculate
from backend.app.services.calculator_config_loader import load_runtime_payload
from pathlib import Path


CFG_PATH = Path(__file__).resolve().parents[2] / "backend" / "app" / "config" / "calculator.yml"


def test_under3_has_no_duty_step():
    payload = load_runtime_payload(CFG_PATH)
    req = EstimateRequest(
        scenario="under_3",
        price_net_eur=86546.0,
        eur_rate=91.08,
        engine_cc=3000,
        power_hp=489.0,
        power_kw=359.66,
        is_electric=False,
        reg_year=2026,
        reg_month=1,
    )
    res = calculate(payload, req)
    titles = [i.get("title") for i in res.get("breakdown", [])]
    assert "Пошлина РФ" not in titles
    assert "НДС" not in titles
    assert "Таможенный сбор" not in titles
