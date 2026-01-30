from decimal import Decimal
from pathlib import Path

from backend.app.services.calculator_config_loader import load_runtime_payload
from backend.app.services.calculator_runtime import EstimateRequest, calculate

CFG_PATH = Path(__file__).resolve().parents[2] / "backend" / "app" / "config" / "calculator.yml"


def test_decimal_price_net_eur_does_not_crash():
    payload = load_runtime_payload(CFG_PATH)
    req = EstimateRequest(
        scenario=None,
        price_net_eur=Decimal("123.45"),
        eur_rate=91.28,
        engine_cc=1995,
        power_hp=None,
        power_kw=None,
        is_electric=False,
        reg_year=2024,
        reg_month=1,
    )
    res = calculate(payload, req)
    assert res["total_rub"] is not None
