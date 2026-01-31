import json
from pathlib import Path

import pytest

from backend.app.services.calculator_config_loader import load_runtime_payload
from backend.app.services.calculator_runtime import EstimateRequest, calculate, is_bev


FIX_DIR = Path(__file__).resolve().parents[2] / "calc_debug"
CFG_PATH = Path(__file__).resolve().parents[2] / "backend" / "app" / "config" / "calculator.yml"


def _load_fixture(car_id: int):
    path = FIX_DIR / f"{car_id}.json"
    if not path.exists():
        pytest.skip(f"fixture {path} not found")
    return json.loads(path.read_text(encoding="utf-8"))


def test_phev_not_electric():
    data = _load_fixture(1759419)
    car = data["car"]
    req = EstimateRequest(
        scenario=None,
        price_net_eur=data["input"]["price_net_eur"] or 0,
        eur_rate=data["input"]["eur_rate"],
        engine_cc=car.get("engine_cc"),
        power_hp=car.get("power_hp"),
        power_kw=car.get("power_kw"),
        is_electric=is_bev(
            car.get("engine_cc"),
            car.get("power_kw"),
            car.get("power_hp"),
            car.get("engine_type"),
        ),
        reg_year=car.get("registration_year"),
        reg_month=car.get("registration_month"),
    )
    payload = load_runtime_payload(CFG_PATH)
    res = calculate(payload, req)
    assert res["scenario"] != "electric"


def test_bev_has_util_and_broker():
    data = _load_fixture(2406109)
    car = data["car"]
    req = EstimateRequest(
        scenario=None,
        price_net_eur=data["input"]["price_net_eur"] or 0,
        eur_rate=data["input"]["eur_rate"],
        engine_cc=car.get("engine_cc") or 0,
        power_hp=car.get("power_hp"),
        power_kw=car.get("power_kw"),
        is_electric=is_bev(
            car.get("engine_cc"),
            car.get("power_kw"),
            car.get("power_hp"),
            car.get("engine_type"),
        ),
        reg_year=car.get("registration_year"),
        reg_month=car.get("registration_month"),
    )
    payload = load_runtime_payload(CFG_PATH)
    res = calculate(payload, req)
    steps = {s.get("title"): s for s in res["breakdown"]}
    assert steps.get("broker_elpts", {}).get("amount", 0) > 0
    util = next((s for s in res["breakdown"] if s.get("title") == "Утилизационный сбор"), None)
    assert util is not None


def test_under3_steps_present():
    data = _load_fixture(285235)
    car = data["car"]
    req = EstimateRequest(
        scenario=None,
        price_net_eur=data["input"]["price_net_eur"] or 0,
        eur_rate=data["input"]["eur_rate"],
        engine_cc=car.get("engine_cc"),
        power_hp=car.get("power_hp") or 150,
        power_kw=car.get("power_kw"),
        is_electric=False,
        reg_year=car.get("registration_year") or 2024,
        reg_month=car.get("registration_month") or 1,
    )
    payload = load_runtime_payload(CFG_PATH)
    res = calculate(payload, req)
    names = {s.get("title") for s in res["breakdown"]}
    assert "bank_transfer_eu" in names
    assert "purchase_netto" in names
    assert "Пошлина" in names
    assert "Утилизационный сбор" in names
