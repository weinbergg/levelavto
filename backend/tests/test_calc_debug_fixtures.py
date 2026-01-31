import json
import math
from decimal import Decimal, ROUND_CEILING
from pathlib import Path

import pytest

from backend.app.services.customs_config import get_customs_config, calc_duty_eur, calc_util_fee_rub
from backend.app.services.calculator_config_loader import load_runtime_payload
from backend.app.services.calculator_runtime import EstimateRequest, calculate, is_bev


FIXTURES_DIR = Path(__file__).resolve().parents[2] / "calc_debug"
CFG_PATH = Path(__file__).resolve().parents[2] / "backend" / "app" / "config" / "calculator.yml"
payload = load_runtime_payload(CFG_PATH)


def _ceil_rub(x: Decimal) -> Decimal:
    return x.quantize(Decimal("1"), rounding=ROUND_CEILING)


def _step_map(steps):
    return {s.get("name"): s for s in steps}


def _expected_under_3(net_eur: Decimal, eur_rate: Decimal, engine_cc: int, hp: int, kw: Decimal):
    bank = net_eur * Decimal("0.014") + Decimal("100")
    purchase = net_eur * Decimal("0.03")
    inspection = Decimal("400")
    delivery_eu_minsk = Decimal("5500")
    customs_by = net_eur * Decimal("0.24")
    delivery_minsk_moscow = Decimal("400")
    customs_transfer_fee = (delivery_eu_minsk + delivery_minsk_moscow + customs_by) * Decimal("0.005")
    elpts = Decimal("600")
    insurance = net_eur * Decimal("0.08")
    investor = Decimal("0")
    subtotal_eur = net_eur + bank + purchase + inspection + delivery_eu_minsk + customs_by + delivery_minsk_moscow + customs_transfer_fee + elpts + insurance + investor
    subtotal_rub = subtotal_eur * eur_rate
    util = Decimal(str(calc_util_fee_rub(engine_cc=engine_cc, kw=float(kw) if kw else None, hp=hp, cfg=get_customs_config(), age_bucket="under_3")))
    total = _ceil_rub(subtotal_rub + util)
    return {
        "Банк, за перевод денег": bank,
        "Покупка по НЕТТО": purchase,
        "Осмотр подборщиком": inspection,
        "Доставка Европы- Минска": delivery_eu_minsk,
        "Таможня РБ": customs_by,
        "Доставка Минск- Москва": delivery_minsk_moscow,
        "Комиссия за перевод таможни": customs_transfer_fee,
        "ЭЛПТС": elpts,
        "Страхование, брокер": insurance,
        "Инвестор": investor,
        "Утилизационный сбор": util,
        "Итого (RUB)": total,
    }


def _expected_3_5(net_eur: Decimal, eur_rate: Decimal, engine_cc: int, hp: int, kw: Decimal):
    bank = net_eur * Decimal("0.014") + Decimal("100")
    purchase = net_eur * Decimal("0.02")
    inspection = Decimal("400")
    delivery = Decimal("5000")
    insurance = net_eur * Decimal("0.08")
    subtotal_eur = net_eur + bank + purchase + inspection + delivery + insurance
    subtotal_rub = subtotal_eur * eur_rate
    duty = Decimal(str(calc_duty_eur(engine_cc, get_customs_config()))) * eur_rate
    util = Decimal(str(calc_util_fee_rub(engine_cc=engine_cc, kw=float(kw) if kw else None, hp=hp, cfg=get_customs_config(), age_bucket="3_5")))
    total = _ceil_rub(subtotal_rub + Decimal("65000") + Decimal("30000") + duty + util)
    return {
        "Банк, за перевод денег": bank,
        "Покупка по НЕТТО": purchase,
        "Осмотр подборщиком": inspection,
        "Доставка Европа- МСК": delivery,
        "Страхование, брокер": insurance,
        "Брокер и ЭлПТС": Decimal("65000"),
        "Таможенный сбор": Decimal("30000"),
        "Пошлина РФ": duty,
        "Утилизационный сбор": util,
        "Итого (RUB)": total,
    }


def _expected_electric(net_eur: Decimal, eur_rate: Decimal, engine_cc: int, hp: int, kw: Decimal):
    bank = net_eur * Decimal("0.014") + Decimal("100")
    purchase = net_eur * Decimal("0.02") + Decimal("500")
    inspection = Decimal("400")
    delivery = Decimal("6000")
    insurance = net_eur * Decimal("0.08")
    subtotal_eur = net_eur + bank + purchase + inspection + delivery + insurance
    subtotal_rub = subtotal_eur * eur_rate
    import_duty = net_eur * Decimal("0.15") * eur_rate
    # excise: choose kw if >0 else hp, use config tables
    excise = Decimal("0")
    excise_by_kw = payload["scenarios"]["electric"].get("excise_by_kw", [])
    excise_by_hp = payload["scenarios"]["electric"].get("excise_by_hp", [])
    if kw and kw > 0:
        for row in excise_by_kw:
            if Decimal(str(row["from_kw"])) <= kw <= Decimal(str(row["to_kw"])):
                excise = kw * Decimal(str(row["rub_per_kw"]))
                break
    else:
        for row in excise_by_hp:
            if Decimal(str(row["from_hp"])) <= Decimal(str(row["to_hp"])) and Decimal(str(row["from_hp"])) <= Decimal(str(hp)) <= Decimal(str(row["to_hp"])):
                excise = Decimal(str(hp)) * Decimal(str(row["rub_per_hp"]))
                break
    vat = (net_eur * eur_rate + excise) * Decimal("0.22")
    util = Decimal(str(calc_util_fee_rub(engine_cc=engine_cc or 0, kw=float(kw) if kw else None, hp=hp, cfg=get_customs_config(), age_bucket="electric")))
    total = _ceil_rub(subtotal_rub + Decimal("115000") + Decimal("30000") + import_duty + excise + vat + util)
    return {
        "Банк, за перевод денег": bank,
        "Покупка по НЕТТО": purchase,
        "Осмотр подборщиком": inspection,
        "Доставка Европа- МСК": delivery,
        "Страхование, брокер": insurance,
        "Брокер и ЭлПТС": Decimal("115000"),
        "Таможенный сбор": Decimal("30000"),
        "Ввозная пошлина": import_duty,
        "НДС": vat,
        "Утилизационный сбор": util,
        "Итого (RUB)": total,
    }


@pytest.mark.parametrize(
    "car_id",
    [1759419, 2406117, 2406098, 2406120, 2406115, 2406119, 2406107, 2406109],
)
def test_calc_debug_fixture_matches_formula(car_id):
    if not FIXTURES_DIR.exists():
        pytest.skip("calc_debug fixtures not present")
    path = FIXTURES_DIR / f"{car_id}.json"
    if not path.exists():
        pytest.skip(f"missing fixture {car_id}")
    data = json.loads(path.read_text(encoding="utf-8"))
    car = data.get("car", {})
    steps = data.get("steps", [])
    step = _step_map(steps)
    net_eur = Decimal(str(step["price_net_eur"]["value"]))
    eur_rate = Decimal(str(step["eur_rate"]["value"]))
    engine_cc = car.get("engine_cc") or 0
    hp = int(car.get("power_hp") or 0)
    kw = Decimal(str(car.get("power_kw") or 0))
    is_electric = is_bev(engine_cc, float(car.get("power_kw") or 0), float(car.get("power_hp") or 0), car.get("engine_type"))

    req = EstimateRequest(
        scenario=None,
        price_net_eur=float(net_eur),
        eur_rate=float(eur_rate),
        engine_cc=engine_cc,
        power_hp=float(hp) if hp else None,
        power_kw=float(kw) if kw else None,
        is_electric=is_electric,
        reg_year=car.get("registration_year"),
        reg_month=car.get("registration_month"),
    )
    res = calculate(payload, req)
    scenario = res.get("scenario")

    if scenario == "under_3":
        expected = _expected_under_3(net_eur, eur_rate, engine_cc, hp, kw)
    elif scenario == "3_5":
        expected = _expected_3_5(net_eur, eur_rate, engine_cc, hp, kw)
    elif scenario == "electric":
        expected = _expected_electric(net_eur, eur_rate, engine_cc, hp, kw)
    else:
        pytest.skip(f"unknown scenario {scenario}")

    # compare key steps from calculated breakdown
    calc_step = _step_map([{"name": s.get("title"), "value": s.get("amount")} for s in res.get("breakdown", [])])
    for key, val in expected.items():
        if key not in calc_step:
            pytest.fail(f"missing step {key} for {car_id}")
        got = Decimal(str(calc_step[key]["value"]))
        assert got == pytest.approx(float(val), abs=1), f"{car_id} {key} expected {val} got {got}"
