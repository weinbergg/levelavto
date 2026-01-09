import math
from backend.app.services.calculator import calculate_import_cost, calc_duty_rub


def test_duty_3_5_example():
    # From Excel: 3-5 years, price 20000 EUR, rate 95.0796, engine 1490 cc -> duty ~240,836.6268 RUB
    payload = {
        "scenario": "3_5",
        "price_net_eur": 20000,
        "eur_rate": 95.0796,
        "engine_cc": 1490,
    }
    duty = calc_duty_rub(1490, 95.0796)
    assert abs(duty - 240_836.6268) < 1
    result = calculate_import_cost(payload)
    total = result["total_rub"]
    # Expect close to Excel total 3,174,967.315
    # допускаем небольшую разницу из-за округлений
    assert abs(total - 3_174_967.315) < 2000


def test_electric_example():
    # From Excel: electric, price 150000 EUR, rate 95.0796, power 750 hp -> total ~29,704,470.82
    payload = {
        "scenario": "electric",
        "price_net_eur": 150000,
        "eur_rate": 95.0796,
        "power_hp": 750,
    }
    result = calculate_import_cost(payload)
    total = result["total_rub"]
    # допускаем небольшую погрешность
    assert abs(total - 29_704_470.82) < 10_000
