import datetime as dt

from backend.app.services.calculator_runtime import EstimateRequest, choose_scenario
import backend.app.services.calculator_runtime as calc_rt


def _req(reg_year: int, reg_month: int) -> EstimateRequest:
    return EstimateRequest(
        scenario=None,
        price_net_eur=10000.0,
        eur_rate=95.0,
        engine_cc=1500,
        power_hp=100.0,
        power_kw=None,
        is_electric=False,
        reg_year=reg_year,
        reg_month=reg_month,
    )


def test_age_bucket_boundaries(monkeypatch):
    monkeypatch.setattr(calc_rt, "_today_date", lambda: dt.date(2025, 2, 1))
    payload = {"rules": {"age_bucket_over_5y_as_3_5": True}}

    assert choose_scenario(_req(2020, 2), payload) == "3_5"  # ровно 5 лет
    assert choose_scenario(_req(2020, 1), payload) == "3_5"  # 5 лет + 1 месяц
    assert choose_scenario(_req(2020, 3), payload) == "3_5"  # 4 года 11 мес
    assert choose_scenario(_req(2022, 2), payload) == "3_5"  # ровно 3 года
    assert choose_scenario(_req(2022, 3), payload) == "under_3"  # 2 года 11 мес
