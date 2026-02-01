from backend.app.utils.price_utils import ceil_to_step


def test_kr_price_usd_to_rub_rounding():
    price_usd = 12345.67
    usd_rate = 91.0
    rub = price_usd * usd_rate
    rounded = ceil_to_step(rub, 100000)
    assert rounded % 100000 == 0
    assert rounded >= rub
