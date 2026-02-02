from backend.app.utils.price_utils import display_price_rub


def test_display_price_prefers_ad_price_and_rounds():
    # price_rub_cached takes precedence and rounds up to 100k
    assert display_price_rub(9_000_001, 14_760_777) == 14_800_000


def test_display_price_fallback_total_when_no_ad_price():
    # fallback to total price and round up to 100k
    assert display_price_rub(12_345_001, None) == 12_400_000
