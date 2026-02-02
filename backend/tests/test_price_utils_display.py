from backend.app.utils.price_utils import display_price_rub


def test_display_price_prefers_total_price_and_rounds():
    # total_price_rub_cached takes precedence and rounds up to 100k
    assert display_price_rub(14_760_777, 9_000_001) == 14_800_000


def test_display_price_fallback_total_when_no_ad_price():
    # fallback to total price and round up to 100k
    assert display_price_rub(12_345_001, None) == 12_400_000


def test_display_price_allows_price_fallback_for_kr_only():
    # fallback to price only when explicitly allowed
    assert display_price_rub(None, 6_548_387.7, allow_price_fallback=True) == 6_600_000
    assert display_price_rub(None, 6_548_387.7, allow_price_fallback=False) is None
