from backend.app.utils.price_utils import (
    display_price_rub,
    has_without_util_marker,
    raw_price_to_rub,
    public_price_allow_without_util,
    resolve_public_display_price_rub,
    resolve_display_price_rub,
    sort_items_by_display_price,
)


def test_display_price_prefers_total_price_and_rounds():
    # total_price_rub_cached takes precedence and rounds up to the configured UI step
    assert display_price_rub(14_760_777, 9_000_001) == 14_770_000


def test_display_price_fallback_total_when_no_ad_price():
    # fallback to total price and round up to the configured UI step
    assert display_price_rub(12_345_001, None) == 12_350_000


def test_display_price_allows_price_fallback_for_kr_only():
    # fallback to price only when explicitly allowed
    assert display_price_rub(None, 6_548_387.7, allow_price_fallback=True) == 6_550_000
    assert display_price_rub(None, 6_548_387.7, allow_price_fallback=False) is None


def test_display_price_ignores_zero_and_negative_values():
    assert display_price_rub(0, 8_000_000) is None
    assert display_price_rub(-1, 8_000_000) is None
    assert display_price_rub(None, 0, allow_price_fallback=True) is None
    assert display_price_rub(None, -500, allow_price_fallback=True) is None


def test_resolve_display_price_uses_raw_price_fx_fallback():
    assert raw_price_to_rub(50_000, "EUR", fx_eur=95.0) == 4_750_000
    assert resolve_display_price_rub(
        None,
        None,
        raw_price=50_000,
        currency="EUR",
        fx_eur=95.0,
    ) == 4_750_000


def test_resolve_display_price_ignores_zero_raw_price():
    assert raw_price_to_rub(0, "EUR", fx_eur=95.0) is None
    assert resolve_display_price_rub(
        None,
        None,
        raw_price=0,
        currency="EUR",
        fx_eur=95.0,
    ) is None


def test_public_display_price_defaults_to_total_only(monkeypatch):
    monkeypatch.delenv("PUBLIC_PRICE_ALLOW_SOURCE_FALLBACK", raising=False)
    assert resolve_public_display_price_rub(
        None,
        6_548_387.7,
        raw_price=50_000,
        currency="EUR",
        fx_eur=95.0,
    ) is None


def test_public_display_price_can_opt_in_to_source_fallback(monkeypatch):
    monkeypatch.setenv("PUBLIC_PRICE_ALLOW_SOURCE_FALLBACK", "1")
    assert resolve_public_display_price_rub(
        None,
        6_548_387.7,
        raw_price=50_000,
        currency="EUR",
        fx_eur=95.0,
    ) == 6_550_000


def test_public_display_price_hides_without_util_rows_by_default(monkeypatch):
    monkeypatch.delenv("PUBLIC_PRICE_ALLOW_WITHOUT_UTIL", raising=False)
    assert has_without_util_marker([{"title": "__without_util_fee", "amount_rub": 0}]) is True
    assert public_price_allow_without_util() is False
    assert resolve_public_display_price_rub(
        2_504_321,
        2_300_000,
        calc_breakdown=[{"title": "__without_util_fee", "amount_rub": 0}],
    ) is None


def test_public_display_price_can_allow_without_util_rows(monkeypatch):
    monkeypatch.setenv("PUBLIC_PRICE_ALLOW_WITHOUT_UTIL", "1")
    assert resolve_public_display_price_rub(
        2_504_321,
        2_300_000,
        calc_breakdown=[{"title": "__without_util_fee", "amount_rub": 0}],
    ) == 2_510_000


def test_sort_items_by_display_price_keeps_visible_order_consistent():
    items = [
        {"id": 3, "display_price_rub": None},
        {"id": 2, "display_price_rub": 5_200_000},
        {"id": 1, "display_price_rub": 4_700_000},
    ]
    sort_items_by_display_price(items, sort="price_asc")
    assert [item["id"] for item in items] == [1, 2, 3]

    sort_items_by_display_price(items, sort="price_desc")
    assert [item["id"] for item in items] == [2, 1, 3]
