from backend.app.utils.price_utils import (
    PRICE_NOTE_EUROPE,
    PRICE_NOTE_MOSCOW,
    PRICE_NOTE_WITHOUT_UTIL,
    price_without_util_note,
)


def test_price_without_util_note_marks_calculated_prices_as_moscow():
    note = price_without_util_note(
        display_price=2_510_000,
        total_price_rub_cached=2_504_321,
        calc_breakdown=[],
        region="EU",
        country="DE",
    )
    assert note == PRICE_NOTE_MOSCOW


def test_price_without_util_note_keeps_europe_marker_for_fallback_prices():
    note = price_without_util_note(
        display_price=2_310_000,
        total_price_rub_cached=None,
        calc_breakdown=[],
        region="EU",
        country="DE",
    )
    assert note == PRICE_NOTE_EUROPE


def test_price_without_util_note_keeps_korea_without_util_marker_when_needed():
    note = price_without_util_note(
        display_price=3_110_000,
        total_price_rub_cached=None,
        calc_breakdown=[],
        region="KR",
        country="KR",
    )
    assert note == PRICE_NOTE_WITHOUT_UTIL
