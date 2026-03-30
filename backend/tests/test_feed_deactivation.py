from backend.app.utils.feed_deactivation import should_deactivate_feed


def test_auto_deactivation_allows_full_enough_feed():
    allow, reason = should_deactivate_feed(
        mode="auto",
        current_seen=1_590_000,
        previous_seen=1_610_000,
        min_ratio=0.93,
        min_seen=100_000,
    )
    assert allow is True
    assert "ratio=" in reason


def test_auto_deactivation_blocks_short_feed_drop():
    allow, reason = should_deactivate_feed(
        mode="auto",
        current_seen=1_200_000,
        previous_seen=1_610_000,
        min_ratio=0.93,
        min_seen=100_000,
    )
    assert allow is False
    assert "<0.9300" in reason


def test_force_and_skip_modes_override_auto_logic():
    assert should_deactivate_feed(
        mode="force",
        current_seen=10,
        previous_seen=None,
        min_ratio=0.93,
        min_seen=100_000,
    )[0] is True
    assert should_deactivate_feed(
        mode="skip",
        current_seen=1_590_000,
        previous_seen=1_610_000,
        min_ratio=0.93,
        min_seen=100_000,
    )[0] is False
