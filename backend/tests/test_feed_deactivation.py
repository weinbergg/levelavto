from pathlib import Path

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


def test_daily_pipeline_and_daily_tool_guard_deactivation_before_followup_steps():
    root = Path(__file__).resolve().parents[2]
    pipeline = (root / "scripts" / "mobilede_daily_pipeline.sh").read_text(encoding="utf-8")
    daily = (root / "backend" / "app" / "tools" / "mobilede_daily.py").read_text(encoding="utf-8")
    importer = (root / "backend" / "app" / "tools" / "mobilede_csv_import.py").read_text(encoding="utf-8")
    assert "MOBILEDE_STRICT_DEACTIVATION_GUARD" in pipeline
    assert "step=verify_deactivation_gate" in pipeline
    assert "step=update_fx_prices" in pipeline
    assert "--strict-deactivation-guard" in daily
    assert "preflight_deactivation_guard" in daily
    assert '"deactivation_allowed": allow_deactivate' in importer
