from pathlib import Path


def test_emavto_chunk_runner_deactivates_only_after_guarded_full_scan():
    root = Path(__file__).resolve().parents[2]
    runner = (root / "backend" / "app" / "tools" / "emavto_chunk_runner.py").read_text(encoding="utf-8")
    assert "should_deactivate_feed" in runner
    assert "deactivate_missing_by_last_seen" in runner
    assert "reached_end of catalog" in runner or "reached end of catalog" in runner
    assert "stop_reason in {\"error\", \"deadline\"}" in runner

