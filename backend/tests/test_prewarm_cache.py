from backend.app.scripts.prewarm_cache import _should_stop


def test_prewarm_max_sec_stop():
    started = 100.0
    max_sec = 1.0
    assert _should_stop(started, max_sec, now=100.5) is False
    assert _should_stop(started, max_sec, now=101.1) is True
