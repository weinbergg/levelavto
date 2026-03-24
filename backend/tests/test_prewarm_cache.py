from fastapi import Query

from backend.app.scripts.prewarm_cache import _call_route_with_defaults
from backend.app.scripts.prewarm_cache import _should_stop


def test_prewarm_max_sec_stop():
    started = 100.0
    max_sec = 1.0
    assert _should_stop(started, max_sec, now=100.5) is False
    assert _should_stop(started, max_sec, now=101.1) is True


def test_call_route_with_defaults_unwraps_fastapi_query_defaults():
    def sample_route(
        a=Query(default=None),
        b=Query(default=1),
        c=Query(default="x"),
    ):
        return a, b, c

    assert _call_route_with_defaults(sample_route) == (None, 1, "x")
