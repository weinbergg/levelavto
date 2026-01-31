from backend.app.parsing.mobile_de_feed import MobileDeFeedParser
from backend.app.parsing.config import SiteConfig, PaginationConfig


def _parser():
    cfg = SiteConfig(
        key="mobilede",
        name="mobile.de",
        country="DE",
        type="json",
        base_search_url="https://example.com",
        pagination=PaginationConfig(),
    )
    return MobileDeFeedParser(cfg)


def test_normalize_engine_prefers_hybrid():
    p = _parser()
    assert p._normalize_engine("Electric", "plug-in hybrid (petrol/electric)") == "Hybrid"


def test_normalize_engine_electric():
    p = _parser()
    assert p._normalize_engine(None, "Electric") == "Electric"


def test_normalize_engine_diesel():
    p = _parser()
    assert p._normalize_engine("Diesel", None) == "Diesel"
