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


def test_parse_first_registration_month_year():
    p = _parser()
    assert p._parse_first_registration("03/2024") == (2024, 3)


def test_parse_first_registration_day_month_year():
    p = _parser()
    assert p._parse_first_registration("21.03.2024") == (2024, 3)


def test_parse_first_registration_year_month():
    p = _parser()
    assert p._parse_first_registration("2024/03") == (2024, 3)


def test_parse_first_registration_year_only():
    p = _parser()
    assert p._parse_first_registration("2024") == (2024, None)


def test_normalize_engine_drops_co2_disclaimer_text():
    """Regression: mobile.de's CSV ships a regulatory disclaimer instead of
    an actual fuel type for ~90% of Porsche Cayenne 2025+ listings (and many
    others). Before the fix, the disclaimer text was silently passed through
    and ended up in cars.engine_type, making the rows invisible to the
    public fuel filter (hybrid/diesel/petrol/electric)."""
    p = _parser()
    assert p._normalize_engine(None, "Based on CO₂ emissions (combined)") is None
    assert p._normalize_engine("based on consumption (combined)", None) is None
    assert p._normalize_engine("based on co2 emissions", "210 kw") is None


def test_normalize_engine_falls_back_from_disclaimer_full_to_raw_keyword():
    """If `full` is disclaimer text but `raw` has a real keyword, still match."""
    p = _parser()
    assert p._normalize_engine("Hybrid", "Based on CO₂ emissions (combined)") == "Hybrid"
    assert p._normalize_engine("Diesel", "Based on CO₂ emissions (combined)") == "Diesel"


def test_normalize_engine_phev_and_electric_synonyms():
    p = _parser()
    assert p._normalize_engine(None, "PHEV") == "Hybrid"
    assert p._normalize_engine(None, "Elektro") == "Electric"
    assert p._normalize_engine(None, "Autogas LPG") == "LPG"
    assert p._normalize_engine(None, "Erdgas (CNG)") == "CNG"
