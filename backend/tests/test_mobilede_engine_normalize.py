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


def test_normalize_engine_recovers_hybrid_from_variant_when_full_is_disclaimer():
    """Real-world Cayenne 2026: envkv_consumption_fuel = disclaimer text but
    the variant title and URL slug always contain `e-hybrid` / `e-hyb`."""
    p = _parser()
    assert (
        p._normalize_engine(
            None,
            "Based on CO₂ emissions (combined)",
            hint_texts=("Cayenne E-Hybrid Platinum Edition", None),
        )
        == "Hybrid"
    )
    # URL slug fallback
    assert (
        p._normalize_engine(
            "Based on CO₂ emissions (combined)",
            "Based on CO₂ emissions (combined)",
            hint_texts=(None, None, None,
                        "https://suchen.mobile.de/auto-inserat/porsche-cayenne-e-hybrid-platinum/420659370.html"),
        )
        == "Hybrid"
    )
    # Variant containing "diesel" wins over disclaimer.
    assert (
        p._normalize_engine(
            None,
            "Based on CO₂ emissions (combined)",
            hint_texts=("X5 xDrive 30d Diesel", None),
        )
        == "Diesel"
    )


def test_normalize_engine_orders_hybrid_before_electric_for_e_hybrid():
    """`Cayenne E-Hybrid` must NOT be misclassified as Electric."""
    p = _parser()
    assert p._normalize_engine(None, "E-Hybrid") == "Hybrid"
    assert p._normalize_engine(None, "Cayenne E-Hyb Coupé") == "Hybrid"
    # But pure EV still maps to Electric.
    assert p._normalize_engine(None, "Taycan EV") == "Electric"
    assert p._normalize_engine(None, "EQE 350+") == "Electric"


def test_normalize_engine_returns_none_when_no_hint_anywhere():
    p = _parser()
    assert (
        p._normalize_engine(
            None,
            "Based on CO₂ emissions (combined)",
            hint_texts=("Cayenne S", "Porsche Cayenne S"),
        )
        is None
    )
