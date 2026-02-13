from backend.app.utils.redis_cache import normalize_filter_params, normalize_count_params


def test_normalize_filters_empty_string_to_none():
    params = {
        "region": "eu",
        "country": "de",
        "brand": "",
        "model": "   ",
    }
    out = normalize_filter_params(params)
    assert out["region"] == "EU"
    assert out["country"] == "DE"
    assert "brand" not in out
    assert "model" not in out


def test_normalize_count_empty_string_to_none():
    params = {
        "region": "kr",
        "country": "kr",
        "brand": "",
    }
    out = normalize_count_params(params)
    assert out["region"] == "KR"
    assert out["country"] == "KR"
    assert "brand" not in out


def test_normalize_count_keeps_extended_catalog_fields():
    params = {
        "region": "eu",
        "country": "de",
        "generation": "G05",
        "q": "diesel",
        "line": "BMW|X5|",
        "source": "mobile_de",
        "reg_month_min": "2",
        "reg_month_max": "11",
    }
    out = normalize_count_params(params)
    assert out["region"] == "EU"
    assert out["country"] == "DE"
    assert out["generation"] == "G05"
    assert out["q"] == "diesel"
    assert out["line"] == "BMW|X5|"
    assert out["source"] == "mobile_de"
    assert out["reg_month_min"] == "2"
    assert out["reg_month_max"] == "11"
