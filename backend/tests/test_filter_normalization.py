from backend.app.utils.redis_cache import normalize_count_params, normalize_filter_params


def test_normalize_removes_empty_brand():
    params = {"region": "KR", "country": "KR", "brand": "   "}
    assert normalize_count_params(params).get("brand") is None
    assert normalize_filter_params(params).get("brand") is None
