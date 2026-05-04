from backend.app.utils.home_recommendation_blocks import (
    HOME_RECOMMENDATION_BLOCK_LIMIT_DEFAULT,
    build_home_recommendation_blocks,
    load_home_recommendation_blocks,
    normalize_block_query,
)


def test_normalize_block_query_extracts_supported_catalog_params_and_defaults_sort():
    query = normalize_block_query(
        "https://levelavto.ru/catalog?country=de&brand=BMW&model=X5&page=4&page_size=40&sort=price_desc"
    )
    assert "country=DE" in query
    assert "brand=BMW" in query
    assert "model=X5" in query
    assert "sort=price_desc" in query
    assert "page=" not in query
    assert "page_size=" not in query


def test_build_home_recommendation_blocks_skips_blank_rows_and_caps_limit():
    blocks = build_home_recommendation_blocks(
        ["BMW", "", "Audi"],
        [
            "/catalog?region=EU&country=DE&brand=BMW",
            "",
            "/catalog?region=EU&country=DE&brand=Audi&sort=price_asc",
        ],
        ["99", "", "3"],
        ["1", "1", "0"],
    )
    assert len(blocks) == 2
    assert blocks[0]["title"] == "BMW"
    assert blocks[0]["limit"] == 12
    assert blocks[1]["title"] == "Audi"
    assert blocks[1]["limit"] == 4
    assert blocks[1]["enabled"] is False


def test_load_home_recommendation_blocks_roundtrips_json_text():
    loaded = load_home_recommendation_blocks(
        """
        [
          {"title": "BMW X5", "query": "/catalog?country=DE&brand=BMW&model=X5", "limit": 6, "enabled": true},
          {"title": "", "query": "", "limit": 0, "enabled": true}
        ]
        """
    )
    assert len(loaded) == 1
    assert loaded[0]["title"] == "BMW X5"
    assert loaded[0]["limit"] == 6
    assert "sort=price_asc" in loaded[0]["query"]
    assert HOME_RECOMMENDATION_BLOCK_LIMIT_DEFAULT >= 4
