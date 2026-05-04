from backend.app.utils.home_recommendation_blocks import (
    HOME_RECOMMENDATION_BLOCK_LIMIT_DEFAULT,
    build_block_catalog_query,
    build_home_recommendation_blocks,
    load_home_recommendation_blocks,
)


def test_build_home_recommendation_blocks_skips_blank_rows_caps_limit_and_parses_ids():
    blocks = build_home_recommendation_blocks(
        ["BMW / X5", "", "Audi свежие"],
        ["99", "", "3"],
        ["1", "1", "0"],
        ["BMW|X5|\nBMW|X6|", "", "Audi|A6|"],
        ["1000000", "", ""],
        ["5000000", "", "7000000"],
        ["60000", "", "30000"],
        ["2023", "", "2024"],
        ["2026", "", "2026"],
        ["350", "", "250"],
        ["3000", "", "2000"],
        ["333438, 300957", "", "12345"],
    )
    assert len(blocks) == 2
    assert blocks[0]["title"] == "BMW / X5"
    assert blocks[0]["limit"] == 12
    assert blocks[0]["lines"] == ["BMW|X5|", "BMW|X6|"]
    assert blocks[0]["car_ids"] == [333438, 300957]
    assert blocks[0]["price_min"] == 1000000.0
    assert blocks[1]["title"] == "Audi свежие"
    assert blocks[1]["limit"] == 4
    assert blocks[1]["enabled"] is False
    assert blocks[1]["car_ids"] == [12345]


def test_load_home_recommendation_blocks_supports_legacy_query_and_explicit_schema():
    loaded = load_home_recommendation_blocks(
        """
        [
          {
            "title": "BMW legacy",
            "query": "/catalog?country=DE&brand=BMW&model=X5&price_max=5000000&reg_year_min=2024",
            "limit": 6,
            "enabled": true,
            "car_ids": [333438]
          },
          {
            "title": "Audi explicit",
            "lines": ["Audi|A6|", "Audi|A4|"],
            "limit": 5,
            "enabled": true,
            "mileage_max": 30000
          }
        ]
        """
    )
    assert len(loaded) == 2
    assert loaded[0]["title"] == "BMW legacy"
    assert loaded[0]["lines"] == ["BMW|X5|"]
    assert loaded[0]["price_max"] == 5000000.0
    assert loaded[0]["reg_year_min"] == 2024
    assert loaded[0]["car_ids"] == [333438]
    assert loaded[1]["lines"] == ["Audi|A6|", "Audi|A4|"]
    assert loaded[1]["mileage_max"] == 30000
    assert HOME_RECOMMENDATION_BLOCK_LIMIT_DEFAULT >= 4


def test_build_block_catalog_query_uses_explicit_block_fields():
    query = build_block_catalog_query(
        {
            "lines": ["Mercedes-Benz|E-Class|", "Audi|A6|"],
            "price_min": 1000000,
            "price_max": 4000000,
            "mileage_max": 50000,
            "reg_year_min": 2023,
            "reg_year_max": 2026,
            "power_hp_max": 350,
            "engine_cc_max": 3000,
        }
    )
    assert "region=EU" in query
    assert "line=Mercedes-Benz%7CE-Class%7C" in query
    assert "line=Audi%7CA6%7C" in query
    assert "price_min=1000000" in query
    assert "price_max=4000000" in query
    assert "mileage_max=50000" in query
    assert "reg_year_min=2023" in query
    assert "reg_year_max=2026" in query
    assert "power_hp_max=350" in query
    assert "engine_cc_max=3000" in query
    assert "sort=price_asc" in query
