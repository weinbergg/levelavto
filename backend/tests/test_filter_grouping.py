from backend.app.utils.color_groups import split_color_facets
from backend.app.utils.taxonomy import (
    build_interior_trim_options,
    interior_trim_label,
    parse_interior_trim_token,
)


def test_split_color_facets_groups_related_shades_into_base_families():
    basics, other = split_color_facets(
        [
            {"value": "Silver metallic", "count": 10},
            {"value": "Graphite", "count": 5},
            {"value": "Beige", "count": 7},
            {"value": "Orange", "count": 3},
            {"value": "Other", "count": 4},
        ]
    )
    by_key = {item["value"]: item for item in basics}
    assert by_key["gray"]["count"] == 15
    assert by_key["brown"]["count"] == 7
    assert by_key["yellow"]["count"] == 3
    assert other == [{"value": "other", "label": "Другие", "hex": "#8c94a3", "count": 4}]


def test_build_interior_trim_options_combines_material_and_color():
    options = build_interior_trim_options(
        [
            "full leather black",
            "leder schwarz",
            "alcantara anthracite",
            "cloth beige",
        ]
    )
    by_value = {item["value"]: item["label"] for item in options}
    assert by_value["trim:m:leather|c:black"] == "Кожа · Чёрный"
    assert by_value["trim:m:alcantara|c:anthracite"] == "Алькантара · Антрацит"
    assert by_value["trim:m:fabric|c:beige"] == "Ткань · Бежевый"


def test_parse_interior_trim_token_roundtrip():
    material_key, color_key = parse_interior_trim_token("trim:m:leather|c:black")
    assert material_key == "leather"
    assert color_key == "black"
    assert interior_trim_label("trim:m:leather|c:black") == "Кожа · Чёрный"
