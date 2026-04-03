from backend.app.utils.color_groups import split_color_facets


def test_split_color_facets_groups_and_shades():
    raw = [
        {"value": "Dunkelblau", "count": 12},
        {"value": "Azurblau metallic", "count": 8},
        {"value": "Kirschrot", "count": 5},
        {"value": "Schwarz", "count": 10},
    ]

    basics, other = split_color_facets(raw)

    basics_by_value = {item["value"]: item for item in basics}
    assert basics_by_value["blue"]["count"] == 20
    assert basics_by_value["black"]["count"] == 10
    assert basics_by_value["red"]["count"] == 5

    assert other == []


def test_split_color_facets_respects_top_limit():
    raw = [
        {"value": "Dunkelblau", "count": 12},
        {"value": "Kirschrot", "count": 11},
        {"value": "Schwarz", "count": 10},
        {"value": "Weiss", "count": 9},
    ]

    basics, other = split_color_facets(raw, top_limit=2)

    assert len(basics) == 2
    assert {item["value"] for item in basics} == {"blue", "red"}
    assert {item["value"] for item in other} == {"black", "white"}
