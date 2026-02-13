from backend.app.utils.color_groups import split_color_facets


def test_split_color_facets_groups_and_shades():
    raw = [
        {"value": "Dunkelblau", "count": 12},
        {"value": "Azurblau metallic", "count": 8},
        {"value": "Kirschrot", "count": 5},
        {"value": "Schwarz", "count": 10},
    ]

    basics, other = split_color_facets(raw, top_limit=12)

    basics_by_value = {item["value"]: item for item in basics}
    assert basics_by_value["blue"]["count"] == 20
    assert basics_by_value["black"]["count"] == 10
    assert basics_by_value["red"]["count"] == 5

    labels = {item["label"] for item in other}
    assert any(label.startswith("Синий: ") for label in labels)
    assert any(label.startswith("Черный: ") for label in labels)
    assert any(label.startswith("Красный: ") for label in labels)


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
    # shades for groups outside top_limit must not be rendered
    assert all(not item["label"].startswith("Черный: ") for item in other)
    assert all(not item["label"].startswith("Белый: ") for item in other)
