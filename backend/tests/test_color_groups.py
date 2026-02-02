from backend.app.utils.color_groups import normalize_color_group, color_groups


def test_normalize_color_group_keywords():
    assert normalize_color_group("dunkelblau") == "blue"
    assert normalize_color_group("azurblau metallic") == "blue"
    assert normalize_color_group("kirschrot") == "red"
    assert normalize_color_group("graphitgrau") == "gray"
    assert normalize_color_group("champagne") == "beige"


def test_normalize_color_group_hex():
    assert normalize_color_group("whatever", "#0d47a1") == "blue"
    assert normalize_color_group("whatever", "#ffffff") == "white"
    assert normalize_color_group("whatever", "#111111") == "black"


def test_color_groups_count():
    assert len(color_groups()) <= 16
