from backend.app.utils.color_groups import normalize_color_group, color_group_label


def test_color_groups_mapping():
    assert normalize_color_group("Anthracite metallic") == "gray"
    assert normalize_color_group("Champagne pearl") == "beige"
    assert normalize_color_group("Burgundy red") == "red"
    assert normalize_color_group("Blau metallic") == "blue"
    assert normalize_color_group("Silber") == "silver"


def test_color_group_label():
    assert color_group_label("gray") == "Серый"
    assert color_group_label("other") == "Другое"
