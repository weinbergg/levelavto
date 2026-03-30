from backend.app.utils.taxonomy import (
    interior_color_key,
    interior_material_key,
)


def test_interior_color_and_material_are_derived_from_single_supplier_value():
    assert interior_color_key("Leather, Brown") == "brown"
    assert interior_material_key("Leather, Brown") == "leather"


def test_interior_split_handles_alcantara_and_dark_colors():
    assert interior_color_key("Alcantara / black") == "black"
    assert interior_material_key("Alcantara / black") == "alcantara"
