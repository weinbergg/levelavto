from backend.app.utils.brand_groups import (
    BRAND_FILTER_PRIORITY,
    group_brands,
    ordered_brands,
)


def test_group_brands_keeps_priority_order_then_alpha():
    brands = [
        "Audi",
        "BMW",
        "Mercedes-Benz",
        "Volkswagen",
        "Skoda",
        "Renault",
        "Aston Martin",
        "Porsche",
        "Volvo",
        "Ford",
        "Dacia",
    ]
    groups = group_brands(brands)
    assert groups["top"] == [
        "Mercedes-Benz",
        "BMW",
        "Audi",
        "Volkswagen",
        "Porsche",
        "Ford",
        "Skoda",
        "Volvo",
    ]
    assert groups["other"] == ["Aston Martin", "Dacia", "Renault"]


def test_group_brands_normalizes_aliases_to_priority_brand():
    groups = group_brands(["mercedes", "ROLLS ROYCE", "Land-Rover", "vw"])
    assert groups["top"] == [
        "Mercedes-Benz",
        "Volkswagen",
        "Land Rover",
        "Rolls-Royce",
    ]
    assert groups["other"] == []


def test_group_brands_handles_dict_facets_and_dedup():
    facets = [
        {"value": "Audi", "count": 5},
        {"value": "Audi", "count": 7},
        {"brand": "BMW", "count": 1},
        "Renault",
    ]
    groups = group_brands(facets)
    assert groups["top"] == ["BMW", "Audi"]
    assert groups["other"] == ["Renault"]


def test_ordered_brands_concatenates_top_then_alpha_rest():
    out = ordered_brands(["BMW", "Audi", "Porsche", "Renault", "Aston Martin"])
    assert out == ["BMW", "Audi", "Porsche", "Aston Martin", "Renault"]


def test_priority_list_is_unique_and_in_expected_order():
    assert BRAND_FILTER_PRIORITY[0] == "Mercedes-Benz"
    assert BRAND_FILTER_PRIORITY[1] == "BMW"
    assert BRAND_FILTER_PRIORITY[-1] == "Volvo"
    assert "Ford" in BRAND_FILTER_PRIORITY
    assert len(BRAND_FILTER_PRIORITY) == len(set(BRAND_FILTER_PRIORITY))
