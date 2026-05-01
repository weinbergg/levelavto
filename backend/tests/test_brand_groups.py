from backend.app.utils.brand_groups import (
    BRAND_FILTER_PRIORITY,
    _coerce_priority_list,
    effective_priority,
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


def test_group_brands_uses_runtime_priority_override():
    brands = ["Audi", "BMW", "Mercedes-Benz", "Renault", "Volvo"]
    override = ["Renault", "Volvo"]
    groups = group_brands(brands, priority=override)
    # Operator-supplied priority wins; everything else falls into "other"
    # alphabetically — even priority brands from the default list.
    assert groups["top"] == ["Renault", "Volvo"]
    assert groups["other"] == ["Audi", "BMW", "Mercedes-Benz"]


def test_effective_priority_falls_back_when_override_invalid():
    assert effective_priority(None) == BRAND_FILTER_PRIORITY
    assert effective_priority("") == BRAND_FILTER_PRIORITY
    # Bad JSON falls back rather than crashing the public site.
    assert effective_priority("{not json") == BRAND_FILTER_PRIORITY
    # Empty list also falls back to the default.
    assert effective_priority([]) == BRAND_FILTER_PRIORITY


def test_effective_priority_accepts_json_string_and_normalizes_aliases():
    assert effective_priority('["mercedes", "vw", "BMW"]') == [
        "Mercedes-Benz",
        "Volkswagen",
        "BMW",
    ]


def test_coerce_priority_list_drops_duplicates_and_blanks():
    assert _coerce_priority_list(["BMW", "", "bmw", "  ", "Audi"]) == ["BMW", "Audi"]
    assert _coerce_priority_list(None) is None
    assert _coerce_priority_list("   ") is None


def test_coerce_models_override_handles_dict_and_json_string():
    from backend.app.utils.brand_groups import _coerce_models_override

    direct = _coerce_models_override({"BMW": ["X5", "X6", "X5"], "  ": ["X1"]})
    assert direct == {"bmw": ["X5", "X6"]}

    text = _coerce_models_override('{"Mercedes-Benz": ["G-Class", "S-Class"], "Audi": []}')
    assert text == {"mercedes-benz": ["G-Class", "S-Class"]}

    assert _coerce_models_override(None) == {}
    assert _coerce_models_override("not-json") == {}
    assert _coerce_models_override({"BMW": "X5"}) == {}  # value must be a list
