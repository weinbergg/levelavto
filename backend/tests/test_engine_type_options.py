from backend.app.utils.taxonomy import build_engine_type_options


def test_build_engine_type_options_normalizes_known_fuels_and_skips_numeric_garbage():
    options = build_engine_type_options(
        [
            {"value": "Diesel", "count": 4},
            {"value": "diesel", "count": 2},
            {"value": "Petrol", "count": 3},
            {"value": "100", "count": 9},
            {"value": "065", "count": 7},
        ]
    )
    by_value = {item["value"]: item for item in options}
    assert by_value["diesel"]["label"] == "Дизель"
    assert by_value["diesel"]["count"] == 6
    assert by_value["petrol"]["label"] == "Бензин"
    assert by_value["petrol"]["count"] == 3
    assert "100" not in by_value
    assert "065" not in by_value
