from backend.app.utils.spec_inference import (
    build_variant_key,
    choose_reference_consensus,
    has_complete_raw_specs,
)


def test_build_variant_key_prefers_informative_tokens_and_strips_noise():
    key = build_variant_key(
        "Land Rover",
        "Range Rover",
        "P530 Autobiography HUD LED PANO",
        {},
    )
    assert key == "p530|autobiography"


def test_build_variant_key_can_fallback_to_title_tokens():
    key = build_variant_key(
        "BMW",
        "X5",
        None,
        {"title": "BMW X5 xDrive40d M Sport Pro"},
    )
    assert key is not None
    assert "xdrive40d" in key


def test_choose_reference_consensus_returns_high_for_exact_variant_and_year():
    candidates = [
        {
            "source_car_id": 10,
            "variant_key": "p530|autobiography",
            "year": 2025,
            "engine_cc": 2997,
            "power_hp": 530,
            "power_kw": 389.8,
        },
        {
            "source_car_id": 11,
            "variant_key": "p530|autobiography",
            "year": 2024,
            "engine_cc": 2997,
            "power_hp": 530,
            "power_kw": 389.8,
        },
    ]
    result = choose_reference_consensus(candidates, target_year=2025, has_variant_key=True)
    assert result is not None
    assert result["engine_cc"] == 2997
    assert result["confidence"] == "high"
    assert result["rule"] == "variant_exact_year_exact"


def test_choose_reference_consensus_rejects_conflicting_specs():
    candidates = [
        {
            "source_car_id": 20,
            "variant_key": "xdrive40d",
            "year": 2025,
            "engine_cc": 2993,
            "power_hp": 352,
            "power_kw": 258.9,
        },
        {
            "source_car_id": 21,
            "variant_key": "xdrive40d",
            "year": 2025,
            "engine_cc": 4395,
            "power_hp": 530,
            "power_kw": 389.8,
        },
    ]
    assert choose_reference_consensus(candidates, target_year=2025, has_variant_key=True) is None


def test_choose_reference_consensus_requires_supported_model_level_consensus():
    candidates = [
        {
            "source_car_id": 31,
            "variant_key": None,
            "year": 2025,
            "engine_cc": 1995,
            "power_hp": 150,
            "power_kw": 110.33,
        },
    ]
    assert choose_reference_consensus(candidates, target_year=2025, has_variant_key=False) is None

    candidates.append(
        {
            "source_car_id": 32,
            "variant_key": None,
            "year": 2025,
            "engine_cc": 1995,
            "power_hp": 150,
            "power_kw": 110.33,
        }
    )
    result = choose_reference_consensus(candidates, target_year=2025, has_variant_key=False)
    assert result is not None
    assert result["confidence"] == "medium"
    assert result["rule"] == "model_exact_year_exact_consensus"


def test_has_complete_raw_specs_supports_ev_and_ice_rules():
    assert has_complete_raw_specs("Diesel", 2993, 286, None) is True
    assert has_complete_raw_specs("Electric", None, 204, None) is True
    assert has_complete_raw_specs("Petrol", None, 245, None) is False
