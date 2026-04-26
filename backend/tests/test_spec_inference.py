from backend.app.utils.spec_inference import (
    build_variant_key,
    choose_reference_consensus,
    filter_candidates_by_target_power,
    has_complete_raw_specs,
    infer_engine_cc_from_text,
    infer_power_from_text,
    normalize_engine_type,
    variant_primary_token,
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


def test_normalize_engine_type_drops_numeric_and_co2_noise():
    assert normalize_engine_type("210") == ""
    assert normalize_engine_type("based on co₂ emissions (combined)") == ""
    assert normalize_engine_type("Diesel") == "diesel"


def test_variant_primary_token_extracts_core_variant():
    assert variant_primary_token("xdrive30d|m-sport|individual|manhattan") == "xdrive30d"
    assert variant_primary_token("p530|autobiography") == "p530"


def test_infer_power_from_text_supports_kw_and_cv_tokens():
    assert infer_power_from_text("Opel Corsa-e 100 kW GS") == (136.0, 100.0)
    assert infer_power_from_text("FORD Mustang mach-e standard range awd 269cv aut") == (269.0, 197.85)


def test_infer_engine_cc_from_compact_suffix_variant():
    assert infer_engine_cc_from_text("Landtrek 1.9D NO EU/KEIN EU/T1") == 1900


def test_choose_reference_consensus_can_infer_engine_only_when_power_conflicts():
    candidates = [
        {
            "source_car_id": 41,
            "variant_key": "xdrive30d",
            "year": 2025,
            "engine_cc": 2993,
            "power_hp": 286,
            "power_kw": 210.35,
        },
        {
            "source_car_id": 42,
            "variant_key": "xdrive30d",
            "year": 2025,
            "engine_cc": 2993,
            "power_hp": 298,
            "power_kw": 219.18,
        },
    ]
    result = choose_reference_consensus(
        candidates,
        target_year=2025,
        has_variant_key=True,
        need_engine_cc=True,
        need_power=False,
    )
    assert result is not None
    assert result["engine_cc"] == 2993


def test_choose_reference_consensus_engine_only_uses_clear_majority():
    candidates = [
        {
            "source_car_id": 61,
            "variant_key": "xdrive30d",
            "year": 2026,
            "engine_cc": 2993,
            "power_hp": 286,
            "power_kw": 210.35,
        },
        {
            "source_car_id": 62,
            "variant_key": "xdrive30d",
            "year": 2026,
            "engine_cc": 2993,
            "power_hp": 298,
            "power_kw": 219.18,
        },
        {
            "source_car_id": 63,
            "variant_key": "xdrive30d",
            "year": 2026,
            "engine_cc": 3000,
            "power_hp": 286,
            "power_kw": 210.35,
        },
    ]
    result = choose_reference_consensus(
        candidates,
        target_year=2023,
        has_variant_key=True,
        need_engine_cc=True,
        need_power=False,
    )
    assert result is not None
    assert result["engine_cc"] == 2993


def test_choose_reference_consensus_can_infer_power_only_when_cc_conflicts():
    candidates = [
        {
            "source_car_id": 51,
            "variant_key": "xdrive40d",
            "year": 2025,
            "engine_cc": 2993,
            "power_hp": 352,
            "power_kw": 258.9,
        },
        {
            "source_car_id": 52,
            "variant_key": "xdrive40d",
            "year": 2025,
            "engine_cc": 3000,
            "power_hp": 352,
            "power_kw": 258.9,
        },
    ]
    result = choose_reference_consensus(
        candidates,
        target_year=2025,
        has_variant_key=True,
        need_engine_cc=False,
        need_power=True,
    )
    assert result is not None
    assert result["power_hp"] == 352


def test_choose_reference_consensus_allows_no_year_model_consensus_when_unanimous():
    candidates = [
        {
            "source_car_id": 71,
            "variant_key": None,
            "year": 2023,
            "engine_cc": 2992,
            "power_hp": 829,
            "power_kw": 609.73,
        },
        {
            "source_car_id": 72,
            "variant_key": None,
            "year": 2024,
            "engine_cc": 2992,
            "power_hp": 829,
            "power_kw": 609.73,
        },
    ]
    result = choose_reference_consensus(candidates, target_year=None, has_variant_key=False)
    assert result is not None
    assert result["engine_cc"] == 2992
    assert result["rule"] == "model_exact_consensus"


def test_infer_engine_cc_from_text_parses_explicit_liter_patterns():
    assert infer_engine_cc_from_text("296 gtb 3.0 turbo v6 hybride 830 ch") == 3000
    assert infer_engine_cc_from_text("4.0 V8", "some title") == 4000
    assert infer_engine_cc_from_text("2993 cc diesel") == 2993
    assert infer_engine_cc_from_text("5514 см³") == 5514


def test_filter_candidates_by_target_power_prefers_close_matches_only():
    candidates = [
        {"power_hp": 551, "power_kw": 405.26, "engine_cc": 3996},
        {"power_hp": 549, "power_kw": 403.79, "engine_cc": 3996},
        {"power_hp": 462, "power_kw": 339.80, "engine_cc": 2995},
        {"power_hp": 650, "power_kw": 478.07, "engine_cc": 3996},
    ]
    matched = filter_candidates_by_target_power(candidates, 551)
    assert len(matched) == 2
    assert {row["engine_cc"] for row in matched} == {3996}
