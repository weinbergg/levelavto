from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (ROOT / rel_path).read_text(encoding="utf-8")


def test_calc_and_import_paths_use_inferred_specs_layer():
    cars_service = _read("app/services/cars_service.py")
    calc_debug = _read("app/services/calc_debug.py")
    calc_router = _read("app/routers/calculator.py")
    parsing_service = _read("app/services/parsing_data_service.py")
    assert "car.inferred_engine_cc" in cars_service
    assert "car.inferred_power_hp" in cars_service
    assert "car.inferred_power_kw" in cars_service
    assert '"effective_engine_cc"' in calc_debug
    assert "effective_engine_cc = car.engine_cc if car.engine_cc is not None else car.inferred_engine_cc" in calc_router
    assert "self._clear_inferred_specs(existing)" in parsing_service


def test_pipeline_and_recalc_support_inferred_specs_refresh():
    pipeline = (ROOT.parents[0] / "scripts" / "mobilede_daily_pipeline.sh").read_text(encoding="utf-8")
    recalc = _read("app/scripts/recalc_calc_cache.py")
    service = _read("app/services/car_spec_inference_service.py")
    util = _read("app/utils/spec_inference.py")
    assert "step=refresh_spec_inference" in pipeline
    assert "step=recalc_inferred_specs" in pipeline
    assert "step=recalc_recoverable_fallbacks" in pipeline
    assert "--only-inferred-specs" in recalc
    assert "--only-recoverable-fallback" in recalc
    assert "class CarSpecInferenceService" in service
    assert "build_variant_key" in util
    assert "expanded_year_window = max(year_window, 4)" in service
    assert "text_engine_cc" in service
    assert "infer_engine_cc_from_text" in util
    assert "filter_candidates_by_target_power" in util
    assert "power_matched" in service
    assert 'region_scope="EU"' in service
    assert "eu_cross_region" in service


def test_che168_import_postprocesses_calc_and_listing_extracts_core_specs():
    runner = _read("app/services/parser_runner.py")
    parser = _read("app/parsing/che168.py")
    assert 'if site_cfg.key == "che168" and seen_all:' in runner
    assert "self._postprocess_che168_import(db, source, seen_all)" in runner
    assert "svc.ensure_calc_cache(car, force=True)" in runner
    assert '[parser] postprocess source=che168 checked=' in runner
    assert "engine_cc = infer_engine_cc_from_text(title, summary)" in parser
    assert '"engine_type": self._fuel_from_cn(title, summary)' in parser
    assert '"transmission": self._transmission_from_cn(summary)' in parser
    assert '"drive_type": self._drive_from_cn(summary)' in parser
    assert '"list_engine_cc": engine_cc' in parser
