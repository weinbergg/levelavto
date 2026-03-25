from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (ROOT / rel_path).read_text(encoding="utf-8")


def test_catalog_template_has_generation_guard_and_air_suspension_guard():
    template = _read("app/templates/catalog.html")
    assert "data-generation-field" in template
    assert "{% if has_air_suspension %}" in template


def test_search_template_has_air_suspension_guard():
    template = _read("app/templates/search.html")
    assert "{% if has_air_suspension %}" in template


def test_search_template_keeps_core_selects_clickable():
    template = _read("app/templates/search.html")
    assert '<select name="transmission">' in template
    assert '<select name="drive_type">' in template
    assert '<select name="engine_type">' in template


def test_js_updates_generation_visibility_and_select_disabled_state():
    script = _read("app/static/js/app.js")
    assert "syncGenerationVisibility" in script
    assert "setRegionSelectOptions" in script
    assert "item.value ?? item.id" in script
    assert "select.disabled = normalizedItems.length === 0" in script or "select.disabled = deduped.length === 0" in script


def test_filter_payload_includes_dynamic_brand_options():
    router = _read("app/routers/catalog.py")
    assert 'field="brand"' in router
    assert '"brands": brands' in router


def test_advanced_search_rebuilds_missing_rows_and_uses_selected_models_for_lines():
    script = _read("app/static/js/app.js")
    assert "initials.slice(currentRows.length).forEach((initial) => addRow(initial))" in script
    assert "const selectedModels = getAccordionSelectedModels(modelSelect)" in script
    assert "const models = selectedModels.length ? selectedModels : [modelSelect?.value || '']" in script
    assert "const currentSelectedModels = getAccordionSelectedModels(modelSelect)" in script
    assert "currentSelectedModels.length ? currentSelectedModels : currentModel" in script
    assert "const uniqueBrands = Array.from(new Set(parsedLines.map((item) => item.brand).filter(Boolean)))" in script
    assert "if (!params.get('brand') && uniqueBrands.length === 1)" in script
    assert "data-line-state-hidden=\"1\"" in script or "data-line-state-hidden='1'" in script
    assert "appendStateInput('brand', uniqueBrands[0])" in script
    assert "window.location.assign(buildCatalogUrl(params))" in script
    assert "if (el.matches?.('[data-line-model], [data-line-variant]')) return false" in script
    assert "function syncCatalogLinesFromState(form)" in script
    assert "const restoredModels = selectedLines.length ? selectedLines : getInitialLineModelsForBrand(normBrand)" in script


def test_base_template_bumps_app_bundle_version():
    template = _read("app/templates/base.html")
    assert '/static/js/app.js?v=75' in template


def test_catalog_template_marks_hidden_line_inputs_as_catalog_state():
    template = _read("app/templates/catalog.html")
    assert 'data-catalog-line="1"' in template


def test_taxonomy_contains_extra_body_and_interior_translations():
    taxonomy = _read("app/resources/taxonomy_ru.csv")
    utils = _read("app/utils/taxonomy.py")
    assert "body_type,sportscar,Спорткар" in taxonomy
    assert "body_type,othercar,Прочее" in taxonomy
    assert '"leder": "кожа"' in utils
    assert '"kunstleder": "экокожа"' in utils


def test_pages_home_uses_recommended_and_media_cache_helpers():
    router = _read("app/routers/pages.py")
    assert "_get_home_recommended(service, db, reco_cfg, limit=12)" in router
    assert "home_media_ctx:v2" in router
    assert "home_recommended:" in router
