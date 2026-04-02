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
    assert 'name="interior_color"' in template
    assert 'name="interior_material"' in template
    assert "payload_deferred or (interior_color_options_eu" in template
    assert 'data-has-eu="{{ 1 if payload_deferred or (seats_options_eu' in template


def test_js_updates_generation_visibility_and_select_disabled_state():
    script = _read("app/static/js/app.js")
    assert "syncGenerationVisibility" in script
    assert "setRegionSelectOptions" in script
    assert "item.value ?? item.id" in script
    assert "parseSelectedColorValues" in script
    assert "removeSelectedColor" in script
    assert "bindChoiceChips" in script
    assert "syncChoiceChips" in script
    assert "select.disabled = normalizedItems.length === 0" in script or "select.disabled = deduped.length === 0" in script


def test_filter_payload_includes_dynamic_brand_options():
    router = _read("app/routers/catalog.py")
    assert 'field="brand"' in router
    assert '"brands": brands' in router
    assert 'field="color_group"' in router
    assert '"interior_design_options_eu": build_interior_trim_options' in router
    assert '"interior_color_options_eu": build_interior_options' in router
    assert '"interior_material_options_eu": build_interior_options' in router


def test_cars_count_supports_line_filters():
    router = _read("app/routers/catalog.py")
    assert 'line: Optional[List[str]] = Query(' in router
    assert '"line": "|".join(line or [])' in router
    assert "lines=line," in router


def test_advanced_search_rebuilds_missing_rows_and_uses_selected_models_for_lines():
    script = _read("app/static/js/app.js")
    assert "initials.slice(currentRows.length).forEach((initial) => addRow(initial))" in script
    assert "const models = selectedModels.length ? selectedModels : [modelSelect?.value || '']" in script
    assert "const uniqueBrands = Array.from(new Set(parsedLines.map((item) => item.brand).filter(Boolean)))" in script
    assert "if (!params.get('brand') && uniqueBrands.length === 1)" in script
    assert "data-line-state-hidden=\"1\"" in script or "data-line-state-hidden='1'" in script
    assert "appendStateInput('brand', uniqueBrands[0])" in script
    assert "window.location.assign(buildCatalogUrl(params))" in script
    assert "if (el.matches?.('[data-line-model], [data-line-variant]')) return false" in script
    assert "function syncCatalogLinesFromState(form)" in script
    assert "const restoredModels = selectedLines.length ? selectedLines : getInitialLineModelsForBrand(normBrand)" in script
    assert "function groupLineSelections(lines = [])" in script
    assert "const initialLines = groupLineSelections(initialParams.getAll('line'))" in script
    assert "fillModels(normalizeBrand(initial.brand || ''), modelSelect, initial.models || initial.model || '')" in script
    assert "const getRowInitialSelectedModels = (row) =>" in script
    assert "const getRowEffectiveSelectedModels = (row, modelSelect) =>" in script
    assert "row.dataset.initialSelectedModels = JSON.stringify(" in script
    assert "const currentSelectedModels = getRowEffectiveSelectedModels(row, modelSelect)" in script
    assert "const selectedModels = getRowEffectiveSelectedModels(row, modelSelect)" in script
    assert "scheduleCount()" in script


def test_base_template_bumps_app_bundle_version():
    template = _read("app/templates/base.html")
    assert '/static/js/app.js?v=87' in template
    assert '/static/css/styles.css?v=49' in template


def test_search_page_passes_payload_deferred_flag():
    router = _read("app/routers/pages.py")
    assert '"payload_deferred": bool(filter_ctx.get("payload_deferred"))' in router
    assert '"payload_deferred": True' in router


def test_home_search_uses_line_params_and_js_submit():
    script = _read("app/static/js/app.js")
    assert "const getHomeSelectedModels = () =>" in script
    assert "const skipKeys = ['region_extra', 'model']" in script
    assert "params.append('line', `${brand}|${String(modelValue || '').trim()}|`)" in script
    assert "window.location.assign(buildCatalogUrl(params))" in script
    assert "sessionStorage.setItem('homeSubmitParams', params.toString())" in script
    assert "updateHomeModels().then(() => updateCount())" in script


def test_model_group_summary_has_visible_selected_states():
    css = _read("app/static/css/styles.css")
    assert ".model-accordion__group.is-active > summary" in css
    assert "background: rgba(255, 98, 79, 0.18);" in css
    assert ".model-accordion__group.is-partial > summary" in css
    assert ".advanced-lines .search-row select.model-select-native" in css
    assert "display: none !important;" in css
    assert ".model-accordion__item.is-active" in css
def test_catalog_template_marks_hidden_line_inputs_as_catalog_state():
    template = _read("app/templates/catalog.html")
    assert 'data-catalog-line="1"' in template


def test_taxonomy_contains_extra_body_and_interior_translations():
    taxonomy = _read("app/resources/taxonomy_ru.csv")
    utils = _read("app/utils/taxonomy.py")
    assert "body_type,sportscar,Спорткар" in taxonomy
    assert "body_type,othercar,Прочее" in taxonomy
    assert "color,pink,Розовый" in taxonomy
    assert '"leder": "кожа"' in utils
    assert '"kunstleder": "экокожа"' in utils
    assert "_INTERIOR_COLOR_LABELS" in utils
    assert "_INTERIOR_MATERIAL_LABELS" in utils
    assert "build_interior_trim_options" in utils
    assert 'return "trim:" + "|".join(parts)' in utils


def test_pages_home_uses_recommended_and_media_cache_helpers():
    router = _read("app/routers/pages.py")
    assert "_get_home_recommended(service, db, reco_cfg, limit=12)" in router
    assert "home_media_ctx:v4" in router
    assert 'static" / "home-collage"' in router
    assert "home_recommended:" in router
    assert 'cfg.get("reg_year_min", 2021)' in router
    assert 'cfg.get("power_hp_max", 160)' in router


def test_home_collage_and_home_content_copy_are_updated():
    template = _read("app/templates/home.html")
    home_content = _read("app/utils/home_content.py")
    assert "collage_images[:75]" in template
    assert "Показываем только марки, которые есть в каталоге" in home_content
    assert 'href="#cases-collage"' in template
    assert 'id="cases-collage"' in template


def test_body_type_options_are_canonicalized_and_partner_logos_support_static_assets():
    pages = _read("app/routers/pages.py")
    catalog = _read("app/routers/catalog.py")
    taxonomy = _read("app/utils/taxonomy.py")
    home = _read("app/templates/home.html")
    readme = _read("app/static/img/partners/README.md")
    assert "build_body_type_options" in taxonomy
    assert "normalize_body_type" in taxonomy
    assert "body_aliases" in taxonomy
    assert 'build_body_type_options(service.facet_counts(field="body_type"' in pages
    assert 'build_body_type_options(service.facet_counts(field="body_type"' in catalog
    assert "partner_logos" in pages
    assert "partner-logo--text" in home
    assert "alfa-leasing.svg" in readme


def test_recommended_auto_uses_reg_year_and_effective_specs_limits():
    service = _read("app/services/cars_service.py")
    assert "reg_year_min: int | None = None" in service
    assert "power_hp_max: int | None = None" in service
    assert "engine_cc_max: int | None = None" in service
    assert "power_hp_expr = func.coalesce(Car.power_hp, Car.inferred_power_hp)" in service
    assert "engine_cc_expr = func.coalesce(Car.engine_cc, Car.inferred_engine_cc)" in service


def test_card_and_detail_templates_render_variant_subtitles():
    home_template = _read("app/templates/home.html")
    catalog_template = _read("app/templates/catalog.html")
    detail_template = _read("app/templates/car_detail.html")
    script = _read("app/static/js/app.js")
    schema = _read("app/schemas/car.py")
    api_router = _read("app/routers/catalog.py")
    css = _read("app/static/css/styles.css")
    assert "car-card__subtitle" in home_template
    assert "car-card__subtitle" in catalog_template
    assert "detail-subtitle" in detail_template
    assert "const variantLine = car.variant" in script
    assert "variant: Optional[str] = None" in schema
    assert '"variant": c.get("variant")' in api_router
    assert ".car-card__subtitle" in css
    assert ".detail-subtitle" in css


def test_catalog_and_search_color_filters_use_non_label_wrapper():
    search_template = _read("app/templates/search.html")
    catalog_template = _read("app/templates/catalog.html")
    assert '<div class="field field--full"><span class="field-label">Цвет кузова</span>' in search_template
    assert '<div class="field field--full"><span class="field-label">Цвет кузова</span>' in catalog_template


def test_catalog_scroll_and_grouped_filters_contracts():
    script = _read("app/static/js/app.js")
    color_utils = _read("app/utils/color_groups.py")
    assert "function scrollCatalogToTop()" in script
    assert "loadCars(p, { scrollToTop: true })" in script
    assert "loadCars(1, { scrollToTop: true })" in script
    assert "ColorFamily" in color_utils
    assert '"Серый / серебристый"' in color_utils
    assert '"Другие"' in color_utils
    assert "return basics, other" in color_utils


def test_catalog_and_search_interior_filters_use_chip_multiselect():
    catalog_template = _read("app/templates/catalog.html")
    search_template = _read("app/templates/search.html")
    script = _read("app/static/js/app.js")
    assert 'data-chip-input="interior_color"' in catalog_template
    assert 'data-chip-input="interior_material"' in catalog_template
    assert 'data-chip-input="interior_color"' in search_template
    assert 'data-chip-input="interior_material"' in search_template
    assert 'Материал салона' in catalog_template
    assert 'Цвет салона' in search_template
    assert 'data-region-chip-options' in search_template
    assert 'syncChoiceInputOptions' in script


def test_search_page_uses_payload_on_initial_render_and_has_telegram_ping_tool():
    pages = _read("app/routers/pages.py")
    tg_ping = _read("app/tools/telegram_ping.py")
    assert "include_payload=True" in pages
    assert "resolve_telegram_chat_id" in tg_ping
    assert 'parser.add_argument("--dry-run"' in tg_ping
    assert "telegram_enabled" in tg_ping


def test_registration_year_filters_fallback_to_car_year_when_missing():
    service = _read("app/services/cars_service.py")
    assert "reg_year_expr = func.coalesce(Car.registration_year, Car.year)" in service
    assert "reg_month_floor_expr = func.coalesce(Car.registration_month, 12)" in service
    assert "reg_month_ceil_expr = func.coalesce(Car.registration_month, 1)" in service


def test_calc_missing_registration_uses_fallback_year_and_detail_template_has_description():
    service = _read("app/services/cars_service.py")
    detail_template = _read("app/templates/car_detail.html")
    parser = _read("app/parsing/mobile_de_feed.py")
    model = _read("app/models/car.py")
    cron = Path(__file__).resolve().parents[2] / "deploy" / "cron.mobilede"
    backfill = _read("app/tools/mobilede_payload_backfill.py")
    reg_defaults = _read("app/utils/registration_defaults.py")
    parsing_service = _read("app/services/parsing_data_service.py")
    recalc_script = _read("app/scripts/recalc_calc_cache.py")
    reg_backfill_script = _read("app/scripts/backfill_missing_registration.py")
    pipeline = Path(__file__).resolve().parents[2] / "scripts" / "mobilede_daily_pipeline.sh"
    fx_daily = Path(__file__).resolve().parents[2] / "scripts" / "fx_daily_update.sh"
    assert "get_missing_registration_default" in service
    assert "car.display_description" in detail_template
    assert "description=row.description" in parser
    assert 'description: Mapped[str | None] = mapped_column(Text, nullable=True)' in model
    assert "mobilede_daily_pipeline.sh" in cron.read_text(encoding="utf-8")
    assert "scripts/fx_daily_update.sh" in cron.read_text(encoding="utf-8")
    assert "TELEGRAM_ENABLED=0" in cron.read_text(encoding="utf-8")
    assert 'car.description = item["payload"].get("description")' in backfill
    assert 'or os.getenv("CALC_MISSING_REG_YEAR")' in reg_defaults
    assert 'or os.getenv("CALC_MISSING_REG_MONTH")' in reg_defaults
    assert 'or "2026"' in reg_defaults
    assert 'or "1"' in reg_defaults
    assert 'source_payload["registration_defaulted"] = True' in reg_defaults
    assert "apply_missing_registration_fallback(payload)" in parsing_service
    assert "--only-defaulted-registration" in recalc_script
    assert 'jsonb_extract_path_text(payload_json, "registration_defaulted")' in recalc_script
    assert "[backfill_missing_registration]" in reg_backfill_script
    assert "step=ensure_services" in fx_daily.read_text(encoding="utf-8")


def test_interior_filters_use_derived_text_fallback_from_payload_and_description():
    service = _read("app/services/cars_service.py")
    pages = _read("app/routers/pages.py")
    catalog = _read("app/routers/catalog.py")
    assert 'func.concat_ws(' in service
    assert 'jsonb_extract_path_text(payload_json, "options")' in service
    assert 'jsonb_extract_path_text(payload_json, "title")' in service
    assert "func.coalesce(Car.description, \"\")" in service
    assert "parse_interior_trim_token" in service
    assert "trim_token_conditions" in service
    assert 'field="color_group"' in pages
    assert 'field="color_group"' in catalog


def test_catalog_and_search_use_separate_interior_color_and_material_filters():
    catalog_template = _read("app/templates/catalog.html")
    search_template = _read("app/templates/search.html")
    script = _read("app/static/js/app.js")
    taxonomy = _read("app/utils/taxonomy.py")
    css = _read("app/static/css/styles.css")
    assert 'data-chip-input="interior_color"' in catalog_template
    assert 'data-chip-input="interior_material"' in catalog_template
    assert 'data-chip-input="interior_color"' in search_template
    assert 'data-chip-input="interior_material"' in search_template
    assert "Материал салона" in catalog_template
    assert "Цвет салона" in search_template
    assert "{{ c.label }}" in catalog_template
    assert "{{ c.label }}" in search_template
    assert "syncChoiceInputOptions" in script
    assert "removeChoiceInput" in script
    assert "interior_color_hex" in taxonomy
    assert ".choice-chip--swatch" in css
    assert ".choice-chip--checkbox" in css
