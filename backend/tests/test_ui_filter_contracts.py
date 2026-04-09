import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HOME_CONTENT_PATH = ROOT / "app/utils/home_content.py"
HOME_CONTENT_SPEC = importlib.util.spec_from_file_location("home_content", HOME_CONTENT_PATH)
assert HOME_CONTENT_SPEC and HOME_CONTENT_SPEC.loader
_home_content = importlib.util.module_from_spec(HOME_CONTENT_SPEC)
HOME_CONTENT_SPEC.loader.exec_module(_home_content)
build_home_content = _home_content.build_home_content


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
    assert 'data-multi-source-select="body_type"' in template
    assert 'data-multi-source-select="engine_type"' in template
    assert 'data-multi-source-select="transmission"' in template
    assert 'data-multi-source-select="drive_type"' in template
    assert '<input type="hidden" name="body_type"' in template
    assert '<input type="hidden" name="engine_type"' in template
    assert '<input type="hidden" name="transmission"' in template
    assert '<input type="hidden" name="drive_type"' in template
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
    assert "bindMultiSelectMenus" in script
    assert "syncMultiSelectMenus" in script
    assert 'className = \'multi-select-menu__apply\'' in script or 'className = "multi-select-menu__apply"' in script
    assert "container.classList.toggle('multi-select-menu--fuel', name === 'engine_type')" in script
    assert "clearBtn.textContent = 'Сбросить выбор'" in script
    assert "clearBtn.disabled = selectedSet.size === 0" in script
    assert "select.disabled = normalizedItems.length === 0" in script or "select.disabled = deduped.length === 0" in script
    assert "bindChoiceChips(filtersForm, () => loadCars(1, { scrollToTop: true }))" in script
    assert "const priceMain = card.querySelector('.price-main')" in script
    assert "void loadCars(initialPage)" in script
    assert "contentWrap.className = 'model-accordion__content'" in script or 'contentWrap.className = "model-accordion__content"' in script


def test_filter_payload_includes_dynamic_brand_options():
    router = _read("app/routers/catalog.py")
    assert 'field="brand"' in router
    assert '"brands": brands' in router
    assert 'field="color_group"' in router
    assert '"reg_years": reg_years' in router
    assert '"_engine_type_source": "normalized"' in router
    assert '"interior_design_options_eu": build_interior_trim_options' in router
    assert '"interior_color_options_eu": build_interior_options' in router
    assert '"interior_material_options_eu": build_interior_options' in router


def test_cars_count_supports_line_filters():
    router = _read("app/routers/catalog.py")
    assert 'line: Optional[List[str]] = Query(' in router
    assert 'source: Optional[str | List[str]] = Query(default=None)' in router
    assert 'interior_design: Optional[str] = Query(default=None)' in router
    assert 'interior_color: Optional[str] = Query(default=None)' in router
    assert 'interior_material: Optional[str] = Query(default=None)' in router
    assert '"line": "|".join(line or [])' in router
    assert '"source": ",".join(source) if isinstance(source, list) else source' in router
    assert "lines=line," in router
    assert "interior_color=interior_color," in router
    assert "interior_material=interior_material," in router


def test_advanced_search_rebuilds_missing_rows_and_uses_selected_models_for_lines():
    script = _read("app/static/js/app.js")
    template = _read("app/templates/search.html")
    assert "initials.slice(currentRows.length).forEach((initial) => addRow(initial))" in script
    assert "const models = selectedModels.length ? selectedModels : [modelSelect?.value || '']" in script
    assert "const uniqueBrands = Array.from(new Set(parsedLines.map((item) => item.brand).filter(Boolean)))" in script
    assert "if (!params.get('brand') && uniqueBrands.length === 1)" in script
    assert "data-line-state-hidden=\"1\"" in script or "data-line-state-hidden='1'" in script
    assert "appendStateInput('brand', uniqueBrands[0])" in script
    assert "window.location.assign(buildCatalogUrl(params))" in script
    assert "if (el.matches?.('[data-line-model]')) return false" in script
    assert "function syncCatalogLinesFromState(form)" in script
    assert "const restoredModelsRaw = selectedLines.length ? selectedLines : getInitialLineModelsForBrand(normBrand)" in script
    assert "function groupLineSelections(lines = [])" in script
    assert "const initialLines = groupLineSelections(initialParams.getAll('line'))" in script
    assert "fillModels(normalizeBrand(initial.brand || ''), modelSelect, initial.models || initial.model || '')" in script
    assert "const getRowInitialSelectedModels = (row) =>" in script
    assert "const getRowEffectiveSelectedModels = (row, modelSelect) =>" in script
    assert "row.dataset.initialSelectedModels = JSON.stringify(" in script
    assert "const currentSelectedModels = getRowEffectiveSelectedModels(row, modelSelect)" in script
    assert "const selectedModels = getRowEffectiveSelectedModels(row, modelSelect)" in script
    assert "scheduleCount()" in script
    assert "name=\"line_variant\"" not in template
    assert "<label>Вариант</label>" not in template


def test_base_template_bumps_app_bundle_version():
    template = _read("app/templates/base.html")
    assert '/static/js/app.js?v=101' in template
    assert '/static/css/styles.css?v=62' in template


def test_search_page_passes_payload_deferred_flag():
    router = _read("app/routers/pages.py")
    assert '"payload_deferred": bool(filter_ctx.get("payload_deferred"))' in router
    assert '"payload_deferred": True' in router
    assert '"_engine_type_source": "normalized"' in router


def test_catalog_template_does_not_duplicate_visible_interior_hidden_inputs():
    template = _read("app/templates/catalog.html")
    assert "data-chip-input=\"interior_color\"" in template
    assert "data-chip-input=\"interior_material\"" in template
    assert "<span class=\"field-label\">Цвет салона</span>" in template
    assert "<span class=\"field-label\">Материал салона</span>" in template
    assert "'interior_color'" not in template.split("{% set adv_keys = ", 1)[1].split("%}", 1)[0]
    assert "'interior_material'" not in template.split("{% set adv_keys = ", 1)[1].split("%}", 1)[0]


def test_pages_base_filter_cache_requires_interior_payload_keys():
    router = _read("app/routers/pages.py")
    assert '"interior_design_options" not in cached' in router
    assert '"interior_color_options" not in cached' in router
    assert '"interior_material_options" not in cached' in router


def test_price_sensitive_catalog_paths_bypass_stale_cache():
    catalog_router = _read("app/routers/catalog.py")
    pages_router = _read("app/routers/pages.py")
    service = _read("app/services/cars_service.py")
    assert "def _bypass_price_sensitive_cache(" in catalog_router
    assert "price_cache_bypass = _bypass_price_sensitive_cache(" in catalog_router
    assert "if not price_cache_bypass:" in catalog_router
    assert "price_cache_bypass = normalized.get(\"price_min\") is not None or normalized.get(\"price_max\") is not None" in pages_router
    assert "def _refresh_price_sensitive_candidates(" in service
    assert "self._refresh_price_sensitive_candidates(" in service


def test_similar_cars_avoids_bare_numeric_order_by_constants():
    service = _read("app/services/cars_service.py")
    assert "def _sort_const(value: int | float):" in service
    assert "interprets as select-list ordinals" in service
    assert "else _sort_const(1)" in service
    assert "else _sort_const(999)" in service
    assert "else _sort_const(999999)" in service
    assert "else _sort_const(999999999)" in service


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
    assert ".filters-primary-grid > .field:has(.model-accordion__root[open])" in css
    assert ".filters-primary-grid > .field:has(.multi-select-menu__root[open])" in css
    assert ".model-accordion__content" in css
    assert ".model-accordion__root[open] .model-accordion__body" in css
    assert "display: grid;" in css
    assert "grid-template-rows: minmax(0, 1fr) auto;" in css
    assert ".multi-select-menu__option" in css
    assert ".advanced-row > .field:has(.multi-select-menu__root[open])" in css
    assert ".multi-select-menu--fuel .multi-select-menu__option" in css
    assert "grid-template-columns: 18px minmax(0, 1fr);" in css


def test_catalog_template_marks_hidden_line_inputs_as_catalog_state():
    template = _read("app/templates/catalog.html")
    assert 'data-catalog-line="1"' in template


def test_catalog_template_preserves_source_and_non_eu_country_filters():
    template = _read("app/templates/catalog.html")
    assert "{% for source_key in params.getlist('source') %}" in template
    assert '<input type="hidden" name="source" value="{{ source_key }}">' in template
    assert "params.get('country') not in countries and params.get('country') != 'KR'" in template
    assert '<input type="hidden" name="country" value="{{ params.get(\'country\') }}">' in template
    assert "{% elif params.get('country') in countries %}" in template


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
    assert "fuel,hybrid_diesel,Гибрид (дизельный / электрический)" in taxonomy
    assert "fuel,phev,Подключаемый гибрид" in taxonomy
    assert "fuel,cng,Природный газ (КПГ)" in taxonomy
    assert "fuel,lpg,Автомобильный газ (сжиженный нефтяной газ)" in taxonomy
    assert 'fuel,ethanol,"Ethanol (FFV, E85 etc.)"' in taxonomy
    assert 'return "hybrid_diesel"' in utils
    assert 'return "phev"' in utils


def test_home_content_keeps_cataloge_wordform_stable():
    content = build_home_content({"hero_subtitle": "Актуальные предложения с ценой под ключ в одном каталоге"})
    assert content["hero"]["subtitle"] == "Актуальные предложения с ценой под ключ в одном каталоге"

    legacy = build_home_content({"hero_subtitle": "Актуальные предложения с ценой под ключ в одном каталог."})
    assert legacy["hero"]["subtitle"] == "Актуальные предложения с ценой под ключ в одном каталоге"


def test_home_css_keeps_model_actions_in_bottom_bar_on_mobile():
    css = _read("app/static/css/home.css")
    assert "#home-search .model-accordion__actions" in css
    assert "#home-search .model-accordion__content" in css
    assert "margin: 0;" in css
    assert "padding: 14px 10px calc(10px + env(safe-area-inset-bottom, 0px));" in css


def test_home_template_bumps_home_css_bundle_version():
    template = _read("app/templates/home.html")
    assert '/static/css/home.css?v=24' in template


def test_home_template_places_partners_block_after_search():
    template = _read("app/templates/home.html")
    assert 'class="la-container hero-search"' in template
    assert 'class="la-container hero-partners" id="home-partners"' in template
    assert template.index('class="la-container hero-search"') < template.index('class="la-container hero-partners" id="home-partners"')


def test_home_css_uses_full_width_partners_block():
    css = _read("app/static/css/home.css")
    assert ".hero-partners {" in css
    assert ".hero-partners-card--full" in css or ".hero-partners-card {" in css
    assert "grid-template-columns: minmax(0, 1fr);" in css


def test_text_query_input_only_lives_in_advanced_search():
    home = _read("app/templates/home.html")
    search = _read("app/templates/search.html")
    catalog = _read("app/templates/catalog.html")
    assert 'name="q"' not in home
    assert 'name="q"' in search
    assert 'name="q"' not in catalog


def test_home_contacts_use_icon_messengers_and_direct_telegram_link():
    template = _read("app/templates/home.html")
    css = _read("app/static/css/home.css")
    assert "https://t.me/DmitriyMotorov" in template
    assert "contact-messenger-links" in template
    assert "lead-messenger-link__icon" in template
    assert ".contact-messenger-links" in css


def test_register_template_and_backend_support_phone_verification():
    router = _read("app/routers/auth.py")
    template = _read("app/templates/auth/register.html")
    service = _read("app/services/phone_verification_service.py")
    model = _read("app/models/phone_verification.py")
    bootstrap = _read("app/schema_bootstrap.py")
    config = _read("app/config.py")
    assert '@router.post("/api/auth/phone/send-code")' in router
    assert '@router.post("/api/auth/phone/verify-code")' in router
    assert "phone_verification_token" in router
    assert 'id="register-phone"' in template
    assert 'id="send-phone-code"' in template
    assert 'id="verify-phone-code"' in template
    assert 'id="phone-verification-token"' in template
    assert "normalize_phone_number" in service
    assert "class SmsRuProvider" in service
    assert "class PhoneVerificationService" in service
    assert "PhoneVerificationChallenge" in model
    assert "ALTER TABLE users ADD COLUMN phone" in bootstrap
    assert "SMS_RU_API_ID" in config


def test_emavto_registration_recovery_and_stale_lock_contracts():
    parser = _read("app/parsing/emavto_klg.py")
    runner = _read("app/tools/emavto_chunk_runner.py")
    wrapper = (ROOT.parents[0] / "scripts" / "run_emavto_job.sh").read_text(encoding="utf-8")
    assert 'REGISTRATION_LABELS = ["Дата постановки на учет", "Дата постановки на учёт"]' in parser
    assert "def _extract_registration(" in parser
    assert "def _parse_registration_value(self, raw: Optional[str])" in parser
    assert 'root.get("data-reg-date")' in parser
    assert "registration_year=detail.get(\"registration_year\")" in parser
    assert "registration_month=detail.get(\"registration_month\")" in parser
    assert 'out["source_payload"] = {' in parser
    assert '"registration_source": "emavto_detail"' in parser
    assert "def needs_detail_refresh(car: Car) -> bool:" in runner
    assert 'payload.get("registration_defaulted") is True' in runner
    assert "same_basic(car, task) and not needs_detail_refresh(car)" in runner
    assert 'PID_FILE="${LOCK_DIR}/pid"' in wrapper
    assert 'echo "[emavto] clearing stale lock at ${LOCK_DIR}"' in wrapper
    assert "printf '%s\\n' \"$$\" > \"${PID_FILE}\"" in wrapper


def test_model_filters_use_canonical_labels_with_alias_restore():
    service = _read("app/services/cars_service.py")
    router = _read("app/routers/catalog.py")
    script = _read("app/static/js/app.js")
    assert "def _canonical_model_label(" in service
    assert "def _resolve_model_aliases(" in service
    assert "def _model_filter_clause(" in service
    assert "if filters.get(\"model\"):" in service
    assert "clause = self._model_filter_clause(" in service
    assert "clause = service._model_filter_clause(" in router
    assert "select.__resolveModelValues = (values = []) => {" in script
    assert "const restoredModelsRaw = selectedLines.length ? selectedLines : getInitialLineModelsForBrand(normBrand)" in script
    assert "const resolvedSelectedValues = typeof modelSelect.__resolveModelValues === 'function'" in script


def test_pages_home_uses_recommended_and_media_cache_helpers():
    router = _read("app/routers/pages.py")
    assert "_get_home_recommended(service, db, reco_cfg, limit=12)" in router
    assert "_get_home_more_offers(service, db, limit=12)" in router
    assert "home_media_ctx:v4" in router
    assert 'static" / "home-collage"' in router
    assert "home_recommended:" in router
    assert "home_more_offers:" in router
    assert 'cfg.get("reg_year_min", 2021)' in router
    assert 'cfg.get("power_hp_max", 160)' in router


def test_home_collage_and_home_content_copy_are_updated():
    template = _read("app/templates/home.html")
    home_content = _read("app/utils/home_content.py")
    assert "collage_base = collage_images[:50] if collage_images|length > 50 else collage_images" in template
    assert 'data-loop-base-count="{{ collage_base|length }}"' in template
    assert "const syncCollageLoop = () => {" in template
    assert 'data-expand-toggle="home-more-offers-grid"' in template
    assert 'data-expand-pages="2"' in template
    assert 'id="home-more-offers-catalog"' in template
    assert 'id="home-more-offers-grid"' in template
    assert "Показываем только марки, которые есть в каталоге" in home_content
    assert "Возим проверенные авто из Европы, Азии и РФ." in home_content
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
    assert "body_type: str | None = None" in service
    assert "power_hp_expr = func.coalesce(Car.power_hp, Car.inferred_power_hp)" in service
    assert "engine_cc_expr = func.coalesce(Car.engine_cc, Car.inferred_engine_cc)" in service


def test_model_filters_normalize_whitespace_and_merge_overlapping_regions():
    service = _read("app/services/cars_service.py")
    router = _read("app/routers/catalog.py")
    css = _read("app/static/css/styles.css")
    assert "def normalize_model_label(" in service
    assert "def model_lookup_key(" in service
    assert "func.regexp_replace(" in service
    assert '"aliases": []' in service
    assert "clause = service._model_filter_clause(" in router
    assert ".multi-select-menu__options" in css
    assert "grid-template-columns: 1fr;" in css
    assert "white-space: normal;" in css


def test_engine_type_facets_use_raw_fuel_source_and_custom_filtering():
    service = _read("app/services/cars_service.py")
    catalog = _read("app/routers/catalog.py")
    assert "def _fuel_source_expr(self):" in service
    assert 'func.jsonb_extract_path_text(payload_json, "full_fuel_type")' in service
    assert 'func.jsonb_extract_path_text(payload_json, "envkv_engine_type")' in service
    assert 'func.jsonb_extract_path_text(payload_json, "envkv_consumption_fuel")' in service
    assert "def _fuel_filter_clause(self, raw_value: str):" in service
    assert 'if key == "hybrid_diesel":' in service
    assert 'if key == "phev":' in service
    assert 'if field in {"color_group", "engine_type"}:' in service
    assert 'int(x.get("sort_order", 999))' in catalog


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
    assert "fav-btn--detail" in detail_template
    assert ".fav-btn--detail" in css


def test_detail_and_header_contact_actions_use_call_and_messengers():
    base_template = _read("app/templates/base.html")
    detail_template = _read("app/templates/car_detail.html")
    account_template = _read("app/templates/account/index.html")
    account_router = _read("app/routers/account.py")
    css = _read("app/static/css/styles.css")
    assert ">Позвонить<" in base_template
    assert "header__contact-actions" in base_template
    assert "header__messenger" in base_template
    assert "detail-messenger-link" in detail_template
    assert 'id="detail-lead-btn"' in detail_template
    assert "display_price_rub" in account_template
    assert "_prepare_favorites" in account_router
    assert ".detail-messenger-link" in css


def test_templates_hide_placeholder_max_and_support_thumb_macro_for_autoimg():
    base_template = _read("app/templates/base.html")
    home_template = _read("app/templates/home.html")
    catalog_template = _read("app/templates/catalog.html")
    detail_template = _read("app/templates/car_detail.html")
    thumbs_macro = _read("app/templates/_thumbs.html")
    script = _read("app/static/js/app.js")
    assert "page_max in ['https://max.ru', 'https://max.ru/'" in base_template
    assert "{% if page_max %}" in base_template
    assert "{% if page_max %}" in home_template
    assert "https://max.ru/u/f9LHodD0cOJOSiOGTyXgt5mtXe_iohF6yKe5cDbjGqNeUpq31QbnCHYfAO8" in base_template
    assert "https://max.ru/u/f9LHodD0cOJOSiOGTyXgt5mtXe_iohF6yKe5cDbjGqNeUpq31QbnCHYfAO8" in detail_template
    assert "autoimg.cn" in thumbs_macro
    assert "shouldProxyThumbSource" in script
    assert "autoimg.cn" in script


def test_che168_supports_cny_and_china_country_label():
    country_map = _read("app/utils/country_map.py")
    parsing_service = _read("app/services/parsing_data_service.py")
    catalog_router = _read("app/routers/catalog.py")
    pages_router = _read("app/routers/pages.py")
    price_utils = _read("app/utils/price_utils.py")
    fx_script = _read("app/scripts/update_fx_prices.py")
    cars_service = _read("app/services/cars_service.py")
    assert '"CN": "Китай"' in country_map
    assert 'if lower == "che168" or "che168" in lower:' in country_map
    assert 'payload.get("country") or ""' in parsing_service
    assert 'payload.get("country") and getattr(existing, "country", None) != payload.get("country")' in parsing_service
    assert '"CNY": cny' in cars_service
    assert 'elif cur == "CNY" and fx_cny > 0:' in catalog_router
    assert 'elif cur == "CNY" and fx_cny > 0:' in pages_router
    assert 'PRICE_NOTE_CHINA = "Цена в Китае"' in price_utils
    assert 'elif car.currency == "CNY" and car.price is not None:' in fx_script
    assert "def _is_retryable_write_error(exc: Exception) -> bool:" in fx_script
    assert 'print(' in fx_script
    assert "[update_fx_prices] retryable_write_error attempt=" in fx_script


def test_che168_parser_and_offline_tool_are_registered():
    base_template = _read("app/templates/base.html")
    detail_template = _read("app/templates/car_detail.html")
    runner = _read("app/services/parser_runner.py")
    parser = _read("app/parsing/che168.py")
    tool = _read("app/tools/che168_offline_parse.py")
    config = _read("app/parsing/sites_config.yaml")
    assert '"che168": Che168Parser' in runner
    assert "class Che168Parser" in parser
    assert "parse_list_html" in parser
    assert "parse_detail_html" in parser
    assert 'currency: "CNY"' in config
    assert "Offline smoke parser for saved che168 HTML" in tool
    assert "DEFAULT_LISTING" in tool
    assert "detail_preview" in tool
    assert "Подбор и поставка автомобилей из Европы и Азии под ключ." in base_template
    assert "варианты из Европы и Азии." in detail_template


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
    assert '"Серебристый"' in color_utils
    assert '"Другие"' in color_utils
    assert "return ordered_items, []" in color_utils


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


def test_catalog_cards_stay_rub_only_and_detail_primary_prefers_orig_with_proxy_fallback():
    catalog_template = _read("app/templates/catalog.html")
    detail_template = _read("app/templates/car_detail.html")
    script = _read("app/static/js/app.js")
    pages = _read("app/routers/pages.py")
    assert "{% elif car.price %}" not in catalog_template
    assert "primary_thumb_src = thumbs.thumb_src(primary_raw, 960, 5)" in detail_template
    assert "data-thumb=\"{{ primary_thumb_src or '' }}\"" in detail_template
    assert "if (!img.dataset.thumbFallbackTried" in script
    assert "applyThumbFallback(primary, { thumbProxy: false })" in script
    assert "ordered_images = sorted(" in pages


def test_count_cars_keeps_interior_filters_in_count_path():
    service = _read("app/services/cars_service.py")
    assert "normalized_interior_color = normalize_csv_values(interior_color) or interior_color" in service
    assert "normalized_interior_material = normalize_csv_values(interior_material) or interior_material" in service
    assert "interior_color=normalized_interior_color" in service
    assert "interior_material=normalized_interior_material" in service
    assert "num_seats=num_seats" in service
    assert "owners_count=owners_count" in service


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
    assert 'interior_payload_expr = func.lower(' in service
    assert 'jsonb_extract_path_text(payload_json, "interior_design")' in service
    assert "func.coalesce(Car.description, \"\")" in service
    assert "def _interior_alias_clause" in service
    assert "parse_interior_trim_token" in service
    assert "trim_token_conditions" in service
    assert 'field="color_group"' in pages
    assert 'field="color_group"' in catalog


def test_price_sorted_cache_can_be_enabled_and_base_selects_dedupe_values():
    router = _read("app/routers/catalog.py")
    script = _read("app/static/js/app.js")
    assert 'os.getenv("CATALOG_PRICE_SORT_CACHE_BYPASS", "0") != "1"' in router
    assert "const seenValues = new Set()" in script
    assert "if (loadCatalogFilterBase.__pending) return loadCatalogFilterBase.__pending" in script


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
