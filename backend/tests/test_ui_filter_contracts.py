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
    assert '<div class="field field--multi-select"><span class="field-label">Кузов</span>' in template
    assert '<div class="field field--multi-select"><span class="field-label">Топливо</span>' in template
    assert 'id="advanced-keywords"' in template
    assert 'data-dynamic-payload="0"' in template
    assert template.index('name="q"') < template.index('id="advanced-tech"')


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
    assert "readRenderedChoiceChipItems" in script
    assert 'className = \'multi-select-menu__apply\'' in script or 'className = "multi-select-menu__apply"' in script
    assert "container.classList.toggle('multi-select-menu--fuel', name === 'engine_type')" in script
    assert "clearBtn.textContent = 'Сбросить выбор'" in script
    assert "clearBtn.disabled = selectedSet.size === 0" in script
    assert "select.disabled = normalizedItems.length === 0" in script or "select.disabled = deduped.length === 0" in script
    assert "bindChoiceChips(filtersForm, () => loadCars(1, { scrollToTop: true }))" in script
    assert "const priceMain = card.querySelector('.price-main')" in script
    assert "let metaNode = card.querySelector('.car-card__meta')" in script
    assert "let specsNode = card.querySelector('.specs')" in script
    assert "function positionFloatingOverlay(" in script
    assert "function bindFloatingOverlayPosition(" in script
    assert "function findOverlayHost(control)" in script
    assert "const field = control.closest('.field')" in script
    assert "const boundsRect = boundsEl?.getBoundingClientRect?.() || null" in script
    assert "boundsEl: root.closest('.filters-panel') || null" in script
    assert "body.style.bottom = openUp ? `calc(100% + ${gap}px)` : 'auto'" in script
    assert "const ssrHydrated = hydrateCatalogFromSSR()" in script
    assert "if (!ssrHydrated) {" in script
    assert "void loadCars(initialPage)" in script
    assert "contentWrap.className = 'model-accordion__content'" in script or 'contentWrap.className = "model-accordion__content"' in script


def test_filter_payload_includes_dynamic_brand_options():
    router = _read("app/routers/catalog.py")
    assert 'field="brand"' in router
    assert '"brands": brands' in router
    assert 'field="color_group"' in router
    assert '"reg_years": reg_years' in router
    assert '"reg_year_min": None' in router
    assert '"reg_month_min": None' in router
    assert '"reg_year_max": None' in router
    assert '"reg_month_max": None' in router
    assert 'body_type_filters = {**common_filters, "body_type": None}' in router
    assert 'engine_type_filters = {**common_filters, "engine_type": None}' in router
    assert 'transmission_filters = {**common_filters, "transmission": None}' in router
    assert 'drive_type_filters = {**common_filters, "drive_type": None}' in router
    assert 'color_filters = {**common_filters, "color": None}' in router
    assert 'generation_filters = {**common_filters, "generation": None}' in router
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
    assert "let skipInitialCountFetch = Boolean(countEl?.dataset.count)" in script
    assert "if (skipInitialCountFetch) return" in script
    assert "scheduleCount()" in script
    assert "name=\"line_variant\"" not in template
    assert "<label>Вариант</label>" not in template


def test_base_template_bumps_app_bundle_version():
    template = _read("app/templates/base.html")
    assert '/static/js/app.js?v=110' in template
    assert '/static/css/styles.css?v=66' in template


def test_deploy_cron_has_midday_public_prewarm():
    repo_root = Path(__file__).resolve().parents[2]
    cron = (repo_root / "deploy" / "cron.mobilede").read_text(encoding="utf-8")
    script = (repo_root / "scripts" / "prewarm_public_site.sh").read_text(encoding="utf-8")
    assert "0 14 * * *" in cron
    assert "scripts/prewarm_public_site.sh" in cron
    assert 'python -m backend.app.scripts.prewarm_cache' in script
    assert 'PREWARM_INCLUDE_PAYLOAD="${PREWARM_INCLUDE_PAYLOAD:-1}"' in script


def test_main_enables_gzip_for_large_html_and_api_payloads():
    main = _read("app/main.py")
    assert "GZipMiddleware" in main
    assert "minimum_size=1024" in main


def test_search_page_passes_payload_deferred_flag():
    router = _read("app/routers/pages.py")
    assert '"payload_deferred": bool(filter_ctx.get("payload_deferred"))' in router
    assert '"payload_deferred": True' in router
    assert '"_engine_type_source": "normalized"' in router


def test_catalog_template_does_not_duplicate_visible_interior_hidden_inputs():
    template = _read("app/templates/catalog.html")
    assert "data-chip-input=\"interior_color\"" in template
    assert "data-chip-input=\"interior_material\"" in template
    assert '<div class="field field--multi-select"><span class="field-label">Кузов</span>' in template
    assert '<div class="field field--multi-select"><span class="field-label">Топливо</span>' in template
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
    assert "def _catalog_inline_price_refresh_enabled(self) -> bool:" in service
    assert 'os.getenv("CATALOG_INLINE_PRICE_REFRESH", "0") != "0"' in service
    assert "def _should_catalog_inline_price_refresh(" in service
    assert 'CATALOG_INLINE_PRICE_REFRESH_DEFAULT", "0") != "0"' in service
    assert 'page_num <= max_page and size_num <= max_page_size' in service
    assert 'service._should_catalog_inline_price_refresh(page=page, page_size=page_size)' in catalog_router
    assert 'timing["initial_list_ms"]' in pages_router
    assert 'timing["initial_decorate_ms"]' in pages_router
    assert 'timing["initial_images_ms"]' in pages_router


def test_home_price_refresh_is_opt_in():
    pages_router = _read("app/routers/pages.py")
    assert 'HOME_REFRESH_VISIBLE_PRICES", "0") == "1"' in pages_router


def test_engine_type_facets_can_use_aggregated_counts_fast_path():
    service = _read("app/services/cars_service.py")
    assert 'ENGINE_TYPE_FACET_RAW_SCAN", "0") == "1"' in service
    assert 'return self.facet_counts(field="engine_type", filters=fast_engine_filters)' in service


def test_filter_payload_can_defer_to_base_ctx_on_public_scope():
    catalog_router = _read("app/routers/catalog.py")
    assert "def _build_deferred_filter_payload_from_base_ctx(" in catalog_router
    assert "if params == base_scope_params and base_ctx is not None:" in catalog_router
    assert "source=base_ctx_deferred" in catalog_router
    assert '"payload_deferred": True' in catalog_router


def test_catalog_perf_path_canonicalizes_free_text_fuel_and_prewarms_engine_lists():
    catalog_router = _read("app/routers/catalog.py")
    service = _read("app/services/cars_service.py")
    prewarm = _read("app/scripts/prewarm_cache.py")
    assert "canonicalize_free_text_filters" in catalog_router
    assert 'CATALOG_CACHE_HIT_REFRESH_PRICES", "0") != "0"' in catalog_router
    assert "explicit_country_code not in self.EU_COUNTRIES" in service
    assert 'PREWARM_INCLUDE_ENGINE_LISTS' in prewarm
    assert 'engine_type=params.get("engine_type")' in prewarm


def test_public_catalog_scope_defaults_to_eu_without_explicit_region_or_country():
    catalog_router = _read("app/routers/catalog.py")
    pages_router = _read("app/routers/pages.py")
    home_template = _read("app/templates/home.html")
    assert "def _apply_public_catalog_default_scope(params: dict) -> dict:" in catalog_router
    assert 'normalized["region"] = "EU"' in catalog_router
    assert "def _apply_public_catalog_default_scope(params: Dict[str, Any]) -> Dict[str, Any]:" in pages_router
    assert 'normalized["region"] = "EU"' in pages_router
    assert 'build_filter_ctx_base_key({"region": "EU"})' in pages_router
    assert 'filters={"region": "EU"}' in pages_router
    assert 'href="/catalog?region=EU&brand={{ item.brand }}"' in home_template


def test_eu_registration_filters_ignore_legacy_generic_default_flag():
    service = _read("app/services/cars_service.py")
    assert 'Car.country.like("KR%")' in service
    assert "EU rows could have only the month defaulted" in service


def test_similar_cars_avoids_bare_numeric_order_by_constants():
    service = _read("app/services/cars_service.py")
    assert "def _sort_const(value: int | float):" in service
    assert "interprets as select-list ordinals" in service
    assert 'SIMILAR_CARS_CANDIDATE_POOL' in service
    assert 'SIMILAR_CARS_MODEL_POOL' in service
    assert "First collect a recent candidate pool cheaply" in service
    assert ".where(Car.id.in_(candidate_ids))" in service
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
    assert "overflow-y: auto;" in css


def test_price_note_matches_price_scale():
    css = _read("app/static/css/styles.css")
    assert ".price-note {" in css
    assert "font-size: inherit;" in css
    assert ".detail-price__note {" in css
    assert "font-weight: 800;" in css


def test_recalc_scripts_support_engine_type_targeting_and_daily_ev_recovery():
    recalc_missing = _read("app/scripts/recalc_missing_prices.py")
    recalc_cache = _read("app/scripts/recalc_calc_cache.py")
    pipeline = _read("../scripts/mobilede_daily_pipeline.sh")
    assert 'ap.add_argument("--engine-type"' in recalc_missing
    assert 'ap.add_argument("--engine-type"' in recalc_cache
    assert '--engine-type electric \\' in pipeline
    assert 'step=recalc_electric_recoverable_fallbacks' in pipeline
    assert 'step=analyze_post_recalc' in pipeline
    assert 'ANALYZE cars;' in pipeline


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


def test_home_content_rewrites_legacy_hero_title_and_note():
    content = build_home_content(
        {
            "hero_title": "Импорт и подбор автомобилей из Европы и Азии",
            "hero_note": "Возим проверенные авто из Европы, Азии и РФ. Минимальная предоплата — основная часть по факту поставки.",
        }
    )
    assert content["hero"]["title"] == "Доставка и подбор автомобилей из Европы и Азии"
    assert content["hero"]["note"] == ""


def test_home_css_keeps_model_actions_in_bottom_bar_on_mobile():
    css = _read("app/static/css/home.css")
    assert "#home-search .model-accordion__actions" in css
    assert "#home-search .model-accordion__content" in css
    assert "margin: 0;" in css
    assert "padding: 14px 10px calc(10px + env(safe-area-inset-bottom, 0px));" in css


def test_home_template_bumps_home_css_bundle_version():
    template = _read("app/templates/home.html")
    assert '/static/css/home.css?v=26' in template
    assert "{% for img in collage_images %}" in template
    assert "collage_images[:120]" not in template


def test_home_template_places_partners_block_after_search():
    template = _read("app/templates/home.html")
    assert 'class="la-container hero-search"' in template


def test_home_media_loader_supports_collage_manifest():
    router = _read("app/routers/pages.py")
    assert 'static_manifest_path = static_collage_dir / "manifest.json"' in router
    assert 'manifest_entries = [item for item in raw_manifest if isinstance(item, dict)]' in router
    assert 'static_collage_dir / str(item.get("file") or "").strip()' in router
    assert 'mobile_rel = str(manifest_item.get("mobile_file") or "").strip()' in router
    assert 'srcset_parts.append(f"{build_static_url(mobile_path)} {mobile_width}w")' in router
    assert '"fallback": "/static/img/no-photo.svg"' in router


def test_home_template_places_partners_block_after_search_and_hides_legacy_copy():
    template = _read("app/templates/home.html")
    assert 'class="la-container hero-partners" id="home-partners"' in template
    assert template.index('class="la-container hero-search"') < template.index('class="la-container hero-partners" id="home-partners"')
    assert "{{ home.hero.why_title }}" not in template
    assert "{% if home.hero.note %}" in template


def test_home_css_uses_full_width_partners_block():
    css = _read("app/static/css/home.css")
    assert ".hero-partners {" in css
    assert ".hero-partners-card--full" in css or ".hero-partners-card {" in css
    assert "grid-template-columns: minmax(0, 1fr);" in css


def test_home_search_stacks_above_partners_and_keeps_overlay_visible():
    css = _read("app/static/css/home.css")
    assert ".hero-search {" in css
    assert "z-index: 8;" in css
    assert "isolation: isolate;" in css
    assert ".hero-partners {" in css
    assert "z-index: 1;" in css
    assert ".search-card--overlay {" in css
    assert "overflow: visible;" in css


def test_text_query_input_is_available_in_search_and_catalog():
    home = _read("app/templates/home.html")
    search = _read("app/templates/search.html")
    catalog = _read("app/templates/catalog.html")
    css = _read("app/static/css/styles.css")
    assert 'name="q"' not in home
    assert 'name="q"' in search
    assert 'name="q"' in catalog
    assert ".advanced-search #advanced-keywords," in css or ".advanced-search #advanced-keywords {" in css


def test_search_template_uses_full_kr_type_options_in_advanced_search():
    search = _read("app/templates/search.html")
    assert '{% for kt in kr_types %}' in search
    assert 'params.get(\'kr_type\')|upper == kt.value' in search
    assert '<option value="import">Импортные</option>' not in search


def test_parsing_service_auto_recalculates_korea_prices_after_upsert():
    parsing_service = _read("app/services/parsing_data_service.py")
    assert 'PARSER_AUTO_CALC_KR' in parsing_service
    assert 'ensure_calc_cache(car, force=True)' in parsing_service
    assert 'country") or source.country' in parsing_service or "country') or source.country" in parsing_service


def test_home_contacts_use_icon_messengers_and_direct_telegram_link():
    template = _read("app/templates/home.html")
    css = _read("app/static/css/home.css")
    assert "https://t.me/DmitriyMotorov" in template
    assert "contact-messenger-links" in template
    assert "lead-messenger-link__icon" in template
    assert ".contact-messenger-links" in css


def test_register_template_and_backend_support_phone_and_email_verification():
    router = _read("app/routers/auth.py")
    template = _read("app/templates/auth/register.html")
    email_service = _read("app/services/email_verification_service.py")
    service = _read("app/services/phone_verification_service.py")
    email_model = _read("app/models/email_verification.py")
    model = _read("app/models/phone_verification.py")
    bootstrap = _read("app/schema_bootstrap.py")
    config = _read("app/config.py")
    assert '@router.post("/api/auth/email/send-code")' in router
    assert '@router.post("/api/auth/email/verify-code")' in router
    assert '@router.post("/api/auth/phone/send-code")' in router
    assert '@router.post("/api/auth/phone/verify-code")' in router
    assert "email_verification_token" in router
    assert "phone_verification_token" in router
    assert 'id="register-email"' in template
    assert 'id="send-email-code"' in template
    assert 'id="verify-email-code"' in template
    assert 'id="email-verification-token"' in template
    assert 'id="register-phone"' in template
    assert 'id="send-phone-code"' in template
    assert 'id="verify-phone-code"' in template
    assert 'id="phone-verification-token"' in template
    assert "class SmtpEmailProvider" in email_service
    assert "class EmailVerificationService" in email_service
    assert "normalize_phone_number" in service
    assert "class SmsRuProvider" in service
    assert "class PhoneVerificationService" in service
    assert "EmailVerificationChallenge" in email_model
    assert "PhoneVerificationChallenge" in model
    assert "ALTER TABLE users ADD COLUMN email_verified_at" in bootstrap
    assert "ALTER TABLE users ADD COLUMN phone" in bootstrap
    assert "EMAIL_PROVIDER" in config
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
    assert "detail_payload: Dict[str, Any] = {}" in parser
    assert 'if detail_payload:' in parser
    assert 'out["source_payload"] = detail_payload' in parser
    assert 'detail_payload["registration_source"] = "emavto_detail"' in parser
    assert "def needs_detail_refresh(car: Car) -> bool:" in runner
    assert 'payload.get("registration_defaulted") is True' in runner
    assert "same_basic(car, task) and not needs_detail_refresh(car)" in runner
    assert 'PID_FILE="${LOCK_DIR}/pid"' in wrapper
    assert 'echo "[emavto] clearing stale lock at ${LOCK_DIR}"' in wrapper
    assert "printf '%s\\n' \"$$\" > \"${PID_FILE}\"" in wrapper
    assert "find_active_worker()" in wrapper
    assert "wait_for_active_worker_without_lock" in wrapper
    assert 'echo "[emavto] detected active worker without lock"' in wrapper
    assert 'if [[ "${STATUS}" != "ok" ]]; then' in wrapper
    assert "exit 1" in wrapper


def test_model_filters_use_canonical_labels_with_alias_restore():
    service = _read("app/services/cars_service.py")
    router = _read("app/routers/catalog.py")
    script = _read("app/static/js/app.js")
    assert "def _canonical_model_label(" in service
    assert "_PORSCHE_MODEL_ALIASES" in service
    assert "def _brand_model_alias_label(" in service
    assert "def _resolve_model_aliases(" in service
    assert "def _model_filter_clause(" in service
    assert "if filters.get(\"model\"):" in service
    assert "clause = self._model_filter_clause(" in service
    assert "clause = service._model_filter_clause(" in router
    assert "select.__resolveModelValues = (values = []) => {" in script
    assert "const restoredModelsRaw = selectedLines.length ? selectedLines : getInitialLineModelsForBrand(normBrand)" in script
    assert "const resolvedSelectedValues = typeof modelSelect.__resolveModelValues === 'function'" in script


def test_cars_count_route_tracks_registration_month_filters():
    router = _read("app/routers/catalog.py")
    service = _read("app/services/cars_service.py")
    assert '@router.get("/cars_count")' in router
    assert "reg_month_min: Optional[int] = Query(default=None)" in router
    assert "reg_month_max: Optional[int] = Query(default=None)" in router
    assert '"reg_month_min": reg_month_min' in router
    assert '"reg_month_max": reg_month_max' in router
    assert "reg_month_min=reg_month_min," in router
    assert "reg_month_max=reg_month_max," in router
    assert "reg_month_min: Optional[int] = None," in service
    assert "reg_month_max: Optional[int] = None," in service


def test_pages_home_uses_recommended_and_media_cache_helpers():
    router = _read("app/routers/pages.py")
    assert "_get_home_recommended(service, db, reco_cfg, limit=12)" in router
    assert "_get_home_more_offers(service, db, limit=12)" in router
    assert "def _home_media_cache_version()" in router
    assert 'return f"home_media_ctx:{_home_media_cache_version()}"' in router
    assert 'static" / "home-collage"' in router
    assert "home_recommended:" in router
    assert "home_more_offers:" in router
    assert 'cfg.get("reg_year_min", 2021)' in router
    assert 'cfg.get("power_hp_max", 160)' in router


def test_home_collage_and_home_content_copy_are_updated():
    template = _read("app/templates/home.html")
    home_content = _read("app/utils/home_content.py")
    assert "{% for img in collage_images %}" in template
    assert "data-loop-base-count" not in template
    assert "const syncCollageLoop = () => {" not in template
    assert "sizes=\"(max-width: 480px) 78px, (max-width: 640px) 88px, (max-width: 820px) 102px, 240px\"" in template
    assert 'data-expand-toggle="home-more-offers-grid"' in template
    assert 'data-expand-pages="2"' in template
    assert 'id="home-more-offers-catalog"' in template
    assert 'id="home-more-offers-grid"' in template
    assert "Показываем только марки, которые есть в каталоге" in home_content
    assert "Доставка и подбор автомобилей из Европы и Азии" in home_content
    assert "Возим проверенные авто из Европы, Азии и РФ." not in home_content
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


def test_engine_type_facets_use_effective_fuel_source_and_custom_filtering():
    service = _read("app/services/cars_service.py")
    catalog = _read("app/routers/catalog.py")
    assert "def _fuel_source_expr(self):" in service
    assert 'func.jsonb_extract_path_text(payload_json, "full_fuel_type")' in service
    assert 'func.jsonb_extract_path_text(payload_json, "envkv_engine_type")' in service
    assert 'func.jsonb_extract_path_text(payload_json, "envkv_consumption_fuel")' in service
    assert "def _fuel_hint_text_expr(self):" in service
    assert "def _bev_hint_expr(self):" in service
    assert "def _effective_electric_fuel_expr(self):" in service
    assert 'literal("electric")' in service
    assert '"электро": ["electric"]' in service
    assert '"electric": ["electric"]' in service
    assert '%ev%' not in service
    assert "def _fuel_filter_clause(self, raw_value: str):" in service
    assert 'if key == "hybrid_diesel":' in service
    assert 'if key == "phev":' in service
    assert 'if field == "engine_type" and os.getenv("ENGINE_TYPE_FACET_RAW_SCAN", "0") == "1":' in service
    assert 'if field == "color_group":' in service
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
    assert "resolve_public_display_price_rub" in catalog_router
    assert "resolve_public_display_price_rub" in pages_router
    assert 'def public_price_fallback_enabled() -> bool:' in price_utils
    assert 'PUBLIC_PRICE_ALLOW_SOURCE_FALLBACK", "0") == "1"' in price_utils
    assert 'def public_price_allow_without_util() -> bool:' in price_utils
    assert 'PUBLIC_PRICE_ALLOW_WITHOUT_UTIL", "0") == "1"' in price_utils
    assert "calc_breakdown=" in catalog_router
    assert "calc_breakdown=" in pages_router
    assert 'DETAIL_INLINE_CALC", "0") == "1"' in catalog_router
    assert 'DETAIL_INLINE_CALC", "0") == "1"' in pages_router
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
    pages = _read("app/routers/pages.py")
    assert '<div class="field field--full"><span class="field-label">Цвет кузова</span>' in search_template
    assert '<div class="field field--full"><span class="field-label">Цвет кузова</span>' in catalog_template
    assert '"_color_source": "color_group"' in pages


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


def test_catalog_cards_stay_rub_only_and_detail_primary_prefers_thumb_with_orig_fallback():
    catalog_template = _read("app/templates/catalog.html")
    detail_template = _read("app/templates/car_detail.html")
    script = _read("app/static/js/app.js")
    pages = _read("app/routers/pages.py")
    assert "{% elif car.price %}" not in catalog_template
    assert "primary_thumb_src = thumbs.thumb_src(primary_raw, 640, 5)" in detail_template
    assert "{% set primary_fast_src = (car.thumbnail_url or primary_thumb_src or '')|trim %}" in detail_template
    assert "{% set primary_src = primary_fast_src or primary_raw %}" in detail_template
    assert 'fetchpriority="high"' in detail_template
    assert "data-thumb=\"{{ primary_fast_src or primary_thumb_src or '' }}\"" in detail_template
    assert "if (!img.dataset.thumbFallbackTried" in script
    assert "def _detail_similar_offers_enabled() -> bool:" in pages
    assert 'DETAIL_SIMILAR_OFFERS_ENABLED", "1") != "0"' in pages
    assert 'os.getenv("DETAIL_REFRESH_SIMILAR_PRICES", "0") == "1"' in pages
    assert "if similar_offers and os.getenv" in pages
    assert 'logger.exception("detail_similar_offers_failed car=%s"' in pages
    assert "applyThumbFallback(primary)" in script
    assert "img.src = nextOrig || nextThumb" in script
    assert "applyThumbFallback(img)" in script
    assert "const renderedSrc = img.getAttribute('src') || ''" in script
    assert "ordered_images = sorted(" in pages
    assert "detail_images = [resolved_thumb] + [u for u in detail_images if u != resolved_thumb]" in pages
    assert "0 if (thumbnail_key and pair[1] == thumbnail_key) else 1" in pages


def test_advanced_search_preserves_server_rendered_filter_options_until_first_live_payload():
    script = _read("app/static/js/app.js")
    template = _read("app/templates/search.html")
    assert "readCurrentSelectOptions" in script
    assert "form.dataset.payloadHydrated === '1'" in script
    assert "const isInitialPayloadMerge = form.dataset.payloadHydrated !== '1'" in script
    assert "const preserved = isInitialPayloadMerge && !eu.length && !kr.length" in script
    assert "readRenderedChoiceChipItems(wrap)" in script
    assert "filtersForm.dataset.baseHydrated !== '1'" in script
    assert "preserveExistingOnEmpty: preserveChoiceChips" in script
    assert "const enableDynamicPayload = form.dataset.dynamicPayload === '1'" in script
    assert "if (!enableDynamicPayload) return" in script
    assert 'data-dynamic-payload="0"' in template


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
    counts = _read("app/tools/car_counts_refresh.py")
    assert "def _registration_defaulted_expr()" in service
    assert "def _registration_uses_model_year_expr(cls)" in service
    assert "def _registration_uses_fallback_month_expr(cls)" in service
    assert "Car.registration_year.is_(None)" in service
    assert "def _effective_registration_year_expr(cls)" in service
    assert "def _effective_registration_month_floor_expr(cls)" in service
    assert "def _effective_registration_month_ceil_expr(cls)" in service
    assert "else_=Car.registration_year" in service
    assert "reg_year_expr = self._effective_registration_year_expr()" in service
    assert "reg_month_floor_expr = self._effective_registration_month_floor_expr()" in service
    assert "reg_month_ceil_expr = self._effective_registration_month_ceil_expr()" in service
    assert "CarsService._effective_registration_year_expr().cast(Integer)" in counts


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
    assert "def _registration_year_defaulted_expr" in service
    assert "def _registration_month_defaulted_expr" in service
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
    assert 'source_payload["registration_year_defaulted"] = True' in reg_defaults
    assert 'source_payload["registration_month_defaulted"] = True' in reg_defaults
    assert "apply_missing_registration_fallback(payload, persist_fields=False)" in parsing_service
    assert "--only-defaulted-registration" in recalc_script
    assert 'jsonb_extract_path_text(payload_json, "registration_defaulted")' in recalc_script
    assert "[backfill_missing_registration]" in reg_backfill_script
    assert "step=ensure_services" in fx_daily.read_text(encoding="utf-8")


def test_catalog_cache_refreshes_stale_rows_from_db_and_mobilede_parser_supports_registration_formats():
    catalog = _read("app/routers/catalog.py")
    parser = _read("app/parsing/mobile_de_feed.py")
    assert "service.sync_light_rows_from_db(items, refresh_prices=refresh_cached_list_prices)" in catalog
    assert 'CATALOG_CACHE_HIT_REFRESH_PRICES", "0") != "0"' in catalog
    assert "_serialize_catalog_payload_items(" in catalog
    assert "def _parse_first_registration" in parser
    assert r"^(?P<month>\d{1,2})[./-](?P<year>\d{4})$" in parser
    assert r"^(?P<day>\d{1,2})[./-](?P<month>\d{1,2})[./-](?P<year>\d{4})$" in parser


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
