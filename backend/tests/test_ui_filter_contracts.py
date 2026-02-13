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
