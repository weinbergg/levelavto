from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (ROOT / rel_path).read_text(encoding="utf-8")


def test_js_has_model_accordion_rendering():
    script = _read("app/static/js/app.js")
    assert "model-accordion__group" in script
    assert "model-accordion__item" in script
    assert "model-accordion__apply" in script
    assert "data-model-value" in script
    assert "data-model-group-values" in script
    assert "model_groups" in script
    assert "setAccordionSelectedModels" in script
    assert "getAccordionSelectedModels" in script
    assert "__modelAccordionSync" in script
    assert "selected.getAll('line')" in script
    assert "const toggleModelSelection = (value) =>" in script
    assert "itemBtn.addEventListener('click', (event) =>" in script
    assert "toggleModelSelection(value)" in script
    assert "applyBtn.textContent = 'Применить'" in script
    assert "root.open = false" in script
    assert "dataset.skipTriggerOnce = '1'" in script
    assert "applySelection()" in script
    assert "meta.baseModel" in script


def test_css_has_model_accordion_styles():
    css = _read("app/static/css/styles.css")
    assert ".model-accordion" in css
    assert ".model-accordion__item" in css
    assert ".model-accordion__model.is-active" in css
    assert ".model-accordion__actions" in css
    assert ".model-accordion__apply" in css
    assert "position: sticky;" in css
    assert "width: 100%;" in css
    assert "font-size: 15px;" in css
