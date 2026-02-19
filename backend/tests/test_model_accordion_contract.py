from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (ROOT / rel_path).read_text(encoding="utf-8")


def test_js_has_model_accordion_rendering():
    script = _read("app/static/js/app.js")
    assert "model-accordion__group" in script
    assert "data-model-value" in script
    assert "model_groups" in script


def test_css_has_model_accordion_styles():
    css = _read("app/static/css/styles.css")
    assert ".model-accordion" in css
    assert ".model-accordion__model.is-active" in css

