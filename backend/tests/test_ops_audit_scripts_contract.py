from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (ROOT / rel_path).read_text(encoding="utf-8")


def test_audit_broken_thumbs_script_has_fix_modes():
    content = _read("app/scripts/audit_broken_thumbs.py")
    assert "--fix" in content
    assert "--clear-unrecoverable" in content
    assert "broken_thumbs_report" in content


def test_audit_prices_script_has_ids_and_fix():
    content = _read("app/scripts/audit_prices.py")
    assert "--ids" in content
    assert "--fix" in content
    assert "price_source_note" in content

