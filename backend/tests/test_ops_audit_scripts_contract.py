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
    assert "calc_updated" in content


def test_refresh_emavto_registration_script_has_targeted_repair_flags():
    content = _read("app/scripts/refresh_emavto_registration.py")
    assert "--car-id" in content
    assert "--limit" in content
    assert '"registration_defaulted"' in content
    assert "[refresh_emavto_registration]" in content


def test_refresh_mobilede_registration_script_has_targeted_repair_flags():
    content = _read("app/scripts/refresh_mobilede_registration.py")
    assert "--car-id" in content
    assert "--limit" in content
    assert '"first_registration"' in content
    assert "[refresh_mobilede_registration]" in content


def test_reclassify_emavto_market_type_script_has_safe_bulk_repair_flags():
    content = _read("app/scripts/reclassify_emavto_market_type.py")
    assert "--reset-first" in content
    assert "--chunk-pages" in content
    assert "--start-page" in content
    assert "skip_details" in content
    assert "[reclassify_emavto_market_type]" in content


def test_debug_kr_price_status_script_reports_missing_spec_causes():
    content = _read("app/scripts/debug_kr_price_status.py")
    assert "--sample-limit" in content
    assert "missing_engine_cc" in content
    assert "missing_power" in content
    assert "without_util_marker" in content
