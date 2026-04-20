from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]


def test_perf_full_audit_script_exists_and_is_valid_bash():
    script = ROOT.parent / "scripts" / "perf_full_audit.sh"
    assert script.exists(), "scripts/perf_full_audit.sh must exist"
    content = script.read_text(encoding="utf-8")
    assert "BENCH_SEARCH" in content
    assert "cars_count_country_brand" in content
    assert "cars_list_country_brand_model" in content
    assert "catalog_ssr_eu" in content
    assert "search_ssr_eu" in content
    assert "detail_ssr" in content
    assert "cars_count_kr_internal" in content
    assert "response_headers_catalog" in content
    assert "response_headers_detail" in content
    assert "cars_price_note_europe" in content
    assert '-D - -o /dev/null' in content
    completed = subprocess.run(["bash", "-n", str(script)], capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr
