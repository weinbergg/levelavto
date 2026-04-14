from pathlib import Path


def test_emavto_parser_separates_domestic_and_import_tabs():
    root = Path(__file__).resolve().parents[2]
    parser = (root / "backend" / "app" / "parsing" / "emavto_klg.py").read_text(encoding="utf-8")
    runner = (root / "backend" / "app" / "tools" / "emavto_chunk_runner.py").read_text(encoding="utf-8")
    assert '"market_type": "domestic"' in parser
    assert '"page_param": "koreaPage"' in parser
    assert '"scope_selector": "#korea-cars"' in parser
    assert '"market_type": "import"' in parser
    assert '"page_param": "importPage"' in parser
    assert '"scope_selector": "#import-cars"' in parser
    assert 'detail_payload["kr_market_type"] = task.get("kr_market_type")' in parser
    assert '"kr_market_type_source": "emavto_tab"' in parser
    assert '"kr_market_type": task.get("kr_market_type")' in runner
