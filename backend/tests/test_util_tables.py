from pathlib import Path

import yaml


def _load_customs():
    path = Path(__file__).resolve().parents[1] / "app" / "config" / "customs.yml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_util_tables_under3_vs_3_5_differs_for_100_2000_hp():
    cfg = _load_customs()
    under3 = cfg["util_tables_under3"]["100-2000"]["hp"][:5]
    over3 = cfg["util_tables_3_5"]["100-2000"]["hp"][:5]
    assert under3 != over3


def test_util_tables_3_5_values_for_known_ranges():
    cfg = _load_customs()
    # 100-2000 hp 221-250 should match Excel-derived values
    rows_100_2000 = cfg["util_tables_3_5"]["100-2000"]["hp"]
    row_221_250 = next(r for r in rows_100_2000 if r["from"] == 221.0 and r["to"] == 250.0)
    assert row_221_250["price_rub"] == 1677600

    # 2000-3000 kw 272.14-294.2 should match Excel-derived values
    rows_2000_3000 = cfg["util_tables_3_5"]["2000-3000"]["kw"]
    row_272 = next(r for r in rows_2000_3000 if r["from"] == 272.14 and r["to"] == 294.2)
    assert row_272["price_rub"] == 4094400
