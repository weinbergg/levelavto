from pathlib import Path


def test_routers_hide_kr_market_filter_without_classified_rows():
    root = Path(__file__).resolve().parents[2]
    pages = (root / "backend" / "app" / "routers" / "pages.py").read_text(encoding="utf-8")
    catalog = (root / "backend" / "app" / "routers" / "catalog.py").read_text(encoding="utf-8")
    service = (root / "backend" / "app" / "services" / "cars_service.py").read_text(encoding="utf-8")
    assert "has_korea_market_type_data" in pages
    assert "has_korea_market_type_data" in catalog
    assert "def has_korea_market_type_data" in service


def test_kr_type_filters_require_market_type_not_broad_kr_scope():
    root = Path(__file__).resolve().parents[2]
    service = (root / "backend" / "app" / "services" / "cars_service.py").read_text(encoding="utf-8")
    assert "conditions.append(func.lower(Car.kr_market_type) == kt)" in service
    assert 'conds = [func.lower(Car.kr_market_type) == kt, Car.country.like("KR%")]' not in service
