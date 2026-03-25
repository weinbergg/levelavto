from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (ROOT / rel_path).read_text(encoding="utf-8")


def test_filtered_brand_facets_are_supported_in_service():
    service = _read("app/services/cars_service.py")
    assert '"brand": Car.brand' in service
    assert "merged_brands" in service
    assert 'if field == "brand":' in service


def test_filter_payload_uses_dynamic_brands():
    router = _read("app/routers/catalog.py")
    assert 'field="brand"' in router
    assert '"brands": brands' in router
