from pathlib import Path

from backend.app.utils.price_utils import display_price_rub


def test_display_price_rub_priority_total():
    assert display_price_rub(1500000, 1200000) == 1500000


def test_display_price_rub_fallback_price():
    assert display_price_rub(None, 990000) == 990000


def test_sort_price_asc_uses_display_price():
    cars = [
        {"id": 1, "total": 100, "price": 50},
        {"id": 2, "total": None, "price": 90},
        {"id": 3, "total": None, "price": None},
        {"id": 4, "total": 80, "price": 70},
        {"id": 5, "total": None, "price": 80},
    ]

    def sort_key(car):
        missing = car["total"] is None and car["price"] is None
        value = display_price_rub(car["total"], car["price"])
        return (missing, value if value is not None else float("inf"), car["id"])

    ordered = [c["id"] for c in sorted(cars, key=sort_key)]
    assert ordered == [4, 5, 2, 1, 3]


def test_card_and_detail_use_same_field():
    base = Path(__file__).resolve().parents[1]
    app_js = (base / "app" / "static" / "js" / "app.js").read_text(encoding="utf-8")
    detail_html = (base / "app" / "templates" / "car_detail.html").read_text(encoding="utf-8")
    catalog_html = (base / "app" / "templates" / "catalog.html").read_text(encoding="utf-8")
    assert "display_price_rub" in app_js
    assert "display_price_rub" in detail_html
    assert "display_price_rub" in catalog_html
