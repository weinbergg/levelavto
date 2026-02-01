from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from backend.app.models.source import Base, Source
from backend.app.models.car import Car
from backend.app.services.cars_service import CarsService
from backend.app.utils.price_utils import display_price_rub


def test_display_price_rub_priority_total():
    assert display_price_rub(100.0, 50.0) == 100.0


def test_display_price_rub_fallback_price():
    assert display_price_rub(None, 80.5) == 80.5


def test_sort_price_asc_uses_display_price():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        db.add(Source(id=1, key="mobile_de", name="Mobile.de", base_url="https://m.de", country="DE"))
        db.add_all(
            [
                Car(
                    id=1,
                    source_id=1,
                    external_id="1",
                    country="DE",
                    is_available=True,
                    total_price_rub_cached=100.0,
                    price_rub_cached=50.0,
                ),
                Car(
                    id=2,
                    source_id=1,
                    external_id="2",
                    country="DE",
                    is_available=True,
                    total_price_rub_cached=None,
                    price_rub_cached=80.0,
                ),
                Car(
                    id=3,
                    source_id=1,
                    external_id="3",
                    country="DE",
                    is_available=True,
                    total_price_rub_cached=70.0,
                    price_rub_cached=None,
                ),
                Car(
                    id=4,
                    source_id=1,
                    external_id="4",
                    country="DE",
                    is_available=True,
                    total_price_rub_cached=None,
                    price_rub_cached=None,
                ),
                Car(
                    id=5,
                    source_id=1,
                    external_id="5",
                    country="DE",
                    is_available=True,
                    total_price_rub_cached=70.0,
                    price_rub_cached=60.0,
                ),
            ]
        )
        db.commit()
        svc = CarsService(db)
        items, _ = svc.list_cars(sort="price_asc", page=1, page_size=10, light=True, use_fast_count=False)
        ids = [item["id"] if isinstance(item, dict) else item.id for item in items]
        assert ids == [3, 5, 2, 1, 4]

        items_desc, _ = svc.list_cars(sort="price_desc", page=1, page_size=10, light=True, use_fast_count=False)
        ids_desc = [item["id"] if isinstance(item, dict) else item.id for item in items_desc]
        assert ids_desc == [1, 2, 3, 5, 4]


def test_card_and_detail_use_same_field():
    base = Path(__file__).resolve().parents[1]
    catalog_tpl = (base / "app" / "templates" / "catalog.html").read_text(encoding="utf-8")
    detail_tpl = (base / "app" / "templates" / "car_detail.html").read_text(encoding="utf-8")
    assert "display_price_rub" in catalog_tpl
    assert "display_price_rub" in detail_tpl
