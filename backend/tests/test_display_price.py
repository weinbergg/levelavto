from pathlib import Path
from datetime import datetime, timedelta

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


def test_light_list_refreshes_stale_card_prices(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        db.add(Source(id=1, key="mobile_de", name="Mobile.de", base_url="https://m.de", country="DE"))
        db.add(
            Car(
                id=1,
                source_id=1,
                external_id="1",
                country="DE",
                brand="BMW",
                model="X5",
                price_rub_cached=5_000_000,
                total_price_rub_cached=4_000_000,
                calc_breakdown_json=[{"title": "__without_util_fee", "amount_rub": 0}],
                calc_updated_at=datetime.utcnow() - timedelta(days=2),
                updated_at=datetime.utcnow() - timedelta(days=1),
                is_available=True,
            )
        )
        db.commit()
        svc = CarsService(db)
        monkeypatch.setattr(svc, "_load_lazy_recalc_versions", lambda: (True, None, None, None))

        def fake_ensure_calc_cache(car, *, force=False):
            car.total_price_rub_cached = 6_500_000
            car.calc_breakdown_json = [{"title": "calc", "amount_rub": 6_500_000}]
            car.calc_updated_at = datetime.utcnow()
            db.commit()
            return {"total_rub": 6_500_000, "breakdown": car.calc_breakdown_json}

        monkeypatch.setattr(svc, "ensure_calc_cache", fake_ensure_calc_cache)
        items, _ = svc.list_cars(page=1, page_size=10, light=True, use_fast_count=False)
        assert items[0]["total_price_rub_cached"] == 6_500_000
        assert items[0]["calc_breakdown_json"] == [{"title": "calc", "amount_rub": 6_500_000}]


def test_refresh_visible_price_cache_updates_visible_foreign_cards_even_when_lazy_disabled(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        db.add(Source(id=1, key="mobile_de", name="Mobile.de", base_url="https://m.de", country="DE"))
        db.add(
            Car(
                id=1,
                source_id=1,
                external_id="1",
                country="DE",
                brand="BMW",
                model="X5",
                engine_type="Diesel",
                engine_cc=2993,
                power_hp=286,
                total_price_rub_cached=4_100_000,
                price_rub_cached=3_700_000,
                calc_breakdown_json=[{"title": "__without_util_fee", "amount_rub": 0}],
                calc_updated_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                is_available=True,
            )
        )
        db.commit()
        svc = CarsService(db)
        monkeypatch.setattr(svc, "_load_lazy_recalc_versions", lambda: (False, None, None, None))

        def fake_ensure_calc_cache(car, *, force=False):
            car.total_price_rub_cached = 6_800_000
            car.calc_breakdown_json = [{"title": "Итого (RUB)", "amount_rub": 6_800_000}]
            car.calc_updated_at = datetime.utcnow()
            db.commit()
            return {"total_rub": 6_800_000, "breakdown": car.calc_breakdown_json}

        monkeypatch.setattr(svc, "ensure_calc_cache", fake_ensure_calc_cache)
        rows = [
            {
                "id": 1,
                "country": "DE",
                "engine_type": "Diesel",
                "engine_cc": 2993,
                "power_hp": 286,
                "total_price_rub_cached": 4_100_000,
                "price_rub_cached": 3_700_000,
                "calc_breakdown_json": [{"title": "__without_util_fee", "amount_rub": 0}],
                "calc_updated_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            }
        ]

        refreshed = svc.refresh_visible_price_cache(rows)
        assert refreshed == 1
        assert rows[0]["total_price_rub_cached"] == 6_800_000
        assert rows[0]["calc_breakdown_json"] == [{"title": "Итого (RUB)", "amount_rub": 6_800_000}]


def test_needs_recalc_when_fx_signature_changes():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        db.add(Source(id=1, key="mobile_de", name="Mobile.de", base_url="https://m.de", country="DE"))
        car = Car(
            id=1,
            source_id=1,
            external_id="1",
            country="DE",
            is_available=True,
            total_price_rub_cached=5_000_000,
            price_rub_cached=4_000_000,
            calc_breakdown_json=[
                {"title": "__config_version", "amount_rub": 0, "version": "cfg:v1"},
                {"title": "__customs_version", "amount_rub": 0, "version": "customs:v1"},
                {"title": "__fx_signature", "amount_rub": 0, "version": "eur:98.0000|usd:90.0000"},
            ],
            calc_updated_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        db.add(car)
        db.commit()
        svc = CarsService(db)
        assert svc._needs_recalc_for_versions(
            car,
            "cfg:v1",
            "customs:v1",
            "eur:99.0000|usd:91.0000",
            lazy_enabled=True,
        ) is True


def test_light_list_exposes_effective_inferred_specs():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        db.add(Source(id=1, key="mobile_de", name="Mobile.de", base_url="https://m.de", country="DE"))
        db.add(
            Car(
                id=1,
                source_id=1,
                external_id="1",
                country="DE",
                brand="BMW",
                model="X5",
                year=2025,
                engine_type="Diesel",
                engine_cc=None,
                inferred_engine_cc=2993,
                power_hp=None,
                inferred_power_hp=352,
                inferred_power_kw=258.9,
                is_available=True,
            )
        )
        db.commit()
        svc = CarsService(db)
        items, _ = svc.list_cars(page=1, page_size=10, light=True, use_fast_count=False)
        assert items[0]["engine_cc"] == 2993
        assert float(items[0]["power_hp"]) == 352.0
        assert float(items[0]["power_kw"]) == 258.9


def test_maybe_infer_specs_for_calc_applies_inference(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        db.add(Source(id=1, key="mobile_de", name="Mobile.de", base_url="https://m.de", country="DE"))
        car = Car(
            id=1,
            source_id=1,
            external_id="1",
            country="DE",
            brand="BMW",
            model="X5",
            year=2025,
            engine_type="Diesel",
            engine_cc=None,
            power_hp=None,
            power_kw=None,
            is_available=True,
        )
        db.add(car)
        db.commit()
        from backend.app.services import car_spec_inference_service as infer_mod

        monkeypatch.setattr(
            infer_mod.CarSpecInferenceService,
            "infer_specs_for_car",
            lambda self, car_obj, year_window=2: {
                "engine_cc": 2993,
                "power_hp": 352.0,
                "power_kw": 258.9,
                "source_car_id": 999,
                "confidence": "high",
                "rule": "variant_exact_year_exact",
            },
        )
        svc = CarsService(db)
        applied = svc._maybe_infer_specs_for_calc(car)
        assert applied is True
        assert car.inferred_engine_cc == 2993
        assert float(car.inferred_power_hp) == 352.0
        assert float(car.inferred_power_kw) == 258.9
