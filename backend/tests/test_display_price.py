from pathlib import Path
from datetime import datetime, timedelta

from sqlalchemy import create_engine, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Session

import backend.app.services.cars_service as cars_service_mod
from backend.app.models.source import Base, Source
from backend.app.models.car import Car
from backend.app.services.calculator_config_service import CalculatorConfigService
from backend.app.services.customs_config import get_customs_config
from backend.app.services.cars_service import CarsService
from backend.app.utils.price_utils import display_price_rub


def test_display_price_rub_priority_total():
    assert display_price_rub(100.0, 50.0) == 10_000.0


def test_display_price_rub_fallback_price():
    assert display_price_rub(None, 80.5) == 10_000.0


def test_get_fx_rates_uses_plus_four_default_when_fetch_fails(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        db.add(Source(id=1, key="mobile_de", name="Mobile.de", base_url="https://m.de", country="DE"))
        db.commit()
        svc = CarsService(db)
        svc._fx_cache = None
        svc._fx_cache_ts = None
        monkeypatch.delenv("FX_ADD_RUB", raising=False)
        monkeypatch.delenv("EURO_RATE", raising=False)
        monkeypatch.delenv("USD_RATE", raising=False)
        monkeypatch.delenv("CNY_RATE", raising=False)

        def _boom(*args, **kwargs):
            raise RuntimeError("offline")

        monkeypatch.setattr(cars_service_mod.requests, "get", _boom)
        rates = svc.get_fx_rates(allow_fetch=True) or {}
        assert rates["EUR"] == 99.0
        assert rates["USD"] == 89.0
        assert rates["CNY"] == 16.0
        assert rates["RUB"] == 1.0


def test_sort_price_asc_uses_display_price(monkeypatch):
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
        monkeypatch.setattr(svc, "_should_catalog_inline_price_refresh", lambda **kwargs: False)
        items, _ = svc.list_cars(sort="price_asc", page=1, page_size=10, light=True, use_fast_count=False)
        ids = [item["id"] if isinstance(item, dict) else item.id for item in items]
        assert ids == [3, 5, 1, 2, 4]

        items_desc, _ = svc.list_cars(sort="price_desc", page=1, page_size=10, light=True, use_fast_count=False)
        ids_desc = [item["id"] if isinstance(item, dict) else item.id for item in items_desc]
        assert ids_desc == [1, 3, 5, 2, 4]


def test_sort_price_asc_treats_source_only_prices_as_missing_by_default(monkeypatch):
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
                    price=50_000,
                    currency="EUR",
                    price_rub_cached=None,
                    total_price_rub_cached=None,
                ),
                Car(
                    id=2,
                    source_id=1,
                    external_id="2",
                    country="DE",
                    is_available=True,
                    price=55_000,
                    currency="EUR",
                    price_rub_cached=5_500_000,
                    total_price_rub_cached=None,
                ),
                Car(
                    id=3,
                    source_id=1,
                    external_id="3",
                    country="DE",
                    is_available=True,
                    price=None,
                    currency="EUR",
                    price_rub_cached=None,
                    total_price_rub_cached=None,
                ),
            ]
        )
        db.commit()
        svc = CarsService(db)
        monkeypatch.setattr(svc, "get_fx_rates", lambda allow_fetch=True: {"EUR": 100.0, "USD": 90.0, "CNY": 12.0, "RUB": 1.0})
        items, _ = svc.list_cars(sort="price_asc", page=1, page_size=10, light=True, use_fast_count=False)
        ids = [item["id"] if isinstance(item, dict) else item.id for item in items]
        assert ids == [1, 2, 3]


def test_sort_price_asc_treats_zero_price_as_missing(monkeypatch):
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
                    price=0,
                    currency="EUR",
                    price_rub_cached=0,
                    total_price_rub_cached=0,
                ),
                Car(
                    id=2,
                    source_id=1,
                    external_id="2",
                    country="DE",
                    is_available=True,
                    total_price_rub_cached=4_700_000,
                    price_rub_cached=None,
                ),
                Car(
                    id=3,
                    source_id=1,
                    external_id="3",
                    country="DE",
                    is_available=True,
                    price=55_000,
                    currency="EUR",
                    price_rub_cached=None,
                    total_price_rub_cached=None,
                ),
            ]
        )
        db.commit()
        svc = CarsService(db)
        monkeypatch.setattr(
            svc,
            "get_fx_rates",
            lambda allow_fetch=True: {"EUR": 100.0, "USD": 90.0, "CNY": 12.0, "RUB": 1.0},
        )
        items, _ = svc.list_cars(sort="price_asc", page=1, page_size=10, light=True, use_fast_count=False)
        ids = [item["id"] if isinstance(item, dict) else item.id for item in items]
        assert ids == [2, 1, 3]


def test_sort_price_asc_keeps_without_util_rows_after_moscow_prices(monkeypatch):
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
                    total_price_rub_cached=30_000,
                    price_rub_cached=29_900,
                    calc_breakdown_json=[{"title": "__without_util_fee", "amount_rub": 0}],
                ),
                Car(
                    id=2,
                    source_id=1,
                    external_id="2",
                    country="DE",
                    is_available=True,
                    total_price_rub_cached=40_000,
                    price_rub_cached=39_900,
                    calc_breakdown_json=[{"title": "Итого (RUB)", "amount_rub": 40_000}],
                ),
                Car(
                    id=3,
                    source_id=1,
                    external_id="3",
                    country="DE",
                    is_available=True,
                    total_price_rub_cached=50_000,
                    price_rub_cached=49_900,
                    calc_breakdown_json=[{"title": "Итого (RUB)", "amount_rub": 50_000}],
                ),
            ]
        )
        db.commit()
        svc = CarsService(db)
        monkeypatch.setattr(svc, "_should_catalog_inline_price_refresh", lambda **kwargs: False)
        items, _ = svc.list_cars(sort="price_asc", page=1, page_size=10, light=True, use_fast_count=False)
        ids = [item["id"] if isinstance(item, dict) else item.id for item in items]
        assert ids == [2, 3, 1]


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


def test_sync_light_rows_from_db_overwrites_stale_card_year_and_price():
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
                brand="Mercedes-Benz",
                model="GLE-Class",
                year=2025,
                registration_year=2024,
                registration_month=7,
                price=35000,
                currency="EUR",
                price_rub_cached=3_500_000,
                total_price_rub_cached=5_200_000,
                is_available=True,
            )
        )
        db.commit()
        svc = CarsService(db)
        rows = [
            {
                "id": 1,
                "brand": "Mercedes-Benz",
                "model": "GLE-Class",
                "year": 2026,
                "registration_year": 2026,
                "registration_month": 1,
                "price": 99999,
                "currency": "USD",
                "price_rub_cached": 9_999_999,
                "total_price_rub_cached": 9_999_999,
            }
        ]

        refreshed = svc.sync_light_rows_from_db(rows, refresh_prices=False)
        assert refreshed == 0
        assert rows[0]["year"] == 2025
        assert rows[0]["registration_year"] == 2024
        assert rows[0]["registration_month"] == 7
        assert float(rows[0]["price"]) == 35000.0
        assert rows[0]["currency"] == "EUR"
        assert float(rows[0]["price_rub_cached"]) == 3_500_000.0
        assert float(rows[0]["total_price_rub_cached"]) == 5_200_000.0


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


def test_maybe_infer_specs_for_calc_uses_text_engine_cc_for_clear_ice_variant():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        db.add(Source(id=1, key="mobile_de", name="Mobile.de", base_url="https://m.de", country="DE"))
        car = Car(
            id=1,
            source_id=1,
            external_id="1",
            country="NL",
            brand="Peugeot",
            model="Other",
            variant="Landtrek 1.9D NO EU/KEIN EU/T1",
            year=2024,
            registration_year=2024,
            registration_month=1,
            engine_type="Diesel",
            engine_cc=None,
            power_hp=150,
            power_kw=110.32,
            price=10_990,
            currency="EUR",
            is_available=True,
        )
        db.add(car)
        db.commit()
        svc = CarsService(db)
        applied = svc._maybe_infer_specs_for_calc(car)
        assert applied is True
        assert car.inferred_engine_cc == 1900
        assert car.inferred_rule == "text_pattern"


def test_ensure_calc_cache_uses_mokka_e_hint_for_dirty_feed_type(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        db.add(Source(id=1, key="mobile_de", name="Mobile.de", base_url="https://m.de", country="DE"))
        db.commit()

        svc = CarsService(db)
        monkeypatch.setattr(
            svc,
            "get_fx_rates",
            lambda allow_fetch=True: {"EUR": 100.0, "USD": 90.0, "CNY": 12.0},
        )
        cfg = CalculatorConfigService(db).ensure_default_from_yaml(
            Path(__file__).resolve().parents[1] / "app" / "config" / "calculator.yml"
        )
        assert cfg is not None
        car = Car(
            id=1,
            source_id=1,
            external_id="14955",
            country="DE",
            brand="Opel",
            model="Mokka-e",
            variant="Ultimate Long Range Navi Alcantara Massa",
            price=26_926.74,
            currency="EUR",
            price_rub_cached=2_692_673.93,
            total_price_rub_cached=2_692_673.93,
            calc_breakdown_json=[],
            calc_updated_at=datetime.utcnow(),
            updated_at=datetime.utcnow() - timedelta(minutes=1),
            registration_year=2025,
            registration_month=1,
            engine_type="based on co₂ emissions (combined)",
            engine_cc=None,
            power_hp=156,
            power_kw=114.74,
            is_available=True,
        )
        db.add(car)
        db.commit()

        result = svc.ensure_calc_cache(car, force=True)

        assert result is not None
        assert float(car.total_price_rub_cached) > 2_692_673.93
        assert not any(
            isinstance(row, dict) and row.get("title") == "__without_util_fee"
            for row in (car.calc_breakdown_json or [])
        )


def test_ensure_calc_cache_recalculates_stale_equal_total_for_bmw_ix(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        db.add(Source(id=1, key="mobile_de", name="Mobile.de", base_url="https://m.de", country="DE"))
        db.commit()

        svc = CarsService(db)
        monkeypatch.setattr(
            svc,
            "get_fx_rates",
            lambda allow_fetch=True: {"EUR": 100.0, "USD": 90.0, "CNY": 12.0},
        )
        cfg = CalculatorConfigService(db).ensure_default_from_yaml(
            Path(__file__).resolve().parents[1] / "app" / "config" / "calculator.yml"
        )
        assert cfg is not None
        fx_signature = svc._fx_signature({"EUR": 100.0, "USD": 90.0, "CNY": 12.0})
        breakdown = [
            {"title": "__config_version", "amount_rub": 0, "version": cfg.payload.get("meta", {}).get("version")},
            {"title": "__customs_version", "amount_rub": 0, "version": get_customs_config().version},
            {"title": "__fx_signature", "amount_rub": 0, "version": fx_signature},
        ]
        now = datetime.utcnow()
        car = Car(
            id=1,
            source_id=1,
            external_id="447996127",
            country="DE",
            brand="BMW",
            model="iX",
            variant="xDrive40",
            price=31_512,
            currency="EUR",
            price_rub_cached=3_151_200,
            total_price_rub_cached=3_151_200,
            calc_breakdown_json=breakdown,
            calc_updated_at=now,
            updated_at=now - timedelta(minutes=1),
            registration_year=2022,
            registration_month=11,
            engine_type="based on co₂ emissions (combined)",
            engine_cc=None,
            power_hp=326,
            power_kw=239.77,
            is_available=True,
        )
        db.add(car)
        db.commit()

        result = svc.ensure_calc_cache(car, force=False)

        assert result is not None
        assert float(car.total_price_rub_cached) > 3_151_200
        assert result["total_rub"] == float(car.total_price_rub_cached)
        assert not any(
            isinstance(row, dict) and row.get("title") == "__without_util_fee"
            for row in (car.calc_breakdown_json or [])
        )


def test_effective_electric_fuel_filter_sql_uses_ev_hints_and_skips_bare_ev_like():
    svc = CarsService(None)
    stmt = select(Car.id).where(svc._fuel_filter_clause("electric"))
    sql = str(
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    ).lower()
    assert "mokka e" in sql
    assert "ariya" in sql
    assert " ix " in sql
    assert "%ev%" not in sql


def test_fuel_source_expr_promotes_hint_based_evs_to_electric_bucket():
    svc = CarsService(None)
    stmt = select(svc._fuel_source_expr().label("fuel")).select_from(Car)
    sql = str(
        stmt.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    ).lower()
    assert "case when" in sql
    assert "'electric'" in sql
    assert "regexp_replace" in sql
    assert "source_url" in sql
