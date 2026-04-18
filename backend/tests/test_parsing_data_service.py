from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("sqlalchemy")

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from backend.app.models.source import Base, Source
from backend.app.models.car import Car
from backend.app.models.car_image import CarImage
from backend.app.services import cars_service as cars_service_mod
from backend.app.services import calculator_config_service as cfg_mod
from backend.app.services.parsing_data_service import ParsingDataService


def test_upsert_parsed_items_auto_recalculates_korea_prices(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        source = Source(id=1, key="emavto_klg", name="EMAVTO", base_url="https://emavto.ru", country="KR")
        db.add(source)
        db.commit()

        monkeypatch.setattr(cars_service_mod.CarsService, "get_fx_rates", lambda self, allow_fetch=True: {"USD": 100.0, "EUR": 100.0, "CNY": 12.0, "RUB": 1.0})
        monkeypatch.setattr(cars_service_mod.CarsService, "_maybe_infer_specs_for_calc", lambda self, car: False)
        monkeypatch.setattr(
            cfg_mod.CalculatorConfigService,
            "ensure_default_from_yaml",
            lambda self, path: SimpleNamespace(payload={"meta": {"version": "test"}}),
        )
        monkeypatch.setattr(
            cfg_mod.CalculatorConfigService,
            "ensure_default_from_path",
            lambda self, path: SimpleNamespace(payload={"meta": {"version": "test"}}),
        )

        service = ParsingDataService(db)
        inserted, updated, seen = service.upsert_parsed_items(
            source,
            [
                {
                    "external_id": "kr-1",
                    "country": "KR",
                    "brand": "BMW",
                    "model": "X5",
                    "price": 10000,
                    "currency": "USD",
                    "engine_type": "Diesel",
                    "engine_cc": 1995,
                    "power_hp": 286,
                    "source_url": "https://emavto.ru/car/kr-1",
                    "source_payload": {"foo": "bar"},
                }
            ],
        )
        assert (inserted, updated, seen) == (1, 0, 1)

        car = db.query(Car).filter(Car.external_id == "kr-1").one()
        assert float(car.total_price_rub_cached or 0) > float(car.price_rub_cached or 0)
        assert any(isinstance(row, dict) and row.get("title") == "Комиссия 3%" for row in (car.calc_breakdown_json or []))


def test_upsert_parsed_items_does_not_rewrite_identical_images(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        source = Source(id=1, key="mobile_de", name="mobile.de", base_url="csv://mobile_de", country="DE")
        db.add(source)
        db.commit()

        monkeypatch.setattr(
            cars_service_mod.CarsService,
            "get_fx_rates",
            lambda self, allow_fetch=True: {"EUR": 100.0, "RUB": 1.0},
        )

        service = ParsingDataService(db)
        payload = {
            "external_id": "de-1",
            "country": "DE",
            "brand": "Mercedes-Benz",
            "model": "V-Class",
            "price": 55000,
            "currency": "EUR",
            "source_url": "https://example.test/cars/de-1",
            "images": [
                "https://img.example.test/1.jpg",
                "https://img.example.test/2.jpg",
            ],
        }

        inserted, updated, seen = service.upsert_parsed_items(source, [dict(payload)])
        assert (inserted, updated, seen) == (1, 0, 1)

        first_images = db.query(CarImage).order_by(CarImage.position.asc()).all()
        first_ids = [img.id for img in first_images]
        assert [img.url for img in first_images] == payload["images"]

        inserted, updated, seen = service.upsert_parsed_items(source, [dict(payload)])
        assert (inserted, updated, seen) == (0, 0, 1)

        second_images = db.query(CarImage).order_by(CarImage.position.asc()).all()
        assert [img.id for img in second_images] == first_ids
        assert [img.url for img in second_images] == payload["images"]


def test_upsert_parsed_items_keeps_mirrored_local_images_without_rewrite(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        source = Source(id=1, key="mobile_de", name="mobile.de", base_url="csv://mobile_de", country="DE")
        db.add(source)
        db.commit()

        monkeypatch.setattr(
            cars_service_mod.CarsService,
            "get_fx_rates",
            lambda self, allow_fetch=True: {"EUR": 100.0, "RUB": 1.0},
        )

        service = ParsingDataService(db)
        payload = {
            "external_id": "de-2",
            "country": "DE",
            "brand": "Mercedes-Benz",
            "model": "V-Class",
            "price": 56000,
            "currency": "EUR",
            "source_url": "https://example.test/cars/de-2",
            "images": [
                "https://img.example.test/original-1.jpg",
                "https://img.example.test/original-2.jpg",
            ],
        }

        service.upsert_parsed_items(source, [dict(payload)])
        car = db.query(Car).filter(Car.external_id == "de-2").one()
        old_images = db.query(CarImage).filter(CarImage.car_id == car.id).order_by(CarImage.position.asc()).all()
        old_images[0].url = "/media/de-2-1.webp"
        old_images[1].url = "/media/de-2-2.webp"
        db.commit()
        preserved_ids = [img.id for img in old_images]

        inserted, updated, seen = service.upsert_parsed_items(source, [dict(payload)])
        assert (inserted, updated, seen) == (0, 0, 1)

        new_images = db.query(CarImage).filter(CarImage.car_id == car.id).order_by(CarImage.position.asc()).all()
        assert [img.id for img in new_images] == preserved_ids
        assert [img.url for img in new_images] == ["/media/de-2-1.webp", "/media/de-2-2.webp"]
