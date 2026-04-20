import pytest

pytest.importorskip("sqlalchemy")

from backend.app.services.cars_service import CarsService


def test_model_grouping_bmw_series():
    service = CarsService(db=None)  # type: ignore[arg-type]
    models = [
        {"value": "X5", "label": "X5", "count": 10},
        {"value": "X6", "label": "X6", "count": 8},
        {"value": "320d", "label": "320d", "count": 7},
        {"value": "330e", "label": "330e", "count": 6},
        {"value": "M5", "label": "M5", "count": 5},
        {"value": "iX", "label": "iX", "count": 4},
    ]
    groups = service.build_model_groups(brand="BMW", models=models)
    labels = {g["label"] for g in groups}
    assert "X серия" in labels
    assert "3 серия" in labels
    x_group = next(g for g in groups if g["label"] == "X серия")
    assert {m["value"] for m in x_group["models"]} == {"X5", "X6"}
    three_group = next(g for g in groups if g["label"] == "3 серия")
    assert {m["value"] for m in three_group["models"]} == {"320d", "330e"}


def test_model_grouping_generic_uses_exact_family_token():
    service = CarsService(db=None)  # type: ignore[arg-type]
    models = [
        {"value": "A6", "label": "A6", "count": 12},
        {"value": "A4", "label": "A4", "count": 6},
        {"value": "Q5", "label": "Q5", "count": 9},
    ]
    groups = service.build_model_groups(brand="Audi", models=models)
    labels = [g["label"] for g in groups]
    assert "A4" in labels
    assert "A6" in labels
    assert "Q5" in labels


def test_model_grouping_porsche_911_series():
    service = CarsService(db=None)  # type: ignore[arg-type]
    models = [
        {"value": "911", "label": "911", "count": 12},
        {"value": "930", "label": "930", "count": 3},
        {"value": "964", "label": "964", "count": 2},
        {"value": "991", "label": "991", "count": 5},
        {"value": "992", "label": "992", "count": 4},
        {"value": "Cayenne", "label": "Cayenne", "count": 7},
    ]
    groups = service.build_model_groups(brand="Porsche", models=models)
    labels = {g["label"] for g in groups}
    assert "Series 911" in labels
    family = next(g for g in groups if g["label"] == "Series 911")
    assert {m["value"] for m in family["models"]} == {"911", "930", "964", "991", "992"}


def test_model_grouping_mercedes_class_family_merges_coupe_variants():
    service = CarsService(db=None)  # type: ignore[arg-type]
    models = [
        {"value": "GLE-Class", "label": "GLE-Class", "count": 12},
        {"value": "GLE Coupe", "label": "GLE Coupe", "count": 5},
        {"value": "GLC-Class", "label": "GLC-Class", "count": 9},
    ]
    groups = service.build_model_groups(brand="Mercedes-Benz", models=models)
    gle_group = next(g for g in groups if g["label"] == "GLE-Class")
    assert {m["value"] for m in gle_group["models"]} == {"GLE-Class", "GLE Coupe"}


def test_model_grouping_mercedes_keeps_vito_separate_from_v_class_family():
    service = CarsService(db=None)  # type: ignore[arg-type]
    models = [
        {"value": "V-Class", "label": "V-Class", "count": 12},
        {"value": "V 250", "label": "V 250", "count": 5},
        {"value": "Vito", "label": "Vito", "count": 9},
    ]
    groups = service.build_model_groups(brand="Mercedes-Benz", models=models)
    v_class_group = next(g for g in groups if g["label"] == "V-Class")
    vito_group = next(g for g in groups if g["label"] == "Vito")
    assert {m["value"] for m in v_class_group["models"]} == {"V-Class", "V 250"}
    assert {m["value"] for m in vito_group["models"]} == {"Vito"}


def test_model_grouping_bentley_hp_variants_keep_base_family_label():
    service = CarsService(db=None)  # type: ignore[arg-type]
    models = [
        {"value": "Bentayga@@hp550", "label": "Bentayga 550 л.с.", "count": 5, "base_model": "Bentayga"},
        {"value": "Bentayga@@hp635", "label": "Bentayga 635 л.с.", "count": 3, "base_model": "Bentayga"},
        {"value": "Continental GT@@hp550", "label": "Continental GT 550 л.с.", "count": 4, "base_model": "Continental GT"},
    ]
    groups = service.build_model_groups(brand="Bentley", models=models)
    bentayga_group = next(g for g in groups if g["label"] == "Bentayga")
    assert {m["value"] for m in bentayga_group["models"]} == {"Bentayga@@hp550", "Bentayga@@hp635"}
