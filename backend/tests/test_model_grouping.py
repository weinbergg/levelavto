from backend.app.services.cars_service import CarsService


def test_model_grouping_bmw_series():
    service = CarsService(db=None)  # type: ignore[arg-type]
    models = [
        {"value": "X5", "label": "X5", "count": 10},
        {"value": "X6", "label": "X6", "count": 8},
        {"value": "320d", "label": "320d", "count": 7},
        {"value": "M5", "label": "M5", "count": 5},
        {"value": "iX", "label": "iX", "count": 4},
    ]
    groups = service.build_model_groups(brand="BMW", models=models)
    labels = {g["label"] for g in groups}
    assert "X серия" in labels
    x_group = next(g for g in groups if g["label"] == "X серия")
    assert {m["value"] for m in x_group["models"]} == {"X5", "X6"}


def test_model_grouping_generic_prefix():
    service = CarsService(db=None)  # type: ignore[arg-type]
    models = [
        {"value": "A6", "label": "A6", "count": 12},
        {"value": "A4", "label": "A4", "count": 6},
        {"value": "Q5", "label": "Q5", "count": 9},
    ]
    groups = service.build_model_groups(brand="Audi", models=models)
    labels = [g["label"] for g in groups]
    assert "A" in labels
    assert "Q" in labels
