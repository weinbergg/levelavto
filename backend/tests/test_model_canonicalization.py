import pytest

pytest.importorskip("sqlalchemy")

from backend.app.services.cars_service import CarsService


def test_canonical_model_label_maps_bmw_trim_titles_to_eu_model_codes():
    service = CarsService(db=None)  # type: ignore[arg-type]
    donors = ["520", "320 Gran Turismo", "X5", "X7", "220 Gran Coupé"]
    assert service._canonical_model_label("BMW", '“5 Series (G30)” 520i M Sport', donors=donors) == "520"
    assert service._canonical_model_label("BMW", "3 Series GT (F34) GT 320d", donors=donors) == "320 Gran Turismo"
    assert service._canonical_model_label("BMW", "X7 (G07) xDrive 40i M Sport 6 seater", donors=donors) == "X7"


def test_canonical_model_label_keeps_bmw_x5m_separate_from_x5():
    service = CarsService(db=None)  # type: ignore[arg-type]
    donors = ["X5", "X5 M", "X6"]
    assert service._canonical_model_label("BMW", "BMW X5M Competition", donors=donors) == "X5 M"


def test_canonical_model_label_maps_generic_kr_titles_to_eu_donors():
    service = CarsService(db=None)  # type: ignore[arg-type]
    kia_donors = ["Sorento", "Carnival", "Sportage"]
    mercedes_donors = ["S-Class", "E-Class", "GLE", "CLS"]
    assert service._canonical_model_label("Kia", "Sorento 4th Generation HEV 1.6 2WD", donors=kia_donors) == "Sorento"
    assert service._canonical_model_label("Kia", "Carnival 4th Generation Nine-seater Prestige", donors=kia_donors) == "Carnival"
    assert service._canonical_model_label("Mercedes-Benz", "Mercedes-Benz S 450 4MATIC Long", donors=mercedes_donors) == "S-Class"
    assert service._canonical_model_label("Mercedes-Benz", "Benz E-Class W213 E350 4MATIC AMG Line", donors=mercedes_donors) == "E-Class"


def test_canonical_model_label_has_short_safe_fallback_without_donors():
    service = CarsService(db=None)  # type: ignore[arg-type]
    assert service._canonical_model_label("Genesis", "GV80 2.5 T Gasoline AWD", donors=[]) == "GV80"
    assert service._canonical_model_label("Hyundai", "The New Granger IG 2.5", donors=[]) == "Granger"


def test_canonical_model_label_merges_porsche_i_typos_into_eu_models():
    service = CarsService(db=None)  # type: ignore[arg-type]
    donors = ["Taycan", "Cayman", "Cayenne"]
    assert service._canonical_model_label("Porsche", "Taican Turbo S", donors=donors) == "Taycan"
    assert service._canonical_model_label("Porsche", "Caiman 718 GTS", donors=donors) == "Cayman"


def test_model_grouping_bmw_normalizes_series_family_labels():
    service = CarsService(db=None)  # type: ignore[arg-type]
    groups = service.build_model_groups(
        brand="BMW",
        models=[
            {"value": "7 серия", "label": "7 серия", "count": 10},
            {"value": "7", "label": "7", "count": 3},
            {"value": "520", "label": "520", "count": 4},
        ],
    )
    labels = {group["label"] for group in groups}
    assert "7 серия" in labels
    family = next(group for group in groups if group["label"] == "7 серия")
    assert {item["value"] for item in family["models"]} == {"7 серия", "7"}


def test_resolve_model_aliases_expands_family_label_to_group_members(monkeypatch):
    service = CarsService(db=object())  # type: ignore[arg-type]

    def fake_models_for_brand_filtered(**kwargs):
        return [
            {"value": "Cayenne", "label": "Cayenne", "count": 12, "aliases": ["Cayenne"]},
            {"value": "Cayenne Coupe", "label": "Cayenne Coupe", "count": 5, "aliases": ["Cayenne Coupe"]},
            {"value": "Macan", "label": "Macan", "count": 8, "aliases": ["Macan"]},
        ]

    monkeypatch.setattr(service, "models_for_brand_filtered", fake_models_for_brand_filtered)

    assert set(service._resolve_model_aliases(region="EU", brand="Porsche", model="Cayenne")) == {
        "Cayenne",
        "Cayenne Coupe",
    }
    assert service._resolve_model_aliases(region="EU", brand="Porsche", model="Cayenne Coupe") == [
        "Cayenne Coupe"
    ]
