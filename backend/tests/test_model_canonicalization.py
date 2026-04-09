from backend.app.services.cars_service import CarsService


def test_canonical_model_label_maps_bmw_trim_titles_to_eu_model_codes():
    service = CarsService(db=None)  # type: ignore[arg-type]
    donors = ["520", "320 Gran Turismo", "X5", "X7", "220 Gran Coupé"]
    assert service._canonical_model_label("BMW", '“5 Series (G30)” 520i M Sport', donors=donors) == "520"
    assert service._canonical_model_label("BMW", "3 Series GT (F34) GT 320d", donors=donors) == "320 Gran Turismo"
    assert service._canonical_model_label("BMW", "X7 (G07) xDrive 40i M Sport 6 seater", donors=donors) == "X7"


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
