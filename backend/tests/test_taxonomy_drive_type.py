from backend.app.utils.taxonomy import ru_drivetrain


def test_ru_drivetrain_maps_known_values():
    assert ru_drivetrain("AWD") == "Полный"
    assert ru_drivetrain("FWD") == "Передний"

