from backend.app.importing.mobilede_csv import MobileDeCsvRow
from backend.app.parsing.config import PaginationConfig, SiteConfig
from backend.app.parsing.mobile_de_feed import MobileDeFeedParser


def _parser() -> MobileDeFeedParser:
    cfg = SiteConfig(
        key="mobilede",
        name="mobile.de",
        country="DE",
        type="json",
        base_search_url="https://example.com",
        pagination=PaginationConfig(),
    )
    return MobileDeFeedParser(cfg)


def _row(**overrides) -> MobileDeCsvRow:
    data = {
        "inner_id": "1",
        "mark": "Ford",
        "model": "Mustang",
        "title": "",
        "sub_title": "",
        "url": "https://example.com/car/1",
        "price_eur": None,
        "price_eur_nt": None,
        "vat": None,
        "year": 2022,
        "km_age": None,
        "color": None,
        "owners_count": None,
        "section": None,
        "address": None,
        "options": [],
        "engine_type": None,
        "displacement": None,
        "displacement_orig": None,
        "horse_power": None,
        "power_kw": None,
        "body_type": None,
        "transmission": None,
        "full_fuel_type": None,
        "fuel_consumption": None,
        "co_emission": None,
        "num_seats": None,
        "doors_count": None,
        "emission_class": None,
        "emissions_sticker": None,
        "climatisation": None,
        "park_assists": None,
        "airbags": None,
        "manufacturer_color": None,
        "interior_design": None,
        "efficiency_class": None,
        "first_registration": None,
        "ready_to_drive": None,
        "price_rating_label": None,
        "seller_country": "DE",
        "created_at": None,
        "envkv_engine_type": None,
        "envkv_energy_consumption": None,
        "envkv_co2_emissions": None,
        "envkv_co2_class": None,
        "envkv_co2_class_value": None,
        "envkv_consumption_fuel": None,
        "features": [],
        "description": None,
        "image_urls": [],
    }
    data.update(overrides)
    return MobileDeCsvRow(**data)


def test_resolve_power_from_kw_text():
    parser = _parser()
    row = _row(sub_title="Opel Corsa-e 100 kW GS")
    assert parser._resolve_power_kw(row) == 100.0
    assert parser._resolve_power_hp(row) == 136.0


def test_resolve_power_from_hp_text():
    parser = _parser()
    row = _row(sub_title="FORD Mustang mach-e standard range awd 269cv aut")
    assert parser._resolve_power_hp(row) == 269.0


def test_iter_parsed_nulls_dirty_engine_cc_for_electric():
    parser = _parser()
    row = _row(
        sub_title="Citroen C-Zero Full Electric Seduction 35 kW",
        displacement_orig=10,
        envkv_consumption_fuel="Electric",
    )
    parsed = next(parser.iter_parsed_from_csv([row]))
    assert parsed.engine_type == "Electric"
    assert parsed.engine_cc is None
    assert parsed.power_kw == 35.0


def test_iter_parsed_recovers_mercedes_model_from_other_placeholder():
    parser = _parser()
    row = _row(
        mark="Mercedes-Benz",
        model="Other",
        title="Mercedes-Benz B 180 d",
        sub_title="Mercedes Benz B-Klasse 170 NGT",
    )
    parsed = next(parser.iter_parsed_from_csv([row]))
    assert parsed.model == "B-Class"


def test_iter_parsed_recovers_mercedes_vito_from_other_placeholder():
    parser = _parser()
    row = _row(
        mark="Mercedes-Benz",
        model="Other",
        title="Mercedes-Benz Vito Tourer",
        sub_title="Mercedes Vito Tourer 114 CDI extralang",
    )
    parsed = next(parser.iter_parsed_from_csv([row]))
    assert parsed.model == "Vito"
