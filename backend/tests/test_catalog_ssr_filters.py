from starlette.applications import Starlette
from starlette.requests import Request

from backend.app.routers import pages


class _Templates:
    def __init__(self) -> None:
        self.name = None
        self.context = None

    def TemplateResponse(self, name, context):
        self.name = name
        self.context = context
        return context


def _request(query_string: str = "") -> Request:
    app = Starlette()
    app.state.templates = _Templates()
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/catalog",
        "raw_path": b"/catalog",
        "query_string": query_string.encode("utf-8"),
        "headers": [],
        "client": ("testclient", 50000),
        "server": ("testserver", 80),
        "app": app,
    }
    return Request(scope)


def test_catalog_ssr_uses_full_filter_set(monkeypatch):
    captured = {}

    class DummyCarsService:
        def __init__(self, db):
            self.db = db

        def facet_counts(self, field, filters):
            if field == "region":
                return [{"value": "EU"}]
            if field == "country":
                return [{"value": "DE"}]
            return []

        def list_cars(self, **kwargs):
            captured.update(kwargs)
            return [], 0

        def get_fx_rates(self, allow_fetch=False):
            return {}

        def has_air_suspension(self):
            return True

    class DummyContentService:
        def __init__(self, db):
            self.db = db

        def content_map(self, keys):
            return {}

    monkeypatch.setattr(pages, "CarsService", DummyCarsService)
    monkeypatch.setattr(pages, "ContentService", DummyContentService)

    request = _request(
        "region=eu&country=de&brand=mercedes%20benz&model=GLE&power_hp_min=250&power_hp_max=400"
        "&engine_cc_min=2000&engine_cc_max=3500&year_min=2021&year_max=2024&num_seats=5"
        "&doors_count=4&emission_class=euro6&efficiency_class=A&climatisation=automatic"
        "&airbags=front&interior_design=full_leather&interior_color=black&interior_material=leather"
        "&vat_reclaimable=1&air_suspension=1&price_rating_label=good_price&owners_count=1"
        "&source=mobile_de&page=3"
    )

    pages.catalog_page(request, db=object(), user=None)

    assert captured["region"] == "EU"
    assert captured["country"] == "DE"
    assert captured["brand"] == "Mercedes-Benz"
    assert captured["model"] == "GLE"
    assert captured["power_hp_min"] == 250.0
    assert captured["power_hp_max"] == 400.0
    assert captured["engine_cc_min"] == 2000
    assert captured["engine_cc_max"] == 3500
    assert captured["year_min"] == 2021
    assert captured["year_max"] == 2024
    assert captured["num_seats"] == "5"
    assert captured["doors_count"] == "4"
    assert captured["emission_class"] == "euro6"
    assert captured["efficiency_class"] == "A"
    assert captured["climatisation"] == "automatic"
    assert captured["airbags"] == "front"
    assert captured["interior_design"] == "full_leather"
    assert captured["interior_color"] == "black"
    assert captured["interior_material"] == "leather"
    assert captured["vat_reclaimable"] == "1"
    assert captured["air_suspension"] is True
    assert captured["price_rating_label"] == "good_price"
    assert captured["owners_count"] == "1"
    assert captured["source_key"] == ["mobile_de"]
    assert captured["page"] == 3
    assert captured["page_size"] == 12
