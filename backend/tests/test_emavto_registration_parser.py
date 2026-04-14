from __future__ import annotations

import pytest

pytest.importorskip("pydantic")

from backend.app.parsing.config import PaginationConfig, SiteConfig
from backend.app.parsing.emavto_klg import EmAvtoKlgParser
from backend.app.utils.registration_defaults import apply_missing_registration_fallback


def _parser() -> EmAvtoKlgParser:
    cfg = SiteConfig(
        key="emavto_klg",
        name="EmAvto",
        country="KR",
        type="html",
        base_search_url="https://example.com",
        pagination=PaginationConfig(),
        selectors={
            "item": "div.car-card",
            "title": "a.car-title",
            "details": "p.car-details",
            "price": "p.car-price",
            "link": "a.car-title",
            "image": "img",
        },
    )
    return EmAvtoKlgParser(cfg)


def test_emavto_parser_supports_month_year_and_day_month_year_registration_formats():
    parser = _parser()
    assert parser._parse_registration_value("12.2016") == (2016, 12)
    assert parser._parse_registration_value("21.12.2016") == (2016, 12)
    assert parser._parse_registration_value("2016-12-21") == (2016, 12)


def test_registration_fallback_can_mark_payload_without_overwriting_real_columns():
    payload = {
        "registration_year": None,
        "registration_month": None,
        "source_payload": {},
    }

    changed = apply_missing_registration_fallback(payload, persist_fields=False)

    assert changed is True
    assert payload["registration_year"] is None
    assert payload["registration_month"] is None
    assert payload["source_payload"]["registration_defaulted"] is True


def test_registration_fallback_marks_month_only_gap_without_year_default_flag():
    payload = {
        "registration_year": 2024,
        "registration_month": None,
        "source_payload": {},
    }

    changed = apply_missing_registration_fallback(payload, persist_fields=False)

    assert changed is True
    assert payload["registration_year"] == 2024
    assert payload["registration_month"] is None
    assert payload["source_payload"]["registration_defaulted"] is True
    assert payload["source_payload"].get("registration_year_defaulted") is None
    assert payload["source_payload"]["registration_month_defaulted"] is True


def test_emavto_parser_splits_domestic_and_import_tabs_without_mixing_cards():
    parser = _parser()
    html = """
    <html><body>
      <div id="korea-cars">
        <div class="car-card">
          <a class="car-title" href="/car/dom-1">Kia Carnival</a>
          <p class="car-details">2022 · 10000 км · Бензин</p>
          <p class="car-price">36052 $</p>
          <img src="https://example.com/dom-1.jpg" />
        </div>
      </div>
      <div id="import-cars">
        <div class="car-card">
          <a class="car-title" href="/car/imp-1">Mercedes Benz S-Class</a>
          <p class="car-details">2021 · 5000 км · Бензин</p>
          <p class="car-price">37430 $</p>
          <img src="https://example.com/imp-1.jpg" />
        </div>
      </div>
    </body></html>
    """

    class _Resp:
        def __init__(self, text: str, url: str):
            self.status_code = 200
            self.text = text
            self.url = url
            self.headers = {}

    parser._request_with_backoff = lambda url, params, bucket, is_detail, client=None, deadline=None: _Resp(  # type: ignore[assignment]
        html,
        f"{url}?{next(iter(params.keys()))}={next(iter(params.values()))}",
    )

    cars = parser.fetch_items(
        {
            "mode": "incremental",
            "resume_page_full": 1,
            "max_pages": 1,
            "skip_details": True,
            "max_runtime_sec": 30,
        }
    )

    assert len(cars) == 2
    by_market = {car.kr_market_type: car for car in cars}
    assert sorted(by_market.keys()) == ["domestic", "import"]
    assert by_market["domestic"].brand == "Kia"
    assert by_market["import"].brand == "Mercedes"
    assert by_market["domestic"].source_payload["kr_market_type"] == "domestic"
    assert by_market["import"].source_payload["kr_market_type"] == "import"
