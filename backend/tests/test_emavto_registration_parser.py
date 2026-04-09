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
