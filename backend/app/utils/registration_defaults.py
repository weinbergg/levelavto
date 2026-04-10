from __future__ import annotations

import os
from typing import Any, MutableMapping, Tuple


def get_missing_registration_default() -> Tuple[int, int]:
    raw_year = (
        os.getenv("REGISTRATION_DEFAULT_YEAR")
        or os.getenv("CALC_MISSING_REG_YEAR")
        or "2026"
    )
    raw_month = (
        os.getenv("REGISTRATION_DEFAULT_MONTH")
        or os.getenv("CALC_MISSING_REG_MONTH")
        or "1"
    )
    try:
        year = int(raw_year)
    except Exception:
        year = 2026
    try:
        month = int(raw_month)
    except Exception:
        month = 1
    if month < 1 or month > 12:
        month = 1
    return year, month


def apply_missing_registration_fallback(
    payload: MutableMapping[str, Any],
    *,
    persist_fields: bool = True,
) -> bool:
    fallback_year, fallback_month = get_missing_registration_default()

    reg_year = payload.get("registration_year")
    reg_month = payload.get("registration_month")
    missing_year = reg_year in (None, "", 0)
    missing_month = reg_month in (None, "", 0)

    if persist_fields and missing_year:
        payload["registration_year"] = fallback_year
    if persist_fields and missing_month:
        payload["registration_month"] = fallback_month

    if missing_year or missing_month:
        raw_source_payload = payload.get("source_payload")
        if isinstance(raw_source_payload, dict):
            source_payload = dict(raw_source_payload)
        else:
            source_payload = {}
        source_payload["registration_defaulted"] = True
        if missing_year:
            source_payload["registration_year_defaulted"] = True
        else:
            source_payload.pop("registration_year_defaulted", None)
        if missing_month:
            source_payload["registration_month_defaulted"] = True
        else:
            source_payload.pop("registration_month_defaulted", None)
        source_payload["registration_default_year"] = int(
            payload.get("registration_year") or fallback_year
        )
        source_payload["registration_default_month"] = int(
            payload.get("registration_month") or fallback_month
        )
        payload["source_payload"] = source_payload

    return missing_year or missing_month
