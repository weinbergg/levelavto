from __future__ import annotations

import os
from typing import Any, MutableMapping, Tuple


def get_missing_registration_default() -> Tuple[int, int]:
    raw_year = (
        os.getenv("REGISTRATION_DEFAULT_YEAR")
        or os.getenv("CALC_MISSING_REG_YEAR")
        or "2025"
    )
    raw_month = (
        os.getenv("REGISTRATION_DEFAULT_MONTH")
        or os.getenv("CALC_MISSING_REG_MONTH")
        or "1"
    )
    try:
        year = int(raw_year)
    except Exception:
        year = 2025
    try:
        month = int(raw_month)
    except Exception:
        month = 1
    if month < 1 or month > 12:
        month = 1
    return year, month


def apply_missing_registration_fallback(payload: MutableMapping[str, Any]) -> bool:
    fallback_year, fallback_month = get_missing_registration_default()
    changed = False

    reg_year = payload.get("registration_year")
    reg_month = payload.get("registration_month")

    if reg_year in (None, "", 0):
        payload["registration_year"] = fallback_year
        changed = True
    if reg_month in (None, "", 0):
        payload["registration_month"] = fallback_month
        changed = True

    if changed:
        raw_source_payload = payload.get("source_payload")
        if isinstance(raw_source_payload, dict):
            source_payload = dict(raw_source_payload)
        else:
            source_payload = {}
        source_payload["registration_defaulted"] = True
        source_payload["registration_default_year"] = int(payload["registration_year"])
        source_payload["registration_default_month"] = int(payload["registration_month"])
        payload["source_payload"] = source_payload

    return changed
