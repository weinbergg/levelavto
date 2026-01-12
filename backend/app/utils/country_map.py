from __future__ import annotations

from typing import Optional, Tuple
import re


COUNTRY_LABELS_RU = {
    "DE": "Германия",
    "IT": "Италия",
    "NL": "Нидерланды",
    "FR": "Франция",
    "ES": "Испания",
    "BE": "Бельгия",
    "AT": "Австрия",
    "CH": "Швейцария",
    "PL": "Польша",
    "CZ": "Чехия",
    "SE": "Швеция",
    "NO": "Норвегия",
    "DK": "Дания",
    "FI": "Финляндия",
    "HU": "Венгрия",
    "RO": "Румыния",
    "BG": "Болгария",
    "PT": "Португалия",
    "GR": "Греция",
    "IE": "Ирландия",
    "GB": "Великобритания",
    "LU": "Люксембург",
    "SI": "Словения",
    "SK": "Словакия",
    "HR": "Хорватия",
    "EE": "Эстония",
    "LV": "Латвия",
    "LT": "Литва",
    "MT": "Мальта",
    "CY": "Кипр",
    "IS": "Исландия",
    "LI": "Лихтенштейн",
    "MC": "Монако",
    "SM": "Сан-Марино",
    "AD": "Андорра",
    "KR": "Корея",
    "EU": "Европа",
    "RU": "Россия",
}

_ALIASES = {
    "UK": "GB",
}

_KOREA_SOURCE_HINTS = ("emavto", "encar", "m-auto", "m_auto")


def normalize_country_code(value: str | None) -> Optional[str]:
    if not value:
        return None
    raw = value.strip().upper()
    if not raw:
        return None
    match = re.search(r"[A-Z]{2}", raw)
    if not match:
        return None
    code = match.group(0)
    return _ALIASES.get(code, code)


def country_label_ru(code: str | None) -> str:
    if not code:
        return ""
    upper = code.strip().upper()
    if not upper:
        return ""
    return COUNTRY_LABELS_RU.get(upper, upper)


def resolve_display_country(car, source_key: str | None = None) -> Tuple[Optional[str], str]:
    key = source_key
    if not key:
        src = getattr(car, "source", None)
        key = getattr(src, "key", None) if src else None
    if key:
        lower = key.lower()
        if any(hint in lower for hint in _KOREA_SOURCE_HINTS):
            return "KR", country_label_ru("KR")
    raw_code = (
        getattr(car, "country", None)
        or getattr(car, "seller_country_code", None)
        or getattr(car, "seller_country", None)
    )
    code = normalize_country_code(raw_code)
    if code:
        return code, country_label_ru(code)
    return None, "Европа"
