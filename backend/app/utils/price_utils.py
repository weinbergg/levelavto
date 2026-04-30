import math
import os
from typing import Any, Optional

PRICE_NOTE_WITHOUT_UTIL = "*Без учета утилизационного сбора РФ"
PRICE_NOTE_EUROPE = "Цена в Европе"
PRICE_NOTE_CHINA = "Цена в Китае"
PRICE_NOTE_MOSCOW = "Цена в Москве"


def get_round_step_rub() -> int:
    try:
        step = int(os.getenv("PRICE_ROUND_STEP_RUB", "10000"))
        return max(1, step)
    except Exception:
        return 10000


def ceil_to_step(value: Optional[float], step: int) -> Optional[float]:
    if value is None:
        return None
    step_f = float(step)
    return float(math.ceil(float(value) / step_f) * step_f)


def display_price_rub(
    total_price_rub_cached: Optional[float],
    price_rub_cached: Optional[float],
    *,
    allow_price_fallback: bool = True,
) -> Optional[float]:
    # Display should use total (calculator) price when available and be rounded for UI.
    raw = None
    if total_price_rub_cached is not None:
        raw = float(total_price_rub_cached)
    elif allow_price_fallback and price_rub_cached is not None:
        raw = float(price_rub_cached)
    if raw is None or raw <= 0:
        return None
    return ceil_to_step(raw, get_round_step_rub())


def raw_price_to_rub(
    raw_price: Optional[float],
    currency: Optional[str],
    *,
    fx_eur: Optional[float] = None,
    fx_usd: Optional[float] = None,
    fx_cny: Optional[float] = None,
) -> Optional[float]:
    if raw_price is None:
        return None
    try:
        amount = float(raw_price)
    except (TypeError, ValueError):
        return None
    if amount <= 0:
        return None
    cur = str(currency or "").strip().upper()
    if cur == "EUR" and fx_eur and fx_eur > 0:
        return amount * float(fx_eur)
    if cur == "USD" and fx_usd and fx_usd > 0:
        return amount * float(fx_usd)
    if cur == "CNY" and fx_cny and fx_cny > 0:
        return amount * float(fx_cny)
    if cur in {"RUB", "₽"}:
        return amount
    return None


def resolve_display_price_rub(
    total_price_rub_cached: Optional[float],
    price_rub_cached: Optional[float],
    *,
    raw_price: Optional[float] = None,
    currency: Optional[str] = None,
    fx_eur: Optional[float] = None,
    fx_usd: Optional[float] = None,
    fx_cny: Optional[float] = None,
    allow_price_fallback: bool = True,
    allow_raw_price_fallback: bool = True,
) -> Optional[float]:
    display = display_price_rub(
        total_price_rub_cached,
        price_rub_cached,
        allow_price_fallback=allow_price_fallback,
    )
    if display is not None or not allow_raw_price_fallback:
        return display
    raw_rub = raw_price_to_rub(
        raw_price,
        currency,
        fx_eur=fx_eur,
        fx_usd=fx_usd,
        fx_cny=fx_cny,
    )
    if raw_rub is None:
        return None
    return display_price_rub(None, raw_rub, allow_price_fallback=True)


def has_without_util_marker(calc_breakdown: Optional[list]) -> bool:
    return any(
        isinstance(row, dict) and row.get("title") == "__without_util_fee"
        for row in (calc_breakdown or [])
    )


def public_price_fallback_enabled() -> bool:
    return os.getenv("PUBLIC_PRICE_ALLOW_SOURCE_FALLBACK", "0") == "1"


def public_price_allow_without_util() -> bool:
    return os.getenv("PUBLIC_PRICE_ALLOW_WITHOUT_UTIL", "0") == "1"


def resolve_public_display_price_rub(
    total_price_rub_cached: Optional[float],
    price_rub_cached: Optional[float],
    *,
    calc_breakdown: Optional[list] = None,
    raw_price: Optional[float] = None,
    currency: Optional[str] = None,
    fx_eur: Optional[float] = None,
    fx_usd: Optional[float] = None,
    fx_cny: Optional[float] = None,
) -> Optional[float]:
    if has_without_util_marker(calc_breakdown) and not public_price_allow_without_util():
        return None
    allow_fallback = public_price_fallback_enabled()
    return resolve_display_price_rub(
        total_price_rub_cached,
        price_rub_cached,
        raw_price=raw_price,
        currency=currency,
        fx_eur=fx_eur,
        fx_usd=fx_usd,
        fx_cny=fx_cny,
        allow_price_fallback=allow_fallback,
        allow_raw_price_fallback=allow_fallback,
    )


def sort_items_by_display_price(items: list[Any], *, sort: Optional[str]) -> list[Any]:
    if sort not in {"price_asc", "price_desc"}:
        return items

    descending = sort == "price_desc"

    def _value(item: Any) -> Optional[float]:
        raw = item.get("display_price_rub") if isinstance(item, dict) else getattr(item, "display_price_rub", None)
        try:
            return float(raw) if raw is not None else None
        except (TypeError, ValueError):
            return None

    def _id(item: Any) -> int:
        raw = item.get("id") if isinstance(item, dict) else getattr(item, "id", 0)
        try:
            return int(raw or 0)
        except (TypeError, ValueError):
            return 0

    items.sort(
        key=lambda item: (
            _value(item) is None,
            -(_value(item) or 0.0) if descending else (_value(item) or 0.0),
            _id(item),
        )
    )
    return items


def price_without_util_note(
    *,
    display_price: Optional[float],
    total_price_rub_cached: Optional[float],
    calc_breakdown: Optional[list] = None,
    region: Optional[str] = None,
    country: Optional[str] = None,
) -> Optional[str]:
    if display_price is None:
        return None
    has_without_util = has_without_util_marker(calc_breakdown)
    reg = str(region or "").upper()
    c = str(country or "").upper()
    is_korea = reg == "KR" or c.startswith("KR")
    if is_korea:
        if has_without_util or total_price_rub_cached is None:
            return PRICE_NOTE_WITHOUT_UTIL
        return PRICE_NOTE_MOSCOW
    if has_without_util:
        if c == "CN":
            return PRICE_NOTE_CHINA
        return PRICE_NOTE_EUROPE
    if total_price_rub_cached is not None:
        return PRICE_NOTE_MOSCOW
    if c == "CN":
        return PRICE_NOTE_CHINA
    if reg == "EU":
        return PRICE_NOTE_EUROPE
    if not reg and c and c != "RU" and not c.startswith("KR"):
        return PRICE_NOTE_EUROPE
    return None
