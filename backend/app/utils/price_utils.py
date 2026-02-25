import math
import os
from typing import Optional

PRICE_NOTE_WITHOUT_UTIL = "*Без учета утилизационного сбора РФ"
PRICE_NOTE_EUROPE = "Цена в Европе"


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
    if raw is None:
        return None
    return ceil_to_step(raw, get_round_step_rub())


def price_without_util_note(
    *,
    display_price: Optional[float],
    total_price_rub_cached: Optional[float],
    region: Optional[str] = None,
    country: Optional[str] = None,
) -> Optional[str]:
    if display_price is None:
        return None
    if total_price_rub_cached is not None:
        return None
    reg = str(region or "").upper()
    c = str(country or "").upper()
    if reg == "KR" or c.startswith("KR"):
        return PRICE_NOTE_WITHOUT_UTIL
    if reg == "EU":
        return PRICE_NOTE_EUROPE
    if not reg and c and c != "RU" and not c.startswith("KR"):
        return PRICE_NOTE_EUROPE
    return None
