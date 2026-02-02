import math
from typing import Optional


def ceil_to_step(value: Optional[float], step: int) -> Optional[float]:
    if value is None:
        return None
    step_f = float(step)
    return float(math.ceil(float(value) / step_f) * step_f)


def display_price_rub(
    total_price_rub_cached: Optional[float],
    price_rub_cached: Optional[float],
    *,
    allow_price_fallback: bool = False,
) -> Optional[float]:
    # Display should use total (calculator) price when available and be rounded for UI.
    raw = None
    if total_price_rub_cached is not None:
        raw = float(total_price_rub_cached)
    elif allow_price_fallback and price_rub_cached is not None:
        raw = float(price_rub_cached)
    if raw is None:
        return None
    return ceil_to_step(raw, 100000)
