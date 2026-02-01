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
) -> Optional[float]:
    if total_price_rub_cached is not None:
        return float(total_price_rub_cached)
    if price_rub_cached is not None:
        return float(price_rub_cached)
    return None
