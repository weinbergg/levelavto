from __future__ import annotations

from typing import Mapping, Optional


def to_rub(
    price: Optional[float],
    currency: Optional[str],
    rates: Optional[Mapping[str, float]],
) -> Optional[float]:
    if price is None:
        return None
    cur = (currency or "EUR").strip().upper()
    if cur in ("RUB", "RUR", "₽"):
        return float(price)
    if not rates:
        return None
    rate = rates.get(cur)
    if rate is None:
        return None
    return float(price) * float(rate)
