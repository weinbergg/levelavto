from __future__ import annotations

from typing import Iterable, List, Optional


def split_csv_values(value: Optional[str | Iterable[str]]) -> List[str]:
    if value is None:
        return []
    raw_items: List[str] = []
    if isinstance(value, str):
        raw_items = [value]
    else:
        for item in value:
            if item is None:
                continue
            raw_items.append(str(item))

    parts: List[str] = []
    seen: set[str] = set()
    for raw in raw_items:
        for piece in str(raw).split(","):
            token = piece.strip()
            if not token:
                continue
            key = token.casefold()
            if key in seen:
                continue
            seen.add(key)
            parts.append(token)
    return parts


def normalize_csv_values(value: Optional[str | Iterable[str]]) -> Optional[str]:
    parts = split_csv_values(value)
    if not parts:
        return None
    ordered = sorted(parts, key=lambda item: item.casefold())
    return ",".join(ordered)
