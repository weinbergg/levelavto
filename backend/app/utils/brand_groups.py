"""Helpers for showing the brand filter with a "top" group + alphabetic rest.

The order of :data:`BRAND_FILTER_PRIORITY` mirrors the mobile experience: the
brands the operations team considers most relevant always come first, then the
remaining brands in alphabetical order. The grouping is used in the home page,
the catalog, and the advanced search to keep the UX consistent across views.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List


BRAND_FILTER_PRIORITY: List[str] = [
    "Mercedes-Benz",
    "BMW",
    "Audi",
    "Volkswagen",
    "Porsche",
    "Land Rover",
    "Rolls-Royce",
    "Ferrari",
    "Lamborghini",
    "Bentley",
    "Ford",
    "Skoda",
    "Opel",
    "Toyota",
    "Volvo",
]


_PRIORITY_ALIASES: Dict[str, str] = {
    "rollsroyce": "Rolls-Royce",
    "rolls royce": "Rolls-Royce",
    "rolls-royce": "Rolls-Royce",
    "landrover": "Land Rover",
    "land-rover": "Land Rover",
    "land rover": "Land Rover",
    "mercedesbenz": "Mercedes-Benz",
    "mercedes benz": "Mercedes-Benz",
    "mercedes-benz": "Mercedes-Benz",
    "mercedes": "Mercedes-Benz",
    "vw": "Volkswagen",
    "volkswagen": "Volkswagen",
    "škoda": "Skoda",
    "skoda": "Skoda",
}


def _canonical_key(value: Any) -> str:
    raw = str(value or "").strip().casefold()
    if not raw:
        return ""
    raw = raw.replace("\u00a0", " ")
    while "  " in raw:
        raw = raw.replace("  ", " ")
    return raw


def _normalize_to_priority(value: Any) -> str | None:
    """Map ``value`` to a canonical priority brand if it matches one."""

    key = _canonical_key(value)
    if not key:
        return None
    if key in _PRIORITY_ALIASES:
        return _PRIORITY_ALIASES[key]
    for priority in BRAND_FILTER_PRIORITY:
        if _canonical_key(priority) == key:
            return priority
    return None


def group_brands(brands: Iterable[Any]) -> Dict[str, List[str]]:
    """Split ``brands`` into ``{"top": [...], "other": [...]}`` lists.

    ``brands`` may contain plain strings or facet dicts (with a ``value``/``brand``
    key). Top brands appear in the order defined by
    :data:`BRAND_FILTER_PRIORITY`; any remaining brands are sorted by their
    case-folded name.
    """

    seen: Dict[str, str] = {}
    for item in brands or []:
        if isinstance(item, dict):
            raw = item.get("value") or item.get("brand") or item.get("label")
        else:
            raw = item
        text = str(raw or "").strip()
        if not text:
            continue
        priority = _normalize_to_priority(text)
        canon = priority or text
        key = _canonical_key(canon)
        if not key or key in seen:
            continue
        seen[key] = canon

    top: List[str] = []
    consumed: set[str] = set()
    for priority in BRAND_FILTER_PRIORITY:
        key = _canonical_key(priority)
        if key in seen:
            top.append(seen[key])
            consumed.add(key)

    other = [
        name
        for key, name in seen.items()
        if key not in consumed
    ]
    other.sort(key=lambda value: value.casefold())
    return {"top": top, "other": other}


def ordered_brands(brands: Iterable[Any]) -> List[str]:
    """Return a flat list with priority brands first, then alphabetic rest."""

    groups = group_brands(brands)
    return [*groups["top"], *groups["other"]]
