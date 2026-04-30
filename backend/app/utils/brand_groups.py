"""Helpers for showing the brand filter with a "top" group + alphabetic rest.

The order of :data:`BRAND_FILTER_PRIORITY` mirrors the mobile experience: the
brands the operations team considers most relevant always come first, then the
remaining brands in alphabetical order. The grouping is used in the home page,
the catalog, and the advanced search to keep the UX consistent across views.

Operators can override the default list at runtime by saving a JSON array to
``site_content.top_brands_json``. When the override is present it replaces
:data:`BRAND_FILTER_PRIORITY` everywhere — see :func:`load_priority_override`.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Iterable, List, Optional


logger = logging.getLogger(__name__)


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


TOP_BRANDS_CONTENT_KEY = "top_brands_json"


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


def _coerce_priority_list(value: Any) -> Optional[List[str]]:
    """Return a clean list of brand names from ``value`` or ``None`` on bad input.

    The DB override is stored as JSON text (an array of brand names). We
    accept a list directly too so unit tests don't need to JSON-encode.
    """

    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = json.loads(text)
        except (TypeError, ValueError):
            logger.warning("top_brands_json is not valid JSON: %r", text[:64])
            return None
        return _coerce_priority_list(parsed)
    if isinstance(value, (list, tuple)):
        cleaned: List[str] = []
        seen: set[str] = set()
        for item in value:
            text = str(item or "").strip()
            if not text:
                continue
            canon = _normalize_to_priority(text) or text
            key = _canonical_key(canon)
            if not key or key in seen:
                continue
            seen.add(key)
            cleaned.append(canon)
        return cleaned or None
    return None


def effective_priority(override: Any | None = None) -> List[str]:
    """Return the active priority list — an override or the hard-coded default.

    The override is accepted as a JSON string, a Python list of names, or
    ``None``. Invalid overrides fall back to :data:`BRAND_FILTER_PRIORITY`
    so the navigation never breaks for visitors when the operator misedits
    the field.
    """

    cleaned = _coerce_priority_list(override)
    if cleaned:
        return cleaned
    return list(BRAND_FILTER_PRIORITY)


def load_priority_override(db: Any) -> Optional[List[str]]:
    """Read the operator override from ``site_content.top_brands_json``.

    Imported lazily to avoid pulling SQLAlchemy into utility modules that
    are also used by lightweight contract tests.
    """

    try:
        from ..services.content_service import ContentService
    except Exception:
        logger.exception("brand_groups: cannot import ContentService for override lookup")
        return None
    try:
        text = ContentService(db).get(TOP_BRANDS_CONTENT_KEY)
    except Exception:
        logger.exception("brand_groups: failed to read %s", TOP_BRANDS_CONTENT_KEY)
        return None
    return _coerce_priority_list(text)


def group_brands(
    brands: Iterable[Any],
    *,
    priority: Iterable[Any] | None = None,
) -> Dict[str, List[str]]:
    """Split ``brands`` into ``{"top": [...], "other": [...]}`` lists.

    ``brands`` may contain plain strings or facet dicts (with a ``value``/``brand``
    key). The optional ``priority`` argument lets the caller inject a
    runtime override (e.g. operator-edited list); when it is ``None`` we
    fall back to :data:`BRAND_FILTER_PRIORITY`. Brands not in the priority
    list are returned alphabetically.
    """

    active_priority = effective_priority(priority)

    seen: Dict[str, str] = {}
    for item in brands or []:
        if isinstance(item, dict):
            raw = item.get("value") or item.get("brand") or item.get("label")
        else:
            raw = item
        text = str(raw or "").strip()
        if not text:
            continue
        priority_match = _normalize_to_priority(text)
        canon = priority_match or text
        key = _canonical_key(canon)
        if not key or key in seen:
            continue
        seen[key] = canon

    top: List[str] = []
    consumed: set[str] = set()
    for priority_brand in active_priority:
        key = _canonical_key(priority_brand)
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


def ordered_brands(
    brands: Iterable[Any],
    *,
    priority: Iterable[Any] | None = None,
) -> List[str]:
    """Return a flat list with priority brands first, then alphabetic rest."""

    groups = group_brands(brands, priority=priority)
    return [*groups["top"], *groups["other"]]
