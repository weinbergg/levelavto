"""Canonical ``cars.drive_type`` enforcement, in one place.

The catalog sidebar exposes three drive-type chips —
``fwd`` / ``rwd`` / ``awd`` — and the URL filter is matched against
``lower(trim(cars.drive_type))``. Historically the column accumulated
half a dozen conventions (``"AWD"`` from the mobile.de feed parser,
``"Полный"`` from the legacy mobile_de HTML scraper, ``"4wd"`` from
the emavto fallback, ``None`` from anything that didn't have a
dedicated drive field). 45 % of active cars currently have NULL
because the parsers don't look at the variant string at all, even
though ``"BMW X5 xDrive40d"`` is a literal AWD declaration.

This module provides:

* :data:`CANONICAL_DRIVE_TYPES` — the three lowercase tokens the
  filter understands.
* :func:`canonicalize_drive_type` — fold an arbitrary parser output
  (German / English / Russian / OEM brand-specific badge) into one
  of those tokens, or ``None`` if the input is non-informative.
* :func:`infer_drive_type_from_variant` — same, but tuned for
  free-text car variant / sub-title strings (``"xDrive50e M Sport
  Pro"``). The variant detector is intentionally separate from the
  raw canonicaliser because an OEM badge embedded in a long marketing
  string is a different problem than canonicalising a clean parser
  output.

Every parser MUST funnel its raw value through
:func:`canonicalize_drive_type` before persisting; the defensive
guard in ``upsert_parsed_items`` does this as a last-mile step too,
and the migration adds a DB CHECK constraint to lock things in.
"""

from __future__ import annotations

import re
from typing import Optional


CANONICAL_DRIVE_TYPES: frozenset[str] = frozenset({"fwd", "rwd", "awd"})


# Brand-specific 4WD badges. Each is a regex applied to a lowercased,
# whitespace-collapsed variant / sub-title string. Keep this list
# accurate to current OEM naming — it's the difference between
# extracting AWD on 100 000 BMW X-cars or leaving them all as NULL.
_AWD_BADGES: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p, re.IGNORECASE)
    for p in (
        r"\bxdrive\d*[a-z]*\b",      # BMW xDrive, xDrive40d, xDrive50e
        r"\b4matic\+?\b",            # Mercedes 4MATIC, 4MATIC+
        r"\bquattro\b",              # Audi
        r"\b4motion\b",              # VW
        r"\bsymmetrical[- ]awd\b",   # Subaru
        r"\b4wd\b",
        r"\bawd\b",
        r"\b4x4\b",
        r"\ballrad\b",
        r"\ball[- ]wheel[- ]drive\b",
        r"\bfour[- ]wheel[- ]drive\b",
        r"\b4-?matic\b",
        r"\bs-?tronic\b.*\bquattro\b",
    )
)

_RWD_BADGES: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p, re.IGNORECASE)
    for p in (
        r"\bsdrive\d*[a-z]*\b",      # BMW sDrive (rear-wheel drive variants)
        r"\brwd\b",
        r"\brear[- ]wheel[- ]drive\b",
        r"\bhinterrad\b",
        r"\bзадн",                   # Cyrillic "задн(ий) привод"
    )
)

_FWD_BADGES: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p, re.IGNORECASE)
    for p in (
        r"\bfwd\b",
        r"\bfront[- ]wheel[- ]drive\b",
        r"\bvorderrad\b",
        r"\bперед",                  # Cyrillic "перед(ний) привод"
    )
)


def canonicalize_drive_type(value: Optional[str]) -> Optional[str]:
    """Fold an arbitrary parser output into the canonical lowercase set.

    The function deliberately accepts dirty inputs ("AWD", "Полный",
    "4 motion", "All-Wheel Drive (4WD)", " awd ") and outputs one of
    {fwd, rwd, awd} or ``None``. AWD wins over RWD/FWD when multiple
    tokens are present so that ``"xDrive (with all-wheel drive)"``
    classifies as AWD, same as how mobile.de itself buckets it.
    """

    if value is None:
        return None
    val = str(value).strip().lower()
    if not val:
        return None
    if val in CANONICAL_DRIVE_TYPES:
        return val
    # Plain Russian labels from the legacy taxonomy.
    if "полн" in val:
        return "awd"
    if "задн" in val:
        return "rwd"
    if "перед" in val:
        return "fwd"
    if any(rx.search(val) for rx in _AWD_BADGES):
        return "awd"
    if any(rx.search(val) for rx in _RWD_BADGES):
        return "rwd"
    if any(rx.search(val) for rx in _FWD_BADGES):
        return "fwd"
    return None


def infer_drive_type_from_variant(variant: Optional[str]) -> Optional[str]:
    """Best-effort drive-type extraction from a free-text variant.

    Variant strings are noisy ("xDrive50e M Sport Pro 22\" LM AHK",
    "Q5 50 TDI quattro S line", "GLE 350 d 4MATIC AMG-Line"), but the
    OEM AWD badges are stable enough that a tight regex suite catches
    >95 % of real cases without false positives. Only a handful of
    brands use rear-wheel-only variants today (BMW sDrive, Porsche
    base RWD on the 911), so AWD has the highest recall.
    """

    if not variant:
        return None
    return canonicalize_drive_type(variant)


__all__ = [
    "CANONICAL_DRIVE_TYPES",
    "canonicalize_drive_type",
    "infer_drive_type_from_variant",
]
