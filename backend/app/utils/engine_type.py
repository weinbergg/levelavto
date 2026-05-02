"""Canonical ``cars.engine_type`` enforcement, in one place.

This module is the *single source of truth* for the canonical fuel-type
set used everywhere downstream:

* the public catalog filter chips (``hybrid|diesel|petrol|electric|...``),
* the per-fuel facet counts the homepage and admin display,
* the operator-facing SQL (``WHERE engine_type='diesel'``),
* the ``cars`` table CHECK constraint.

Every parser and every admin script MUST funnel its raw value through
:func:`canonicalize_engine_type` before persisting. The defensive guard
in :func:`backend.app.services.parsing_data_service.upsert_parsed_items`
calls this function as the *last* step, so even a buggy/new parser that
forgets to normalise its input cannot pollute the column.

The set is intentionally small and stable. New fuels (e.g. methanol,
ammonia) MUST be added here AND to the migration that establishes the
CHECK constraint, in lockstep, otherwise upserts will start failing.
"""

from __future__ import annotations

import re
from typing import Optional


CANONICAL_ENGINE_TYPES: frozenset[str] = frozenset(
    {
        "petrol",
        "diesel",
        "hybrid",
        "electric",
        "lpg",
        "cng",
        "hydrogen",
        "ethanol",
        "other",
    }
)

# Tokens that mean "we do not know the fuel" — must NEVER make it into
# the column. Originally surfaced from mobile.de's regulatory
# disclaimer text in ``envkv.consumption_fuel`` ("Based on CO₂
# emissions (combined)") leaking through naive parsers.
_DISCLAIMER_FRAGMENTS: tuple[str, ...] = (
    "based on",
    "co2",
    "co₂",
    "emission",
    "consumption",
    "combined",
)


def canonicalize_engine_type(value: Optional[str]) -> Optional[str]:
    """Return one of :data:`CANONICAL_ENGINE_TYPES` or ``None``.

    Heuristics mirror what the mobile.de feed parser, the Encar mapper
    and the cleanup script all individually used to do (badly, in
    Title-case English mixed with Cyrillic). Order matters: ``diesel``
    wins over ``hybrid`` so a ``"Diesel/Electric (Hybrid)"`` envkv text
    is treated as a (mild-hybrid) diesel — same as mobile.de itself.
    ``e-hybrid`` and ``plug-in`` are checked before ``electric`` so
    the Porsche ``Cayenne E-Hybrid`` is not misclassified as an EV.

    Pure-numeric values (``"110"``, ``"160"`` — leftovers from a
    wrongly-shifted CSV column in a legacy parser) and disclaimer
    snippets return ``None`` so the upstream layer either replaces
    them with a payload-derived value or stores ``NULL``.
    """

    if value is None:
        return None
    val = str(value).strip().lower()
    if not val:
        return None
    # Already canonical → fast-path, common case for the upsert guard.
    if val in CANONICAL_ENGINE_TYPES:
        return val
    if any(noise in val for noise in _DISCLAIMER_FRAGMENTS):
        return None
    if re.fullmatch(r"[0-9]+(?:[.,][0-9]+)?", val):
        return None

    # Diesel wins over every "+ electric" combination so that
    # "Diesel/Electric (Hybrid)" — mobile.de's label for mild-hybrid
    # diesels — is still classified as diesel, matching how mobile.de
    # itself counts those listings under the "Diesel" filter.
    if "diesel" in val or "дизель" in val or re.search(r"\btdi\b", val):
        return "diesel"

    # Real PHEV / hybrid keywords (Porsche "E-Hybrid", BMW "PHEV",
    # generic "Vollhybrid"). Comes before the Cyrillic compound
    # detection so the explicit hybrid signal always wins.
    if (
        "e-hybrid" in val
        or "e-hyb" in val
        or "e-hibri" in val
        or "phev" in val
        or "plug-in" in val
        or "plug in" in val
        or "plugin" in val
        or "hybrid" in val
        or "гибрид" in val
        or "vollhybrid" in val
    ):
        return "hybrid"

    has_petrol_token = (
        "petrol" in val
        or "benzin" in val
        or "benzina" in val
        or "gasoline" in val
        or "бензин" in val
    )
    has_electric_token = (
        "electric" in val
        or "elektro" in val
        or "электро" in val
        or re.search(r"\bev\b", val)
        or re.search(r"\beq[a-z]\b", val)  # Mercedes EQE / EQS / EQA / EQB
    )
    has_lpg_token = (
        "lpg" in val
        or re.search(r"\bgpl\b", val)
        or "autogas" in val
        or "пропан" in val
    )
    has_cng_token = (
        "cng" in val
        or "natural gas" in val
        or "erdgas" in val
        or "метан" in val
        or "природный газ" in val
    )

    # Compound fuels — order reflects mobile.de's own category routing:
    #   * petrol + electric → hybrid (e.g. "Бензин + электро")
    #   * petrol + LPG     → lpg     (bivalent → searched for as LPG)
    #   * petrol + CNG     → cng     (bivalent → searched for as CNG)
    if has_petrol_token and has_electric_token:
        return "hybrid"
    if has_petrol_token and has_lpg_token:
        return "lpg"
    if has_petrol_token and has_cng_token:
        return "cng"

    if has_electric_token:
        return "electric"
    if has_petrol_token:
        return "petrol"
    if has_lpg_token:
        return "lpg"
    if has_cng_token:
        return "cng"
    if "hydrogen" in val or "fuel cell" in val or "водород" in val:
        return "hydrogen"
    if "ethanol" in val or re.search(r"\be85\b", val) or "ffv" in val or "flexfuel" in val:
        return "ethanol"
    # mobile.de "Andere" / "Other" / Russian "Остальные" — explicit
    # non-fuel-specific bucket. Keep it as a real value rather than
    # NULL so the operator can see how many such rows exist.
    if val == "other" or "остальн" in val or "andere" in val:
        return "other"
    return None


__all__ = ["CANONICAL_ENGINE_TYPES", "canonicalize_engine_type"]
