"""Lock the canonical engine_type contract used by every parser, the
upsert defensive guard and the DB CHECK constraint.

If any of these tests fail, you are about to ship a regression that
will leak non-canonical values into ``cars.engine_type`` again. Either
extend :data:`backend.app.utils.engine_type.CANONICAL_ENGINE_TYPES`
*and* the matching list in
``migrations/versions/0038_engine_type_check_constraint.py`` together,
or fix the parser so its output stays in the existing set.
"""

from __future__ import annotations

import pytest

from backend.app.utils.engine_type import (
    CANONICAL_ENGINE_TYPES,
    canonicalize_engine_type,
)


def test_canonical_set_is_lowercase_and_stable():
    # The frontend filter and the DB constraint both depend on this set
    # being lowercase and limited.
    assert all(v == v.lower() for v in CANONICAL_ENGINE_TYPES)
    assert "petrol" in CANONICAL_ENGINE_TYPES
    assert "diesel" in CANONICAL_ENGINE_TYPES
    assert "hybrid" in CANONICAL_ENGINE_TYPES
    assert "electric" in CANONICAL_ENGINE_TYPES


@pytest.mark.parametrize(
    "raw, expected",
    [
        # Already canonical — pass through.
        ("petrol", "petrol"),
        ("diesel", "diesel"),
        ("hybrid", "hybrid"),
        ("electric", "electric"),
        ("lpg", "lpg"),
        ("cng", "cng"),
        ("hydrogen", "hydrogen"),
        # Title-case English → lowercase.
        ("Diesel", "diesel"),
        ("HYBRID", "hybrid"),
        ("Electric", "electric"),
        ("Petrol", "petrol"),
        # Russian labels — used to leak in via legacy parsers.
        ("Бензин", "petrol"),
        ("Дизель", "diesel"),
        ("Гибрид", "hybrid"),
        ("Электро", "electric"),
        ("Бензин + электро", "hybrid"),
        ("Пропан-бутан + бензин", "lpg"),
        ("Природный газ + бензин", "cng"),
        ("Пропан-бутан", "lpg"),
        ("Природный газ", "cng"),
        ("Водород", "hydrogen"),
        ("Остальные", "other"),
        ("other, e10-enabled", "other"),
        ("ethanol (FFV, E85, etc.)", "ethanol"),
        # Substring / synonym matches.
        ("Vollhybrid", "hybrid"),
        ("Plug-In Hybrid", "hybrid"),
        ("plug-in hybrid (petrol/electric)", "hybrid"),
        ("E-Hybrid", "hybrid"),
        ("PHEV", "hybrid"),
        ("Cayenne E-Hyb Coupé", "hybrid"),
        ("xDrive 30d Diesel", "diesel"),
        ("Diesel/Electric (Hybrid)", "diesel"),  # mild-hybrid diesel → diesel
        ("Elektro", "electric"),
        ("EQE 350+", "electric"),
        ("Taycan EV", "electric"),
        ("Autogas LPG", "lpg"),
        ("Erdgas (CNG)", "cng"),
        ("Fuel Cell", "hydrogen"),
        ("E85 ethanol", "ethanol"),
        # Garbage / disclaimers / numbers — must drop to None so the
        # column is left NULL rather than poisoned.
        ("Based on CO₂ emissions (combined)", None),
        ("110", None),
        ("160", None),
        ("103", None),
        ("3.0", None),
        ("", None),
        ("   ", None),
        (None, None),
    ],
)
def test_canonicalize_engine_type(raw, expected):
    assert canonicalize_engine_type(raw) == expected


def test_canonicalizer_output_is_always_in_canonical_set_or_none():
    """No matter what we feed it, the output is None or in the set."""
    samples = [
        None, "", "   ", "Diesel", "DIESEL", "Бензин", "Бензин + электро",
        "based on co2", "110", "Vollhybrid", "Plug-In Hybrid",
        "Hybrid (Diesel/Electric)", "Cayenne E-Hybrid", "Taycan EV",
        "Erdgas", "Wasserstoff", "no idea", "petrol/diesel mix",
    ]
    for s in samples:
        out = canonicalize_engine_type(s)
        assert out is None or out in CANONICAL_ENGINE_TYPES, (s, out)
