"""Lock the canonical drive_type contract used by every parser, the
upsert defensive guard, the backfill script and the DB CHECK
constraint.
"""

from __future__ import annotations

import pytest

from backend.app.utils.drive_type import (
    CANONICAL_DRIVE_TYPES,
    canonicalize_drive_type,
    infer_drive_type_from_variant,
)


def test_canonical_set_is_lowercase_and_stable():
    assert CANONICAL_DRIVE_TYPES == frozenset({"fwd", "rwd", "awd"})


@pytest.mark.parametrize(
    "raw, expected",
    [
        # Already canonical.
        ("awd", "awd"),
        ("rwd", "rwd"),
        ("fwd", "fwd"),
        # Mixed case.
        ("AWD", "awd"),
        ("RWD", "rwd"),
        ("FWD", "fwd"),
        # Russian taxonomy from the legacy parser.
        ("Полный", "awd"),
        ("полный привод", "awd"),
        ("Задний", "rwd"),
        ("Передний", "fwd"),
        # Free text.
        ("4x4", "awd"),
        ("4WD", "awd"),
        ("All-Wheel Drive", "awd"),
        ("All Wheel Drive", "awd"),
        ("Four-Wheel Drive", "awd"),
        ("Allrad", "awd"),
        ("Rear-Wheel Drive", "rwd"),
        ("Hinterrad", "rwd"),
        ("Front Wheel Drive", "fwd"),
        ("Vorderrad", "fwd"),
        # OEM badges.
        ("xDrive40d", "awd"),
        ("X5 xDrive50e M Sport", "awd"),
        ("sDrive18i", "rwd"),
        ("Q5 50 TDI quattro S line", "awd"),
        ("GLE 350 d 4MATIC AMG-Line", "awd"),
        ("Tiguan 2.0 TDI 4MOTION", "awd"),
        ("4MATIC+", "awd"),
        # Garbage.
        (None, None),
        ("", None),
        ("   ", None),
        ("Manual", None),
        ("Hybrid", None),
        ("110", None),
    ],
)
def test_canonicalize_drive_type(raw, expected):
    assert canonicalize_drive_type(raw) == expected


@pytest.mark.parametrize(
    "variant, expected",
    [
        ("xDrive40d", "awd"),
        ("X5 xDrive50e M Sport Pro 22\" LM AHK GSD", "awd"),
        ("sDrive 18i Advantage Navi LED PDC", "rwd"),
        ("Q7 50 TDI quattro S line", "awd"),
        ("GLE 350 d 4MATIC AMG-Line", "awd"),
        ("VW Tiguan 2.0 TSI 4MOTION DSG", "awd"),
        # Without an OEM badge → no inference, stays None.
        ("328i Sport Line", None),
        ("S 500 lang", None),
        (None, None),
        ("", None),
    ],
)
def test_infer_drive_type_from_variant(variant, expected):
    assert infer_drive_type_from_variant(variant) == expected


def test_canonicalizer_output_is_always_in_set_or_none():
    samples = [
        None, "", "   ", "AWD", "Полный", "xDrive", "sDrive18i",
        "4MATIC", "quattro", "4x4", "Manual", "garbage", "EV",
    ]
    for s in samples:
        out = canonicalize_drive_type(s)
        assert out is None or out in CANONICAL_DRIVE_TYPES, (s, out)
