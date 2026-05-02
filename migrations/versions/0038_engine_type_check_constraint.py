"""DB-level guard: cars.engine_type ∈ canonical set ∪ {NULL}

Why: the column accumulated three different conventions over the
lifetime of the project — Title-case English ("Diesel", "Hybrid"),
Cyrillic legacy labels ("Бензин", "Дизель", "Бензин + электро") and
even raw numbers ("110", "160", ...) leaked from a wrongly-shifted CSV
column in an earlier parser. Each of those forms was invisible to the
public catalog filter (which case-insensitively matches the canonical
lowercase set), so the same row could be present in the DB but missing
from every fuel chip.

Application-level fixes (one canonicaliser, defensive guard in the
upsert path, parser rewrites) are now in place. This migration adds
defence-in-depth: PostgreSQL itself rejects any future write that
violates the canonical set, so a bug in a *new* parser cannot silently
poison the column. Adding a new fuel type therefore requires updating
both ``utils.engine_type.CANONICAL_ENGINE_TYPES`` and this constraint
in lockstep — exactly the coordination we want.

Revision ID: 0038_engine_type_check
Revises: 0037_notif_pagevisits
Create Date: 2026-05-02
"""

from alembic import op


revision = "0038_engine_type_check"
down_revision = "0037_notif_pagevisits"
branch_labels = None
depends_on = None


# Keep this tuple in sync with
# ``backend.app.utils.engine_type.CANONICAL_ENGINE_TYPES`` and with the
# ``test_canonical_set_is_lowercase_and_stable`` regression test.
# Inlined deliberately: importing application code from a migration
# requires fiddling with sys.path (Alembic launches a fresh Python
# process whose cwd is not on sys.path) and ties the schema migration
# to a specific application version, which we want to avoid.
_CANONICAL_FUELS = (
    "petrol",
    "diesel",
    "hybrid",
    "electric",
    "lpg",
    "cng",
    "hydrogen",
    "ethanol",
    "other",
)
_FUEL_LIST_SQL = ", ".join(f"'{f}'" for f in _CANONICAL_FUELS)

# Recovery mapping for every non-canonical form ever observed in
# production (logs from data_quality_check, plus historical Russian
# labels from the legacy mobile_de HTML scraper). Each rule is a
# pair of SQL fragments evaluated against ``lower(trim(engine_type))``.
# Order matters: more specific compound rules MUST come before the
# single-fuel rules, otherwise 'бензин + электро' would match the
# bare 'бензин' rule and become 'petrol' instead of 'hybrid'.
_NORMALISATION_RULES = [
    # ── compound: <fuel> + electric → hybrid (PHEV / mild-hybrid petrol) ──
    (r"(val LIKE '%бензин%' OR val LIKE '%petrol%' OR val LIKE '%benzin%' "
     r" OR val LIKE '%gasoline%') "
     r"AND (val LIKE '%электро%' OR val LIKE '%electric%' OR val LIKE '%elektro%')",
     "hybrid"),
    # ── compound: <fuel> + LPG → lpg (bivalent → searched as LPG) ──
    (r"(val LIKE '%бензин%' OR val LIKE '%petrol%' OR val LIKE '%benzin%' "
     r" OR val LIKE '%gasoline%') "
     r"AND (val LIKE '%пропан%' OR val LIKE '%lpg%' OR val LIKE '%autogas%')",
     "lpg"),
    # ── compound: <fuel> + CNG → cng ──
    (r"(val LIKE '%бензин%' OR val LIKE '%petrol%' OR val LIKE '%benzin%' "
     r" OR val LIKE '%gasoline%') "
     r"AND (val LIKE '%природный газ%' OR val LIKE '%cng%' OR val LIKE '%erdgas%' "
     r"  OR val LIKE '%метан%')",
     "cng"),
    # ── single fuels (in priority order) ──
    ("val LIKE '%diesel%' OR val LIKE '%дизель%' OR val ~ '\\mtdi\\M'", "diesel"),
    ("val LIKE '%e-hybrid%' OR val LIKE '%phev%' OR val LIKE '%plug%hybrid%' "
     "OR val LIKE '%hybrid%' OR val LIKE '%гибрид%' OR val LIKE '%vollhybrid%'",
     "hybrid"),
    ("val LIKE '%electric%' OR val LIKE '%elektro%' OR val LIKE '%электро%' "
     "OR val ~ '\\mev\\M' OR val ~ '\\meq[a-z]\\M'",
     "electric"),
    ("val LIKE '%petrol%' OR val LIKE '%benzin%' OR val LIKE '%benzina%' "
     "OR val LIKE '%gasoline%' OR val LIKE '%бензин%'",
     "petrol"),
    ("val LIKE '%lpg%' OR val ~ '\\mgpl\\M' OR val LIKE '%autogas%' OR val LIKE '%пропан%'",
     "lpg"),
    ("val LIKE '%cng%' OR val LIKE '%natural gas%' OR val LIKE '%erdgas%' "
     "OR val LIKE '%метан%' OR val LIKE '%природный газ%'",
     "cng"),
    ("val LIKE '%hydrogen%' OR val LIKE '%fuel cell%' OR val LIKE '%водород%'",
     "hydrogen"),
    ("val LIKE '%ethanol%' OR val ~ '\\me85\\M' OR val LIKE '%ffv%' OR val LIKE '%flexfuel%'",
     "ethanol"),
    ("val = 'other' OR val LIKE '%остальн%' OR val LIKE '%andere%'",
     "other"),
]


def upgrade() -> None:
    # Step 1: rewrite every recoverable non-canonical value (Cyrillic
    # legacy labels, compound forms, mobile.de's verbose ethanol
    # description) via pure-SQL canonicalisation. This MUST stay
    # self-contained: Alembic spawns its own Python interpreter whose
    # cwd is not on sys.path, so importing application code here
    # crashes with ModuleNotFoundError before the migration even runs.
    for predicate, target in _NORMALISATION_RULES:
        op.execute(
            f"""
            WITH cte AS (
                SELECT lower(trim(engine_type)) AS val, engine_type AS raw
                FROM cars
                WHERE engine_type IS NOT NULL
                  AND engine_type NOT IN ({_FUEL_LIST_SQL})
            )
            UPDATE cars
            SET engine_type = '{target}'
            FROM cte
            WHERE cars.engine_type = cte.raw
              AND ({predicate})
            """
        )

    # Step 2: whatever is still non-canonical was genuine garbage
    # (random kW numbers, mobile.de disclaimer fragments). Neutralise
    # so the CHECK constraint can be added without violations.
    op.execute(
        f"""
        UPDATE cars
        SET engine_type = NULL
        WHERE engine_type IS NOT NULL
          AND engine_type NOT IN ({_FUEL_LIST_SQL})
        """
    )

    # Step 3: lock the column going forward. Any future write that
    # tries to insert e.g. 'Diesel' or '110' will fail at the DB
    # level — the upsert defensive guard catches it earlier, but this
    # is defence-in-depth in case someone adds a new write path.
    op.execute(
        f"""
        ALTER TABLE cars
        ADD CONSTRAINT cars_engine_type_canonical
        CHECK (
            engine_type IS NULL
            OR engine_type IN ({_FUEL_LIST_SQL})
        )
        """
    )


def downgrade() -> None:
    op.execute("ALTER TABLE cars DROP CONSTRAINT IF EXISTS cars_engine_type_canonical")
