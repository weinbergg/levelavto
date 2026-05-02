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


# Keep this tuple in sync with backend.app.utils.engine_type.CANONICAL_ENGINE_TYPES.
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


def upgrade() -> None:
    # Belt + braces: any leftover non-canonical value at the moment of
    # the migration is rewritten via the application-level fallback to
    # NULL so the constraint can be added without "violates check
    # constraint" errors. The cleanup_bad_engine_type +
    # backfill_engine_type + normalize_engine_type_values scripts
    # already brought prod into compliance, but the migration must be
    # rerunnable from any historical state.
    op.execute(
        f"""
        UPDATE cars
        SET engine_type = NULL
        WHERE engine_type IS NOT NULL
          AND lower(trim(engine_type)) NOT IN ({_FUEL_LIST_SQL})
        """
    )
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
