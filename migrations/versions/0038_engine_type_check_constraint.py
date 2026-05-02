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
from sqlalchemy import text

from backend.app.utils.engine_type import (
    CANONICAL_ENGINE_TYPES,
    canonicalize_engine_type,
)


revision = "0038_engine_type_check"
down_revision = "0037_notif_pagevisits"
branch_labels = None
depends_on = None


_FUEL_LIST_SQL = ", ".join(f"'{f}'" for f in sorted(CANONICAL_ENGINE_TYPES))


def upgrade() -> None:
    # Step 1: rewrite every non-canonical leftover via the project-wide
    # canonicaliser so the migration does NOT silently throw away data
    # like 'Бензин' → NULL when it could safely become 'petrol'. Only
    # the values the canonicaliser cannot map go to NULL in step 2.
    bind = op.get_bind()
    distinct = bind.execute(
        text(
            "SELECT DISTINCT engine_type FROM cars "
            "WHERE engine_type IS NOT NULL "
            f"  AND lower(trim(engine_type)) NOT IN ({_FUEL_LIST_SQL})"
        )
    ).fetchall()
    for (raw,) in distinct:
        target = canonicalize_engine_type(raw)
        if not target:
            continue
        bind.execute(
            text("UPDATE cars SET engine_type = :tgt WHERE engine_type = :src"),
            {"tgt": target, "src": raw},
        )

    # Step 2: anything still non-canonical was unrecoverable garbage
    # (random numbers, mobile.de disclaimer text). Neutralise it so
    # the CHECK constraint can be added.
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
