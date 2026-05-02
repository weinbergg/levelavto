"""DB-level guard: cars.drive_type ∈ {fwd, rwd, awd} ∪ {NULL}

Why: ``drive_type`` accumulated half a dozen conventions over the
years — ``"AWD"`` (mobile.de feed parser, uppercase), ``"Полный"``
(legacy mobile_de HTML scraper), ``"4x4"`` / ``"all"`` / random
free-text values from the emavto fallback. The catalog filter does
``lower(trim(drive_type))`` so most of these still kind-of work, but
the column is impossible to inspect by SQL and a misspelling silently
falls out of the AWD/RWD/FWD chips.

This migration:
  1. Folds every existing value via the same set of OEM badges the
     application's :func:`canonicalize_drive_type` uses (xDrive →
     awd, sDrive → rwd, quattro → awd, etc.). Pure-SQL rules so the
     migration has no Python dependencies on application code.
  2. Drops the ``options`` and ``manufacturer`` free-text values that
     do not map to a known drive type — better NULL than poisoning
     the column.
  3. Locks the column with a CHECK constraint matching the canonical
     set; future writes that violate it fail at the DB level.

Revision ID: 0039_drive_type_check
Revises: 0038_engine_type_check
Create Date: 2026-05-02
"""

from alembic import op


revision = "0039_drive_type_check"
down_revision = "0038_engine_type_check"
branch_labels = None
depends_on = None


# Keep this tuple in sync with
# ``backend.app.utils.drive_type.CANONICAL_DRIVE_TYPES`` and with the
# variant-detection regex set in that module. Inlined deliberately so
# the migration runs without importing application code (Alembic
# spawns a fresh interpreter whose cwd is not on sys.path).
_CANONICAL_DRIVES = ("fwd", "rwd", "awd")
_DRIVE_LIST_SQL = ", ".join(f"'{d}'" for d in _CANONICAL_DRIVES)

def upgrade() -> None:
    # Bulk data normalisation has been moved out of this migration —
    # the previous ``op.execute("COMMIT")`` + DO-block trick to escape
    # Alembic's transactional DDL wrapper is fragile (it broke silently
    # on prod and left 2 052 204 rows uppercase). The dedicated script
    # ``backend.app.scripts.normalize_drive_type_values`` does the same
    # work in plain Python with ``Session.commit()`` per 50 000-row
    # batch, with visible progress and safe resume after a kill.
    #
    # All the operator has to do BEFORE running this migration is::
    #
    #   docker compose run --rm web python -m \
    #       backend.app.scripts.normalize_drive_type_values --apply
    #
    # As belt + braces, this migration nullifies any leftover
    # non-canonical row so the CHECK constraint can be added even if
    # the operator forgets the normalisation step (in which case some
    # data is lost — much better than the migration failing and
    # blocking the whole release pipeline).
    op.execute(
        f"""
        UPDATE cars
        SET drive_type = NULL
        WHERE drive_type IS NOT NULL
          AND drive_type NOT IN ({_DRIVE_LIST_SQL})
        """
    )

    op.execute(
        f"""
        ALTER TABLE cars
        ADD CONSTRAINT cars_drive_type_canonical
        CHECK (
            drive_type IS NULL
            OR drive_type IN ({_DRIVE_LIST_SQL})
        )
        """
    )


def downgrade() -> None:
    op.execute("ALTER TABLE cars DROP CONSTRAINT IF EXISTS cars_drive_type_canonical")
