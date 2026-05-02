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

# Order matters: AWD-specific badges win over generic FWD/RWD when the
# string mentions both ("xDrive" inside a marketing blurb that also
# contains "front-wheel"). Each rule is matched against
# ``lower(trim(drive_type))`` (NOT against the variant — the variant
# backfill runs as a separate post-migration step via
# ``backfill_drive_type.py``).
_NORMALISATION_RULES = [
    # Russian taxonomy first — the legacy parser persisted these directly.
    ("val LIKE '%полн%' OR val LIKE '%4x4%' OR val LIKE '%4wd%' "
     "OR val LIKE '%awd%' OR val LIKE '%allrad%' "
     "OR val LIKE '%all wheel%' OR val LIKE '%all-wheel%' "
     "OR val LIKE '%four-wheel%' OR val LIKE '%four wheel%' "
     "OR val LIKE '%xdrive%' OR val LIKE '%4matic%' "
     "OR val LIKE '%quattro%' OR val LIKE '%4motion%'",
     "awd"),
    ("val LIKE '%задн%' OR val LIKE '%rwd%' "
     "OR val LIKE '%rear-wheel%' OR val LIKE '%rear wheel%' "
     "OR val LIKE '%hinterrad%' OR val LIKE '%sdrive%'",
     "rwd"),
    ("val LIKE '%перед%' OR val LIKE '%fwd%' "
     "OR val LIKE '%front-wheel%' OR val LIKE '%front wheel%' "
     "OR val LIKE '%vorderrad%'",
     "fwd"),
]


def upgrade() -> None:
    # Step 1: rewrite recoverable non-canonical values via the SQL
    # rule table (mirrors backend/app/utils/drive_type.py). Skipped
    # values fall through to step 2.
    for predicate, target in _NORMALISATION_RULES:
        op.execute(
            f"""
            WITH cte AS (
                SELECT lower(trim(drive_type)) AS val, drive_type AS raw
                FROM cars
                WHERE drive_type IS NOT NULL
                  AND drive_type NOT IN ({_DRIVE_LIST_SQL})
            )
            UPDATE cars
            SET drive_type = '{target}'
            FROM cte
            WHERE cars.drive_type = cte.raw
              AND ({predicate})
            """
        )

    # Step 2: anything still non-canonical was useless free-text
    # ("Hybrid", random color names mistakenly stored here). Null it
    # so the CHECK can be added.
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
