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

# Per-token predicates expressed against ``lower(trim(drive_type))``.
# AWD is checked first when both AWD and FWD/RWD tokens are present
# (e.g. an OEM marketing blurb containing both ``"xDrive"`` and
# ``"front-wheel"``) — same precedence the application canonicaliser
# uses, same precedence mobile.de uses for filter routing.
_AWD_LIKE = (
    "val LIKE '%полн%' OR val LIKE '%4x4%' OR val LIKE '%4wd%' "
    "OR val LIKE '%awd%' OR val LIKE '%allrad%' "
    "OR val LIKE '%all wheel%' OR val LIKE '%all-wheel%' "
    "OR val LIKE '%four-wheel%' OR val LIKE '%four wheel%' "
    "OR val LIKE '%xdrive%' OR val LIKE '%4matic%' "
    "OR val LIKE '%quattro%' OR val LIKE '%4motion%'"
)
_RWD_LIKE = (
    "val LIKE '%задн%' OR val LIKE '%rwd%' "
    "OR val LIKE '%rear-wheel%' OR val LIKE '%rear wheel%' "
    "OR val LIKE '%hinterrad%' OR val LIKE '%sdrive%'"
)
_FWD_LIKE = (
    "val LIKE '%перед%' OR val LIKE '%fwd%' "
    "OR val LIKE '%front-wheel%' OR val LIKE '%front wheel%' "
    "OR val LIKE '%vorderrad%'"
)


def upgrade() -> None:
    # Single-pass CASE rewrite — one sequential scan instead of four
    # separate UPDATEs. Each row touched at most once; rows already in
    # the canonical set are skipped by the WHERE clause so the operation
    # is idempotent and re-runnable after a partial / killed previous
    # attempt. The whole thing typically finishes in well under a minute
    # on a 1.8 M-row table even without a supporting index.
    op.execute(
        f"""
        UPDATE cars AS c
        SET drive_type = CASE
            WHEN {_AWD_LIKE} THEN 'awd'
            WHEN {_RWD_LIKE} THEN 'rwd'
            WHEN {_FWD_LIKE} THEN 'fwd'
            ELSE NULL
        END
        FROM (
            SELECT id, lower(trim(drive_type)) AS val
            FROM cars
            WHERE drive_type IS NOT NULL
              AND drive_type NOT IN ({_DRIVE_LIST_SQL})
        ) AS s
        WHERE c.id = s.id
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
