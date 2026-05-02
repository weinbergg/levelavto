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
    # Chunked CASE rewrite. A single monolithic UPDATE over ~1 M rows
    # means one giant transaction that bloats WAL, holds many row
    # locks at once, and is unkillable without rolling back hours of
    # work. We instead loop in 50 000-row id ranges with an explicit
    # COMMIT after each batch — per-batch wall time is a few seconds,
    # progress is visible in pg_stat_user_tables.n_tup_upd, and a
    # mid-run kill loses at most one batch (autocommit guarantees
    # everything before is durable).
    #
    # Outer driver runs the loop in PL/pgSQL because Alembic wraps
    # ``op.execute`` in its own transactional DDL block; ``COMMIT``
    # inside a procedure is the standard PG ≥ 11 escape hatch for
    # writing batched data migrations that need to commit between
    # batches.
    op.execute("COMMIT")  # leave Alembic's wrapping txn so PROCEDURE can COMMIT
    op.execute(
        f"""
        DO $$
        DECLARE
            cur_id BIGINT := 0;
            max_id BIGINT;
            batch_size CONSTANT INTEGER := 50000;
            updated_in_batch INTEGER;
            total_updated BIGINT := 0;
        BEGIN
            SELECT max(id) INTO max_id FROM cars;
            IF max_id IS NULL THEN
                RETURN;
            END IF;
            RAISE NOTICE 'drive_type normalisation: max_id=%', max_id;
            WHILE cur_id <= max_id LOOP
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
                    WHERE id >= cur_id AND id < cur_id + batch_size
                      AND drive_type IS NOT NULL
                      AND drive_type NOT IN ({_DRIVE_LIST_SQL})
                ) AS s
                WHERE c.id = s.id;
                GET DIAGNOSTICS updated_in_batch = ROW_COUNT;
                total_updated := total_updated + updated_in_batch;
                COMMIT;
                IF updated_in_batch > 0 THEN
                    RAISE NOTICE
                        'drive_type batch [%, %): updated=%, running_total=%',
                        cur_id, cur_id + batch_size, updated_in_batch, total_updated;
                END IF;
                cur_id := cur_id + batch_size;
            END LOOP;
            RAISE NOTICE 'drive_type normalisation finished, total_updated=%', total_updated;
        END
        $$;
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
