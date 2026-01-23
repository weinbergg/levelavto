"""uppercase country codes in cars and sources

Revision ID: 0026_uppercase_country
Revises: 0025_perf_model_range_idx
Create Date: 2026-01-23
"""

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "0026_uppercase_country"
down_revision = "0025_perf_model_range_idx"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Batch update to avoid long-running transaction and heavy locks on large tables.
    ctx = op.get_context()
    with ctx.autocommit_block():
        conn = op.get_bind()
        batch_size = 50000
        total_updated = 0
        while True:
            ids = conn.execute(
                text(
                    """
                    SELECT id
                    FROM cars
                    WHERE country IS NOT NULL
                      AND country <> UPPER(country)
                    ORDER BY id
                    LIMIT :limit
                    """
                ),
                {"limit": batch_size},
            ).scalars().all()
            if not ids:
                break
            conn.execute(
                text("UPDATE cars SET country = UPPER(country) WHERE id = ANY(:ids)"),
                {"ids": ids},
            )
            total_updated += len(ids)
            print(f"[0026] updated cars batch={len(ids)} total={total_updated}")

        conn.execute(
            text("UPDATE sources SET country = UPPER(country) WHERE country IS NOT NULL")
        )


def downgrade() -> None:
    # no safe downgrade for case normalization
    pass
