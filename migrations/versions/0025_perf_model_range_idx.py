"""indexes for model/range counts

Revision ID: 0025_perf_model_range_idx
Revises: 0024_counts_tables_split
Create Date: 2026-01-22 20:00:00
"""

from alembic import op


revision = "0025_perf_model_range_idx"
down_revision = "0024_counts_tables_split"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_source_brand_model_mileage_avail
            ON cars (source_id, brand, model, mileage, id)
            WHERE COALESCE(is_available, true)
            """
        )
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_source_brand_model_reg_avail
            ON cars (source_id, brand, model, reg_sort_key, id)
            WHERE COALESCE(is_available, true)
            """
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX IF EXISTS idx_cars_source_brand_model_reg_avail")
        op.execute("DROP INDEX IF EXISTS idx_cars_source_brand_model_mileage_avail")

