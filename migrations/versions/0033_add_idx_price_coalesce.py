"""add coalesce price indexes for catalog sort

Revision ID: 0033_idx_price_coalesce
Revises: 0032_add_color_group
Create Date: 2026-02-16
"""

from alembic import op


revision = "0033_idx_price_coalesce"
down_revision = "0032_add_color_group"
branch_labels = None
depends_on = None


def upgrade() -> None:
    ctx = op.get_context()
    with ctx.autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_country_price_coalesce_avail
            ON cars (country, (COALESCE(total_price_rub_cached, price_rub_cached)), id)
            WHERE is_available = true
            """
        )
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_source_price_coalesce_avail
            ON cars (source_id, (COALESCE(total_price_rub_cached, price_rub_cached)), id)
            WHERE is_available = true
            """
        )


def downgrade() -> None:
    ctx = op.get_context()
    with ctx.autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_cars_source_price_coalesce_avail")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_cars_country_price_coalesce_avail")

