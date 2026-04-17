"""add engine_type + price coalesce indexes for catalog filters

Revision ID: 0036_idx_engine_price_coalesce
Revises: 0035_add_inferred_specs
Create Date: 2026-04-17
"""

from alembic import op


revision = "0036_idx_engine_price_coalesce"
down_revision = "0035_add_inferred_specs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    ctx = op.get_context()
    with ctx.autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_country_engine_price_coalesce_avail
            ON cars (country, (lower(trim(engine_type))), (COALESCE(total_price_rub_cached, price_rub_cached)), id)
            WHERE is_available = true AND engine_type IS NOT NULL
            """
        )
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_source_engine_price_coalesce_avail
            ON cars (source_id, (lower(trim(engine_type))), (COALESCE(total_price_rub_cached, price_rub_cached)), id)
            WHERE is_available = true AND engine_type IS NOT NULL
            """
        )


def downgrade() -> None:
    ctx = op.get_context()
    with ctx.autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_cars_source_engine_price_coalesce_avail")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_cars_country_engine_price_coalesce_avail")
