"""add idx_cars_country_price_avail

Revision ID: 0029_idx_country_price
Revises: 0028_idx_country_brand_model
Create Date: 2026-01-26
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "0029_idx_country_price"
down_revision = "0028_idx_country_brand_model"
branch_labels = None
depends_on = None


def upgrade() -> None:
    ctx = op.get_context()
    with ctx.autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_country_price_avail "
            "ON cars (country, total_price_rub_cached, price_rub_cached, id) "
            "WHERE is_available = true"
        )


def downgrade() -> None:
    ctx = op.get_context()
    with ctx.autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_cars_country_price_avail")
