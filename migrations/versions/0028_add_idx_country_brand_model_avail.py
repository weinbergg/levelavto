"""add idx_cars_country_brand_model_avail

Revision ID: 0028_idx_country_brand_model
Revises: 0027_drop_invalid_index
Create Date: 2026-01-24
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "0028_idx_country_brand_model"
down_revision = "0027_drop_invalid_index"
branch_labels = None
depends_on = None


def upgrade() -> None:
    ctx = op.get_context()
    with ctx.autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_country_brand_model_avail "
            "ON cars (country, brand, model) WHERE is_available = true"
        )


def downgrade() -> None:
    ctx = op.get_context()
    with ctx.autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_cars_country_brand_model_avail")
