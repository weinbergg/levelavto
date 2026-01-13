"""add calc cache fields to cars

Revision ID: 0013_calc_cache
Revises: 0012_listing_date
Create Date: 2026-01-18
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0013_calc_cache"
down_revision = "0012_listing_date"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cars", sa.Column("total_price_rub_cached", sa.Numeric(14, 2), nullable=True))
    op.add_column("cars", sa.Column("calc_breakdown_json", sa.JSON(), nullable=True))
    op.add_column("cars", sa.Column("calc_updated_at", sa.DateTime(), nullable=True))


def downgrade() -> None:
    op.drop_column("cars", "calc_updated_at")
    op.drop_column("cars", "calc_breakdown_json")
    op.drop_column("cars", "total_price_rub_cached")
