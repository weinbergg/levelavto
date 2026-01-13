"""add listing_date to cars

Revision ID: 0012_listing_date
Revises: 0011_variant_payload
Create Date: 2026-01-18
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0012_listing_date"
down_revision = "0011_variant_payload"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cars", sa.Column("listing_date", sa.DateTime(), nullable=True))
    op.create_index("ix_cars_listing_date", "cars", ["listing_date"])


def downgrade() -> None:
    op.drop_index("ix_cars_listing_date", table_name="cars")
    op.drop_column("cars", "listing_date")
