"""add kr_market_type and price total index

Revision ID: 0015_kr_market_price_total_idx
Revises: 0014_perf_indexes
Create Date: 2026-01-13 00:00:00
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0015_kr_market_price_total_idx"
down_revision = "0014_perf_indexes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "cars",
        sa.Column("kr_market_type", sa.String(length=16), nullable=True),
    )
    # backfill current KR cars to domestic
    op.execute(
        """
        UPDATE cars
        SET kr_market_type = 'domestic'
        WHERE country = 'KR' AND (kr_market_type IS NULL OR kr_market_type = '')
        """
    )
    op.create_index(
        "idx_cars_available_country_market",
        "cars",
        ["is_available", "country", "kr_market_type"],
    )
    op.create_index(
        "idx_cars_price_total_sort",
        "cars",
        ["is_available", "total_price_rub_cached"],
    )


def downgrade() -> None:
    op.drop_index("idx_cars_price_total_sort", table_name="cars")
    op.drop_index("idx_cars_available_country_market", table_name="cars")
    op.drop_column("cars", "kr_market_type")
