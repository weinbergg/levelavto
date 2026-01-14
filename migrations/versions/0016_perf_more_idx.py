"""add more perf indexes for counts and sorts

Revision ID: 0016_perf_more_idx
Revises: 0015_kr_market_price_total_idx
Create Date: 2026-01-13 00:00:01
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "0016_perf_more_idx"
down_revision = "0015_kr_market_price_total_idx"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # country-only quick counts + id tie-break
    op.create_index(
        "idx_cars_available_country_id",
        "cars",
        ["is_available", "country", "id"],
    )
    # listing date sort
    op.create_index(
        "idx_cars_available_listing_id",
        "cars",
        ["is_available", "listing_date", "id"],
    )
    # registration sort
    op.create_index(
        "idx_cars_available_reg_id",
        "cars",
        ["is_available", "registration_year", "registration_month", "id"],
    )
    # brand/model filter
    op.create_index(
        "idx_cars_available_brand_model_id",
        "cars",
        ["is_available", "brand", "model", "id"],
    )


def downgrade() -> None:
    op.drop_index("idx_cars_available_brand_model_id", table_name="cars")
    op.drop_index("idx_cars_available_reg_id", table_name="cars")
    op.drop_index("idx_cars_available_listing_id", table_name="cars")
    op.drop_index("idx_cars_available_country_id", table_name="cars")
