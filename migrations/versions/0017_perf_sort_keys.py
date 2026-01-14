"""add sort helper columns and indexes for performance

Revision ID: 0017_perf_sort_keys
Revises: 0016_perf_more_idx
Create Date: 2026-01-13 00:00:02
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0017_perf_sort_keys"
down_revision = "0016_perf_more_idx"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # generated columns to avoid expression sorts
    op.add_column(
        "cars",
        sa.Column(
            "listing_sort_ts",
            sa.TIMESTAMP(timezone=True),
            sa.Computed("COALESCE(listing_date, updated_at, created_at)", persisted=True),
            nullable=True,
        ),
    )
    op.add_column(
        "cars",
        sa.Column(
            "reg_sort_key",
            sa.Integer,
            sa.Computed(
                "(COALESCE(registration_year, year, 0) * 12) + COALESCE(registration_month, 1)",
                persisted=True,
            ),
            nullable=True,
        ),
    )

    # indexes tailored for frequent sorts/filters
    op.create_index(
        "idx_cars_available_total_id",
        "cars",
        ["is_available", "total_price_rub_cached", "id"],
    )
    op.create_index(
        "idx_cars_available_price_base_id",
        "cars",
        ["is_available", "price_rub_cached", "id"],
    )
    op.create_index(
        "idx_cars_available_mileage_id",
        "cars",
        ["is_available", "mileage", "id"],
    )
    op.create_index(
        "idx_cars_available_year_id",
        "cars",
        ["is_available", "year", "id"],
    )
    op.create_index(
        "idx_cars_available_listing_sort_id",
        "cars",
        ["is_available", "listing_sort_ts", "id"],
    )
    op.create_index(
        "idx_cars_available_reg_sort_id",
        "cars",
        ["is_available", "reg_sort_key", "id"],
    )


def downgrade() -> None:
    op.drop_index("idx_cars_available_reg_sort_id", table_name="cars")
    op.drop_index("idx_cars_available_listing_sort_id", table_name="cars")
    op.drop_index("idx_cars_available_year_id", table_name="cars")
    op.drop_index("idx_cars_available_mileage_id", table_name="cars")
    op.drop_index("idx_cars_available_price_base_id", table_name="cars")
    op.drop_index("idx_cars_available_total_id", table_name="cars")
    op.drop_column("cars", "reg_sort_key")
    op.drop_column("cars", "listing_sort_ts")
