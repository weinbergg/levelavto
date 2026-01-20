"""add car_counts table for fast counters

Revision ID: 0021_car_counts_table
Revises: 0020_perf_filter_sort_idx
Create Date: 2026-01-13 01:05:00
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0021_car_counts_table"
down_revision = "0018_is_available_default_true"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "car_counts",
        sa.Column("region", sa.String(length=8), nullable=False),
        sa.Column("country", sa.String(length=8), nullable=True),
        sa.Column("brand", sa.String(length=120), nullable=True),
        sa.Column("model", sa.String(length=160), nullable=True),
        sa.Column("total", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index(
        "idx_car_counts_region_country_brand_model",
        "car_counts",
        ["region", "country", "brand", "model"],
        unique=True,
    )
    op.create_index(
        "idx_car_counts_region_total",
        "car_counts",
        ["region", "total"],
    )


def downgrade() -> None:
    op.drop_index("idx_car_counts_region_total", table_name="car_counts")
    op.drop_index("idx_car_counts_region_country_brand_model", table_name="car_counts")
    op.drop_table("car_counts")
