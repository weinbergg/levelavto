"""extend car_counts with facet dimensions

Revision ID: 0022_car_counts_facets
Revises: 0021_car_counts_table
Create Date: 2026-01-14 12:00:00
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0022_car_counts_facets"
down_revision = "0021_car_counts_table"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("car_counts", sa.Column("color", sa.String(length=80), nullable=True))
    op.add_column("car_counts", sa.Column("engine_type", sa.String(length=80), nullable=True))
    op.add_column("car_counts", sa.Column("transmission", sa.String(length=80), nullable=True))
    op.add_column("car_counts", sa.Column("body_type", sa.String(length=80), nullable=True))
    op.add_column("car_counts", sa.Column("drive_type", sa.String(length=80), nullable=True))
    op.add_column("car_counts", sa.Column("price_bucket", sa.String(length=32), nullable=True))
    op.add_column("car_counts", sa.Column("mileage_bucket", sa.String(length=32), nullable=True))
    op.add_column("car_counts", sa.Column("reg_year", sa.Integer(), nullable=True))

    op.drop_index("idx_car_counts_region_country_brand_model", table_name="car_counts")
    op.create_index(
        "idx_car_counts_region_country_brand_model",
        "car_counts",
        ["region", "country", "brand", "model"],
    )
    op.create_index(
        "idx_car_counts_region_color",
        "car_counts",
        ["region", "color"],
    )
    op.create_index(
        "idx_car_counts_region_engine_type",
        "car_counts",
        ["region", "engine_type"],
    )
    op.create_index(
        "idx_car_counts_region_transmission",
        "car_counts",
        ["region", "transmission"],
    )
    op.create_index(
        "idx_car_counts_region_body_type",
        "car_counts",
        ["region", "body_type"],
    )
    op.create_index(
        "idx_car_counts_region_drive_type",
        "car_counts",
        ["region", "drive_type"],
    )
    op.create_index(
        "idx_car_counts_region_price_bucket",
        "car_counts",
        ["region", "price_bucket"],
    )
    op.create_index(
        "idx_car_counts_region_mileage_bucket",
        "car_counts",
        ["region", "mileage_bucket"],
    )
    op.create_index(
        "idx_car_counts_region_reg_year",
        "car_counts",
        ["region", "reg_year"],
    )


def downgrade() -> None:
    op.drop_index("idx_car_counts_region_reg_year", table_name="car_counts")
    op.drop_index("idx_car_counts_region_mileage_bucket", table_name="car_counts")
    op.drop_index("idx_car_counts_region_price_bucket", table_name="car_counts")
    op.drop_index("idx_car_counts_region_body_type", table_name="car_counts")
    op.drop_index("idx_car_counts_region_drive_type", table_name="car_counts")
    op.drop_index("idx_car_counts_region_transmission", table_name="car_counts")
    op.drop_index("idx_car_counts_region_engine_type", table_name="car_counts")
    op.drop_index("idx_car_counts_region_color", table_name="car_counts")
    op.drop_index("idx_car_counts_region_country_brand_model", table_name="car_counts")
    op.create_index(
        "idx_car_counts_region_country_brand_model",
        "car_counts",
        ["region", "country", "brand", "model"],
        unique=True,
    )
    op.drop_column("car_counts", "reg_year")
    op.drop_column("car_counts", "mileage_bucket")
    op.drop_column("car_counts", "price_bucket")
    op.drop_column("car_counts", "body_type")
    op.drop_column("car_counts", "transmission")
    op.drop_column("car_counts", "engine_type")
    op.drop_column("car_counts", "color")
    op.drop_column("car_counts", "drive_type")
