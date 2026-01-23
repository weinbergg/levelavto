"""split car_counts into smaller aggregates

Revision ID: 0024_counts_tables_split
Revises: 0023_perf_indexes_v2
Create Date: 2026-01-22 19:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = "0024_counts_tables_split"
down_revision = "0023_perf_indexes_v2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "car_counts_core",
        sa.Column("region", sa.String(length=8), nullable=False),
        sa.Column("country", sa.String(length=8), nullable=True),
        sa.Column("total", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index(
        "idx_counts_core_region_country",
        "car_counts_core",
        ["region", "country"],
    )

    op.create_table(
        "car_counts_brand",
        sa.Column("region", sa.String(length=8), nullable=False),
        sa.Column("country", sa.String(length=8), nullable=True),
        sa.Column("brand", sa.String(length=120), nullable=False),
        sa.Column("total", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index(
        "idx_counts_brand_region_country_brand",
        "car_counts_brand",
        ["region", "country", "brand"],
    )

    op.create_table(
        "car_counts_model",
        sa.Column("region", sa.String(length=8), nullable=False),
        sa.Column("country", sa.String(length=8), nullable=True),
        sa.Column("brand", sa.String(length=120), nullable=False),
        sa.Column("model", sa.String(length=160), nullable=False),
        sa.Column("total", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index(
        "idx_counts_model_region_country_brand_model",
        "car_counts_model",
        ["region", "country", "brand", "model"],
    )

    facet_tables = [
        ("car_counts_color", "color", 80),
        ("car_counts_engine_type", "engine_type", 80),
        ("car_counts_transmission", "transmission", 80),
        ("car_counts_body_type", "body_type", 80),
        ("car_counts_drive_type", "drive_type", 80),
        ("car_counts_price_bucket", "price_bucket", 32),
        ("car_counts_mileage_bucket", "mileage_bucket", 32),
    ]
    for table, col, length in facet_tables:
        op.create_table(
            table,
            sa.Column("region", sa.String(length=8), nullable=False),
            sa.Column("country", sa.String(length=8), nullable=True),
            sa.Column("brand", sa.String(length=120), nullable=True),
            sa.Column(col, sa.String(length=length), nullable=False),
            sa.Column("total", sa.Integer(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        )
        op.create_index(
            f"idx_{table}_region_country",
            table,
            ["region", "country", "brand", col],
        )

    op.create_table(
        "car_counts_reg_year",
        sa.Column("region", sa.String(length=8), nullable=False),
        sa.Column("country", sa.String(length=8), nullable=True),
        sa.Column("reg_year", sa.Integer(), nullable=False),
        sa.Column("total", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index(
        "idx_counts_reg_year_region_country",
        "car_counts_reg_year",
        ["region", "country", "reg_year"],
    )


def downgrade() -> None:
    op.drop_index("idx_counts_reg_year_region_country", table_name="car_counts_reg_year")
    op.drop_table("car_counts_reg_year")

    facet_tables = [
        "car_counts_color",
        "car_counts_engine_type",
        "car_counts_transmission",
        "car_counts_body_type",
        "car_counts_drive_type",
        "car_counts_price_bucket",
        "car_counts_mileage_bucket",
    ]
    for table in facet_tables:
        op.drop_table(table)

    op.drop_index("idx_counts_model_region_country_brand_model", table_name="car_counts_model")
    op.drop_table("car_counts_model")
    op.drop_index("idx_counts_brand_region_country_brand", table_name="car_counts_brand")
    op.drop_table("car_counts_brand")
    op.drop_index("idx_counts_core_region_country", table_name="car_counts_core")
    op.drop_table("car_counts_core")
