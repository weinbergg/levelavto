from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0001_init"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "sources",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=100), nullable=False, unique=True),
        sa.Column("base_url", sa.String(length=255), nullable=False),
        sa.Column("country", sa.String(length=2), nullable=False),
    )
    op.create_table(
        "cars",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source_id", sa.Integer(), sa.ForeignKey("sources.id"), nullable=False, index=True),
        sa.Column("external_id", sa.String(length=128), nullable=False),
        sa.Column("country", sa.String(length=2), nullable=False),
        sa.Column("brand", sa.String(length=80), nullable=True),
        sa.Column("model", sa.String(length=120), nullable=True),
        sa.Column("generation", sa.String(length=120), nullable=True),
        sa.Column("year", sa.Integer(), nullable=True),
        sa.Column("mileage", sa.Integer(), nullable=True),
        sa.Column("price", sa.Numeric(12, 2), nullable=True),
        sa.Column("currency", sa.String(length=3), nullable=True),
        sa.Column("body_type", sa.String(length=80), nullable=True),
        sa.Column("engine_type", sa.String(length=80), nullable=True),
        sa.Column("transmission", sa.String(length=80), nullable=True),
        sa.Column("drive_type", sa.String(length=80), nullable=True),
        sa.Column("color", sa.String(length=80), nullable=True),
        sa.Column("vin", sa.String(length=64), nullable=True),
        sa.Column("url", sa.String(length=500), nullable=True),
        sa.Column("thumbnail_url", sa.String(length=500), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("is_available", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.UniqueConstraint("source_id", "external_id", name="uq_cars_source_external"),
    )
    op.create_index("ix_cars_country", "cars", ["country"])
    op.create_index("ix_cars_brand", "cars", ["brand"])
    op.create_index("ix_cars_year", "cars", ["year"])
    op.create_index("ix_cars_mileage", "cars", ["mileage"])
    op.create_index("ix_cars_price", "cars", ["price"])
    op.create_index("ix_cars_is_available", "cars", ["is_available"])


def downgrade() -> None:
    op.drop_index("ix_cars_is_available", table_name="cars")
    op.drop_index("ix_cars_price", table_name="cars")
    op.drop_index("ix_cars_mileage", table_name="cars")
    op.drop_index("ix_cars_year", table_name="cars")
    op.drop_index("ix_cars_brand", table_name="cars")
    op.drop_index("ix_cars_country", table_name="cars")
    op.drop_table("cars")
    op.drop_table("sources")


