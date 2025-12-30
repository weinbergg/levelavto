"""car_images table

Revision ID: 0003_car_images
Revises: 0002_parsing_schema
Create Date: 2025-12-04
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0003_car_images"
down_revision = "0002_parsing_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    # Create table if it does not exist
    if not inspector.has_table("car_images"):
        op.create_table(
            "car_images",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("car_id", sa.Integer(), sa.ForeignKey("cars.id", ondelete="CASCADE"), nullable=False),
            sa.Column("url", sa.String(length=1000), nullable=False),
            sa.Column("is_primary", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("position", sa.Integer(), nullable=False, server_default=sa.text("0")),
        )
    # Create index if it does not exist
    existing_indexes = {idx["name"] for idx in inspector.get_indexes("car_images")} if inspector.has_table("car_images") else set()
    if "ix_car_images_car_id" not in existing_indexes:
        op.create_index("ix_car_images_car_id", "car_images", ["car_id"])


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if inspector.has_table("car_images"):
        existing_indexes = {idx["name"] for idx in inspector.get_indexes("car_images")}
        if "ix_car_images_car_id" in existing_indexes:
            op.drop_index("ix_car_images_car_id", table_name="car_images")
        op.drop_table("car_images")


