"""add color_group to cars

Revision ID: 0032_add_color_group
Revises: 0031_add_idx_kr_available_total_price
Create Date: 2026-02-02 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0032_add_color_group"
down_revision = "0031_add_idx_kr_available_total_price"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cars", sa.Column("color_group", sa.String(length=32), nullable=True))
    op.create_index(
        "idx_cars_country_color_group_avail",
        "cars",
        ["country", "color_group", "is_available"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_cars_country_color_group_avail", table_name="cars")
    op.drop_column("cars", "color_group")
