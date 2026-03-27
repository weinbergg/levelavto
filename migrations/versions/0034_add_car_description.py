"""add description column to cars

Revision ID: 0034_add_car_description
Revises: 0033_idx_price_coalesce
Create Date: 2026-03-27
"""

from alembic import op
import sqlalchemy as sa


revision = "0034_add_car_description"
down_revision = "0033_idx_price_coalesce"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cars", sa.Column("description", sa.Text(), nullable=True))
    op.execute(
        """
        UPDATE cars
        SET description = NULLIF(BTRIM(source_payload ->> 'description'), '')
        WHERE description IS NULL
          AND source_payload IS NOT NULL
        """
    )


def downgrade() -> None:
    op.drop_column("cars", "description")
