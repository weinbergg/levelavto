"""add price_rub_cached to cars

Revision ID: 0007_price_rub_cached
Revises: 0006_merge_0005_heads
Create Date: 2026-01-08
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '0007_price_rub_cached'
down_revision = '0006_merge_0005_heads'
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.add_column('cars', sa.Column('price_rub_cached', sa.Numeric(14, 2), nullable=True))
    op.create_index('ix_cars_price_rub_cached', 'cars', ['price_rub_cached'])


def downgrade() -> None:
    op.drop_index('ix_cars_price_rub_cached', table_name='cars')
    op.drop_column('cars', 'price_rub_cached')
