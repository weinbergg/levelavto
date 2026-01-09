"""add registration_year_month

Revision ID: 0008_registration_fields
Revises: 0007_price_rub_cached
Create Date: 2026-01-08
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '0008_registration_fields'
down_revision = '0007_price_rub_cached'
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.add_column('cars', sa.Column('registration_year', sa.Integer(), nullable=True))
    op.add_column('cars', sa.Column('registration_month', sa.Integer(), nullable=True))
    op.create_index('ix_cars_registration_year', 'cars', ['registration_year'])
    op.create_index('ix_cars_registration_month', 'cars', ['registration_month'])


def downgrade() -> None:
    op.drop_index('ix_cars_registration_month', table_name='cars')
    op.drop_index('ix_cars_registration_year', table_name='cars')
    op.drop_column('cars', 'registration_month')
    op.drop_column('cars', 'registration_year')
