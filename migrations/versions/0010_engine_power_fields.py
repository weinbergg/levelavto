"""add engine_cc and power fields

Revision ID: 0010_engine_power_fields
Revises: 0009_calculator_config
Create Date: 2026-01-09
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '0010_engine_power_fields'
down_revision = '0009_calculator_config'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('cars', sa.Column('engine_cc', sa.Integer(), nullable=True))
    op.add_column('cars', sa.Column('power_hp', sa.Numeric(10, 2), nullable=True))
    op.add_column('cars', sa.Column('power_kw', sa.Numeric(10, 2), nullable=True))


def downgrade() -> None:
    op.drop_column('cars', 'power_kw')
    op.drop_column('cars', 'power_hp')
    op.drop_column('cars', 'engine_cc')
