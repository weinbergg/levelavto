"""add calculator_configs table

Revision ID: 0009_calculator_config
Revises: 0008_registration_fields
Create Date: 2026-01-09
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '0009_calculator_config'
down_revision = '0008_registration_fields'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'calculator_configs',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('version', sa.Integer(), nullable=False, index=True),
        sa.Column('comment', sa.Text(), nullable=True),
        sa.Column('source', sa.String(length=50), nullable=True),
        sa.Column('payload', sa.JSON(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now(), nullable=False),
    )
    op.create_unique_constraint('uq_calculator_configs_version', 'calculator_configs', ['version'])


def downgrade() -> None:
    op.drop_constraint('uq_calculator_configs_version', 'calculator_configs', type_='unique')
    op.drop_table('calculator_configs')
