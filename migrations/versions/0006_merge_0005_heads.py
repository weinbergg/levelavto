"""Merge heads 0005_favorites and 0005_progress_kv

Revision ID: 0006_merge_0005_heads
Revises: 0005_favorites, 0005_progress_kv
Create Date: 2026-01-08
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '0006_merge_0005_heads'
down_revision = ('0005_favorites', '0005_progress_kv')
branch_labels = None
depends_on = None

def upgrade() -> None:
    pass

def downgrade() -> None:
    pass
