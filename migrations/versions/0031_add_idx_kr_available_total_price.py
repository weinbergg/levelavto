"""add idx for KR price sorting

Revision ID: 0031_add_idx_kr_available_total_price
Revises: 0030_set_is_available_not_null
Create Date: 2026-02-01
"""

from alembic import op

revision = "0031_add_idx_kr_available_total_price"
down_revision = "0030_set_is_available_not_null"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_kr_available_total_price "
            "ON cars (country, is_available, total_price_rub_cached, id)"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_cars_kr_available_total_price")
