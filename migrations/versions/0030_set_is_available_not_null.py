"""set is_available not null default true

Revision ID: 0030_set_is_available_not_null
Revises: 0029_add_idx_country_price_avail
Create Date: 2026-01-30
"""

from alembic import op


revision = "0030_set_is_available_not_null"
down_revision = "0029_add_idx_country_price_avail"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("UPDATE cars SET is_available = true WHERE is_available IS NULL")
    op.execute("ALTER TABLE cars ALTER COLUMN is_available SET DEFAULT true")
    op.execute("ALTER TABLE cars ALTER COLUMN is_available SET NOT NULL")


def downgrade() -> None:
    op.execute("ALTER TABLE cars ALTER COLUMN is_available DROP NOT NULL")
    op.execute("ALTER TABLE cars ALTER COLUMN is_available DROP DEFAULT")
