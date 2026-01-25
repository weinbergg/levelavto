"""drop invalid idx_cars_country_brand_avail

Revision ID: 0027_drop_invalid_index
Revises: 0026_uppercase_country
Create Date: 2026-01-24
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0027_drop_invalid_index"
down_revision = "0026_uppercase_country"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Invalid index: drop without CONCURRENTLY to avoid transactional errors in alembic
    op.execute("DROP INDEX IF EXISTS idx_cars_country_brand_avail")


def downgrade() -> None:
    # no-op: index can be recreated by perf migrations if needed
    pass
