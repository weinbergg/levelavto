"""uppercase country codes in cars and sources

Revision ID: 0026_uppercase_country
Revises: 0025_perf_model_range_idx
Create Date: 2026-01-23
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0026_uppercase_country"
down_revision = "0025_perf_model_range_idx"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("UPDATE cars SET country = UPPER(country) WHERE country IS NOT NULL;")
    op.execute("UPDATE sources SET country = UPPER(country) WHERE country IS NOT NULL;")


def downgrade() -> None:
    # no safe downgrade for case normalization
    pass
