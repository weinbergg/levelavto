"""backfill is_available nulls and set default true

Revision ID: 0018_is_available_default_true
Revises: 0017_perf_sort_keys
Create Date: 2026-01-13 00:00:02
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0018_is_available_default_true"
down_revision = "0017_perf_sort_keys"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("UPDATE cars SET is_available = TRUE WHERE is_available IS NULL")
    op.alter_column(
        "cars",
        "is_available",
        server_default=sa.text("TRUE"),
        existing_type=sa.Boolean(),
    )


def downgrade() -> None:
    op.alter_column(
        "cars",
        "is_available",
        server_default=None,
        existing_type=sa.Boolean(),
    )
