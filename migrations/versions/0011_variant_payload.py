"""add variant and source_payload to cars

Revision ID: 0011_variant_payload
Revises: 0010_engine_power_fields
Create Date: 2026-01-15
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0011_variant_payload"
down_revision = "0010_engine_power_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cars", sa.Column("variant", sa.String(length=160), nullable=True))
    op.add_column("cars", sa.Column("source_payload", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("cars", "source_payload")
    op.drop_column("cars", "variant")
