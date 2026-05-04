"""add gin index for exact payload filters

Revision ID: 0040_payload_exact_filter_gin
Revises: 0039_drive_type_check
Create Date: 2026-05-04
"""

from alembic import op


revision = "0040_payload_exact_filter_gin"
down_revision = "0039_drive_type_check"
branch_labels = None
depends_on = None


def upgrade() -> None:
    ctx = op.get_context()
    with ctx.autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_payload_jsonb_exact_avail
            ON cars USING GIN ((CAST(source_payload AS jsonb)) jsonb_path_ops)
            WHERE is_available = true AND source_payload IS NOT NULL
            """
        )


def downgrade() -> None:
    ctx = op.get_context()
    with ctx.autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_cars_payload_jsonb_exact_avail")
