"""add btree expression indexes for exact payload text filters

Revision ID: 0041_payload_exact_text_btree
Revises: 0040_payload_exact_filter_gin
Create Date: 2026-05-04 18:55:00
"""

from alembic import op


revision = "0041_payload_exact_text_btree"
down_revision = "0040_payload_exact_filter_gin"
branch_labels = None
depends_on = None


def upgrade() -> None:
    ctx = op.get_context()
    with ctx.autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_src_country_num_seats_avail
            ON cars (
                source_id,
                country,
                (jsonb_extract_path_text(CAST(source_payload AS jsonb), 'num_seats'))
            )
            WHERE is_available = true AND source_payload IS NOT NULL
            """
        )
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_src_country_doors_count_avail
            ON cars (
                source_id,
                country,
                (jsonb_extract_path_text(CAST(source_payload AS jsonb), 'doors_count'))
            )
            WHERE is_available = true AND source_payload IS NOT NULL
            """
        )
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_src_country_owners_count_avail
            ON cars (
                source_id,
                country,
                (jsonb_extract_path_text(CAST(source_payload AS jsonb), 'owners_count'))
            )
            WHERE is_available = true AND source_payload IS NOT NULL
            """
        )


def downgrade() -> None:
    ctx = op.get_context()
    with ctx.autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_cars_src_country_num_seats_avail")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_cars_src_country_doors_count_avail")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_cars_src_country_owners_count_avail")
