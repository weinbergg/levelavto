"""add performance indexes and pg_stat_statements

Revision ID: 0023_perf_indexes_v2
Revises: 0022_car_counts_facets
Create Date: 2026-01-22 18:00:00
"""

from alembic import op


revision = "0023_perf_indexes_v2"
down_revision = "0022_car_counts_facets"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
    op.execute(
        """
        ALTER TABLE cars
        SET (
            autovacuum_vacuum_scale_factor = 0.02,
            autovacuum_analyze_scale_factor = 0.01,
            autovacuum_vacuum_threshold = 50000,
            autovacuum_analyze_threshold = 20000
        )
        """
    )

    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_country_total_avail
            ON cars (country, total_price_rub_cached, id)
            WHERE COALESCE(is_available, true)
            """
        )
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_country_price_avail
            ON cars (country, price_rub_cached, id)
            WHERE COALESCE(is_available, true)
            """
        )
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_country_listing_avail
            ON cars (country, listing_sort_ts, id)
            WHERE COALESCE(is_available, true)
            """
        )
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_country_reg_avail
            ON cars (country, reg_sort_key, id)
            WHERE COALESCE(is_available, true)
            """
        )
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_country_mileage_avail
            ON cars (country, mileage, id)
            WHERE COALESCE(is_available, true)
            """
        )
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_source_total_avail
            ON cars (source_id, total_price_rub_cached, id)
            WHERE COALESCE(is_available, true)
            """
        )
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_source_listing_avail
            ON cars (source_id, listing_sort_ts, id)
            WHERE COALESCE(is_available, true)
            """
        )
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_country_brand_avail
            ON cars (country, brand, id)
            WHERE COALESCE(is_available, true)
            """
        )
        op.execute(
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cars_country_brand_model_avail
            ON cars (country, brand, model, id)
            WHERE COALESCE(is_available, true)
            """
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX IF EXISTS idx_cars_country_brand_model_avail")
        op.execute("DROP INDEX IF EXISTS idx_cars_country_brand_avail")
        op.execute("DROP INDEX IF EXISTS idx_cars_source_listing_avail")
        op.execute("DROP INDEX IF EXISTS idx_cars_source_total_avail")
        op.execute("DROP INDEX IF EXISTS idx_cars_country_mileage_avail")
        op.execute("DROP INDEX IF EXISTS idx_cars_country_reg_avail")
        op.execute("DROP INDEX IF EXISTS idx_cars_country_listing_avail")
        op.execute("DROP INDEX IF EXISTS idx_cars_country_price_avail")
        op.execute("DROP INDEX IF EXISTS idx_cars_country_total_avail")

