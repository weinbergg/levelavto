"""add performance indexes for counts and price sort

Revision ID: 0014_perf_indexes
Revises: 0013_calc_cache
Create Date: 2026-01-13 00:00:00
"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "0014_perf_indexes"
down_revision = "0013_calc_cache"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # индекс для быстрых count по доступным авто и стране
    op.create_index(
        "idx_cars_available_country",
        "cars",
        ["is_available", "country"],
        postgresql_where=None,
    )
    # индекс для быстрых count/фильтрации по источнику (используется для регионов)
    op.create_index(
        "idx_cars_available_source",
        "cars",
        ["is_available", "source_id"],
        postgresql_where=None,
    )
    # индекс для сортировки/фильтрации по цене (числовое поле в рублях)
    op.create_index(
        "idx_cars_price_sort",
        "cars",
        ["is_available", "total_price_rub_cached", "price_rub_cached"],
        postgresql_where=None,
    )


def downgrade() -> None:
    op.drop_index("idx_cars_price_sort", table_name="cars")
    op.drop_index("idx_cars_available_source", table_name="cars")
    op.drop_index("idx_cars_available_country", table_name="cars")
