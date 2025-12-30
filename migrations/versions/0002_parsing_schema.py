from __future__ import annotations

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0002_parsing_schema"
down_revision = "0001_init"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # sources: add key
    with op.batch_alter_table("sources") as batch_op:
        batch_op.add_column(sa.Column("key", sa.String(length=100), nullable=True))
        batch_op.create_unique_constraint("uq_sources_key", ["key"])
    # Backfill 'key' from name if possible
    op.execute("UPDATE sources SET key = REPLACE(name, '.', '_') WHERE key IS NULL")
    with op.batch_alter_table("sources") as batch_op:
        batch_op.alter_column("key", nullable=False)

    # cars: add new columns and rename url->source_url
    with op.batch_alter_table("cars") as batch_op:
        # rename url to source_url if column exists
        conn = op.get_bind()
        inspector = sa.inspect(conn)
        cols = [c["name"] for c in inspector.get_columns("cars")]
        if "url" in cols:
            batch_op.alter_column("url", new_column_name="source_url", existing_type=sa.String(length=500))
        batch_op.add_column(sa.Column("thumbnail_local_path", sa.String(length=500), nullable=True))
        batch_op.add_column(sa.Column("hash", sa.String(length=128), nullable=True))
        batch_op.add_column(sa.Column("first_seen_at", sa.DateTime(), nullable=True))
        batch_op.add_column(sa.Column("last_seen_at", sa.DateTime(), nullable=True))
        batch_op.create_index("ix_cars_hash", ["hash"])

    # search_profiles
    op.create_table(
        "search_profiles",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=120), nullable=False, unique=True),
        sa.Column("source_id", sa.Integer(), sa.ForeignKey("sources.id"), nullable=True),
        sa.Column("countries", sa.String(length=64), nullable=True),
        sa.Column("brands", sa.String(length=512), nullable=True),
        sa.Column("min_price", sa.Numeric(12, 2), nullable=True),
        sa.Column("max_price", sa.Numeric(12, 2), nullable=True),
        sa.Column("min_year", sa.Integer(), nullable=True),
        sa.Column("max_year", sa.Integer(), nullable=True),
        sa.Column("max_mileage", sa.Integer(), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default=sa.true(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_search_profiles_is_active", "search_profiles", ["is_active"])

    # parser runs
    op.create_table(
        "parser_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("started_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("trigger", sa.String(length=20), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="partial"),
        sa.Column("total_seen", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("inserted", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("updated", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("deactivated", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_message", sa.Text(), nullable=True),
    )
    op.create_table(
        "parser_run_sources",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("parser_run_id", sa.Integer(), sa.ForeignKey("parser_runs.id"), nullable=False),
        sa.Column("source_id", sa.Integer(), sa.ForeignKey("sources.id"), nullable=False),
        sa.Column("total_seen", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("inserted", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("updated", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("deactivated", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_index("ix_prs_parser_run_id", "parser_run_sources", ["parser_run_id"])
    op.create_index("ix_prs_source_id", "parser_run_sources", ["source_id"])


def downgrade() -> None:
    op.drop_index("ix_prs_source_id", table_name="parser_run_sources")
    op.drop_index("ix_prs_parser_run_id", table_name="parser_run_sources")
    op.drop_table("parser_run_sources")
    op.drop_table("parser_runs")
    op.drop_index("ix_search_profiles_is_active", table_name="search_profiles")
    op.drop_table("search_profiles")
    with op.batch_alter_table("cars") as batch_op:
        batch_op.drop_index("ix_cars_hash")
        batch_op.drop_column("last_seen_at")
        batch_op.drop_column("first_seen_at")
        batch_op.drop_column("hash")
        batch_op.drop_column("thumbnail_local_path")
        # rename source_url back to url
        conn = op.get_bind()
        inspector = sa.inspect(conn)
        cols = [c["name"] for c in inspector.get_columns("cars")]
        if "source_url" in cols:
            batch_op.alter_column("source_url", new_column_name="url", existing_type=sa.String(length=500))
    with op.batch_alter_table("sources") as batch_op:
        batch_op.drop_constraint("uq_sources_key", type_="unique")
        batch_op.drop_column("key")


