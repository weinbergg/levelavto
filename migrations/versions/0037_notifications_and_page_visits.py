"""notifications + page_visits tables

Revision ID: 0037_notif_pagevisits
Revises: 0036_idx_engine_price_coalesce
Create Date: 2026-05-01
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0037_notif_pagevisits"
down_revision = "0036_idx_engine_price_coalesce"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "notifications",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "user_id",
            sa.Integer(),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        sa.Column(
            "sender_admin_id",
            sa.Integer(),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("title", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("body", sa.Text(), nullable=False, server_default=""),
        sa.Column("kind", sa.String(length=32), nullable=False, server_default="manual"),
        sa.Column("attached_car_ids", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("is_read", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("read_at", sa.DateTime(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_notifications_user_unread",
        "notifications",
        ["user_id", "is_read", "created_at"],
    )

    op.create_table(
        "page_visits",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("visitor_id", sa.String(length=64), nullable=False, index=True),
        sa.Column(
            "user_id",
            sa.Integer(),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
            index=True,
        ),
        sa.Column("path", sa.String(length=500), nullable=False),
        sa.Column("query", sa.String(length=1000), nullable=True),
        sa.Column("referer", sa.String(length=500), nullable=True),
        sa.Column("user_agent", sa.String(length=500), nullable=True),
        sa.Column("is_bot", sa.Boolean(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.now(),
            index=True,
        ),
    )
    op.create_index(
        "ix_page_visits_created_at_path",
        "page_visits",
        ["created_at", "path"],
    )
    op.create_index(
        "ix_page_visits_visitor_created",
        "page_visits",
        ["visitor_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_page_visits_visitor_created", table_name="page_visits")
    op.drop_index("ix_page_visits_created_at_path", table_name="page_visits")
    op.drop_table("page_visits")
    op.drop_index("ix_notifications_user_unread", table_name="notifications")
    op.drop_table("notifications")
