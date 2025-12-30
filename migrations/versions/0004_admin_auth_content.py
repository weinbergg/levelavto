"""users, site_content, featured_cars

Revision ID: 0004_admin_auth_content
Revises: 0003_car_images
Create Date: 2025-12-12
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0004_admin_auth_content"
down_revision = "0003_car_images"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("email", sa.String(length=255), nullable=False, unique=True, index=True),
        sa.Column("full_name", sa.String(length=255), nullable=True),
        sa.Column("password_hash", sa.String(length=255), nullable=False),
        sa.Column("is_admin", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("last_login_at", sa.DateTime(), nullable=True),
    )
    op.create_index("ix_users_is_admin", "users", ["is_admin"])
    op.create_index("ix_users_is_active", "users", ["is_active"])

    op.create_table(
        "site_content",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("key", sa.String(length=100), nullable=False, unique=True, index=True),
        sa.Column("value", sa.Text(), nullable=False, server_default=""),
        sa.Column("description", sa.String(length=255), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )

    op.create_table(
        "featured_cars",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("placement", sa.String(length=50), nullable=False, index=True),
        sa.Column("car_id", sa.Integer(), sa.ForeignKey("cars.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("position", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("placement", "car_id", name="uq_featured_car_placement"),
    )
    op.create_index("ix_featured_cars_is_active", "featured_cars", ["is_active"])


def downgrade() -> None:
    op.drop_index("ix_featured_cars_is_active", table_name="featured_cars")
    op.drop_table("featured_cars")
    op.drop_table("site_content")
    op.drop_index("ix_users_is_active", table_name="users")
    op.drop_index("ix_users_is_admin", table_name="users")
    op.drop_table("users")


