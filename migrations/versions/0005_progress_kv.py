"""add progress kv table"""

from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "0005_progress_kv"
down_revision: Union[str, None] = "0004_admin_auth_content"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "progress_kv",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("key", sa.String(length=255), nullable=False),
        sa.Column("value", sa.String(length=2048), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False,
                  server_default=sa.func.now()),
        sa.UniqueConstraint("key", name="uq_progress_kv_key"),
    )


def downgrade() -> None:
    op.drop_table("progress_kv")
