"""add inferred specs and reference encyclopedia

Revision ID: 0035_add_inferred_specs
Revises: 0034_add_car_description
Create Date: 2026-03-28
"""

from alembic import op
import sqlalchemy as sa


revision = "0035_add_inferred_specs"
down_revision = "0034_add_car_description"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cars", sa.Column("inferred_engine_cc", sa.Integer(), nullable=True))
    op.add_column("cars", sa.Column("inferred_power_hp", sa.Numeric(10, 2), nullable=True))
    op.add_column("cars", sa.Column("inferred_power_kw", sa.Numeric(10, 2), nullable=True))
    op.add_column("cars", sa.Column("inferred_source_car_id", sa.Integer(), nullable=True))
    op.add_column("cars", sa.Column("inferred_confidence", sa.String(length=16), nullable=True))
    op.add_column("cars", sa.Column("inferred_rule", sa.String(length=64), nullable=True))
    op.add_column("cars", sa.Column("spec_inferred_at", sa.DateTime(), nullable=True))
    op.create_index("ix_cars_inferred_engine_cc", "cars", ["inferred_engine_cc"], unique=False)
    op.create_index("ix_cars_inferred_source_car_id", "cars", ["inferred_source_car_id"], unique=False)

    op.create_table(
        "car_spec_references",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("source_car_id", sa.Integer(), nullable=False),
        sa.Column("car_hash", sa.String(length=128), nullable=True),
        sa.Column("brand_norm", sa.String(length=80), nullable=False),
        sa.Column("model_norm", sa.String(length=120), nullable=False),
        sa.Column("variant_key", sa.String(length=160), nullable=True),
        sa.Column("engine_type_norm", sa.String(length=80), nullable=True),
        sa.Column("body_type_norm", sa.String(length=80), nullable=True),
        sa.Column("year", sa.Integer(), nullable=True),
        sa.Column("engine_cc", sa.Integer(), nullable=True),
        sa.Column("power_hp", sa.Numeric(10, 2), nullable=True),
        sa.Column("power_kw", sa.Numeric(10, 2), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["source_car_id"], ["cars.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_car_id", name="uq_car_spec_references_source_car"),
    )
    op.create_index("ix_car_spec_references_source_car_id", "car_spec_references", ["source_car_id"], unique=False)
    op.create_index("ix_car_spec_references_car_hash", "car_spec_references", ["car_hash"], unique=False)
    op.create_index("ix_car_spec_references_brand_norm", "car_spec_references", ["brand_norm"], unique=False)
    op.create_index("ix_car_spec_references_model_norm", "car_spec_references", ["model_norm"], unique=False)
    op.create_index("ix_car_spec_references_variant_key", "car_spec_references", ["variant_key"], unique=False)
    op.create_index("ix_car_spec_references_engine_type_norm", "car_spec_references", ["engine_type_norm"], unique=False)
    op.create_index("ix_car_spec_references_body_type_norm", "car_spec_references", ["body_type_norm"], unique=False)
    op.create_index("ix_car_spec_references_year", "car_spec_references", ["year"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_car_spec_references_year", table_name="car_spec_references")
    op.drop_index("ix_car_spec_references_body_type_norm", table_name="car_spec_references")
    op.drop_index("ix_car_spec_references_engine_type_norm", table_name="car_spec_references")
    op.drop_index("ix_car_spec_references_variant_key", table_name="car_spec_references")
    op.drop_index("ix_car_spec_references_model_norm", table_name="car_spec_references")
    op.drop_index("ix_car_spec_references_brand_norm", table_name="car_spec_references")
    op.drop_index("ix_car_spec_references_car_hash", table_name="car_spec_references")
    op.drop_index("ix_car_spec_references_source_car_id", table_name="car_spec_references")
    op.drop_table("car_spec_references")

    op.drop_index("ix_cars_inferred_source_car_id", table_name="cars")
    op.drop_index("ix_cars_inferred_engine_cc", table_name="cars")
    op.drop_column("cars", "spec_inferred_at")
    op.drop_column("cars", "inferred_rule")
    op.drop_column("cars", "inferred_confidence")
    op.drop_column("cars", "inferred_source_car_id")
    op.drop_column("cars", "inferred_power_kw")
    op.drop_column("cars", "inferred_power_hp")
    op.drop_column("cars", "inferred_engine_cc")
