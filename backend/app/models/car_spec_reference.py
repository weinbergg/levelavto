from __future__ import annotations

from datetime import datetime

from sqlalchemy import ForeignKey, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .source import Base


class CarSpecReference(Base):
    __tablename__ = "car_spec_references"
    __table_args__ = (
        UniqueConstraint("source_car_id", name="uq_car_spec_references_source_car"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    source_car_id: Mapped[int] = mapped_column(ForeignKey("cars.id", ondelete="CASCADE"), nullable=False, index=True)
    car_hash: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    brand_norm: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    model_norm: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    variant_key: Mapped[str | None] = mapped_column(String(160), nullable=True, index=True)
    engine_type_norm: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    body_type_norm: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    year: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    engine_cc: Mapped[int | None] = mapped_column(Integer, nullable=True)
    power_hp: Mapped[float | None] = mapped_column(Numeric(10, 2), nullable=True)
    power_kw: Mapped[float | None] = mapped_column(Numeric(10, 2), nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
