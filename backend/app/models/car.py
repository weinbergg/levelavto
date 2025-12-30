from __future__ import annotations

from datetime import datetime
from sqlalchemy import String, Integer, Numeric, Boolean, ForeignKey, UniqueConstraint, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .source import Base


class Car(Base):
    __tablename__ = "cars"
    __table_args__ = (
        UniqueConstraint("source_id", "external_id", name="uq_cars_source_external"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), nullable=False, index=True)
    external_id: Mapped[str] = mapped_column(String(128), nullable=False)
    country: Mapped[str] = mapped_column(String(2), nullable=False, index=True)
    brand: Mapped[str] = mapped_column(String(80), nullable=True, index=True)
    model: Mapped[str] = mapped_column(String(120), nullable=True, index=True)
    generation: Mapped[str | None] = mapped_column(String(120), nullable=True)
    year: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    mileage: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    price: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True, index=True)
    currency: Mapped[str | None] = mapped_column(String(3), nullable=True)
    body_type: Mapped[str | None] = mapped_column(String(80), nullable=True)
    engine_type: Mapped[str | None] = mapped_column(String(80), nullable=True)
    transmission: Mapped[str | None] = mapped_column(String(80), nullable=True)
    drive_type: Mapped[str | None] = mapped_column(String(80), nullable=True)
    color: Mapped[str | None] = mapped_column(String(80), nullable=True)
    vin: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    thumbnail_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    thumbnail_local_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    hash: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    first_seen_at: Mapped[datetime | None] = mapped_column(nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    is_available: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False, index=True)

    source = relationship("Source")
    images = relationship("CarImage", back_populates="car", cascade="all, delete-orphan")


