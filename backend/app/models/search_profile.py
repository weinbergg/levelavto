from __future__ import annotations

from datetime import datetime
from sqlalchemy import String, Integer, Numeric, Boolean, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .source import Base, Source


class SearchProfile(Base):
    __tablename__ = "search_profiles"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)
    source_id: Mapped[int | None] = mapped_column(ForeignKey("sources.id"), nullable=True)
    countries: Mapped[str | None] = mapped_column(String(64), nullable=True)  # simple CSV or single country
    brands: Mapped[str | None] = mapped_column(String(512), nullable=True)    # CSV
    min_price: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    max_price: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    min_year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_mileage: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    source = relationship("Source")


