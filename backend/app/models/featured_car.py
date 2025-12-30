from __future__ import annotations

from datetime import datetime
from sqlalchemy import Integer, String, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .source import Base
from .car import Car


class FeaturedCar(Base):
    __tablename__ = "featured_cars"
    __table_args__ = (
        UniqueConstraint("placement", "car_id", name="uq_featured_car_placement"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    placement: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    car_id: Mapped[int] = mapped_column(ForeignKey("cars.id", ondelete="CASCADE"), nullable=False, index=True)
    position: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)

    car = relationship(Car)


