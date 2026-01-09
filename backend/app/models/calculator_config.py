from __future__ import annotations

from datetime import datetime
from sqlalchemy import Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column
from .source import Base


class CalculatorConfig(Base):
    __tablename__ = "calculator_configs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, index=True, unique=True)
    comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    source: Mapped[str | None] = mapped_column(String(50), nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
