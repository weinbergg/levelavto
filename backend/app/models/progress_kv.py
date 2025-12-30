from __future__ import annotations

from datetime import datetime
from sqlalchemy import String, Integer, DateTime, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .source import Base


class ProgressKV(Base):
    __tablename__ = "progress_kv"
    __table_args__ = (UniqueConstraint("key", name="uq_progress_kv_key"),)

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String(255), nullable=False)
    value: Mapped[str] = mapped_column(String(2048), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow)
