from __future__ import annotations

from datetime import datetime
from sqlalchemy import String, Integer, ForeignKey, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .source import Base, Source


class ParserRun(Base):
    __tablename__ = "parser_runs"

    id: Mapped[int] = mapped_column(primary_key=True)
    started_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(nullable=True)
    trigger: Mapped[str] = mapped_column(String(20), nullable=False)  # auto/manual/telegram
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="partial")
    total_seen: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    inserted: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    updated: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    deactivated: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    sources = relationship("ParserRunSource", back_populates="run", cascade="all, delete-orphan")


class ParserRunSource(Base):
    __tablename__ = "parser_run_sources"

    id: Mapped[int] = mapped_column(primary_key=True)
    parser_run_id: Mapped[int] = mapped_column(ForeignKey("parser_runs.id"), nullable=False, index=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), nullable=False, index=True)
    total_seen: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    inserted: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    updated: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    deactivated: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    run = relationship("ParserRun", back_populates="sources")
    source = relationship("Source")


