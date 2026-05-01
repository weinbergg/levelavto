from __future__ import annotations

from datetime import datetime
from sqlalchemy import String, Integer, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column

from .source import Base


class PageVisit(Base):
    """One row per server-rendered page hit (with cookie consent).

    Aggregations rolled up by the analytics page; raw rows are kept for
    180 days and then dropped by a scheduled cleanup. The middleware
    only writes here when the visitor accepted the analytics cookie.
    """

    __tablename__ = "page_visits"

    id: Mapped[int] = mapped_column(primary_key=True)
    # Stable random id stored in a first-party cookie, lets us count
    # unique visitors without storing IPs or any other PII.
    visitor_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    user_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    path: Mapped[str] = mapped_column(String(500), nullable=False)
    # Strip query string out of `path` and stash it here so we can group
    # by route in the dashboard but still see filter usage if needed.
    query: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    referer: Mapped[str | None] = mapped_column(String(500), nullable=True)
    user_agent: Mapped[str | None] = mapped_column(String(500), nullable=True)
    is_bot: Mapped[bool | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        default=datetime.utcnow, nullable=False, index=True
    )

    __table_args__ = (
        Index("ix_page_visits_created_at_path", "created_at", "path"),
        Index("ix_page_visits_visitor_created", "visitor_id", "created_at"),
    )
