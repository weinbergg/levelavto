from __future__ import annotations

from datetime import datetime
from sqlalchemy import String, Text, Integer, ForeignKey, Boolean, JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .source import Base


class Notification(Base):
    """In-app message sent by an admin to a particular user.

    Stored separately from email/SMS so the user always has a record
    inside their account, even if delivery channels fail. The admin
    composes once; we render the message inside ``/account`` and
    optionally also fire-and-forget an email copy.
    """

    __tablename__ = "notifications"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    sender_admin_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )

    # Short subject shown in the inbox list.
    title: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    # Body is plain text with newlines preserved; we render it with
    # ``nl2br`` in the template, no HTML allowed in compose form.
    body: Mapped[str] = mapped_column(Text, nullable=False, default="")
    kind: Mapped[str] = mapped_column(String(32), nullable=False, default="manual")
    # Optional list of attached car ids, JSON-encoded for portability.
    attached_car_ids: Mapped[list | None] = mapped_column(JSON, nullable=True)

    is_read: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)
    read_at: Mapped[datetime | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow, nullable=False, index=True)

    user = relationship("User", foreign_keys=[user_id])
    sender = relationship("User", foreign_keys=[sender_admin_id])
