from __future__ import annotations

from datetime import datetime
from typing import Iterable, List, Optional

from sqlalchemy import select, func, update
from sqlalchemy.orm import Session

from ..models import Notification, User


class NotificationService:
    """CRUD wrapper around the ``notifications`` table.

    Kept small on purpose: every public method either returns Notification
    rows or scalars, and we centralise the unread bookkeeping here so the
    rest of the app does not have to remember to update ``read_at``.
    """

    def __init__(self, db: Session) -> None:
        self.db = db

    def list_for_user(
        self,
        user_id: int,
        *,
        limit: int = 50,
        only_unread: bool = False,
    ) -> List[Notification]:
        stmt = (
            select(Notification)
            .where(Notification.user_id == user_id)
            .order_by(Notification.created_at.desc())
            .limit(limit)
        )
        if only_unread:
            stmt = stmt.where(Notification.is_read.is_(False))
        return list(self.db.execute(stmt).scalars().all())

    def unread_count(self, user_id: int) -> int:
        return int(
            self.db.execute(
                select(func.count(Notification.id)).where(
                    Notification.user_id == user_id,
                    Notification.is_read.is_(False),
                )
            ).scalar_one()
            or 0
        )

    def mark_read(self, user_id: int, notif_ids: Optional[Iterable[int]] = None) -> int:
        stmt = update(Notification).where(
            Notification.user_id == user_id,
            Notification.is_read.is_(False),
        )
        if notif_ids is not None:
            ids = [int(x) for x in notif_ids if x]
            if not ids:
                return 0
            stmt = stmt.where(Notification.id.in_(ids))
        stmt = stmt.values(is_read=True, read_at=datetime.utcnow())
        result = self.db.execute(stmt)
        self.db.commit()
        return int(result.rowcount or 0)

    def send(
        self,
        *,
        user_id: int,
        sender: Optional[User],
        title: str,
        body: str,
        attached_car_ids: Optional[List[int]] = None,
        kind: str = "manual",
    ) -> Notification:
        title_clean = (title or "").strip()[:255] or "Сообщение от LevelAvto"
        body_clean = (body or "").strip()
        if not body_clean:
            raise ValueError("Тело сообщения не может быть пустым")
        notif = Notification(
            user_id=user_id,
            sender_admin_id=sender.id if sender else None,
            title=title_clean,
            body=body_clean,
            kind=kind,
            attached_car_ids=list(attached_car_ids) if attached_car_ids else None,
        )
        self.db.add(notif)
        self.db.commit()
        self.db.refresh(notif)
        return notif

    def latest_inbox_summary(self, user_id: int, *, limit: int = 5) -> List[Notification]:
        return self.list_for_user(user_id, limit=limit, only_unread=False)
