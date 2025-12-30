from __future__ import annotations

from typing import Dict, Iterable, Optional
from sqlalchemy.orm import Session
from sqlalchemy import select

from ..models import SiteContent


class ContentService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def content_map(self, keys: Optional[Iterable[str]] = None) -> Dict[str, str]:
        query = select(SiteContent)
        if keys:
            query = query.where(SiteContent.key.in_(list(keys)))
        rows = self.db.execute(query).scalars().all()
        return {row.key: row.value for row in rows}

    def upsert_content(self, key: str, value: str, description: str | None = None) -> SiteContent:
        existing = (
            self.db.execute(
                select(SiteContent).where(SiteContent.key == key)
            ).scalar_one_or_none()
        )
        if existing:
            existing.value = value
            if description is not None:
                existing.description = description
            entry = existing
        else:
            entry = SiteContent(key=key, value=value, description=description)
            self.db.add(entry)
        self.db.commit()
        self.db.refresh(entry)
        return entry

    def upsert_bulk(self, data: Dict[str, str]) -> None:
        for key, value in data.items():
            self.upsert_content(key, value)


