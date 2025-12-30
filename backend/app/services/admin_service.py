from __future__ import annotations

from typing import Dict, Iterable, List
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from ..models import FeaturedCar, Car, SiteContent


class AdminService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def set_site_content(self, values: Dict[str, str]) -> None:
        from .content_service import ContentService  # lazy import to avoid cycles

        content = ContentService(self.db)
        content.upsert_bulk(values)

    def list_site_content(self, keys: Iterable[str] | None = None) -> Dict[str, str]:
        from .content_service import ContentService

        return ContentService(self.db).content_map(keys)

    def list_featured(self, placement: str) -> List[FeaturedCar]:
        stmt = (
            select(FeaturedCar)
            .options(selectinload(FeaturedCar.car))
            .where(FeaturedCar.placement == placement)
            .order_by(FeaturedCar.position.asc(), FeaturedCar.id.asc())
        )
        return list(self.db.execute(stmt).scalars().all())

    def set_featured(self, placement: str, car_ids: List[int]) -> List[FeaturedCar]:
        unique_ids: List[int] = []
        seen = set()
        for cid in car_ids:
            if cid and cid not in seen:
                unique_ids.append(cid)
                seen.add(cid)

        # ensure cars exist
        if unique_ids:
            existing_cars = {
                c.id: c
                for c in self.db.execute(select(Car).where(Car.id.in_(unique_ids))).scalars().all()
            }
            unique_ids = [cid for cid in unique_ids if cid in existing_cars]

        existing = {
            fc.car_id: fc
            for fc in self.db.execute(
                select(FeaturedCar).where(FeaturedCar.placement == placement)
            ).scalars()
        }

        position = 1
        for cid in unique_ids:
            if cid in existing:
                fc = existing[cid]
                fc.position = position
                fc.is_active = True
            else:
                self.db.add(
                    FeaturedCar(
                        placement=placement,
                        car_id=cid,
                        position=position,
                        is_active=True,
                    )
                )
            position += 1

        # deactivate entries that are not requested
        for cid, fc in existing.items():
            if cid not in unique_ids:
                fc.is_active = False

        self.db.commit()
        return self.list_featured(placement)


