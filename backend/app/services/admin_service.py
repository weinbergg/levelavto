from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List
from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from ..models import FeaturedCar, Car, PageVisit, SiteContent, User, Favorite


class AdminService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def overview_stats(self) -> Dict[str, int]:
        """Lightweight summary numbers shown on the admin dashboard.

        Each row touches a single small index/aggregate, so the call stays
        well under the SSR budget even on the production DB.
        """
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        today_start = datetime(now.year, now.month, now.day)
        week_start = now - timedelta(days=7)

        users_total = self.db.execute(select(func.count(User.id))).scalar_one() or 0
        users_today = (
            self.db.execute(
                select(func.count(User.id)).where(User.created_at >= today_start)
            ).scalar_one()
            or 0
        )
        users_week = (
            self.db.execute(
                select(func.count(User.id)).where(User.created_at >= week_start)
            ).scalar_one()
            or 0
        )
        users_verified = (
            self.db.execute(
                select(func.count(User.id)).where(
                    (User.email_verified_at.is_not(None)) | (User.phone_verified_at.is_not(None))
                )
            ).scalar_one()
            or 0
        )
        favorites_total = self.db.execute(select(func.count(Favorite.id))).scalar_one() or 0
        favorites_week = (
            self.db.execute(
                select(func.count(Favorite.id)).where(Favorite.created_at >= week_start)
            ).scalar_one()
            or 0
        )
        return {
            "users_total": int(users_total),
            "users_today": int(users_today),
            "users_week": int(users_week),
            "users_verified": int(users_verified),
            "favorites_total": int(favorites_total),
            "favorites_week": int(favorites_week),
        }

    def traffic_overview(self, *, days: int = 30) -> Dict[str, object]:
        """Aggregate ``page_visits`` rows for the analytics dashboard.

        Returns counts for the requested window plus a per-day timeline
        and a breakdown by top routes. The middleware drops anything
        without cookie consent, so these numbers reflect only visitors
        who opted in to analytics — that matches the legal expectation.
        """

        days = max(1, min(int(days or 30), 90))
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        since = now - timedelta(days=days)
        today_start = datetime(now.year, now.month, now.day)
        week_start = now - timedelta(days=7)

        def _count(stmt) -> int:
            return int(self.db.execute(stmt).scalar_one() or 0)

        visits_total = _count(
            select(func.count(PageVisit.id)).where(PageVisit.created_at >= since)
        )
        visits_today = _count(
            select(func.count(PageVisit.id)).where(PageVisit.created_at >= today_start)
        )
        visits_week = _count(
            select(func.count(PageVisit.id)).where(PageVisit.created_at >= week_start)
        )
        visitors_unique = _count(
            select(func.count(func.distinct(PageVisit.visitor_id))).where(
                PageVisit.created_at >= since
            )
        )
        visitors_unique_today = _count(
            select(func.count(func.distinct(PageVisit.visitor_id))).where(
                PageVisit.created_at >= today_start
            )
        )

        # Per-day timeline (most recent first), trimmed to the window.
        day_col = func.date_trunc("day", PageVisit.created_at).label("d")
        timeline_rows = self.db.execute(
            select(
                day_col,
                func.count(PageVisit.id).label("visits"),
                func.count(func.distinct(PageVisit.visitor_id)).label("uniq"),
            )
            .where(PageVisit.created_at >= since)
            .group_by(day_col)
            .order_by(day_col.desc())
        ).all()
        timeline = [
            {
                "day": row.d.strftime("%Y-%m-%d") if row.d else "",
                "visits": int(row.visits or 0),
                "unique": int(row.uniq or 0),
            }
            for row in timeline_rows
        ]

        top_pages_rows = self.db.execute(
            select(
                PageVisit.path,
                func.count(PageVisit.id).label("visits"),
                func.count(func.distinct(PageVisit.visitor_id)).label("uniq"),
            )
            .where(PageVisit.created_at >= since)
            .group_by(PageVisit.path)
            .order_by(func.count(PageVisit.id).desc())
            .limit(15)
        ).all()
        top_pages = [
            {
                "path": row.path or "/",
                "visits": int(row.visits or 0),
                "unique": int(row.uniq or 0),
            }
            for row in top_pages_rows
        ]

        return {
            "days": days,
            "visits_total": visits_total,
            "visits_today": visits_today,
            "visits_week": visits_week,
            "visitors_unique": visitors_unique,
            "visitors_unique_today": visitors_unique_today,
            "timeline": timeline,
            "top_pages": top_pages,
        }

    def recent_users(self, *, days: int = 7, limit: int = 20) -> List[User]:
        since = datetime.utcnow() - timedelta(days=max(1, days))
        stmt = (
            select(User)
            .where(User.created_at >= since)
            .order_by(User.created_at.desc())
            .limit(max(1, limit))
        )
        return list(self.db.execute(stmt).scalars().all())

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


