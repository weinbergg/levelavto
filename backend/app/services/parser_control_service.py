from __future__ import annotations

from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import select
from ..models import SearchProfile
from ..models.parser_run import ParserRun, ParserRunSource


class ParserControlService:
    def __init__(self, db: Session) -> None:
        self.db = db

    # Profiles
    def list_search_profiles(self) -> List[SearchProfile]:
        stmt = select(SearchProfile).order_by(SearchProfile.id.desc())
        return list(self.db.execute(stmt).scalars().all())

    def get_search_profile(self, profile_id: int) -> Optional[SearchProfile]:
        stmt = select(SearchProfile).where(SearchProfile.id == profile_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def create_search_profile(self, data: Dict[str, Any]) -> SearchProfile:
        profile = SearchProfile(**data)
        self.db.add(profile)
        self.db.commit()
        self.db.refresh(profile)
        return profile

    def update_search_profile(self, profile_id: int, data: Dict[str, Any]) -> Optional[SearchProfile]:
        profile = self.get_search_profile(profile_id)
        if not profile:
            return None
        for k, v in data.items():
            if hasattr(profile, k):
                setattr(profile, k, v)
        self.db.commit()
        self.db.refresh(profile)
        return profile

    def set_search_profile_active(self, profile_id: int, active: bool) -> bool:
        profile = self.get_search_profile(profile_id)
        if not profile:
            return False
        profile.is_active = active
        self.db.commit()
        return True

    # Runs
    def list_recent_parser_runs(self, limit: int = 10) -> List[ParserRun]:
        stmt = select(ParserRun).order_by(ParserRun.id.desc()).limit(limit)
        return list(self.db.execute(stmt).scalars().all())

    def get_parser_run_summary(self, run_id: int) -> Optional[Dict[str, Any]]:
        run = self.db.execute(select(ParserRun).where(ParserRun.id == run_id)).scalar_one_or_none()
        if not run:
            return None
        srcs = self.db.execute(select(ParserRunSource).where(ParserRunSource.parser_run_id == run_id)).scalars().all()
        return {
            "run_id": run.id,
            "started_at": run.started_at,
            "finished_at": run.finished_at,
            "trigger": run.trigger,
            "status": run.status,
            "totals": {
                "total_seen": run.total_seen,
                "inserted": run.inserted,
                "updated": run.updated,
                "deactivated": run.deactivated,
            },
            "per_source": {
                str(s.source_id): {
                    "total_seen": s.total_seen,
                    "inserted": s.inserted,
                    "updated": s.updated,
                    "deactivated": s.deactivated,
                }
                for s in srcs
            },
            "error_message": run.error_message,
        }


