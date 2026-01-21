from __future__ import annotations

from typing import Optional, Dict, List, Type, Any, Tuple
from datetime import datetime
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import select
from ..db import SessionLocal
from ..models import Source, SearchProfile
from ..models.parser_run import ParserRun, ParserRunSource
from ..parsing.config import load_sites_config, SiteConfig
from ..parsing.base import CarParsed
from ..parsing.mobile_de import MobileDeParser
from ..parsing.encar_carapis import EncarCarapisParser
from ..parsing.emavto_klg import EmAvtoKlgParser
from .parsing_data_service import ParsingDataService


class ParserRunSummary(BaseModel):
    run_id: int
    started_at: datetime
    finished_at: datetime
    trigger: str
    status: str
    totals: Dict[str, int]
    per_source: Dict[str, Dict[str, int]]
    error_message: Optional[str] = None


PARSER_CLASSES: Dict[str, Type] = {
    "mobile_de": MobileDeParser,
    "encar": EncarCarapisParser,
    "emavto_klg": EmAvtoKlgParser,
}


class ParserRunner:
    def __init__(self, db_session_factory=SessionLocal):
        self.db_session_factory = db_session_factory
        self.sites_config = load_sites_config()

    def _get_profiles(self, db: Session, source: Optional[Source], profile_ids: Optional[List[int]]) -> List[SearchProfile]:
        stmt = select(SearchProfile).where(SearchProfile.is_active.is_(True))
        if profile_ids:
            stmt = stmt.where(SearchProfile.id.in_(profile_ids))
        elif source:
            # Source-specific + generic (null)
            stmt = stmt.where((SearchProfile.source_id == source.id) | (
                SearchProfile.source_id.is_(None)))
        return list(db.execute(stmt).scalars().all())

    def _make_profile_payload(self, profile: SearchProfile) -> Dict[str, Any]:
        return {
            "brand": None if not profile.brands else profile.brands,
            "country": profile.countries,
            "price_min": float(profile.min_price) if profile.min_price is not None else None,
            "price_max": float(profile.max_price) if profile.max_price is not None else None,
            "year_min": profile.min_year,
            "year_max": profile.max_year,
            "mileage_max": profile.max_mileage,
        }

    def _parser_for(self, site_cfg: SiteConfig):
        ParserCls = PARSER_CLASSES.get(site_cfg.key)
        if not ParserCls:
            raise ValueError(f"No parser class for {site_cfg.key}")
        return ParserCls(site_cfg)

    def run_all(
        self,
        trigger: str = "manual",
        source_keys: Optional[List[str]] = None,
        search_profile_ids: Optional[List[int]] = None,
        mode: Optional[str] = None,
    ) -> ParserRunSummary:
        db = self.db_session_factory()
        try:
            run = ParserRun(started_at=datetime.utcnow(),
                            trigger=trigger, status="partial")
            db.add(run)
            db.commit()
            db.refresh(run)

            per_source: Dict[str, Dict[str, int]] = {}
            total_seen = inserted_total = updated_total = deactivated_total = 0
            failures = 0
            successes = 0
            keys = source_keys or list(self.sites_config.sites.keys())
            for key in keys:
                site_cfg = self.sites_config.get(key)
                if not site_cfg.enabled:
                    # Skip disabled source
                    per_source[key] = {
                        "total_seen": 0, "inserted": 0, "updated": 0, "deactivated": 0}
                    continue
                try:
                    data = self._run_for_source(
                        db, run, site_cfg, search_profile_ids, mode)
                    per_source[key] = data
                    total_seen += data["total_seen"]
                    inserted_total += data["inserted"]
                    updated_total += data["updated"]
                    deactivated_total += data["deactivated"]
                    successes += 1
                except Exception as exc:
                    db.rollback()
                    failures += 1
                    msg = f"source={key} failed: {type(exc).__name__}: {exc}"
                    if run.error_message:
                        run.error_message += f" | {msg}"
                    else:
                        run.error_message = msg
                    # Record zero stats row for failed source
                    prs = ParserRunSource(
                        parser_run_id=run.id,
                        source_id=self._ensure_source_id(db, site_cfg),
                        total_seen=0,
                        inserted=0,
                        updated=0,
                        deactivated=0,
                    )
                    db.add(prs)
                    db.commit()
            # Determine overall status
            if failures == 0 and successes > 0:
                run.status = "success"
            elif successes > 0 and failures > 0:
                run.status = "partial"
            else:
                run.status = "failed"
            run.finished_at = datetime.utcnow()
            run.total_seen = total_seen
            run.inserted = inserted_total
            run.updated = updated_total
            run.deactivated = deactivated_total
            db.commit()

            return ParserRunSummary(
                run_id=run.id,
                started_at=run.started_at,
                finished_at=run.finished_at or datetime.utcnow(),
                trigger=run.trigger,
                status=run.status,
                totals={
                    "total_seen": total_seen,
                    "inserted": inserted_total,
                    "updated": updated_total,
                    "deactivated": deactivated_total,
                },
                per_source=per_source,
                error_message=run.error_message,
            )
        finally:
            db.close()

    def run_for_source(
        self,
        source_key: str,
        trigger: str = "manual",
        search_profile_ids: Optional[List[int]] = None,
        mode: Optional[str] = None,
    ) -> ParserRunSummary:
        return self.run_all(
            trigger=trigger,
            source_keys=[source_key],
            search_profile_ids=search_profile_ids,
            mode=mode,
        )

    def _run_for_source(
        self,
        db: Session,
        run: ParserRun,
        site_cfg: SiteConfig,
        search_profile_ids: Optional[List[int]],
        mode: Optional[str],
    ) -> Dict[str, int]:
        print(f"[parser] start source={site_cfg.key} mode={mode or 'default'}")
        data_service = ParsingDataService(db)
        source = data_service.ensure_source(
            key=site_cfg.key, name=site_cfg.name, country=site_cfg.country, base_url=site_cfg.base_search_url
        )
        profiles = self._get_profiles(db, source, search_profile_ids)
        if not profiles:
            # Use a default generic profile when none defined
            profiles = [
                SearchProfile(
                    name=f"default_{site_cfg.key}",
                    source_id=source.id,
                    is_active=True,
                )
            ]
        parser = self._parser_for(site_cfg)
        seen_all: List[str] = []
        inserted = updated = total_seen = 0
        for p in profiles:
            payload = self._make_profile_payload(p)
            if mode:
                payload["mode"] = mode
            if site_cfg.key == "emavto_klg" and mode == "full":
                last_page = data_service.get_progress(
                    f"{site_cfg.key}.last_page_full")
                if last_page:
                    try:
                        payload["resume_page_full"] = int(last_page) + 1
                    except ValueError:
                        pass
            parsed = parser.fetch_items(payload)
            total_seen += len(parsed)
            inserted_i, updated_i, seen_i = data_service.upsert_parsed_items(
                source, [c.as_dict() for c in parsed])
            inserted += inserted_i
            updated += updated_i
            seen_all.extend([c.external_id for c in parsed])
        # Record advisory warning from parser if any (e.g. mobile.de 403)
        if getattr(parser, "last_warning", None):
            warn = f"{site_cfg.key}: {parser.last_warning}"
            run.error_message = (run.error_message +
                                 " | " if run.error_message else "") + warn
            db.commit()
        if site_cfg.key == "emavto_klg" and getattr(parser, "progress", None):
            if "last_page_full" in parser.progress:
                data_service.set_progress(f"{site_cfg.key}.last_page_full", str(
                    parser.progress["last_page_full"]))
            if "last_incremental_run_at" in parser.progress:
                data_service.set_progress(
                    f"{site_cfg.key}.last_incremental_run_at", str(
                        parser.progress["last_incremental_run_at"])
                )
        deactivated = data_service.deactivate_missing(source, seen_all)
        prs = ParserRunSource(
            parser_run_id=run.id,
            source_id=source.id,
            total_seen=total_seen,
            inserted=inserted,
            updated=updated,
            deactivated=deactivated,
        )
        db.add(prs)
        db.commit()
        print(
            f"[parser] done source={site_cfg.key} seen={total_seen} ins={inserted} upd={updated} deact={deactivated}"
        )
        return {
            "total_seen": total_seen,
            "inserted": inserted,
            "updated": updated,
            "deactivated": deactivated,
        }

    def _ensure_source_id(self, db: Session, site_cfg: SiteConfig) -> int:
        # helper to get or create source to be able to record a failed row
        data_service = ParsingDataService(db)
        source = data_service.ensure_source(
            key=site_cfg.key, name=site_cfg.name, country=site_cfg.country, base_url=site_cfg.base_search_url
        )
        return source.id
