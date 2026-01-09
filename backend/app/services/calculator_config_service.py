from __future__ import annotations

from typing import Optional
from sqlalchemy import select, func
from sqlalchemy.orm import Session
from ..models import CalculatorConfig


class CalculatorConfigService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def latest(self) -> Optional[CalculatorConfig]:
        stmt = (
            select(CalculatorConfig)
            .order_by(CalculatorConfig.version.desc())
            .limit(1)
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def create(self, payload: dict, source: str = "upload", comment: str | None = None) -> CalculatorConfig:
        latest = self.latest()
        next_version = (latest.version + 1) if latest else 1
        cfg = CalculatorConfig(
            version=next_version,
            payload=payload,
            source=source,
            comment=comment,
        )
        self.db.add(cfg)
        self.db.commit()
        self.db.refresh(cfg)
        return cfg

    def ensure_default_from_path(self, path) -> CalculatorConfig | None:
        """If no configs exist, try to load from provided Excel path."""
        if self.latest():
            return self.latest()
        from .calculator_extractor import CalculatorExtractor
        from pathlib import Path
        p = Path(path)
        if not p.exists():
            return None
        payload = CalculatorExtractor(p).extract()
        return self.create(payload=payload, source="bootstrap", comment=str(p))

    def all_versions(self, limit: int = 20) -> list[CalculatorConfig]:
        stmt = (
            select(CalculatorConfig)
            .order_by(CalculatorConfig.version.desc())
            .limit(limit)
        )
        return list(self.db.execute(stmt).scalars().all())

    def diff_payloads(self, old: dict | None, new: dict) -> dict:
        changes = {}
        def flat(d, prefix=""):
            out = {}
            if isinstance(d, dict):
                for k, v in d.items():
                    out.update(flat(v, f"{prefix}.{k}" if prefix else k))
            elif isinstance(d, list):
                out[prefix] = d
            else:
                out[prefix] = d
            return out
        old_flat = flat(old or {})
        new_flat = flat(new)
        for k, v in new_flat.items():
            if k not in old_flat or old_flat[k] != v:
                changes[k] = {"old": old_flat.get(k), "new": v}
        return changes
