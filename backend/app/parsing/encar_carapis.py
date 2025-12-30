from __future__ import annotations

from typing import Any, Dict, List

from .base import CarParsed, logger
from .config import SiteConfig
from ..integrations.encar_carapis import EncarCarapisAdapter


class EncarCarapisParser:
    def __init__(self, config: SiteConfig) -> None:
        self.config = config
        self.adapter = EncarCarapisAdapter(config)
        self.last_warning = None

    def fetch_items(self, profile: Dict[str, Any]) -> List[CarParsed]:
        try:
            cars = self.adapter.fetch_items(profile)
            self.last_warning = self.adapter.last_warning
            return cars
        except Exception as exc:
            logger.exception("[encar] failed to fetch via carapis: %s", exc)
            self.last_warning = str(exc)
            return []


__all__ = ["EncarCarapisParser"]
