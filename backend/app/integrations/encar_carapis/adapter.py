from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

from ...config import settings
from ...parsing.base import CarParsed, logger
from ...parsing.config import SiteConfig
from .client import EncarCarapisClient, RetryableError
from .mapper import map_vehicle_detail


class EncarCarapisAdapter:
    def __init__(self, config: SiteConfig) -> None:
        self.config = config
        self.currency = config.defaults.get("currency") or "KRW"
        self.default_brands: List[str] = list(
            config.defaults.get(
                "brands",
                ["BMW", "Audi", "Mercedes-Benz", "Volkswagen"],
            )
        )
        self.limit_per_query = int(
            config.defaults.get("limit_per_query") or 50)
        self.max_pages = int(config.defaults.get(
            "max_pages") or config.pagination.max_pages or 2)
        self.detail_delay = float(config.defaults.get("detail_delay") or 0.2)
        self.last_warning: Optional[str] = None
        self._manufacturer_map: Optional[Dict[str, str]] = None
        self.client = EncarCarapisClient(api_key=self._resolve_api_key())

    def _resolve_api_key(self) -> Optional[str]:
        return (
            settings.ENCAR_CARAPIS_API_KEY
            or os.getenv("CARAPIS_API_KEY")
            or os.getenv("ENCAR_API_KEY")
        )

    def _load_manufacturer_map(self) -> Dict[str, str]:
        if self._manufacturer_map is not None:
            return self._manufacturer_map
        page = 1
        mapping: Dict[str, str] = {}
        while True:
            data = self.client.list_manufacturers(limit=200, page=page)
            for item in data.get("results", []) or []:
                name = (item.get("name") or "").lower()
                slug = item.get("slug")
                if name and slug:
                    mapping[name] = slug
            pages_total = data.get("pages") or 1
            if page >= pages_total:
                break
            page += 1
        self._manufacturer_map = mapping
        return mapping

    def _brand_queries(self, brands: List[str]) -> List[Dict[str, Optional[str]]]:
        try:
            mapping = self._load_manufacturer_map()
        except RetryableError as exc:
            msg = f"[{self.config.key}] manufacturer lookup failed, falling back to search-only: {exc}"
            logger.warning(msg)
            self.last_warning = msg
            mapping = {}
        queries = []
        for raw in brands:
            name = raw.strip()
            if not name:
                continue
            slug = mapping.get(name.lower())
            queries.append({"name": name, "slug": slug})
        return queries

    def _brands_from_profile(self, profile: Dict[str, Any]) -> List[str]:
        brands = profile.get("brand") or profile.get("brands")
        if not brands:
            return self.default_brands
        if isinstance(brands, str):
            return [brands]
        return [b for b in brands if b]

    def fetch_items(self, profile: Dict[str, Any]) -> List[CarParsed]:
        brands = self._brands_from_profile(profile)
        limit = int(profile.get("limit_per_query") or self.limit_per_query)
        max_pages = int(profile.get("max_pages") or self.max_pages)

        results: List[CarParsed] = []
        seen_ids: set[str] = set()
        for query in self._brand_queries(brands):
            brand_name = query["name"]
            try:
                results.extend(
                    self._fetch_for_brand(
                        brand_name=brand_name,
                        manufacturer_slug=query.get("slug"),
                        limit=limit,
                        max_pages=max_pages,
                        seen_ids=seen_ids,
                    )
                )
            except RetryableError as exc:
                msg = f"[{self.config.key}] failed brand={brand_name}: {exc}"
                logger.warning(msg)
                self.last_warning = msg
        return results

    def _fetch_for_brand(
        self,
        *,
        brand_name: str,
        manufacturer_slug: Optional[str],
        limit: int,
        max_pages: int,
        seen_ids: set[str],
    ) -> List[CarParsed]:
        items: List[CarParsed] = []
        for page in range(1, max_pages + 1):
            payload = {
                "manufacturer_slug": manufacturer_slug,
                "search": None if manufacturer_slug else brand_name,
                "limit": limit,
                "page": page,
            }
            data = self.client.list_vehicles(**payload)
            listings = data.get("results") or []
            if not listings:
                break

            for listing in listings:
                vehicle_id = listing.get("vehicle_id")
                if vehicle_id is None:
                    continue
                ext_id = str(vehicle_id)
                if ext_id in seen_ids:
                    continue
                seen_ids.add(ext_id)

                detail = self.client.get_vehicle(vehicle_id=vehicle_id)
                car = map_vehicle_detail(
                    detail,
                    source_key=self.config.key,
                    currency=self.currency,
                    country=self.config.country,
                    fallback_main_photo=listing.get("main_photo"),
                )
                items.append(car)
                if self.detail_delay:
                    time.sleep(self.detail_delay)

            total_pages = data.get("pages") or 0
            if total_pages and page >= total_pages:
                break
            if len(listings) < limit:
                break
        logger.info(
            f"[{self.config.key}] brand={brand_name} fetched {len(items)} cars")
        return items


__all__ = ["EncarCarapisAdapter"]
