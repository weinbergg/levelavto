from __future__ import annotations

from typing import Any, Dict, Optional
from pydantic import BaseModel, Field
from pathlib import Path
import yaml


class PaginationConfig(BaseModel):
    start_page: int = 1
    page_param: str = "page"
    max_pages: int = 50


class SiteConfig(BaseModel):
    key: str
    name: str
    country: str
    type: str = Field(pattern="^(html|json)$")
    enabled: bool = True
    base_search_url: str
    query_params: Dict[str, str] = Field(default_factory=dict)
    defaults: Dict[str, Any] = Field(default_factory=dict)
    pagination: PaginationConfig
    selectors: Dict[str, str] = Field(default_factory=dict)


class SitesConfig(BaseModel):
    sites: Dict[str, SiteConfig]

    def get(self, key: str) -> SiteConfig:
        return self.sites[key]


def load_sites_config(config_path: Optional[Path] = None) -> SitesConfig:
    if config_path is None:
        # default location under backend/app/parsing/sites_config.yaml
        here = Path(__file__).resolve().parent
        config_path = here / "sites_config.yaml"
    with config_path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    sites: Dict[str, SiteConfig] = {}
    for key, data in raw.items():
        # Ensure key consistency
        data = dict(data or {})
        data.setdefault("key", key)
        # Pagination defaults handling
        if "pagination" not in data:
            data["pagination"] = {}
        site = SiteConfig(**data)
        sites[site.key] = site
    return SitesConfig(sites=sites)


