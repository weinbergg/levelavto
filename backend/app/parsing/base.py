from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Dict, Any
import httpx
import time
import random
import logging
from pathlib import Path
from ..config import settings
from .config import SiteConfig
from bs4 import BeautifulSoup  # type: ignore
from bs4 import FeatureNotFound  # type: ignore


logger = logging.getLogger("parsing")
logger.setLevel(logging.INFO)
_log_file = Path(settings.PARSER_LOG_FILE)
_log_file.parent.mkdir(parents=True, exist_ok=True)
_fh = logging.FileHandler(_log_file, encoding="utf-8")
_fh.setLevel(logging.INFO)
_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
_fh.setFormatter(_formatter)
if not logger.handlers:
    logger.addHandler(_fh)


@dataclass
class CarParsed:
    source_key: str
    external_id: str  # unique ID from source
    country: str
    brand: Optional[str] = None
    model: Optional[str] = None
    generation: Optional[str] = None
    variant: Optional[str] = None
    year: Optional[int] = None
    registration_year: Optional[int] = None
    registration_month: Optional[int] = None
    mileage: Optional[int] = None
    price: Optional[float] = None
    currency: Optional[str] = None
    body_type: Optional[str] = None
    engine_type: Optional[str] = None
    engine_cc: Optional[int] = None
    power_hp: Optional[float] = None
    power_kw: Optional[float] = None
    transmission: Optional[str] = None
    drive_type: Optional[str] = None
    color: Optional[str] = None
    vin: Optional[str] = None
    source_url: Optional[str] = None
    thumbnail_url: Optional[str] = None
    source_payload: Optional[Dict[str, Any]] = None
    is_available: bool = True
    # optional list of image URLs in display order (first is primary)
    images: Optional[List[str]] = None

    def as_dict(self) -> dict:
        return self.__dict__.copy()


class BaseParser:
    def __init__(self, config: SiteConfig) -> None:
        self.config = config
        self.last_warning: Optional[str] = None
        timeout = httpx.Timeout(
            connect=15.0,
            read=30.0,
            write=30.0,
            pool=60.0,
        )
        self.client = httpx.Client(
            headers={"User-Agent": settings.PARSER_USER_AGENT},
            timeout=timeout,
            follow_redirects=True,
            verify=True,
            http2=False,
        )

    def _delay(self) -> None:
        delay = random.uniform(settings.PARSER_MIN_DELAY_SECONDS, settings.PARSER_MAX_DELAY_SECONDS)
        time.sleep(delay)

    # Build query dict from provided profile filters mapped via config
    def _build_query(self, profile: Dict[str, Any], page: int) -> Dict[str, str]:
        qp = {}
        for logical, value in profile.items():
            if value is None or value == "":
                continue
            key = self.config.query_params.get(logical)
            if not key:
                continue
            qp[key] = str(value)
        # Pagination
        qp[self.config.pagination.page_param] = str(page)
        return qp

    def fetch_items(self, profile: Dict[str, Any]) -> List[CarParsed]:
        raise NotImplementedError

    # Helper for simple HTML list parsing using selectors from config
    def _parse_html_list(self, html: str) -> List[Dict[str, Optional[str]]]:
        # Prefer lxml, gracefully fall back to html.parser if lxml is unavailable
        try:
            soup = BeautifulSoup(html, "lxml")
        except FeatureNotFound:
            logger.warning(f"[{self.config.key}] lxml not available, falling back to html.parser")
            soup = BeautifulSoup(html, "html.parser")
        sel = self.config.selectors
        items = []
        for card in soup.select(sel.get("item", ""))[:1000]:
            link_el = card.select_one(sel.get("link", "")) if sel.get("link") else card
            img_el = card.select_one(sel.get("image", "")) if sel.get("image") else None
            title_el = card.select_one(sel.get("title", "")) if sel.get("title") else None
            price_el = card.select_one(sel.get("price", "")) if sel.get("price") else None
            year_el = card.select_one(sel.get("year", "")) if sel.get("year") else None
            mileage_el = card.select_one(sel.get("mileage", "")) if sel.get("mileage") else None
            details_el = card.select_one(sel.get("details", "")) if sel.get("details") else None
            items.append(
                {
                    "raw_html": str(card),
                    "title": title_el.get_text(strip=True) if title_el else None,
                    "price": price_el.get_text(strip=True) if price_el else None,
                    "year": year_el.get_text(strip=True) if year_el else None,
                    "mileage": mileage_el.get_text(strip=True) if mileage_el else None,
                    "link": link_el.get("href") if link_el else None,
                    "image": (img_el.get("src") if img_el else None),
                    "details": details_el.get_text(strip=True) if details_el else None,
                    "details_html": str(details_el) if details_el else None,
                }
            )
        return items

    def _make_full_url(self, href: Optional[str]) -> Optional[str]:
        if not href:
            return None
        if href.startswith("http://") or href.startswith("https://"):
            return href
        base = self.config.base_search_url
        # crude join; for correctness we could use urllib.parse.urljoin
        if href.startswith("/"):
            from urllib.parse import urlparse, urlunparse
            parsed = httpx.URL(base)
            return f"{parsed.scheme}://{parsed.host}{href}"
        return base.rstrip("/") + "/" + href.lstrip("/")

    def _http_get(self, url: str, *, params: Optional[Dict[str, Any]] = None):
        logger.info(f"[{self.config.key}] GET {url} params={params}")
        return self.client.get(url, params=params)

