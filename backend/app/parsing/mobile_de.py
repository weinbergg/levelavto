from __future__ import annotations

from typing import List, Dict, Any, Optional
import re
from bs4 import BeautifulSoup  # type: ignore
from .base import BaseParser, CarParsed, logger
from .config import SiteConfig
from ..config import settings


class MobileDeParser(BaseParser):
    def __init__(self, config: SiteConfig):
        super().__init__(config)
        # Override client with realistic headers and optional proxy
        headers = {
            "User-Agent": settings.PARSER_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.8,ru;q=0.7",
            "Referer": "https://www.mobile.de/",
            "Connection": "keep-alive",
        }
        timeout = self.client.timeout
        proxies = None
        if settings.MOBILE_DE_HTTP_PROXY:
            proxies = {"http://": settings.MOBILE_DE_HTTP_PROXY, "https://": settings.MOBILE_DE_HTTP_PROXY}
        import httpx
        self.client = httpx.Client(headers=headers, timeout=timeout, follow_redirects=True, verify=True, http2=False, proxies=proxies)

    def _parse_int(self, text: Optional[str]) -> Optional[int]:
        if not text:
            return None
        digits = re.sub(r"[^\d]", "", text)
        return int(digits) if digits else None

    def _parse_year(self, text: Optional[str]) -> Optional[int]:
        if not text:
            return None
        m = re.search(r"\b(19|20)\d{2}\b", text)
        return int(m.group(0)) if m else None

    def _parse_mileage_km(self, text: Optional[str]) -> Optional[int]:
        # handles "120.000 km", "120 000 km", etc.
        return self._parse_int(text)

    def _parse_price_eur(self, text: Optional[str]) -> Optional[int]:
        if not text:
            return None
        t = text.replace("\xa0", " ").replace("€", "").replace("EUR", "").strip()
        digits = re.sub(r"[^\d]", "", t)
        return int(digits) if digits else None

    def _parse_from_details_html(self, raw_html: Optional[str], details_html: Optional[str]) -> Dict[str, Optional[int] | Optional[str]]:
        year: Optional[int] = None
        mileage: Optional[int] = None
        fuel: Optional[str] = None
        text_chunks: List[str] = []
        # Prefer provided details_html (inner snippet), then fallback to search in raw_html
        if details_html:
            try:
                s = BeautifulSoup(details_html, "html.parser")
                text_chunks.append(s.get_text(" ", strip=True))
            except Exception:
                text_chunks.append(BeautifulSoup(details_html, "html.parser").get_text(" ", strip=True))
        if raw_html:
            try:
                s = BeautifulSoup(raw_html, "html.parser")
                for div in s.select(".BaseListing_listingAttributeLine__t6UvN div"):
                    text_chunks.append(div.get_text(" ", strip=True))
            except Exception:
                pass
        # details_html already added; nothing else to add here
        joined = " | ".join(text_chunks)
        # year
        m = re.search(r"\b(19|20)\d{2}\b", joined)
        if m:
            try:
                year = int(m.group(0))
            except ValueError:
                year = None
        # mileage like "120 000 км" or "120.000 км"
        m2 = re.search(r"([\d\.\s]+)\s*км", joined.lower())
        if m2:
            mileage = self._parse_int(m2.group(1))
        # fuel — возьмём последний «токен» среди известных слов
        # упрощённо: ищем русские варианты
        fuels = []
        for chunk in text_chunks:
            low = chunk.lower()
            if "бензин" in low:
                fuels.append("Бензин")
            if "дизел" in low:
                fuels.append("Дизель")
            if "гибрид" in low:
                fuels.append("Гибрид")
            if "электро" in low:
                fuels.append("Электро")
        if fuels:
            fuel = fuels[-1]
        return {"year": year, "mileage": mileage, "fuel": fuel}

    def parse_html(self, html: str) -> List[CarParsed]:
        raw_items = self._parse_html_list(html)
        logger.info(f"[mobile_de] items found by selector={self.config.selectors.get('item')!r}: {len(raw_items)}")
        items: List[CarParsed] = []
        for idx, r in enumerate(raw_items):
            # Parse inside article HTML to be robust against class hash changes
            soup = None
            try:
                soup = BeautifulSoup(r.get("raw_html") or "", "html.parser")
            except Exception:
                soup = None

            link_el = None
            img_el = None
            title_el = None
            price_el = None
            details_els: List = []

            if soup:
                link_el = soup.select_one('a[class*="BaseListing_containerLink__"]') or soup.select_one('a[href]')
                img_el = (
                    soup.select_one('[class*="ListingPreviewImage_image__"] img')
                    or soup.select_one('img[class*="ListingPreviewImage_image__"]')
                    or soup.select_one("img[src]")
                )
                title_el = soup.select_one('[class*="ListingTitle_title__"]') or soup.select_one("h2, h3")
                price_el = soup.select_one('[class*="PriceLabel_mainPrice__"]')
                details_els = soup.select('[class*="BaseListing_listingAttributeLine__"] div')

            link = link_el.get("href") if link_el else r.get("link")
            link = self._make_full_url(link)
            if link and link.startswith("/"):
                link = f"https://www.mobile.de{link}"
            external_id = (link or "")[-120:]
            title = (title_el.get_text(" ", strip=True) if title_el else (r.get("title") or "")).strip()
            title_clean = title.replace("\xa0", " ").strip()
            # Remove common prefixes like "Спонсируемое" or "НОВОЕ" that are not part of the model name
            for bad_prefix in ("Спонсируемое", "НОВОЕ"):
                if title_clean.startswith(bad_prefix):
                    title_clean = title_clean[len(bad_prefix):].strip()
            brand = title_clean.split(" ", 1)[0] if title_clean else None
            model = title_clean.split(" ", 1)[1] if title_clean and " " in title_clean else None
            price_text = price_el.get_text(" ", strip=True) if price_el else r.get("price")
            price = self._parse_price_eur(price_text)
            # Build details html snippet if available
            details_html = None
            if details_els:
                details_html = "".join(str(d) for d in details_els)
            else:
                details_html = r.get("details_html") or r.get("details")
            det = self._parse_from_details_html(r.get("raw_html"), details_html)
            year = det["year"]  # type: ignore
            mileage = det["mileage"]  # type: ignore
            fuel_raw = det["fuel"]  # type: ignore
            fuel = None
            if isinstance(fuel_raw, str):
                if "бензин" in fuel_raw.lower():
                    fuel = "Petrol"
                elif "дизел" in fuel_raw.lower():
                    fuel = "Diesel"
                elif "гибрид" in fuel_raw.lower():
                    fuel = "Hybrid"
                else:
                    fuel = fuel_raw
            image_url = img_el.get("src") if img_el else r.get("image")
            # Collect all image URLs in card: primary + thumbnails + other imgs present
            all_images: List[str] = []
            if image_url:
                all_images.append(image_url)
            if soup:
                # Known thumbnail class on mobile.de cards (can vary by deployment hash)
                thumb_els = soup.select('[class*="ListingPreviewThumbnail_image__"]')
                for te in thumb_els:
                    src = te.get("src")
                    if src:
                        all_images.append(src)
                # Fallback: any other <img> within the listing container
                for im in soup.select("img[src]"):
                    src = im.get("src")
                    if src:
                        all_images.append(src)
            # Deduplicate while preserving order and keep within reasonable limit
            seen = set()
            dedup_images: List[str] = []
            for u in all_images:
                if u not in seen:
                    dedup_images.append(u)
                    seen.add(u)
            if len(dedup_images) > 12:
                dedup_images = dedup_images[:12]
            if not title or (not details_html) or (price is None):
                logger.warning(f"[mobile_de] missing fields in item idx={idx}: title_present={bool(title)} details_present={bool(details_html)} price_present={price is not None}")
            logger.debug(f"MOBILE_DE PARSED: brand={brand}, model={model}, year={year}, mileage={mileage}, price={price}")
            items.append(
                CarParsed(
                    source_key=self.config.key,
                    external_id=external_id or f"mobile_de_offline_{idx}",
                    country=(self.config.country or "DE").upper(),
                    brand=brand,
                    model=model,
                    year=year,
                    mileage=mileage,
                    price=price,
                    currency=self.config.defaults.get("currency"),
                    source_url=link,
                    thumbnail_url=image_url,
                    engine_type=fuel if isinstance(fuel, str) else None,
                    images=dedup_images or ([image_url] if image_url else None),
                )
            )
        return items

    def fetch_items(self, profile: Dict[str, Any]) -> List[CarParsed]:
        items: List[CarParsed] = []
        pag = self.config.pagination
        for page in range(pag.start_page, pag.max_pages + 1):
            # Default query params required by mobile.de search results page
            base_defaults = {
                "dam": "false",
                "isSearchRequest": "true",
                "ref": "quickSearch",
                "s": "Car",
                "vc": "Car",
            }
            query = {**base_defaults, **self._build_query(profile, page)}
            resp = self._http_get(self.config.base_search_url, params=query)
            logger.info(f"[mobile_de] GET {resp.url} -> {resp.status_code}, body_len={len(resp.text) if resp.text else 0}")
            if not resp or not resp.text:
                break
            if resp.status_code == 403:
                self.last_warning = "HTTP 403 Forbidden from mobile.de (environment likely blocked)"
                logger.warning("[mobile_de] HTTP 403 from %s; source unavailable from this environment.", self.config.base_search_url)
                # Save snapshot for offline analysis
                try:
                    from pathlib import Path
                    tmp = Path("/app/tmp")
                    tmp.mkdir(exist_ok=True, parents=True)
                    (tmp / "mobile_de_last.html").write_text(resp.text or "", encoding="utf-8")
                except Exception:
                    pass
                # graceful degradation: return what we have (likely zero)
                break
            batch_items = self.parse_html(resp.text)
            if not batch_items:
                # write snapshot for offline tuning
                try:
                    from pathlib import Path
                    tmp = Path("/app/tmp")
                    tmp.mkdir(exist_ok=True, parents=True)
                    (tmp / "mobile_de_last.html").write_text(resp.text, encoding="utf-8")
                    logger.warning(f"[mobile_de] no items found; snapshot saved to /app/tmp/mobile_de_last.html")
                except Exception as e:
                    logger.warning(f"[mobile_de] failed to write HTML snapshot: {e}")
                break
            items.extend(batch_items)
            self._delay()
        return items

