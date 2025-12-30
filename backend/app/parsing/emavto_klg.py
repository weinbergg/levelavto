from __future__ import annotations

from typing import List, Dict, Any, Optional, Tuple
import re
import time
import random
from datetime import datetime

import httpx
from bs4 import BeautifulSoup  # type: ignore

from .base import BaseParser, CarParsed, logger
from .config import SiteConfig
from ..utils.rate_limiter import TokenBucket


class EmAvtoKlgParser(BaseParser):
    """
    Two-stage pipeline:
    - list producer obeying list rate-limit
    - detail workers obeying detail rate-limit
    Supports modes: full / incremental (via profile["mode"]).
    """

    DETAILS_SEP_RE = re.compile(r"[·•|\u00b7]")
    DIGITS_RE = re.compile(r"\d+")
    LABELS = {
        "body_type": ["Кузов", "Тип кузова"],
        "transmission": ["КПП", "Коробка", "Коробка передач", "Трансмиссия"],
        "drive_type": ["Привод"],
        "color": ["Цвет"],
        "vin": ["VIN", "ВИН"],
    }

    def __init__(self, config: SiteConfig):
        super().__init__(config)
        d = config.defaults
        self.list_rps = float(d.get("list_rps", 1.0))
        self.list_concurrency = int(d.get("list_concurrency", 2))
        self.detail_rps = float(d.get("detail_rps", 0.4))
        self.detail_concurrency = int(d.get("detail_concurrency", 1))
        self.max_pages_full = int(
            d.get("max_pages_full", config.pagination.max_pages))
        self.max_pages_incremental = int(
            d.get("max_pages_incremental", config.pagination.max_pages))
        self.progress: Dict[str, Any] = {}
        self.metrics: Dict[str, Any] = {
            "list_requests": 0,
            "detail_requests": 0,
            "detail_429": 0,
            "list_429": 0,
            "list_latency": [],
            "detail_latency": [],
        }
        self.last_tasks_total: int = 0
        self.last_details_done: int = 0
        self.missing_tasks: List[Dict[str, Any]] = []

    def _build_query(self, profile: Dict[str, Any], page: int) -> Dict[str, str]:
        # Base query from profile + pagination
        qp = super()._build_query(profile, page)
        # emavto requires koreaPage=1 to return the KR catalog
        qp.setdefault("koreaPage", "1")
        return qp

    # --- public API ---
    def fetch_items(self, profile: Dict[str, Any]) -> List[CarParsed]:
        mode = (profile.get("mode") or "full").lower()
        start_page = int(profile.get("resume_page_full")
                         or self.config.pagination.start_page)
        # Allow CLI profile to override page count; fallback to defaults per mode
        max_pages = int(profile.get("max_pages") or (
            self.max_pages_full if mode == "full" else self.max_pages_incremental))
        max_items = int(profile.get("max_items") or 0)
        skip_details = bool(profile.get("skip_details", False))
        # default 30m for larger batches
        deadline_sec = int(profile.get("max_runtime_sec") or 1800)
        deadline = time.monotonic() + deadline_sec
        deadline_hit = False

        list_bucket = TokenBucket(rate_per_sec=self.list_rps)
        detail_bucket = TokenBucket(rate_per_sec=self.detail_rps)

        results: List[CarParsed] = []
        processed_pages: List[int] = []
        tasks: List[Dict[str, Any]] = []

        for page in range(start_page, start_page + max_pages):
            if time.monotonic() > deadline:
                deadline_hit = True
                break
            logger.info(f"[emavto_klg] list start page={page}")
            ok = self._produce_page(
                page,
                list_bucket,
                tasks,
                profile,
                skip_details,
                max_items,
                deadline,
            )
            if not ok:
                break
            processed_pages.append(page)
            if max_items and len(tasks) >= max_items:
                break

        # If skipping details, we already appended results inside produce_page
        if skip_details:
            for t in tasks:
                dc = t.get("direct_car")
                if dc:
                    results.append(dc)
        else:
            self.last_tasks_total = len(tasks)
            logger.info(
                "[emavto_klg] detail loop start tasks=%s max_items=%s", len(
                    tasks), max_items or "inf"
            )
            client = httpx.Client(
                headers={"User-Agent": self.client.headers.get("User-Agent")},
                timeout=httpx.Timeout(10.0, read=20.0),
                follow_redirects=True,
            )
            for task in tasks:
                if time.monotonic() > deadline:
                    deadline_hit = True
                    break
                if max_items and len(results) >= max_items:
                    break
                logger.info(
                    "[emavto_klg] detail start ext_id=%s url=%s",
                    task.get("external_id"),
                    task.get("source_url"),
                )
                detail = self._fetch_detail(
                    task["source_url"], detail_bucket, client=client, deadline=deadline)
                car = CarParsed(
                    source_key=self.config.key,
                    external_id=task["external_id"],
                    country=self.config.country,
                    brand=task["brand"],
                    model=task["model"],
                    year=task["year"],
                    mileage=task["mileage"],
                    price=task["price"],
                    currency=self.config.defaults.get("currency"),
                    engine_type=task["engine_type"],
                    body_type=detail.get("body_type"),
                    transmission=detail.get("transmission"),
                    drive_type=detail.get("drive_type"),
                    color=detail.get("color"),
                    vin=detail.get("vin"),
                    source_url=task["source_url"],
                    thumbnail_url=detail.get(
                        "thumbnail") or task["thumbnail_url"],
                    images=detail.get("images"),
                )
                results.append(car)
                logger.info(
                    "[emavto_klg] detail done ext_id=%s images=%s",
                    car.external_id,
                    len(car.images or []),
                )
            client.close()
            self.last_details_done = len(results)
            if len(results) < len(tasks):
                self.missing_tasks = tasks[len(results):]
            else:
                self.missing_tasks = []

        last_page_processed = max(
            processed_pages) if processed_pages else (start_page - 1)

        if mode == "full":
            self.progress["last_page_full"] = last_page_processed
        else:
            self.progress["last_incremental_run_at"] = datetime.utcnow(
            ).isoformat()

        logger.info(
            "[emavto_klg] run done mode=%s pages_done=%s total_items=%s list_req=%s detail_req=%s list_429=%s detail_429=%s",
            mode,
            processed_pages,
            len(results),
            self.metrics["list_requests"],
            self.metrics["detail_requests"],
            self.metrics["list_429"],
            self.metrics["detail_429"],
        )
        if deadline_hit:
            logger.warning(
                "[emavto_klg] deadline_hit max_runtime_sec=%s tasks_total=%s details_done=%s missing=%s",
                deadline_sec,
                len(tasks),
                len(results),
                len(self.missing_tasks),
            )

        return results

    def fetch_missing_details(self, tasks: List[Dict[str, Any]], max_items: int = 0, max_runtime_sec: int = 900) -> List[CarParsed]:
        """
        Run detail loop only for provided tasks (e.g., after timeout). Returns parsed cars.
        """
        if not tasks:
            return []
        detail_bucket = TokenBucket(rate_per_sec=self.detail_rps)
        deadline = time.monotonic() + max_runtime_sec
        results: List[CarParsed] = []
        client = httpx.Client(
            headers={"User-Agent": self.client.headers.get("User-Agent")},
            timeout=httpx.Timeout(10.0, read=20.0),
            follow_redirects=True,
        )
        for task in tasks:
            if time.monotonic() > deadline:
                break
            if max_items and len(results) >= max_items:
                break
            logger.info(
                "[emavto_klg] backfill detail start ext_id=%s url=%s",
                task.get("external_id"),
                task.get("source_url"),
            )
            detail = self._fetch_detail(task["source_url"], detail_bucket, client=client, deadline=deadline)
            car = CarParsed(
                source_key=self.config.key,
                external_id=task["external_id"],
                country=self.config.country,
                brand=task["brand"],
                model=task["model"],
                year=task["year"],
                mileage=task["mileage"],
                price=task["price"],
                currency=self.config.defaults.get("currency"),
                engine_type=task["engine_type"],
                body_type=detail.get("body_type"),
                transmission=detail.get("transmission"),
                drive_type=detail.get("drive_type"),
                color=detail.get("color"),
                vin=detail.get("vin"),
                source_url=task["source_url"],
                thumbnail_url=detail.get("thumbnail") or task["thumbnail_url"],
                images=detail.get("images"),
            )
            results.append(car)
            logger.info(
                "[emavto_klg] backfill detail done ext_id=%s images=%s",
                car.external_id,
                len(car.images or []),
            )
        client.close()
        return results

    # --- list producer ---
    def _produce_page(
        self,
        page: int,
        bucket: TokenBucket,
        tasks: List[Dict[str, Any]],
        profile: Dict[str, Any],
        skip_details: bool,
        max_items: int,
        deadline: float,
    ) -> bool:
        query = self._build_query(profile, page)
        url = self.config.base_search_url
        expected_per_page = 50
        resp = self._request_with_backoff(
            url, query, bucket, is_detail=False, deadline=deadline)
        if not resp or resp.status_code != 200 or not resp.text:
            logger.warning(
                f"[emavto_klg] stop at page={page} status={getattr(resp, 'status_code', None)}")
            return False
        logger.info(
            "[emavto_klg] page=%s status=%s body_len=%s url=%s",
            page,
            getattr(resp, "status_code", None),
            len(resp.text or ""),
            getattr(resp, "url", url),
        )
        raw_items = self._parse_html_list(resp.text)
        logger.info(f"[emavto_klg] page {page} parsed {len(raw_items)} cards")
        if len(raw_items) < expected_per_page:
            logger.warning(
                "[emavto_klg] page %s parsed %s (<%s expected)", page, len(raw_items), expected_per_page
            )
        if not raw_items:
            return False
        for idx, r in enumerate(raw_items):
            if time.monotonic() > deadline:
                break
            link = self._make_full_url(r.get("link"))
            if not link:
                continue
            external_id = (link or "")[-120:]
            title_text = r.get("title") or ""
            brand, model = self._split_brand_model(title_text)
            details_text = r.get("details") or ""
            mileage, year, fuel = self._parse_emavto_details(details_text)
            price = self._parse_price_usd(r.get("price"))
            if skip_details:
                car = CarParsed(
                    source_key=self.config.key,
                    external_id=external_id or f"emavto_klg_{page}_{idx}",
                    country=self.config.country,
                    brand=brand,
                    model=model,
                    year=year,
                    mileage=mileage,
                    price=price,
                    currency=self.config.defaults.get("currency"),
                    engine_type=fuel,
                    body_type=None,
                    transmission=None,
                    drive_type=None,
                    color=None,
                    vin=None,
                    source_url=link,
                    thumbnail_url=r.get("image"),
                    images=None,
                )
                tasks.append(
                    {
                        "external_id": car.external_id,
                        "source_url": car.source_url,
                        "brand": car.brand,
                        "model": car.model,
                        "year": car.year,
                        "mileage": car.mileage,
                        "price": car.price,
                        "engine_type": car.engine_type,
                        "thumbnail_url": car.thumbnail_url,
                        "details_text": details_text,
                        "direct_car": car,
                    }
                )
            else:
                tasks.append(
                    {
                        "external_id": external_id or f"emavto_klg_{page}_{idx}",
                        "source_url": link,
                        "brand": brand,
                        "model": model,
                        "year": year,
                        "mileage": mileage,
                        "price": price,
                        "engine_type": fuel,
                        "thumbnail_url": r.get("image"),
                        "details_text": details_text,
                    }
                )
                if max_items and len(tasks) >= max_items:
                    break
        return True

    # --- request helpers ---
    def _request_with_backoff(
        self,
        url: str,
        params: Optional[Dict[str, Any]],
        bucket: TokenBucket,
        is_detail: bool,
        client: Optional[httpx.Client] = None,
        deadline: Optional[float] = None,
    ) -> Optional[httpx.Response]:
        last_resp: Optional[httpx.Response] = None
        for attempt in range(3):
            if deadline and time.monotonic() > deadline:
                logger.warning(
                    f"[emavto_klg] deadline hit before request url={url}")
                return last_resp
            bucket.acquire()
            t0 = time.monotonic()
            sess = client or self.client
            try:
                resp = sess.get(url, params=params)
            except httpx.TimeoutException:
                logger.warning(
                    f"[emavto_klg] timeout {'detail' if is_detail else 'list'} attempt={attempt+1} url={url}")
                # Backoff similar to 5xx
                delay = 2 + attempt * 2 + random.uniform(0, 2)
                if deadline:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return last_resp
                    delay = min(delay, remaining)
                time.sleep(max(1.0, delay))
                continue
            except httpx.RemoteProtocolError as e:
                logger.warning(
                    f"[emavto_klg] remote protocol error {'detail' if is_detail else 'list'} attempt={attempt+1} url={url} err={e}"
                )
                delay = 2 + attempt * 2 + random.uniform(0, 2)
                if deadline:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return last_resp
                    delay = min(delay, remaining)
                time.sleep(max(1.0, delay))
                continue
            latency = time.monotonic() - t0
            if is_detail:
                self.metrics["detail_requests"] += 1
                self.metrics["detail_latency"].append(latency)
            else:
                self.metrics["list_requests"] += 1
                self.metrics["list_latency"].append(latency)
            last_resp = resp
            if resp.status_code == 429:
                if is_detail:
                    self.metrics["detail_429"] += 1
                else:
                    self.metrics["list_429"] += 1
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        delay = float(retry_after)
                    except ValueError:
                        delay = 10.0
                else:
                    delay = [5, 10, 20, 30][min(
                        attempt, 3)] + random.uniform(0, 2)
                delay = max(1.0, delay)
                if deadline:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        logger.warning(
                            f"[emavto_klg] deadline reached after 429 url={url}")
                        return resp
                    delay = min(delay, remaining)
                logger.warning(
                    f"[emavto_klg] 429 {'detail' if is_detail else 'list'} retry in {delay:.1f}s url={url}")
                time.sleep(delay)
                continue
            if resp.status_code >= 500:
                delay = 2 + attempt * 2 + random.uniform(0, 2)
                if deadline:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        logger.warning(
                            f"[emavto_klg] deadline reached after {resp.status_code} url={url}")
                        return resp
                    delay = min(delay, remaining)
                logger.warning(
                    f"[emavto_klg] {resp.status_code} retry in {delay:.1f}s url={url}")
                time.sleep(max(1.0, delay))
                continue
            return resp
        return last_resp

    def _fetch_detail(self, url: str, bucket: TokenBucket, client: Optional[httpx.Client] = None, deadline: Optional[float] = None) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        close_client = False
        if client is None:
            client = httpx.Client(
                headers={"User-Agent": self.client.headers.get("User-Agent")},
                timeout=httpx.Timeout(10.0, read=20.0),
                follow_redirects=True,
            )
            close_client = True
        resp = self._request_with_backoff(
            url, None, bucket, is_detail=True, client=client, deadline=deadline)
        if not resp or resp.status_code != 200 or not resp.text:
            logger.warning("[emavto_klg] detail failed url=%s status=%s", url, getattr(
                resp, "status_code", None))
            if close_client:
                client.close()
            return out
        soup = BeautifulSoup(resp.text, "html.parser")
        page_text = soup.get_text(" ", strip=True)
        pairs: Dict[str, str] = {}
        for dt in soup.find_all("dt"):
            dd = dt.find_next_sibling("dd")
            if not dd:
                continue
            label = (dt.get_text(strip=True) or "").strip(" :")
            val = dd.get_text(" ", strip=True)
            if label and val:
                pairs[label.lower()] = val

        def pair_equals(lbl: str, keys: List[str]) -> bool:
            clean = lbl.replace(":", "").strip().lower()
            for k in keys:
                if clean == k.lower():
                    return True
            return False

        def from_pairs(keys: List[str]) -> Optional[str]:
            for k in keys:
                pattern = re.compile(
                    rf"\b{re.escape(k.lower())}\b", re.IGNORECASE)
                for lbl, val in pairs.items():
                    if pattern.search(lbl) or pair_equals(lbl, [k]):
                        return val
            return None

        body_raw = from_pairs(self.LABELS["body_type"]) or self._extract_label_value(
            soup, self.LABELS["body_type"]) or self._extract_by_regex(page_text, self.LABELS["body_type"])
        trans_raw = from_pairs(self.LABELS["transmission"]) or self._extract_label_value(
            soup, self.LABELS["transmission"]) or self._extract_by_regex(page_text, self.LABELS["transmission"])
        drive_raw = from_pairs(self.LABELS["drive_type"]) or self._extract_label_value(
            soup, self.LABELS["drive_type"]) or self._extract_by_regex(page_text, self.LABELS["drive_type"])
        color_raw = from_pairs(self.LABELS["color"]) or self._extract_label_value(
            soup, self.LABELS["color"]) or self._extract_by_regex(page_text, self.LABELS["color"])
        vin_raw = from_pairs(self.LABELS["vin"]) or self._extract_label_value(
            soup, self.LABELS["vin"]) or self._extract_by_regex(page_text, self.LABELS["vin"])

        out["body_type"] = self._normalize_body(body_raw)
        out["transmission"] = self._normalize_transmission(trans_raw)
        out["drive_type"] = self._normalize_drive(drive_raw)
        out["color"] = color_raw.strip().capitalize() if color_raw else None
        if vin_raw:
            vin_clean = re.sub(r"\s+", "", vin_raw)
            if 11 <= len(vin_clean) <= 20:
                out["vin"] = vin_clean.upper()

        # images: prefer gallery; fallback to first images on page
        imgs = []
        for img in soup.find_all("img"):
            src = img.get("src") or ""
            if not src:
                continue
            if any(bad in src for bad in ["svg", "icon", "logo", "data:"]):
                continue
            if src not in imgs:
                imgs.append(src)
            if len(imgs) >= 12:
                break
        if imgs:
            out["images"] = imgs
            out["thumbnail"] = imgs[0]
        if close_client:
            client.close()
        return out

    # --- parsing helpers ---
    def _parse_price_usd(self, price_text: Optional[str]) -> Optional[int]:
        if not price_text:
            return None
        text = price_text.replace("\xa0", " ").strip()
        nums = self.DIGITS_RE.findall(text)
        if not nums:
            return None
        try:
            return int("".join(nums))
        except ValueError:
            return None

    def _parse_emavto_details(self, details_text: Optional[str]) -> Tuple[Optional[int], Optional[int], Optional[str]]:
        if not details_text:
            return None, None, None
        text = details_text.replace("\xa0", " ")
        for sep in ["·", "•", "|"]:
            text = text.replace(sep, "·")
        parts = [p.strip().lower()
                 for p in self.DETAILS_SEP_RE.split(text) if p.strip()]
        mileage: Optional[int] = None
        year: Optional[int] = None
        fuel: Optional[str] = None
        for part in parts:
            if "км" in part or "пробег" in part:
                nums = self.DIGITS_RE.findall(part)
                if nums:
                    try:
                        mileage = int("".join(nums))
                    except ValueError:
                        pass
            elif re.search(r"\b(19|20)\d{2}\b", part):
                m = re.search(r"\b(19|20)\d{2}\b", part)
                if m:
                    try:
                        year = int(m.group(0))
                    except ValueError:
                        pass
            else:
                fuel = part.strip().capitalize()
        return mileage, year, fuel

    def _split_brand_model(self, title: str) -> Tuple[Optional[str], Optional[str]]:
        clean = re.sub(r"^\[[^\]]+\]\s*", "", title).strip()
        tokens = clean.split()
        if not tokens:
            return None, None
        brand = tokens[0]
        model = " ".join(tokens[1:]) if len(tokens) > 1 else None
        return brand, model

    def _normalize_transmission(self, raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        t = raw.lower()
        if any(k in t for k in ["авто", "акп", "ат", "tiptronic", "dsg", "робот", "вариатор", "cvt"]):
            return "Automatic"
        if any(k in t for k in ["мех", "mt", "ручн", "manual"]):
            return "Manual"
        return raw.strip().capitalize()

    def _normalize_drive(self, raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        t = raw.lower()
        if "полн" in t or "4x4" in t or "awd" in t:
            return "AWD"
        if "задн" in t or "rwd" in t:
            return "RWD"
        if "перед" in t or "fwd" in t:
            return "FWD"
        return raw.strip().upper()

    def _normalize_body(self, raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        return raw.strip().lower()

    def _extract_label_value(self, soup: BeautifulSoup, labels: List[str]) -> Optional[str]:
        for lab in labels:
            pat = re.compile(rf"\b{re.escape(lab)}\b", re.IGNORECASE)
            for node in soup.find_all(string=pat):
                if node.parent:
                    sib = node.parent.find_next(string=True)
                    if sib:
                        val = sib.strip()
                        if val and not pat.search(val):
                            return val
                    sib_el = node.parent.find_next()
                    if sib_el and sib_el != node.parent:
                        val = sib_el.get_text(strip=True)
                        if val and not pat.search(val):
                            return val
        return None

    def _extract_by_regex(self, text: str, labels: List[str]) -> Optional[str]:
        for lab in labels:
            m = re.search(
                rf"{lab}\s*[:\-]\s*([A-Za-zА-Яа-я0-9\s\-\+_/.]+)", text, re.IGNORECASE)
            if m:
                return m.group(1).strip()
        return None
