from __future__ import annotations

from datetime import datetime
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup  # type: ignore

from .base import BaseParser, CarParsed, logger
from .config import SiteConfig
from ..utils.spec_inference import infer_engine_cc_from_text


class Che168Parser(BaseParser):
    LIST_SELECTOR = "li.cards-li.list-photo-li[infoid]"
    PRICE_WAN_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?)")
    YEAR_MONTH_RE = re.compile(r"(20\d{2})[/-年]\s*(\d{1,2})")
    HORSEPOWER_RE = re.compile(r"(\d+(?:\.\d+)?)\s*马力")
    LATIN_MODEL_RE = re.compile(r"[A-Za-z]+[A-Za-z0-9-]*|[A-Za-z]*\d+[A-Za-z0-9-]*")
    PAGE_RE = re.compile(r"csp(\d+)exx0", re.IGNORECASE)
    PAGE_SLOT_RE = re.compile(r"cspexx0", re.IGNORECASE)

    BRAND_NAME_MAP: Dict[str, str] = {
        "奥迪": "Audi",
        "宝马": "BMW",
        "奔驰": "Mercedes-Benz",
        "梅赛德斯-奔驰": "Mercedes-Benz",
        "大众": "Volkswagen",
        "丰田": "Toyota",
        "本田": "Honda",
        "日产": "Nissan",
        "雷克萨斯": "Lexus",
        "保时捷": "Porsche",
        "宾利": "Bentley",
        "捷豹": "Jaguar",
        "路虎": "Land Rover",
        "揽胜": "Land Rover",
        "沃尔沃": "Volvo",
        "特斯拉": "Tesla",
        "现代": "Hyundai",
        "起亚": "Kia",
        "斯柯达": "Skoda",
        "福特": "Ford",
        "雪佛兰": "Chevrolet",
        "别克": "Buick",
        "MINI": "Mini",
        "迈巴赫": "Maybach",
        "劳斯莱斯": "Rolls-Royce",
        "法拉利": "Ferrari",
        "兰博基尼": "Lamborghini",
        "阿斯顿·马丁": "Aston Martin",
        "阿斯顿马丁": "Aston Martin",
        "玛莎拉蒂": "Maserati",
        "比亚迪": "BYD",
        "蔚来": "NIO",
        "理想": "Li Auto",
        "小鹏": "XPeng",
        "极氪": "Zeekr",
        "smart": "Smart",
        "SMART": "Smart",
        "MG": "MG",
    }

    MODEL_HINT_MAP: Dict[str, str] = {
        "添越": "Bentayga",
        "卡宴": "Cayenne",
        "揽胜运动版": "Range Rover Sport",
        "揽胜极光": "Range Rover Evoque",
        "揽胜": "Range Rover",
        "发现运动版": "Discovery Sport",
        "发现": "Discovery",
        "卫士": "Defender",
        "途锐": "Touareg",
        "高尔夫": "Golf",
        "帕萨特": "Passat",
        "迈腾": "Passat",
        "探岳": "Tiguan",
        "途观": "Tiguan",
        "途昂": "Teramont",
        "途安": "Touran",
        "RAV4荣放": "RAV4",
        "荣放": "RAV4",
        "汉兰达": "Highlander",
        "普拉多": "Land Cruiser Prado",
        "兰德酷路泽": "Land Cruiser",
        "陆地巡洋舰": "Land Cruiser",
        "E级": "E-Class",
        "S级": "S-Class",
        "C级": "C-Class",
        "A级": "A-Class",
        "CLA级": "CLA",
        "GLA级": "GLA",
        "GLB级": "GLB",
        "GLC级": "GLC",
        "GLE级": "GLE",
        "GLS级": "GLS",
        "X5": "X5",
        "X7": "X7",
        "A6L": "A6L",
        "A4L": "A4L",
        "Q5L": "Q5L",
        "Q7": "Q7",
        "Q8": "Q8",
        "XC60": "XC60",
        "XC90": "XC90",
        "ES8": "ES8",
        "ES6": "ES6",
        "ET5": "ET5",
        "ET7": "ET7",
    }

    COLOR_MAP: Dict[str, str] = {
        "黑": "black",
        "白": "white",
        "灰": "gray",
        "银": "silver",
        "红": "red",
        "蓝": "blue",
        "绿": "green",
        "黄": "yellow",
        "橙": "orange",
        "棕": "brown",
        "褐": "brown",
        "米": "beige",
        "紫": "purple",
        "粉": "pink",
        "金": "gold",
    }

    def _decode_html(self, raw: bytes | str) -> str:
        if isinstance(raw, str):
            return raw
        for encoding in ("gb18030", "gb2312", "utf-8"):
            try:
                return raw.decode(encoding)
            except Exception:
                continue
        return raw.decode("utf-8", errors="ignore")

    def _page_url(self, page: int) -> str:
        base = self.config.base_search_url
        if page <= 1:
            return base
        if self.PAGE_RE.search(base):
            return self.PAGE_RE.sub(f"csp{page}exx0", base)
        if self.PAGE_SLOT_RE.search(base):
            return self.PAGE_SLOT_RE.sub(f"csp{page}exx0", base)
        suffix = f"csp{page}exx0"
        if "/#" in base:
            url, frag = base.split("/#", 1)
            return f"{url.rstrip('/')}/{suffix}/#{frag}"
        return f"{base.rstrip('/')}/{suffix}/"

    def _make_full_url(self, href: Optional[str]) -> Optional[str]:
        if not href:
            return None
        return urljoin(self.config.base_search_url, href)

    def _normalize_image_url(self, raw: Optional[str]) -> Optional[str]:
        value = str(raw or "").strip()
        if not value:
            return None
        if value.startswith("//"):
            return f"https:{value}"
        if value.startswith("http://"):
            return f"https://{value[7:]}"
        if value.startswith("https://"):
            return value
        if value.startswith("/"):
            return urljoin(self.config.base_search_url, value)
        return None

    def _parse_price_cny(self, raw: Optional[str]) -> Optional[float]:
        text = str(raw or "").strip()
        if not text:
            return None
        match = self.PRICE_WAN_RE.search(text)
        if not match:
            return None
        try:
            return round(float(match.group(1)) * 10000, 2)
        except ValueError:
            return None

    def _parse_mileage_km(self, raw: Optional[str]) -> Optional[int]:
        text = str(raw or "").strip().replace("公里", "").replace("km", "").replace("KM", "")
        if not text:
            return None
        match = self.PRICE_WAN_RE.search(text)
        if not match:
            return None
        try:
            value = float(match.group(1))
        except ValueError:
            return None
        if "万" in text:
            value *= 10000
        return int(round(value))

    def _parse_reg(self, raw: Optional[str]) -> tuple[Optional[int], Optional[int]]:
        text = str(raw or "").strip()
        if not text:
            return None, None
        match = self.YEAR_MONTH_RE.search(text)
        if not match:
            return None, None
        try:
            return int(match.group(1)), int(match.group(2))
        except ValueError:
            return None, None

    def _parse_iso_dt(self, raw: Optional[str]) -> Optional[datetime]:
        text = str(raw or "").strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return None

    def _clean_title(self, raw: Optional[str]) -> str:
        text = re.sub(r"\s+", " ", str(raw or "").strip())
        return text.replace("新上架", "").strip()

    def _translate_brand(self, raw: Optional[str]) -> Optional[str]:
        text = str(raw or "").strip()
        if not text:
            return None
        for key, value in sorted(self.BRAND_NAME_MAP.items(), key=lambda item: len(item[0]), reverse=True):
            if text.startswith(key) or text == key:
                return value
        return text

    def _translate_model_hint(self, raw: Optional[str]) -> Optional[str]:
        text = str(raw or "").strip()
        if not text:
            return None
        for key, value in sorted(self.MODEL_HINT_MAP.items(), key=lambda item: len(item[0]), reverse=True):
            if key in text:
                return value
        return None

    def _latin_model_from_text(self, raw: Optional[str]) -> Optional[str]:
        text = str(raw or "").strip()
        if not text:
            return None
        matches = self.LATIN_MODEL_RE.findall(text)
        filtered: list[str] = []
        for token in matches:
            low = token.lower()
            if re.fullmatch(r"(19|20)\d{2}", token):
                continue
            if low in {"t", "l", "v", "vi", "v6", "v8", "at", "mt", "cvt", "dct"}:
                continue
            if token.isdigit():
                continue
            filtered.append(token)
        if not filtered:
            return None
        if filtered[0].lower() == "e" and len(filtered) > 1 and filtered[1].isdigit():
            return "E-Class"
        token = filtered[0]
        if token.islower():
            return token.title()
        return token

    def _body_type_from_cn(self, raw: Optional[str]) -> Optional[str]:
        text = str(raw or "").strip().upper()
        if not text:
            return None
        if "SUV" in text or "越野" in text or "跨界" in text:
            return "suv"
        if "MPV" in text or "轻客" in text or "面包" in text or "厢式" in text or "厢货" in text:
            return "van"
        if "皮卡" in text:
            return "pickup"
        if "敞篷" in text or "CABRIO" in text:
            return "convertible"
        if "跑车" in text or "超跑" in text:
            return "sportscar"
        if "旅行" in text or "猎装" in text or "WAGON" in text:
            return "wagon"
        if "掀背" in text or "两厢" in text or "HATCH" in text:
            return "hatchback"
        if "轿车" in text or "三厢" in text or "SEDAN" in text:
            return "sedan"
        if "COUPE" in text or "轿跑" in text:
            return "coupe"
        return None

    def _transmission_from_cn(self, raw: Optional[str]) -> Optional[str]:
        text = re.sub(r"\s+", "", str(raw or "").strip())
        if not text:
            return None
        if "手动" in text or text == "MT":
            return "manual"
        if "CVT" in text:
            return "automatic"
        if "双离合" in text or "自动" in text or "AT" in text or "DCT" in text or "DSG" in text or "手自一体" in text:
            return "automatic"
        return None

    def _drive_from_cn(self, raw: Optional[str]) -> Optional[str]:
        text = re.sub(r"\s+", "", str(raw or "").strip())
        if not text:
            return None
        if "四驱" in text or "全轮" in text or "AWD" in text or "4WD" in text:
            return "awd"
        if "前驱" in text or "FWD" in text:
            return "fwd"
        if "后驱" in text or "RWD" in text:
            return "rwd"
        return None

    def _color_from_cn(self, raw: Optional[str]) -> Optional[str]:
        text = str(raw or "").strip()
        if not text:
            return None
        for token, value in self.COLOR_MAP.items():
            if token in text:
                return value
        return None

    def _fuel_from_cn(self, *values: Any) -> Optional[str]:
        text = " ".join(str(v or "") for v in values).strip().lower()
        if not text:
            return None
        if any(token in text for token in ("纯电", "电动", "ev", "edrive", "bev")):
            return "electric"
        if any(token in text for token in ("插电", "phev", "增程", "混动", "混合", "双擎", "dm-i", "dm-p", "hev")):
            return "hybrid"
        if "柴油" in text:
            return "diesel"
        if any(token in text for token in ("cng", "天然气")):
            return "cng"
        if any(token in text for token in ("lpg", "液化石油气")):
            return "lpg"
        if any(token in text for token in ("氢", "hydrogen")):
            return "hydrogen"
        if any(token in text for token in ("汽油", "95号", "92号", "98号")):
            return "petrol"
        return None

    def _power_hp_from_text(self, *values: Any) -> Optional[float]:
        text = " ".join(str(v or "") for v in values).strip()
        if not text:
            return None
        match = self.HORSEPOWER_RE.search(text)
        if not match:
            return None
        try:
            return float(match.group(1))
        except ValueError:
            return None

    def _parse_basic_pairs(self, soup: BeautifulSoup) -> Dict[str, str]:
        pairs: Dict[str, str] = {}
        for li in soup.select("ul.basic-item-ul li"):
            label_el = li.select_one(".item-name")
            if not label_el:
                continue
            label = re.sub(r"[\s\xa0]+", "", label_el.get_text(" ", strip=True))
            value = li.get_text(" ", strip=True).replace(label_el.get_text(" ", strip=True), "", 1).strip()
            value = re.sub(r"\s+", " ", value)
            if label and value and label not in pairs:
                pairs[label] = value
        return pairs

    def _extract_breadcrumb_hints(self, html: str) -> Dict[str, str]:
        out: Dict[str, str] = {}
        pattern = re.compile(
            r'<a href="/[^"/]+/(?P<brand_slug>[^"/]+)/#pvareaid[^"]*"[^>]*>(?P<brand_cn>[^<]+)</a>'
            r'.*?<a href="/[^"/]+/(?P=brand_slug)/(?P<model_slug>[^"/]+)/#pvareaid[^"]*"[^>]*>二手(?P<model_cn>[^<]+)</a>',
            re.S,
        )
        match = pattern.search(html)
        if not match:
            return out
        out["brand_slug"] = match.group("brand_slug")
        out["brand_cn"] = match.group("brand_cn").strip()
        out["model_slug"] = match.group("model_slug")
        out["model_cn"] = match.group("model_cn").strip()
        return out

    def _base_payload_from_card(self, card) -> Dict[str, Any]:
        title = self._clean_title(card.select_one(".card-name").get_text(" ", strip=True) if card.select_one(".card-name") else card.get("carname"))
        summary = (card.select_one(".cards-unit").get_text(" ", strip=True) if card.select_one(".cards-unit") else "").strip()
        link_el = card.select_one("a.carinfo")
        img_el = card.select_one(".img-box img")
        thumb = None
        if img_el is not None:
            thumb = img_el.get("src2") or img_el.get("data-original") or img_el.get("src")
        reg_year, reg_month = self._parse_reg(card.get("regdate") or summary)
        brand = None
        model = self._translate_model_hint(title) or self._latin_model_from_text(title)
        for key, value in sorted(self.BRAND_NAME_MAP.items(), key=lambda item: len(item[0]), reverse=True):
            if title.startswith(key):
                brand = value
                break
        if brand and title.startswith(next(k for k, v in self.BRAND_NAME_MAP.items() if v == brand or title.startswith(k))):
            pass
        year = reg_year
        mileage = self._parse_mileage_km(card.get("milage") or summary)
        listing_date = self._parse_iso_dt(card.get("publicdate"))
        return {
            "external_id": str(card.get("infoid") or "").strip(),
            "brand": brand,
            "model": model,
            "variant": title,
            "year": year,
            "registration_year": reg_year,
            "registration_month": reg_month,
            "mileage": mileage,
            "price": self._parse_price_cny(card.get("price") or (card.select_one(".pirce") and card.select_one(".pirce").get_text(" ", strip=True))),
            "currency": self.config.defaults.get("currency", "CNY"),
            "source_url": self._make_full_url(link_el.get("href") if link_el else None),
            "thumbnail_url": self._normalize_image_url(thumb),
            "listing_date": listing_date,
            "source_payload": {
                "title": title,
                "summary": summary,
                "price_wan": str(card.get("price") or "").strip() or None,
                "milage_wan_km": str(card.get("milage") or "").strip() or None,
                "regdate": str(card.get("regdate") or "").strip() or None,
                "publicdate": str(card.get("publicdate") or "").strip() or None,
                "specid": str(card.get("specid") or "").strip() or None,
                "dealerid": str(card.get("dealerid") or "").strip() or None,
                "seriesid": str(card.get("seriesid") or "").strip() or None,
                "city_code": str(card.get("cid") or "").strip() or None,
                "province_code": str(card.get("pid") or "").strip() or None,
            },
        }

    def parse_list_html(self, html: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        out: List[Dict[str, Any]] = []
        for card in soup.select(self.LIST_SELECTOR):
            external_id = str(card.get("infoid") or "").strip()
            if not external_id:
                continue
            payload = self._base_payload_from_card(card)
            out.append(payload)
        return out

    def parse_detail_html(self, html: str, *, fallback: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        soup = BeautifulSoup(html, "html.parser")
        box = soup.select_one(".car-box")
        pairs = self._parse_basic_pairs(soup)
        breadcrumb = self._extract_breadcrumb_hints(html)

        title = self._clean_title(box.select_one(".car-brand-name").get_text(" ", strip=True) if box and box.select_one(".car-brand-name") else (fallback or {}).get("variant"))
        brand_cn = breadcrumb.get("brand_cn")
        brand = self._translate_brand(brand_cn) or (fallback or {}).get("brand")

        model_hint = (
            self._translate_model_hint(breadcrumb.get("model_cn"))
            or self._translate_model_hint(title)
            or self._latin_model_from_text(breadcrumb.get("model_cn"))
            or self._latin_model_from_text(title)
            or (fallback or {}).get("model")
        )

        engine_text = pairs.get("发动机") or ""
        displacement_text = pairs.get("排量") or ""
        fuel_text = pairs.get("燃油类型") or pairs.get("燃料类型") or pairs.get("燃油标号") or ""
        body_text = pairs.get("车辆级别") or pairs.get("车身结构") or ""
        color_text = pairs.get("车身颜色") or pairs.get("车辆颜色") or ""
        drive_text = pairs.get("驱动方式") or ""
        transmission_text = pairs.get("变速箱") or ""
        reg_text = pairs.get("上牌时间") or ""
        mileage_text = pairs.get("表显里程") or ""
        listing_date_text = pairs.get("发布时间") or ""

        reg_year, reg_month = self._parse_reg(reg_text or (fallback or {}).get("source_payload", {}).get("regdate"))
        listing_date = self._parse_iso_dt(listing_date_text) or (fallback or {}).get("listing_date")
        engine_cc = infer_engine_cc_from_text(title, engine_text, displacement_text)
        power_hp = self._power_hp_from_text(engine_text, title)
        power_kw = round(power_hp / 1.35962, 2) if power_hp else None
        price = self._parse_price_cny(soup.select_one("#car_price").get("value") if soup.select_one("#car_price") else None)
        if price is None and box:
            price = self._parse_price_cny(box.select_one(".price").get_text(" ", strip=True) if box.select_one(".price") else None)

        images: List[str] = []
        for img in soup.select(".foucs-box img"):
            url = self._normalize_image_url(img.get("src2") or img.get("data-original") or img.get("src"))
            if not url or "default-220x165" in url:
                continue
            if url not in images:
                images.append(url)
        if not images and fallback and fallback.get("thumbnail_url"):
            images = [fallback["thumbnail_url"]]

        description = ""
        meta_desc = soup.select_one('meta[name="description"]')
        if meta_desc and meta_desc.get("content"):
            description = re.sub(r"\s+", " ", meta_desc.get("content")).strip()

        source_payload = dict((fallback or {}).get("source_payload") or {})
        source_payload.update(
            {
                "title_cn": title,
                "brand_cn": brand_cn,
                "model_cn": breadcrumb.get("model_cn"),
                "brand_slug": breadcrumb.get("brand_slug"),
                "model_slug": breadcrumb.get("model_slug"),
                "basic_fields": pairs,
                "engine_raw": engine_text or None,
                "body_raw": body_text or None,
                "color_raw": color_text or None,
                "drive_raw": drive_text or None,
                "transmission_raw": transmission_text or None,
                "fuel_raw": fuel_text or None,
            }
        )

        return {
            "brand": brand,
            "model": model_hint,
            "variant": title,
            "registration_year": reg_year,
            "registration_month": reg_month,
            "year": reg_year or (fallback or {}).get("year"),
            "mileage": self._parse_mileage_km(mileage_text) or (fallback or {}).get("mileage"),
            "price": price if price is not None else (fallback or {}).get("price"),
            "currency": self.config.defaults.get("currency", "CNY"),
            "engine_type": self._fuel_from_cn(title, engine_text, fuel_text),
            "engine_cc": engine_cc,
            "power_hp": power_hp,
            "power_kw": power_kw,
            "body_type": self._body_type_from_cn(body_text),
            "transmission": self._transmission_from_cn(transmission_text),
            "drive_type": self._drive_from_cn(drive_text),
            "color": self._color_from_cn(color_text),
            "description": description or None,
            "listing_date": listing_date,
            "thumbnail_url": images[0] if images else (fallback or {}).get("thumbnail_url"),
            "images": images or (fallback or {}).get("images"),
            "source_payload": source_payload,
        }

    def _fetch_html(self, url: str) -> str:
        response = self._http_get(url)
        response.raise_for_status()
        return self._decode_html(response.content)

    def fetch_items(self, profile: Dict[str, Any]) -> List[CarParsed]:
        max_pages = max(1, int(profile.get("max_pages") or self.config.pagination.max_pages))
        skip_details = bool(profile.get("skip_details", False))
        detail_limit = int(profile.get("detail_limit") or 0)
        detail_checked = 0
        items: List[CarParsed] = []

        for page in range(self.config.pagination.start_page, self.config.pagination.start_page + max_pages):
            list_url = self._page_url(page)
            html = self._fetch_html(list_url)
            parsed_cards = self.parse_list_html(html)
            logger.info("[che168] page=%s cards=%s", page, len(parsed_cards))
            if not parsed_cards:
                break
            for payload in parsed_cards:
                detail_payload = dict(payload)
                if not skip_details and payload.get("source_url"):
                    if detail_limit <= 0 or detail_checked < detail_limit:
                        try:
                            detail_html = self._fetch_html(str(payload["source_url"]))
                            detail_payload.update(self.parse_detail_html(detail_html, fallback=payload))
                            detail_checked += 1
                            self._delay()
                        except Exception as exc:
                            logger.warning("[che168] detail failed url=%s err=%s", payload.get("source_url"), str(exc)[:200])
                items.append(
                    CarParsed(
                        source_key=self.config.key,
                        external_id=str(detail_payload.get("external_id") or payload.get("external_id")),
                        country=(self.config.country or "CN").upper(),
                        brand=detail_payload.get("brand"),
                        model=detail_payload.get("model"),
                        variant=detail_payload.get("variant"),
                        year=detail_payload.get("year"),
                        registration_year=detail_payload.get("registration_year"),
                        registration_month=detail_payload.get("registration_month"),
                        mileage=detail_payload.get("mileage"),
                        price=detail_payload.get("price"),
                        currency=detail_payload.get("currency") or self.config.defaults.get("currency"),
                        body_type=detail_payload.get("body_type"),
                        engine_type=detail_payload.get("engine_type"),
                        engine_cc=detail_payload.get("engine_cc"),
                        power_hp=detail_payload.get("power_hp"),
                        power_kw=detail_payload.get("power_kw"),
                        transmission=detail_payload.get("transmission"),
                        drive_type=detail_payload.get("drive_type"),
                        color=detail_payload.get("color"),
                        description=detail_payload.get("description"),
                        source_url=detail_payload.get("source_url") or payload.get("source_url"),
                        thumbnail_url=detail_payload.get("thumbnail_url") or payload.get("thumbnail_url"),
                        source_payload=detail_payload.get("source_payload"),
                        listing_date=detail_payload.get("listing_date"),
                        images=detail_payload.get("images"),
                    )
                )
            self._delay()
        return items


__all__ = ["Che168Parser"]
