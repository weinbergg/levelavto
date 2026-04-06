from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from backend.app.parsing.che168 import Che168Parser
from backend.app.parsing.config import load_sites_config


DEFAULT_LISTING = "/Users/georgij/Desktop/АвтоПарсер/сайты/che168_listing.html"
DEFAULT_DETAIL = "/Users/georgij/Desktop/АвтоПарсер/сайты/che168_car.html"


def _load_text(path: str) -> str:
    raw = Path(path).read_bytes()
    parser = Che168Parser(load_sites_config().get("che168"))
    return parser._decode_html(raw)


def _preview(item: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "external_id",
        "brand",
        "model",
        "variant",
        "year",
        "registration_year",
        "registration_month",
        "mileage",
        "price",
        "currency",
        "body_type",
        "engine_type",
        "engine_cc",
        "power_hp",
        "power_kw",
        "transmission",
        "drive_type",
        "color",
        "thumbnail_url",
        "source_url",
    ]
    return {key: item.get(key) for key in keys}


def main() -> None:
    parser = argparse.ArgumentParser(description="Offline smoke parser for saved che168 HTML")
    parser.add_argument("--listing", default=DEFAULT_LISTING, help="Path to saved che168 listing HTML")
    parser.add_argument("--detail", default=DEFAULT_DETAIL, help="Path to saved che168 detail HTML")
    parser.add_argument("--limit", type=int, default=3, help="How many listing cards to preview")
    args = parser.parse_args()

    cfg = load_sites_config().get("che168")
    che = Che168Parser(cfg)
    listing_html = _load_text(args.listing)
    cards = che.parse_list_html(listing_html)
    print(json.dumps({"listing_count": len(cards), "listing_preview": [_preview(card) for card in cards[: max(0, args.limit)]]}, ensure_ascii=False, indent=2))

    if cards and args.detail and Path(args.detail).exists():
        detail_html = _load_text(args.detail)
        detail = che.parse_detail_html(detail_html, fallback=cards[0])
        detail_preview = _preview(detail)
        detail_preview["images"] = (detail.get("images") or [])[:5]
        detail_preview["source_payload"] = {
            "brand_cn": (detail.get("source_payload") or {}).get("brand_cn"),
            "model_cn": (detail.get("source_payload") or {}).get("model_cn"),
            "color_raw": (detail.get("source_payload") or {}).get("color_raw"),
            "body_raw": (detail.get("source_payload") or {}).get("body_raw"),
            "engine_raw": (detail.get("source_payload") or {}).get("engine_raw"),
        }
        print(json.dumps({"detail_preview": detail_preview}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
