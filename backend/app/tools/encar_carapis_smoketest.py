from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List

from ..config import settings
from ..integrations.encar_carapis import EncarCarapisAdapter
from ..parsing.base import CarParsed
from ..parsing.config import load_sites_config


def _resolve_api_key() -> str | None:
    return settings.ENCAR_CARAPIS_API_KEY or os.getenv("CARAPIS_API_KEY") or os.getenv("ENCAR_API_KEY")


def _car_preview(car: CarParsed) -> Dict[str, Any]:
    return {
        "external_id": car.external_id,
        "brand": car.brand,
        "model": car.model,
        "year": car.year,
        "mileage": car.mileage,
        "price": car.price,
        "currency": car.currency,
        "thumbnail_url": car.thumbnail_url,
        "images_count": len(car.images or []),
        "source_url": car.source_url,
    }


def main() -> None:
    api_key = _resolve_api_key()
    if not api_key:
        print("❗ ENCAR_CARAPIS_API_KEY / CARAPIS_API_KEY is not set. Add it to .env.")
        return

    sites = load_sites_config()
    cfg = sites.get("encar")

    adapter = EncarCarapisAdapter(cfg)
    brands: List[str] = list(cfg.defaults.get("brands") or ["BMW", "Audi"])
    if not brands:
        brands = ["BMW", "Audi"]
    brands = brands[:2]

    limit = int(min(cfg.defaults.get("limit_per_query") or 50, 50))
    max_pages = int(min(cfg.defaults.get("max_pages")
                    or cfg.pagination.max_pages, cfg.pagination.max_pages, 2))

    all_results: List[CarParsed] = []
    samples: List[Dict[str, Any]] = []

    for brand in brands:
        cars = adapter.fetch_items(
            {"brand": [brand], "limit_per_query": limit, "max_pages": max_pages})
        all_results.extend(cars)
        count = len(cars)
        first = cars[0] if cars else None
        print(
            f"Brand '{brand}': found {count} cars (limit={limit}, pages={max_pages})")
        if first:
            print(
                f"  Example: {first.brand} {first.model} {first.year} | "
                f"{first.mileage} km | {first.price} {first.currency} | photos={len(first.images or [])} | url={first.source_url}"
            )
            samples.append({"brand": brand, "count": count,
                           "sample": _car_preview(first)})
        else:
            samples.append({"brand": brand, "count": count, "sample": None})

    tmp_path = Path("/app/tmp")
    tmp_path.mkdir(parents=True, exist_ok=True)
    sample_path = tmp_path / "encar_sample.json"
    payload = {
        "brands": brands,
        "total_found": len(all_results),
        "samples": samples,
        "warning": adapter.last_warning,
    }
    sample_path.write_text(json.dumps(
        payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved sample payload to {sample_path}")


if __name__ == "__main__":
    main()
