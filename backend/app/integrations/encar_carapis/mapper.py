from __future__ import annotations

from typing import Any, Dict, List, Optional

from ...parsing.base import CarParsed


def _normalize_body(body: Optional[str]) -> Optional[str]:
    if not body:
        return None
    t = str(body).lower()
    mapping = {
        "suv": "suv",
        "rv": "suv",
        "van": "van",
        "micro_van": "van",
        "truck": "pickup",
        "sports": "coupe",
        "micro": "hatchback",
        "small": "hatchback",
        "semi_mid": "sedan",
        "mid": "sedan",
        "large": "sedan",
    }
    return mapping.get(t, None)


def _normalize_fuel(fuel: Optional[str]) -> Optional[str]:
    if not fuel:
        return None
    t = str(fuel).lower()
    mapping = {
        "gasoline": "Petrol",
        "diesel": "Diesel",
        "hybrid": "Hybrid",
        "electric": "Electric",
        "lpg": "LPG",
        "cng": "CNG",
        "gasoline_lpg": "Petrol/LPG",
        "gasoline_cng": "Petrol/CNG",
        "hydrogen": "Hydrogen",
    }
    return mapping.get(t, t.title())


def _normalize_transmission(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    t = str(val).lower()
    mapping = {
        "auto": "Automatic",
        "manual": "Manual",
        "semi_auto": "Semi-automatic",
        "cvt": "CVT",
    }
    return mapping.get(t, t.title())


def _normalize_color(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    t = str(val).lower().replace("_", " ")
    palette = {
        "white": "white",
        "black": "black",
        "gray": "gray",
        "grey": "gray",
        "silver": "silver",
        "bright silver": "silver",
        "silver gray": "gray",
        "blue": "blue",
        "light blue": "blue",
        "dark green": "green",
        "green": "green",
        "teal": "green",
        "red": "red",
        "maroon": "red",
        "orange": "orange",
        "yellow": "yellow",
        "gold": "gold",
        "light gold": "gold",
        "beige": "beige",
        "brown": "brown",
        "pink": "pink",
        "purple": "purple",
        "pearl": "silver",
    }
    for key, norm in palette.items():
        if key in t:
            return norm
    return t


def _collect_images(detail: Dict[str, Any], fallback: Optional[str]) -> List[str]:
    urls: List[str] = []
    photos = detail.get("photos") or []
    for ph in photos:
        url = None
        if isinstance(ph, dict):
            url = ph.get("image_url") or ph.get("path")
        elif isinstance(ph, str):
            url = ph
        if url and url not in urls:
            urls.append(url)
    if not urls and fallback:
        urls.append(fallback)
    return urls


def map_vehicle_detail(
    detail: Dict[str, Any],
    *,
    source_key: str,
    currency: str,
    country: str,
    fallback_main_photo: Optional[str] = None,
) -> CarParsed:
    model = detail.get("model") or {}
    model_group = model.get("model_group") or {}
    manufacturer = model_group.get("manufacturer") or {}

    vehicle_id = detail.get("vehicle_id") or detail.get(
        "id") or detail.get("vehicle")
    external_id = str(
        vehicle_id) if vehicle_id is not None else f"{source_key}-missing-id"
    images = _collect_images(detail, fallback_main_photo)
    thumbnail = detail.get("main_photo") or (
        images[0] if images else fallback_main_photo)
    if thumbnail and not images:
        images = [thumbnail]

    return CarParsed(
        source_key=source_key,
        external_id=external_id,
        country=country,
        brand=manufacturer.get("name") or manufacturer.get("slug"),
        model=model.get("name") or model_group.get("name"),
        year=detail.get("year"),
        mileage=detail.get("mileage"),
        price=detail.get("price"),
        currency=currency,
        body_type=_normalize_body(detail.get("body_type")),
        engine_type=_normalize_fuel(detail.get("fuel_type")),
        transmission=_normalize_transmission(detail.get("transmission")),
        drive_type=None,
        color=_normalize_color(detail.get("color")),
        vin=detail.get("vin"),
        source_url=f"https://www.encar.com/dc/dc_cardetailview.do?carid={vehicle_id}" if vehicle_id else None,
        thumbnail_url=thumbnail,
        images=images if images else None,
    )


__all__ = ["map_vehicle_detail"]
