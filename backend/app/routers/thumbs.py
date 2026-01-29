from __future__ import annotations

import hashlib
import io
import os
from urllib.parse import urlparse, unquote

import requests
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from PIL import Image

router = APIRouter()

_ALLOWED_HOSTS = {"img.classistatic.de"}


def _cache_dir() -> str:
    base = os.getenv("THUMB_CACHE_DIR") or "/opt/levelavto/thumb_cache"
    os.makedirs(base, exist_ok=True)
    return base


def _cache_path(url: str, width: int, fmt: str) -> str:
    key = hashlib.sha1(f"{url}|{width}|{fmt}".encode("utf-8")).hexdigest()
    return os.path.join(_cache_dir(), f"{key}.{fmt}")


@router.get("/thumb")
def thumb(
    u: str = Query(..., description="Source image URL"),
    w: int = Query(360, ge=120, le=1024),
    fmt: str = Query("webp", regex="^(webp|jpg|jpeg)$"),
):
    src = unquote(u)
    parsed = urlparse(src)
    if parsed.scheme not in {"http", "https"}:
        raise HTTPException(status_code=400, detail="invalid url scheme")
    if parsed.hostname not in _ALLOWED_HOSTS:
        raise HTTPException(status_code=400, detail="invalid host")

    fmt = "jpg" if fmt == "jpeg" else fmt
    path = _cache_path(src, w, fmt)
    if os.path.exists(path):
        return FileResponse(
            path,
            headers={
                "Cache-Control": "public, max-age=604800",
                "ETag": os.path.basename(path),
            },
        )

    try:
        res = requests.get(src, timeout=(0.5, 2.5))
    except Exception:
        raise HTTPException(status_code=502, detail="upstream fetch failed")
    if res.status_code != 200:
        raise HTTPException(status_code=404, detail="upstream not found")

    try:
        img = Image.open(io.BytesIO(res.content))
        img = img.convert("RGB")
        if w and img.width > w:
            h = int(img.height * w / img.width)
            img = img.resize((w, h), Image.LANCZOS)
        tmp_path = f"{path}.tmp"
        img.save(tmp_path, format=fmt.upper(), quality=80, method=6)
        os.replace(tmp_path, path)
    except Exception:
        raise HTTPException(status_code=500, detail="thumb processing failed")

    return FileResponse(
        path,
        headers={
            "Cache-Control": "public, max-age=604800",
            "ETag": os.path.basename(path),
        },
    )
