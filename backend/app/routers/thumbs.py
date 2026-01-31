from __future__ import annotations

import hashlib
import io
import os
import subprocess
import uuid
from urllib.parse import urlparse, unquote

import logging
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from PIL import Image

router = APIRouter()
logger = logging.getLogger(__name__)

_ALLOWED_HOSTS = {"img.classistatic.de"}


def _cache_dir() -> str:
    base = os.getenv("THUMB_CACHE_DIR") or "/opt/levelavto/thumb_cache"
    os.makedirs(base, exist_ok=True)
    return base


def _cache_path(url: str, width: int, fmt: str) -> str:
    key = hashlib.sha1(f"{url}|{width}|{fmt}".encode("utf-8")).hexdigest()
    return os.path.join(_cache_dir(), f"{key}.{fmt}")


def _normalize_source_url(u: str | None, url: str | None) -> str:
    if not u and url:
        u = url
    if not u:
        raise HTTPException(status_code=400, detail="missing url")
    src = unquote(u)
    parsed = urlparse(src)
    if parsed.scheme not in {"http", "https"}:
        raise HTTPException(status_code=400, detail="invalid url scheme")
    if parsed.hostname not in _ALLOWED_HOSTS:
        raise HTTPException(status_code=400, detail="invalid host")
    return src


def _fetch_with_curl(src: str, dest: str, max_bytes: int) -> int:
    cmd = [
        "curl",
        "--http2",
        "-L",
        "--connect-timeout",
        "3",
        "--max-time",
        "12",
        "--retry",
        "2",
        "--retry-delay",
        "0",
        "-A",
        "Mozilla/5.0",
        "-sS",
        "-w",
        "%{http_code}",
        "-o",
        dest,
        src,
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True)
    except FileNotFoundError:
        logger.error("curl not found in container")
        return 0
    code = 0
    try:
        code = int((proc.stdout or "").strip() or "0")
    except Exception:
        code = 0
    if proc.returncode != 0:
        logger.warning(
            "thumb_fetch_failed url=%s code=%s err=%s",
            src,
            code,
            (proc.stderr or "").strip(),
        )
        return code or 0
    try:
        if os.path.getsize(dest) > max_bytes:
            os.remove(dest)
            return 413
    except FileNotFoundError:
        return 0
    return code


@router.get("/thumb")
def thumb(
    u: str | None = Query(None, description="Source image URL"),
    url: str | None = Query(None, description="Source image URL (alias)"),
    w: int = Query(360, ge=120, le=1024),
    fmt: str = Query("webp", regex="^(webp|jpg|jpeg)$"),
):
    src = _normalize_source_url(u, url)

    fmt = "jpg" if fmt == "jpeg" else fmt
    path = _cache_path(src, w, fmt)
    media_type = "image/webp" if fmt == "webp" else "image/jpeg"
    if os.path.exists(path):
        return FileResponse(
            path,
            media_type=media_type,
            headers={
                "Cache-Control": "public, max-age=604800",
                "ETag": os.path.basename(path),
            },
        )

    tmp_fetch = os.path.join(_cache_dir(), f".fetch-{uuid.uuid4().hex}")
    code = _fetch_with_curl(src, tmp_fetch, max_bytes=2_000_000)
    if code in (404, 410):
        raise HTTPException(status_code=404, detail="upstream not found")
    if code == 413:
        raise HTTPException(status_code=413, detail="upstream too large")
    if code != 200:
        raise HTTPException(status_code=502, detail="upstream fetch failed")

    try:
        with open(tmp_fetch, "rb") as handle:
            img = Image.open(io.BytesIO(handle.read()))
        img = img.convert("RGB")
        if w and img.width > w:
            h = int(img.height * w / img.width)
            img = img.resize((w, h), Image.LANCZOS)
        tmp_path = f"{path}.tmp"
        save_fmt = "JPEG" if fmt == "jpg" else fmt.upper()
        img.save(tmp_path, format=save_fmt, quality=80, method=6)
        os.replace(tmp_path, path)
    except Exception:
        raise HTTPException(status_code=500, detail="thumb processing failed")
    finally:
        try:
            if os.path.exists(tmp_fetch):
                os.remove(tmp_fetch)
        except Exception:
            pass

    return FileResponse(
        path,
        media_type=media_type,
        headers={
            "Cache-Control": "public, max-age=604800",
            "ETag": os.path.basename(path),
        },
    )
