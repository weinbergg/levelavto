from __future__ import annotations

import hashlib
import io
import os
import subprocess
import time
import uuid
from urllib.parse import urlparse, unquote

import logging
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from PIL import Image

router = APIRouter()
logger = logging.getLogger(__name__)

_ALLOWED_HOSTS = {"img.classistatic.de"}
_LOCK_TTL_SEC = 30
_CACHE_TTL_SEC = 7 * 24 * 3600


def _cache_dir() -> str:
    base = os.getenv("THUMB_CACHE_DIR") or "/app/thumb_cache"
    os.makedirs(base, exist_ok=True)
    return base


def _cache_path(url: str, width: int, fmt: str) -> str:
    key = hashlib.sha1(f"{url}|{width}|{fmt}".encode("utf-8")).hexdigest()
    return os.path.join(_cache_dir(), f"{key}.{fmt}")


def _meta_path(path: str) -> str:
    return f"{path}.meta"


def _is_fresh(path: str, ttl: int) -> bool:
    try:
        return (time.time() - os.path.getmtime(path)) <= ttl
    except FileNotFoundError:
        return False


def _normalize_source_url(u: str | None, url: str | None) -> str:
    if not u and url:
        u = url
    if not u:
        raise HTTPException(status_code=400, detail="missing url")
    src = unquote(u).strip()
    while src.startswith("."):
        src = src[1:]
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


def _placeholder_response() -> FileResponse:
    path = "/app/backend/app/static/img/no-photo.svg"
    return FileResponse(
        path,
        media_type="image/svg+xml",
        headers={
            "Cache-Control": "public, max-age=604800",
        },
    )


def _acquire_lock(path: str) -> bool:
    lock_path = f"{path}.lock"
    now = time.time()
    try:
        if os.path.exists(lock_path) and now - os.path.getmtime(lock_path) < _LOCK_TTL_SEC:
            return False
        with open(lock_path, "w") as f:
            f.write(str(now))
        return True
    except Exception:
        return False


def _release_lock(path: str) -> None:
    lock_path = f"{path}.lock"
    try:
        if os.path.exists(lock_path):
            os.remove(lock_path)
    except Exception:
        pass


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
        # stale-while-revalidate: serve cached even if stale, refresh in background on next request
        logger.info("thumb_cache_hit path=%s", path)
        return FileResponse(
            path,
            media_type=media_type,
            headers={
                "Cache-Control": "public, max-age=604800, stale-while-revalidate=86400",
                "ETag": os.path.basename(path),
            },
        )

    if not _acquire_lock(path):
        # another worker is fetching; return placeholder to avoid stampede
        logger.info("thumb_lock_busy url=%s", src)
        return _placeholder_response()

    tmp_fetch = os.path.join(_cache_dir(), f".fetch-{uuid.uuid4().hex}")
    code = _fetch_with_curl(src, tmp_fetch, max_bytes=2_000_000)
    if code in (404, 410):
        try:
            if os.path.exists(tmp_fetch):
                os.remove(tmp_fetch)
        except Exception:
            pass
        _release_lock(path)
        return _placeholder_response()
    if code == 413:
        try:
            if os.path.exists(tmp_fetch):
                os.remove(tmp_fetch)
        except Exception:
            pass
        _release_lock(path)
        raise HTTPException(status_code=413, detail="upstream too large")
    if code != 200:
        try:
            if os.path.exists(tmp_fetch):
                os.remove(tmp_fetch)
        except Exception:
            pass
        _release_lock(path)
        return _placeholder_response()

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
        logger.info("thumb_cache_write_ok path=%s bytes=%s fmt=%s w=%s", path, os.path.getsize(path), fmt, w)
        # touch meta for freshness
        try:
            with open(_meta_path(path), "w") as mh:
                mh.write(str(time.time()))
        except Exception:
            pass
    except Exception:
        _release_lock(path)
        return _placeholder_response()
    finally:
        try:
            if os.path.exists(tmp_fetch):
                os.remove(tmp_fetch)
        except Exception:
            pass
        _release_lock(path)

    return FileResponse(
        path,
        media_type=media_type,
        headers={
            "Cache-Control": "public, max-age=604800, stale-while-revalidate=86400",
            "ETag": os.path.basename(path),
        },
    )
