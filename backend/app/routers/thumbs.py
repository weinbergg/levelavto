from __future__ import annotations

import hashlib
import io
import os
import time
import uuid
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse, unquote

import logging
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse, RedirectResponse
from PIL import Image
import requests

from ..utils.thumbs import resolve_thumbnail_url

router = APIRouter()
logger = logging.getLogger(__name__)

_ALLOWED_HOSTS = {"img.classistatic.de"}
_ALLOWED_HOST_SUFFIXES = (".autoimg.cn",)
_LOCK_TTL_SEC = 30
_CACHE_TTL_SEC = 7 * 24 * 3600
_NEGATIVE_TTL_NOT_FOUND_SEC = int(os.getenv("THUMB_NEGATIVE_TTL_NOT_FOUND_SEC", "86400"))
_NEGATIVE_TTL_ERROR_SEC = int(os.getenv("THUMB_NEGATIVE_TTL_ERROR_SEC", "120"))
_CLASSISTATIC_RULE_CANDIDATES = ("mo-1024.jpg", "mo-640.jpg", "mo-360.jpg", "mo-240.jpg")
_FETCH_CONNECT_TIMEOUT_SEC = str(int(os.getenv("THUMB_FETCH_CONNECT_TIMEOUT_SEC", "4")))
_FETCH_MAX_TIME_SEC = str(int(os.getenv("THUMB_FETCH_MAX_TIME_SEC", "4")))


def _cache_dir() -> str:
    base = os.getenv("THUMB_CACHE_DIR") or "/app/thumb_cache"
    os.makedirs(base, exist_ok=True)
    return base


def _cache_path(url: str, width: int, fmt: str) -> str:
    key = hashlib.sha1(f"{url}|{width}|{fmt}".encode("utf-8")).hexdigest()
    return os.path.join(_cache_dir(), f"{key}.{fmt}")


def _meta_path(path: str) -> str:
    return f"{path}.meta"


def _negative_path(path: str) -> str:
    return f"{path}.neg"


def _read_negative(path: str) -> tuple[int, int] | None:
    neg = _negative_path(path)
    try:
        if not os.path.exists(neg):
            return None
        with open(neg, "r", encoding="utf-8") as fh:
            raw = (fh.read() or "").strip()
        parts = raw.split("|")
        if len(parts) >= 3:
            code = int(parts[1])
            ttl = int(parts[2])
            if (time.time() - os.path.getmtime(neg)) <= ttl:
                return code, ttl
            return None
        # backward compatibility: old format, treat as transient error marker
        if (time.time() - os.path.getmtime(neg)) <= _NEGATIVE_TTL_ERROR_SEC:
            return 0, _NEGATIVE_TTL_ERROR_SEC
        return None
    except Exception:
        return None


def _mark_negative(path: str, code: int) -> None:
    ttl = _NEGATIVE_TTL_NOT_FOUND_SEC if code in (404, 410) else _NEGATIVE_TTL_ERROR_SEC
    neg = _negative_path(path)
    try:
        with open(neg, "w", encoding="utf-8") as fh:
            fh.write(f"{int(time.time())}|{int(code)}|{int(ttl)}")
    except Exception:
        return


def _clear_negative(path: str) -> None:
    neg = _negative_path(path)
    try:
        if os.path.exists(neg):
            os.remove(neg)
    except Exception:
        pass


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
    hostname = (parsed.hostname or "").lower()
    if hostname not in _ALLOWED_HOSTS and not any(hostname.endswith(suffix) for suffix in _ALLOWED_HOST_SUFFIXES):
        raise HTTPException(status_code=400, detail="invalid host")
    return src


def _preferred_classistatic_rules(width: int) -> list[str]:
    if width <= 240:
        return ["mo-240.jpg", "mo-360.jpg", "mo-640.jpg", "mo-1024.jpg"]
    if width <= 360:
        return ["mo-360.jpg", "mo-640.jpg", "mo-240.jpg", "mo-1024.jpg"]
    if width <= 640:
        return ["mo-640.jpg", "mo-1024.jpg", "mo-360.jpg", "mo-240.jpg"]
    # Main gallery/detail images should try the larger upstream source first.
    return ["mo-1024.jpg", "mo-640.jpg", "mo-360.jpg", "mo-240.jpg"]


def _classistatic_variants(src: str, width: int) -> list[str]:
    parsed = urlparse(src)
    if parsed.hostname not in _ALLOWED_HOSTS:
        return [src]
    params = parse_qsl(parsed.query, keep_blank_values=True)
    base_params: list[tuple[str, str]] = []
    current_rule: str | None = None
    for k, v in params:
        if k.lower() == "rule":
            current_rule = v
            continue
        base_params.append((k, v))

    rules: list[str] = []
    for rule in _preferred_classistatic_rules(width):
        if rule not in rules:
            rules.append(rule)
    if current_rule and current_rule not in rules:
        rules.append(current_rule)
    for rule in _CLASSISTATIC_RULE_CANDIDATES:
        if rule not in rules:
            rules.append(rule)

    variants: list[str] = []
    seen: set[str] = set()
    for rule in rules:
        query = urlencode([*base_params, ("rule", rule)], doseq=True)
        candidate = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, query, parsed.fragment))
        if candidate in seen:
            continue
        seen.add(candidate)
        variants.append(candidate)
    return variants or [src]


def _fetch_with_curl(src: str, dest: str, max_bytes: int) -> int:
    try:
        timeout = (float(_FETCH_CONNECT_TIMEOUT_SEC), float(_FETCH_MAX_TIME_SEC))
        headers = {"User-Agent": "Mozilla/5.0"}
        with requests.get(src, timeout=timeout, stream=True, allow_redirects=True, headers=headers) as resp:
            code = int(resp.status_code or 0)
            if code != 200:
                return code
            total = 0
            with open(dest, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=128 * 1024):
                    if not chunk:
                        continue
                    total += len(chunk)
                    if total > max_bytes:
                        fh.close()
                        try:
                            os.remove(dest)
                        except FileNotFoundError:
                            pass
                        return 413
                    fh.write(chunk)
            return 200
    except requests.RequestException as exc:
        logger.warning("thumb_fetch_failed url=%s err=%s", src, str(exc)[:200])
        try:
            if os.path.exists(dest):
                os.remove(dest)
        except Exception:
            pass
        return 0


def _placeholder_response(cache_control: str = "public, max-age=604800") -> FileResponse:
    path = "/app/backend/app/static/img/no-photo.svg"
    return FileResponse(
        path,
        media_type="image/svg+xml",
        headers={
            "Cache-Control": cache_control,
        },
    )


def _temporary_redirect_to_source(src: str) -> RedirectResponse:
    return RedirectResponse(
        url=src,
        status_code=307,
        headers={
            "Cache-Control": "no-store",
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

    # Negative cache for dead/failed upstream URLs (prevents repeated expensive retries).
    neg = _read_negative(path)
    if neg:
        neg_code, _ = neg
        if neg_code in (404, 410):
            # Permanent-ish missing upstream image, cacheable placeholder is fine.
            return _placeholder_response("public, max-age=86400")
        # transient upstream/network issue: fail fast with short-lived placeholder
        return _placeholder_response("public, max-age=30")

    if not _acquire_lock(path):
        # another worker is fetching; avoid stampede.
        # Placeholder is more stable than redirect to flaky upstream.
        logger.info("thumb_lock_busy url=%s", src)
        return _placeholder_response("public, max-age=30")

    tmp_fetch = os.path.join(_cache_dir(), f".fetch-{uuid.uuid4().hex}")
    codes: list[int] = []
    used_src = src
    ok = False
    for candidate in _classistatic_variants(src, w):
        used_src = candidate
        code = _fetch_with_curl(candidate, tmp_fetch, max_bytes=2_000_000)
        codes.append(code)
        if code == 200:
            ok = True
            break
        logger.info("thumb_variant_failed src=%s candidate=%s code=%s", src, candidate, code)
        if code == 413:
            try:
                if os.path.exists(tmp_fetch):
                    os.remove(tmp_fetch)
            except Exception:
                pass
            _release_lock(path)
            raise HTTPException(status_code=413, detail="upstream too large")

    if not ok:
        try:
            if os.path.exists(tmp_fetch):
                os.remove(tmp_fetch)
        except Exception:
            pass
        final_code = next((c for c in reversed(codes) if c), 0)
        logger.warning("thumb_fetch_exhausted src=%s codes=%s", src, codes)
        if codes and all(c in (404, 410) for c in codes if c):
            _mark_negative(path, final_code or 404)
            _release_lock(path)
            return _placeholder_response()
        _mark_negative(path, final_code)
        _release_lock(path)
        return _placeholder_response("public, max-age=30")

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
        _clear_negative(path)
        logger.info(
            "thumb_cache_write_ok path=%s bytes=%s fmt=%s w=%s src=%s",
            path,
            os.path.getsize(path),
            fmt,
            w,
            used_src,
        )
        # touch meta for freshness
        try:
            with open(_meta_path(path), "w") as mh:
                mh.write(str(time.time()))
        except Exception:
            pass
    except Exception:
        logger.exception("thumb_decode_failed src=%s used_src=%s", src, used_src)
        _release_lock(path)
        return _placeholder_response("public, max-age=30")
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
