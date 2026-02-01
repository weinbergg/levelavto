import pytest

from fastapi import HTTPException

from backend.app.routers import thumbs


def test_normalize_source_url_accepts_alias():
    src = "https://img.classistatic.de/api/v1/mo-prod/images/aa/aa.jpg?rule=mo-1024.jpg"
    assert thumbs._normalize_source_url(None, src) == src


def test_normalize_source_url_accepts_u():
    src = "https://img.classistatic.de/api/v1/mo-prod/images/bb/bb.jpg?rule=mo-1024.jpg"
    assert thumbs._normalize_source_url(src, None) == src


def test_normalize_source_url_rejects_scheme():
    with pytest.raises(HTTPException):
        thumbs._normalize_source_url("ftp://img.classistatic.de/x", None)


def test_normalize_source_url_rejects_host():
    with pytest.raises(HTTPException):
        thumbs._normalize_source_url("https://example.com/x.jpg", None)


def test_lock_acquire_and_busy(tmp_path, monkeypatch):
    monkeypatch.setenv("THUMB_CACHE_DIR", str(tmp_path))
    src = "https://img.classistatic.de/api/v1/mo-prod/images/aa/aa.jpg?rule=mo-1024.jpg"
    path = thumbs._cache_path(src, 360, "webp")
    assert thumbs._acquire_lock(path) is True
    # second acquire should be blocked while lock is fresh
    assert thumbs._acquire_lock(path) is False
    thumbs._release_lock(path)


def test_thumb_placeholder_on_error(tmp_path, monkeypatch):
    monkeypatch.setenv("THUMB_CACHE_DIR", str(tmp_path))
    monkeypatch.setattr(thumbs, "_fetch_with_curl", lambda *args, **kwargs: 500)
    resp = thumbs.thumb(
        u="https://img.classistatic.de/api/v1/mo-prod/images/aa/aa.jpg?rule=mo-1024.jpg",
        w=360,
        fmt="webp",
    )
    assert resp.media_type.startswith("image/")
