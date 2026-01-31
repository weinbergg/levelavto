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
