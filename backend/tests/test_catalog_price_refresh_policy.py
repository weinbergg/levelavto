from backend.app.services.cars_service import CarsService


def test_catalog_inline_price_refresh_defaults_to_visible_pages(monkeypatch):
    monkeypatch.delenv("CATALOG_INLINE_PRICE_REFRESH", raising=False)
    monkeypatch.delenv("CATALOG_INLINE_PRICE_REFRESH_DEFAULT", raising=False)
    monkeypatch.delenv("CATALOG_INLINE_PRICE_REFRESH_MAX_PAGE", raising=False)
    monkeypatch.delenv("CATALOG_INLINE_PRICE_REFRESH_MAX_PAGE_SIZE", raising=False)
    service = CarsService(db=None)  # type: ignore[arg-type]

    assert service._should_catalog_inline_price_refresh(page=1, page_size=12) is True
    assert service._should_catalog_inline_price_refresh(page=3, page_size=24) is True
    assert service._should_catalog_inline_price_refresh(page=4, page_size=12) is False
    assert service._should_catalog_inline_price_refresh(page=1, page_size=36) is False


def test_catalog_inline_price_refresh_explicit_env_overrides_defaults(monkeypatch):
    service = CarsService(db=None)  # type: ignore[arg-type]

    monkeypatch.setenv("CATALOG_INLINE_PRICE_REFRESH", "0")
    assert service._should_catalog_inline_price_refresh(page=1, page_size=12) is False

    monkeypatch.setenv("CATALOG_INLINE_PRICE_REFRESH", "1")
    assert service._should_catalog_inline_price_refresh(page=10, page_size=100) is True
