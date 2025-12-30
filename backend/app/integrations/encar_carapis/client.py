from __future__ import annotations

import os
from typing import Any, Dict, Optional

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ...config import settings

# The upstream client reads ENCAR_API_URL at import time; set override before import.
if settings.ENCAR_CARAPIS_BASE_URL and not os.getenv("ENCAR_API_URL"):
    os.environ["ENCAR_API_URL"] = settings.ENCAR_CARAPIS_BASE_URL

from encar import CarapisClient, CarapisClientError  # noqa: E402


RetryableError = CarapisClientError


def _retryable():
    return retry(
        retry=retry_if_exception_type(RetryableError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        reraise=True,
    )


class EncarCarapisClient:
    """
    Thin wrapper around the encar.CarapisClient with retry semantics.
    """

    def __init__(self, api_key: Optional[str]) -> None:
        self.client = CarapisClient(api_key=api_key)

    @_retryable()
    def list_manufacturers(self, *, limit: int = 200, page: int = 1) -> Dict[str, Any]:
        return self.client.list_manufacturers(limit=limit, page=page)

    @_retryable()
    def list_vehicles(
        self,
        *,
        manufacturer_slug: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = 50,
        page: int = 1,
    ) -> Dict[str, Any]:
        return self.client.list_vehicles(
            manufacturer_slug=manufacturer_slug,
            search=search,
            limit=limit,
            page=page,
        )

    @_retryable()
    def get_vehicle(self, *, vehicle_id: int) -> Dict[str, Any]:
        return self.client.get_vehicle(vehicle_id=vehicle_id)


__all__ = ["EncarCarapisClient", "RetryableError"]
