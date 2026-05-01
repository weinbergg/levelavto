"""HTTP middleware that logs page visits with cookie-based consent.

Design notes:

* Only HTML GET responses are recorded — we do not care about `/api/*`,
  static assets, favicons, healthchecks, or anything that does not have
  the user actually looking at it.
* Visitor identity is a 32-byte random id stored in a first-party
  ``la_vid`` cookie. We never store IPs or any other PII.
* The cookie is **only** issued after the visitor accepts analytics via
  the cookie banner (``la_consent=1`` cookie). Without consent, we skip
  all logging.
* We swallow every storage error: a flaky DB write must never break the
  page render for the visitor.
* Writes happen in the background (``BackgroundTasks``) so the response
  is not blocked.
"""

from __future__ import annotations

import logging
import os
import secrets
from typing import Optional

from sqlalchemy.exc import SQLAlchemyError
from starlette.types import ASGIApp, Receive, Scope, Send

from ..db import SessionLocal
from ..models import PageVisit


logger = logging.getLogger(__name__)


_BOT_HINTS = ("bot", "spider", "crawler", "preview", "monitor", "wget", "curl/")
_SKIP_PREFIXES = (
    "/static/",
    "/media/",
    "/api/",
    "/favicon",
    "/robots",
    "/sitemap",
    "/health",
    "/admin",
    "/login",
    "/logout",
    "/register",
    "/account",
)
_VISITOR_COOKIE = "la_vid"
_CONSENT_COOKIE = "la_consent"


def _looks_like_bot(user_agent: Optional[str]) -> bool:
    if not user_agent:
        return True
    ua = user_agent.lower()
    return any(hint in ua for hint in _BOT_HINTS)


def _shorten(value: Optional[str], length: int) -> Optional[str]:
    if not value:
        return None
    return str(value)[:length]


class PageVisitMiddleware:
    """Pure-ASGI middleware so we can stamp a cookie before send()."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        method = scope.get("method", "GET").upper()
        path = scope.get("path") or "/"
        if method != "GET" or any(path.startswith(p) for p in _SKIP_PREFIXES):
            await self.app(scope, receive, send)
            return

        headers = {k.decode("latin-1"): v.decode("latin-1") for k, v in scope.get("headers", [])}
        cookies = _parse_cookies(headers.get("cookie", ""))
        consent = cookies.get(_CONSENT_COOKIE) == "1"
        visitor_id = cookies.get(_VISITOR_COOKIE)
        new_visitor_id: Optional[str] = None
        if consent and not visitor_id:
            new_visitor_id = secrets.token_hex(16)
            visitor_id = new_visitor_id

        captured: dict = {"status": 200, "is_html": False}

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                captured["status"] = message.get("status", 200)
                content_type = ""
                for raw_name, raw_value in message.get("headers", []) or []:
                    if raw_name.lower() == b"content-type":
                        content_type = raw_value.decode("latin-1", "ignore")
                        break
                captured["is_html"] = "text/html" in content_type
                if new_visitor_id:
                    cookie_value = (
                        f"{_VISITOR_COOKIE}={new_visitor_id}; "
                        "Path=/; Max-Age=15552000; SameSite=Lax; HttpOnly"
                    )
                    message.setdefault("headers", []).append(
                        (b"set-cookie", cookie_value.encode("latin-1"))
                    )
            await send(message)

        await self.app(scope, receive, send_wrapper)

        if (
            consent
            and visitor_id
            and captured["is_html"]
            and 200 <= captured["status"] < 400
            and os.getenv("PAGE_VISITS_DISABLED", "0") != "1"
        ):
            try:
                _store_visit(
                    visitor_id=visitor_id,
                    path=path,
                    query=scope.get("query_string", b"").decode("latin-1") or None,
                    referer=headers.get("referer"),
                    user_agent=headers.get("user-agent"),
                )
            except SQLAlchemyError:
                logger.exception("page_visits insert failed")
            except Exception:
                logger.exception("page_visits unexpected error")


def _parse_cookies(raw: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for piece in (raw or "").split(";"):
        if "=" in piece:
            name, _, value = piece.strip().partition("=")
            out[name] = value
    return out


def _store_visit(
    *,
    visitor_id: str,
    path: str,
    query: Optional[str],
    referer: Optional[str],
    user_agent: Optional[str],
) -> None:
    with SessionLocal() as db:
        db.add(
            PageVisit(
                visitor_id=visitor_id,
                user_id=None,
                path=_shorten(path, 500) or "/",
                query=_shorten(query, 1000),
                referer=_shorten(referer, 500),
                user_agent=_shorten(user_agent, 500),
                is_bot=_looks_like_bot(user_agent),
            )
        )
        db.commit()
