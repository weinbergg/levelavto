from __future__ import annotations

from pathlib import Path
from ..parsing.config import load_sites_config
from ..parsing.base import logger
import httpx


def main() -> None:
    cfg = load_sites_config().get("mobile_de")
    timeout = httpx.Timeout(connect=15.0, read=30.0, write=30.0, pool=60.0)
    with httpx.Client(headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout, follow_redirects=True) as client:
        params = {cfg.pagination.page_param: "1"}
        params.update({})  # keep empty filters by default
        resp = client.get(cfg.base_search_url, params=params)
        print(f"URL: {resp.url}")
        print(f"Status: {resp.status_code}")
        print(f"Length: {len(resp.text) if resp.text else 0}")
        tmp = Path("/app/tmp")
        tmp.mkdir(exist_ok=True, parents=True)
        out = tmp / "mobile_de_debug.html"
        out.write_text(resp.text or "", encoding="utf-8")
        print(f"Saved HTML snapshot to {out}")


if __name__ == "__main__":
    main()


