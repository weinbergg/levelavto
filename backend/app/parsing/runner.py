from __future__ import annotations

import argparse
from typing import Dict, Type, List, Optional
from pathlib import Path
from ..config import settings
from .base import logger, CarParsed
from ..services.parser_runner import ParserRunner


PARSERS: Dict[str, Type] = {}


def _normalize_site_line(line: str) -> str | None:
    key = line.strip().lower()
    if not key:
        return None
    if "mobile.de" in key or key == "mobile.de" or key == "mobile_de":
        return "mobile_de"
    if "encar" in key:
        return "encar"
    if "emavto" in key or "emavto.ru" in key or "klg.emavto.ru" in key:
        return "emavto_klg"
    # If exact key is directly known
    if key in ("mobile_de", "encar", "emavto_klg"):
        return key
    return None


def read_sites_file() -> List[str]:
    # Find sites.txt from project root upwards
    candidates = [
        settings.project_root / "sites.txt",
        Path.cwd() / "sites.txt",
    ]
    for path in candidates:
        if path.exists():
            with path.open("r", encoding="utf-8") as f:
                keys: List[str] = []
                for raw in f:
                    if not raw.strip() or raw.strip().startswith("#"):
                        continue
                    k = _normalize_site_line(raw)
                    if k and k not in keys:
                        keys.append(k)
                if keys:
                    return keys
    logger.warning("sites.txt not found; defaulting to all known parsers")
    return ["mobile_de", "encar", "emavto_klg"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Parsing runner")
    parser.add_argument("--all", action="store_true",
                        help="Run all sources from sites.txt")
    parser.add_argument("--source", type=str,
                        help="Run specific source key(s), comma-separated")
    parser.add_argument("--profiles", type=str,
                        help="Run only specific profile IDs, comma-separated")
    parser.add_argument("--trigger", type=str, default="manual",
                        help="Trigger type: auto|manual|telegram")
    parser.add_argument(
        "--mode", type=str, choices=["full", "incremental"], help="Optional mode hint for sources")
    args = parser.parse_args()

    runner = ParserRunner()
    if args.source:
        keys_preview = [k.strip() for k in args.source.split(",") if k.strip()]
        print(
            f"Running sources: {', '.join(keys_preview)} (trigger={args.trigger})")
    elif args.all:
        keys_preview = read_sites_file()
        print(
            f"Running sources from sites.txt: {', '.join(keys_preview)} (trigger={args.trigger})")
    if args.all:
        keys = read_sites_file()
        summary = runner.run_all(
            trigger=args.trigger, source_keys=keys, search_profile_ids=_parse_ids(args.profiles), mode=args.mode
        )
        print(_fmt_summary(summary))
    elif args.source:
        keys = [k.strip() for k in args.source.split(",") if k.strip()]
        summary = runner.run_all(
            trigger=args.trigger, source_keys=keys, search_profile_ids=_parse_ids(args.profiles), mode=args.mode
        )
        print(_fmt_summary(summary))
    else:
        print("Usage examples:")
        print("  python -m backend.app.parsing.runner --all")
        print("  python -m backend.app.parsing.runner --source mobile_de")
        print("  python -m backend.app.parsing.runner --source mobile_de,emavto_klg --profiles 1,2 --trigger auto")


def _parse_ids(s: Optional[str]) -> Optional[List[int]]:
    if not s:
        return None
    try:
        return [int(x.strip()) for x in s.split(",") if x.strip()]
    except Exception:
        return None


def _fmt_summary(summary) -> str:
    parts = [
        f"Run #{summary.run_id} status={summary.status}, totals={summary.totals}",
    ]
    for key, stats in summary.per_source.items():
        parts.append(f"  - {key}: {stats}")
    if summary.error_message:
        parts.append(f"Error: {summary.error_message}")
    return "\n".join(parts)


if __name__ == "__main__":
    main()
