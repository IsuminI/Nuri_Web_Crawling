from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from src.crawler.nuri import CrawlConfig, crawl_once
from src.utils.logging import setup_logging
from src.utils.paths import ensure_dir, workspace_root


log = logging.getLogger("nuri.cli")


def utc_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_config(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="누리장터 입찰공고 크롤러")
    p.add_argument("--config", default="configs/default.json")
    p.add_argument("--mode", choices=["once", "interval"], default="once")
    p.add_argument("--interval-min", type=int, default=60)
    head_mode = p.add_mutually_exclusive_group()
    head_mode.add_argument("--headless", action="store_true", help="헤드리스 실행(기본)")
    head_mode.add_argument("--headed", action="store_true", help="브라우저 창 표시(디버그용)")
    p.add_argument("--slowmo-ms", type=int, default=0, help="디버그용 슬로모(ms)")
    p.add_argument("--max-pages", type=int, default=1)
    p.add_argument("--max-items", type=int, default=30)
    p.add_argument("--list-only", action="store_true", help="목록만 수집하고 상세는 생략")
    p.add_argument("--keyword", action="append", default=[], help="포함 키워드(복수 가능)")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    root = workspace_root()
    cfg_path = (root / args.config).resolve()
    cfg_dict = load_config(cfg_path)
    cfg = CrawlConfig.from_dict(cfg_dict)

    data_dir = ensure_dir(root / "data")
    raw_dir = ensure_dir(data_dir / "raw")
    norm_dir = ensure_dir(data_dir / "normalized")
    errors_dir = ensure_dir(data_dir / "errors")
    state_dir = ensure_dir(root / "state")

    run_id = utc_run_id()
    out_raw_list = raw_dir / f"list_{run_id[:8]}.jsonl"
    out_normalized = norm_dir / "notices.jsonl"
    state_db = state_dir / "state.sqlite"

    keywords: List[str] = []
    keywords.extend(cfg_dict.get("filters", {}).get("keywords", []) or [])
    keywords.extend(args.keyword or [])

    headed = bool(args.headed)
    headless = not headed
    slowmo = args.slowmo_ms if headed else 0

    def _run_once():
        import asyncio

        asyncio.run(
            crawl_once(
                cfg=cfg,
                run_id=run_id,
                out_raw_list=out_raw_list,
                out_normalized=out_normalized,
                errors_dir=errors_dir,
                state_db=state_db,
                headless=headless,
                max_pages=args.max_pages,
                max_items=args.max_items,
                keywords=keywords,
                list_only=bool(args.list_only),
                debug_slowmo_ms=slowmo,
            )
        )

    if args.mode == "once":
        _run_once()
        return

    while True:
        try:
            _run_once()
        except Exception:
            log.exception("interval run failed")
        time.sleep(max(args.interval_min, 1) * 60)


if __name__ == "__main__":
    main()

