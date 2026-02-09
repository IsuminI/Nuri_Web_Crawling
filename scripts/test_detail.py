#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""상세 추출(extract_detail_kv) 테스트 스크립트.

사용법:
  # HTML 파일로 테스트 (저장한 상세페이지 HTML)
  python scripts/test_detail.py --html "C:/Users/Home/Desktop/예시1상세화면2.html"

  # 실제 사이트 1건만 크롤링 후 추출 테스트
  python scripts/test_detail.py --live --max-items 1

  # headed 모드로 브라우저 창 표시
  python scripts/test_detail.py --live --headed
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from playwright.async_api import async_playwright

from src.crawler.nuri import (
    CrawlConfig,
    extract_detail_kv,
    navigate_to_bid_list,
    _open_detail,
    extract_list_items,
)


async def test_from_html(html_path: Path, config_path: Path) -> None:
    """HTML 파일을 로드해 추출 테스트."""
    html = html_path.read_text(encoding="utf-8")
    cfg_dict = json.loads(config_path.read_text(encoding="utf-8"))
    cfg = CrawlConfig.from_dict(cfg_dict)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.set_content(html, wait_until="domcontentloaded")
        await page.wait_for_timeout(500)

        result = await extract_detail_kv(page, cfg)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        await browser.close()


async def test_from_live(config_path: Path, max_items: int, headed: bool) -> None:
    """실제 사이트에서 1건 진입 후 추출 테스트."""
    cfg_dict = json.loads(config_path.read_text(encoding="utf-8"))
    cfg = CrawlConfig.from_dict(cfg_dict)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not headed)
        page = await browser.new_page()
        await navigate_to_bid_list(page, cfg)

        items = await extract_list_items(page, cfg, limit=max_items)
        if not items:
            print("추출된 목록이 없습니다.")
            await browser.close()
            return

        await _open_detail(page, items[0])
        await page.wait_for_timeout(2000)
        result = await extract_detail_kv(page, cfg)

        print(json.dumps(result, ensure_ascii=False, indent=2))
        await browser.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="상세 추출 테스트")
    root = Path(__file__).resolve().parent.parent
    parser.add_argument(
        "--html",
        type=Path,
        help="테스트할 HTML 파일 경로",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="실제 사이트에서 1건 크롤링 후 추출",
    )
    parser.add_argument("--max-items", type=int, default=1)
    parser.add_argument("--headed", action="store_true", help="브라우저 창 표시")
    parser.add_argument("--config", type=Path, default=root / "configs" / "default.json")
    args = parser.parse_args()

    if args.html:
        if not args.html.exists():
            print(f"파일 없음: {args.html}")
            sys.exit(1)
        asyncio.run(test_from_html(args.html, args.config))
    elif args.live:
        asyncio.run(test_from_live(args.config, args.max_items, args.headed))
    else:
        parser.print_help()
        print("\n예: python scripts/test_detail.py --html C:/path/to/detail.html")
        print("예: python scripts/test_detail.py --live --max-items 1")


if __name__ == "__main__":
    main()
