import json
from pathlib import Path

import pytest

from src.crawler.nuri import CrawlConfig, crawl_once
from src.utils.paths import ensure_dir, workspace_root


@pytest.mark.integration
@pytest.mark.asyncio
async def test_crawl_once_happy_path(tmp_path: Path) -> None:
    """
    실제 누리장터에 대해 소량 크롤링이 정상 동작하는지 확인하는 통합 테스트.

    - 네트워크 / 사이트 변경에 따라 실패할 수 있으므로 integration 마커를 사용한다.
    """
    root = workspace_root()
    cfg_path = root / "configs" / "default.json"
    cfg_dict = json.loads(cfg_path.read_text(encoding="utf-8"))
    cfg = CrawlConfig.from_dict(cfg_dict)

    data_dir = ensure_dir(tmp_path / "data")
    raw_dir = ensure_dir(data_dir / "raw")
    norm_dir = ensure_dir(data_dir / "normalized")
    errors_dir = ensure_dir(tmp_path / "errors")
    state_db = tmp_path / "state.sqlite"

    out_raw_list = raw_dir / "list_test.jsonl"
    out_normalized = norm_dir / "notices_test.jsonl"

    await crawl_once(
        cfg=cfg,
        run_id="TEST_RUN",
        out_raw_list=out_raw_list,
        out_normalized=out_normalized,
        errors_dir=errors_dir,
        state_db=state_db,
        headless=True,
        max_pages=1,
        max_items=3,
        keywords=[],
        list_only=False,
        debug_slowmo_ms=0,
    )

    # 최소한 raw/normalized 파일이 생성되었는지 및 라인 수가 1~3 사이인지 확인
    assert out_raw_list.exists()
    assert out_normalized.exists()

    raw_lines = out_raw_list.read_text(encoding="utf-8").strip().splitlines()
    norm_lines = out_normalized.read_text(encoding="utf-8").strip().splitlines()

    assert 1 <= len(raw_lines) <= 3
    assert 1 <= len(norm_lines) <= 3

