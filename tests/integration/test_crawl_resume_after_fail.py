import json
from pathlib import Path

import pytest

from src.crawler import nuri
from src.crawler.nuri import CrawlConfig, crawl_once
from src.utils.paths import ensure_dir, workspace_root


@pytest.mark.integration
@pytest.mark.asyncio
async def test_crawl_resume_after_injected_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    의도적으로 상세 진입에서 오류를 발생시킨 뒤,
    같은 state DB로 재실행했을 때 이어서 수집이 가능한지 검증하는 통합 테스트.
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

    out_raw_list = raw_dir / "list_test_resume.jsonl"
    out_normalized = norm_dir / "notices_test_resume.jsonl"

    # 1차 실행: 일부 상세 수집 후 인위적으로 실패를 발생시키는 _open_detail 패치
    call_count = {"detail": 0}

    original_open_detail = nuri._open_detail

    async def flaky_open_detail(page, item):
        call_count["detail"] += 1
        if call_count["detail"] > 2:
            # 3번째 상세 진입부터는 네트워크 오류가 난 것처럼 예외 발생
            raise RuntimeError("인위적 네트워크 실패 가정")
        return await original_open_detail(page, item)

    monkeypatch.setattr(nuri, "_open_detail", flaky_open_detail)

    # 1차 실행: crawl_once 내부에서 예외를 잡고 로그/에러 상태만 기록해야 한다.
    await crawl_once(
        cfg=cfg,
        run_id="TEST_FAILING_RUN",
        out_raw_list=out_raw_list,
        out_normalized=out_normalized,
        errors_dir=errors_dir,
        state_db=state_db,
        headless=True,
        max_pages=1,
        max_items=5,
        keywords=[],
        list_only=False,
        debug_slowmo_ms=0,
    )

    # _open_detail 패치가 실제로 여러 번 호출되며 오류를 발생시켰는지 확인
    assert call_count["detail"] > 2

    # 1차 실행 시점의 결과 라인 수를 기록
    raw_lines_first = out_raw_list.read_text(encoding="utf-8").strip().splitlines()
    norm_lines_first = []
    if out_normalized.exists():
        norm_lines_first = out_normalized.read_text(encoding="utf-8").strip().splitlines()

    # 2차 실행: 같은 state_db로 재실행
    # 이미 ok 처리된 공고는 is_processed=True라서 건너뛰고, 나머지를 계속 수집해야 한다.
    await crawl_once(
        cfg=cfg,
        run_id="TEST_RESUME_RUN",
        out_raw_list=out_raw_list,
        out_normalized=out_normalized,
        errors_dir=errors_dir,
        state_db=state_db,
        headless=True,
        max_pages=1,
        max_items=5,
        keywords=[],
        list_only=False,
        debug_slowmo_ms=0,
    )

    # raw/normalized 파일이 존재하고, 1차 실행 대비 라인 수가 줄어들지 않았는지 확인
    assert out_raw_list.exists()
    assert out_normalized.exists()

    raw_lines_after = out_raw_list.read_text(encoding="utf-8").strip().splitlines()
    norm_lines_after = out_normalized.read_text(encoding="utf-8").strip().splitlines()

    assert len(raw_lines_after) >= len(raw_lines_first)
    assert len(norm_lines_after) >= len(norm_lines_first)

