from pathlib import Path

from src.storage.state import StateStore, sha256_text


def test_checkpoint_roundtrip(tmp_path: Path) -> None:
    """체크포인트 저장/조회가 올바르게 동작하는지 테스트."""
    db_path = tmp_path / "state.sqlite"
    store = StateStore(db_path)

    # 초기 상태: 체크포인트 없음
    assert store.get_checkpoint("bid_list.page") is None

    # 체크포인트 기록
    store.set_checkpoint("bid_list.page", "3")
    assert store.get_checkpoint("bid_list.page") == "3"

    # 새 인스턴스로 다시 열어도 값이 유지되는지 확인
    store2 = StateStore(db_path)
    assert store2.get_checkpoint("bid_list.page") == "3"


def test_seen_and_ok_processed(tmp_path: Path) -> None:
    """seen / ok 상태에 따라 is_processed 동작이 달라지는지 테스트."""
    db_path = tmp_path / "state.sqlite"
    store = StateStore(db_path)

    notice_id = sha256_text("dummy-notice")

    # 처음에는 processed 아님
    assert store.is_processed(notice_id) is False

    # 목록에서 한 번 본 상태(seen)는 processed 로 보지 않는다.
    store.mark_seen(notice_id, "2026-02-08T10:00:00Z")
    assert store.is_processed(notice_id) is False

    # 상세까지 정상 수집 후 ok 상태로 업서트
    store.upsert_processed(
        notice_id=notice_id,
        status="ok",
        last_seen_utc="2026-02-08T10:05:00Z",
        content_hash="hash123",
    )
    assert store.is_processed(notice_id) is True


def test_resume_after_failure_uses_checkpoint(tmp_path: Path) -> None:
    """
    '중간 실패 후 재실행' 시나리오를 체크포인트 관점에서 검증.

    1차 실행에서 3페이지를 다음 시작 지점으로 기록했다면,
    2차 실행에서 같은 state DB를 열었을 때 3페이지부터 시작해야 한다.
    """
    db_path = tmp_path / "state.sqlite"

    # 1차 실행: 1~2페이지 처리 후 3페이지에서 재시작하도록 체크포인트 저장했다고 가정
    store1 = StateStore(db_path)
    store1.set_checkpoint("bid_list.page", "3")

    # 2차 실행: 같은 DB로 새 StateStore 생성 시, 3페이지부터 시작해야 함
    store2 = StateStore(db_path)
    start_page = int(store2.get_checkpoint("bid_list.page") or "1")
    assert start_page == 3

