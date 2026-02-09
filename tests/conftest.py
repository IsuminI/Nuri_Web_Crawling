from __future__ import annotations

import sys
from pathlib import Path


def _add_project_root_to_sys_path() -> None:
    """
    pytest에서 'src' 패키지를 찾을 수 있도록 프로젝트 루트를 sys.path에 추가.

    tests/ 디렉터리 기준으로 한 단계 위(crawling/)가 루트라고 가정한다.
    """
    root = Path(__file__).resolve().parent.parent
    root_str = str(root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)


_add_project_root_to_sys_path()

