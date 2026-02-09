from __future__ import annotations

from pathlib import Path


def workspace_root() -> Path:
    # src/ 기준 2단계 위 = 프로젝트 루트
    return Path(__file__).resolve().parents[2]


def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

