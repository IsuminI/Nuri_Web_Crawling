from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path
from typing import Optional


def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()


class StateStore:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _init(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS processed (
                  notice_id TEXT PRIMARY KEY,
                  status TEXT NOT NULL,
                  content_hash TEXT,
                  last_seen_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS checkpoints (
                  key TEXT PRIMARY KEY,
                  value TEXT NOT NULL
                )
                """
            )

    def get_checkpoint(self, key: str) -> Optional[str]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value FROM checkpoints WHERE key = ?", (key,)
            ).fetchone()
            return row[0] if row else None

    def set_checkpoint(self, key: str, value: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO checkpoints(key, value)
                VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    def is_processed(self, notice_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM processed WHERE notice_id = ? AND status = 'ok'",
                (notice_id,),
            ).fetchone()
            return row is not None

    def mark_seen(self, notice_id: str, last_seen_utc: str) -> None:
        """
        목록 단계에서 발견한 공고를 기록.
        - 상세 수집 성공 여부와 무관하게 'seen' 상태로 남겨 중복 방지/재개 판단에 활용.
        """
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO processed(notice_id, status, content_hash, last_seen_utc)
                VALUES(?, 'seen', NULL, ?)
                ON CONFLICT(notice_id) DO UPDATE SET last_seen_utc = excluded.last_seen_utc
                """,
                (notice_id, last_seen_utc),
            )

    def upsert_processed(
        self,
        notice_id: str,
        status: str,
        last_seen_utc: str,
        content_hash: Optional[str] = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO processed(notice_id, status, content_hash, last_seen_utc)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(notice_id) DO UPDATE SET
                  status = excluded.status,
                  content_hash = COALESCE(excluded.content_hash, processed.content_hash),
                  last_seen_utc = excluded.last_seen_utc
                """,
                (notice_id, status, content_hash, last_seen_utc),
            )

    def get_content_hash(self, notice_id: str) -> Optional[str]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT content_hash FROM processed WHERE notice_id = ?",
                (notice_id,),
            ).fetchone()
            return row[0] if row else None

