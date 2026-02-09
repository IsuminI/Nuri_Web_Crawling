from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class SourceMeta(BaseModel):
    site: str = "nuri.g2b.go.kr"
    collected_at_utc: str
    run_id: str


class ListItem(BaseModel):
    notice_id: str
    title: Optional[str] = None
    organization: Optional[str] = None
    posted_at: Optional[str] = None
    deadline_at: Optional[str] = None
    detail_url: Optional[str] = None
    raw_text: Optional[str] = None
    raw: Dict[str, Any] = Field(default_factory=dict)


class NoticeRecord(BaseModel):
    source: SourceMeta
    notice: Dict[str, Any]
    raw: Dict[str, Any] = Field(default_factory=dict)

