#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""누리장터 크롤러 Tkinter UI.

실행: python app.py
"""
from __future__ import annotations

import asyncio
import json
import sys
import threading
import tkinter as tk
from datetime import datetime, timezone
from pathlib import Path
from tkinter import ttk, messagebox, scrolledtext

sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.crawler.nuri import CrawlConfig, crawl_once
from src.utils.paths import ensure_dir, workspace_root

BASE_URL = "https://nuri.g2b.go.kr/"


def load_config() -> CrawlConfig:
    root = workspace_root()
    cfg_path = root / "configs" / "default.json"
    cfg_dict = json.loads(cfg_path.read_text(encoding="utf-8"))
    return CrawlConfig.from_dict(cfg_dict)


def check_url() -> tuple[bool, str]:
    """URL 접속 가능 여부 확인."""
    try:
        import urllib.request

        req = urllib.request.Request(BASE_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            if 200 <= resp.status < 400:
                return True, f"정상 (HTTP {resp.status})"
            return False, f"HTTP {resp.status}"
    except Exception as e:
        return False, str(e)


def run_crawl(
    max_items: int,
    list_only: bool,
    log_callback,
    raw_name: str,
    normalized_name: str,
):
    """크롤링 실행."""
    root = workspace_root()
    cfg = load_config()
    data_dir = ensure_dir(root / "data")
    raw_dir = ensure_dir(data_dir / "raw")
    norm_dir = ensure_dir(data_dir / "normalized")
    errors_dir = ensure_dir(data_dir / "errors")
    state_dir = ensure_dir(root / "state")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    def _base(s: str, default: str) -> str:
        s = s.strip()
        if not s:
            return default
        if s.lower().endswith(".jsonl"):
            return s[:-6]  # strip .jsonl
        return s

    raw_base = _base(raw_name, f"list_{run_id[:8]}")
    norm_base = _base(normalized_name, f"list_{run_id[:8]}_상세")
    out_raw_list = raw_dir / f"{raw_base}.jsonl"
    out_normalized = norm_dir / f"{norm_base}.jsonl"
    # 출력 파일마다 별도 state 사용 (이전 state가 새 normalized를 건너뛰지 않도록)
    state_db = state_dir / f"state_{norm_base}.sqlite"

    log_callback(f"출력: raw={out_raw_list.name}, normalized={out_normalized.name}")

    asyncio.run(
        crawl_once(
            cfg=cfg,
            run_id=run_id,
            out_raw_list=out_raw_list,
            out_normalized=out_normalized,
            errors_dir=errors_dir,
            state_db=state_db,
            headless=True,
            max_pages=5,
            max_items=max_items,
            keywords=[],
            list_only=list_only,
            debug_slowmo_ms=0,
        )
    )
    log_callback(f"완료: raw={out_raw_list.name}, normalized={out_normalized.name}")


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("누리장터 입찰공고 크롤러")
        self.geometry("520x440")
        self.resizable(True, True)

        main = ttk.Frame(self, padding=12)
        main.pack(fill=tk.BOTH, expand=True)

        ttk.Label(main, text="누리장터 입찰공고 크롤러", font=("", 14)).pack(anchor=tk.W)
        ttk.Label(main, text="nuri.g2b.go.kr", foreground="gray").pack(anchor=tk.W)

        ttk.Separator(main, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=8)

        # URL 체크
        row1 = ttk.Frame(main)
        row1.pack(fill=tk.X, pady=4)
        ttk.Button(row1, text="URL 체크", command=self._on_url_check).pack(side=tk.LEFT, padx=(0, 8))
        self.url_label = ttk.Label(row1, text="", foreground="gray")
        self.url_label.pack(side=tk.LEFT)
        ttk.Label(row1, text="nuri.g2b.go.kr 정상 작동 여부 확인").pack(side=tk.LEFT, padx=(16, 0))

        ttk.Separator(main, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=8)

        # 출력 파일명
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        ttk.Label(main, text="출력 파일명 (비우면 기본값 사용)", font=("", 11)).pack(
            anchor=tk.W, pady=(0, 4)
        )
        row_fn = ttk.Frame(main)
        row_fn.pack(fill=tk.X, pady=4)
        ttk.Label(row_fn, text="Raw 목록:").pack(side=tk.LEFT, padx=(0, 4))
        self.raw_name_var = tk.StringVar(value=f"list_{date_str}")
        ttk.Entry(row_fn, textvariable=self.raw_name_var, width=24).pack(
            side=tk.LEFT, padx=(0, 16)
        )
        ttk.Label(row_fn, text="정규화:").pack(side=tk.LEFT, padx=(0, 4))
        self.normalized_name_var = tk.StringVar(value=f"list_{date_str}_상세")
        ttk.Entry(row_fn, textvariable=self.normalized_name_var, width=24).pack(
            side=tk.LEFT
        )
        ttk.Label(main, text="  → list_날짜 / list_날짜_상세 형식", foreground="gray").pack(
            anchor=tk.W
        )

        ttk.Separator(main, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=8)

        # 입찰공고목록 추출
        ttk.Label(main, text="입찰공고 추출", font=("", 11)).pack(anchor=tk.W, pady=(0, 4))
        row2 = ttk.Frame(main)
        row2.pack(fill=tk.X, pady=4)
        ttk.Label(row2, text="추출 건수:").pack(side=tk.LEFT, padx=(0, 4))
        self.max_items_spinbox = tk.Spinbox(row2, from_=1, to=100, width=5)
        self.max_items_spinbox.delete(0, tk.END)
        self.max_items_spinbox.insert(0, "10")
        self.max_items_spinbox.pack(side=tk.LEFT, padx=(0, 16))
        self.list_only_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            row2, text="목록만 추출 (상세 생략)", variable=self.list_only_var
        ).pack(side=tk.LEFT)

        row3 = ttk.Frame(main)
        row3.pack(fill=tk.X, pady=8)
        ttk.Button(
            row3, text="입찰공고목록 추출", command=self._on_crawl
        ).pack(side=tk.LEFT)

        ttk.Separator(main, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=8)

        # 로그
        ttk.Label(main, text="로그").pack(anchor=tk.W)
        self.log_text = scrolledtext.ScrolledText(
            main, height=8, font=("Consolas", 9), state=tk.DISABLED
        )
        self.log_text.pack(fill=tk.BOTH, expand=True, pady=4)

        ttk.Label(main, text="결과: data/raw/, data/normalized/", foreground="gray").pack(
            anchor=tk.W
        )

    def _log(self, msg: str) -> None:
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.insert(tk.END, msg + "\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state=tk.DISABLED)

    def _on_url_check(self) -> None:
        ok, msg = check_url()
        if ok:
            self.url_label.configure(text=f"정상: {msg}", foreground="green")
            self._log(f"[URL 체크] {BASE_URL} {msg}")
        else:
            self.url_label.configure(text=f"실패: {msg}", foreground="red")
            self._log(f"[URL 체크] 실패: {msg}")

    def _on_crawl(self) -> None:
        # 메인 스레드에서 UI 값 캡처 (Tkinter 스레드 안전)
        try:
            max_items = min(100, max(1, int(self.max_items_spinbox.get())))
        except (ValueError, tk.TclError):
            messagebox.showerror("오류", "추출 건수는 1~100 사이의 숫자를 입력하세요.")
            return
        list_only = self.list_only_var.get()
        raw_name = self.raw_name_var.get()
        normalized_name = self.normalized_name_var.get()

        def _thread_safe_log(msg: str) -> None:
            self.after(0, lambda: self._log(msg))

        def _run():
            try:
                _thread_safe_log("[크롤링 시작] 브라우저 실행 중...")
                run_crawl(
                    max_items, list_only, _thread_safe_log,
                    raw_name=raw_name, normalized_name=normalized_name,
                )
                self.after(0, lambda: messagebox.showinfo("완료", "크롤링이 완료되었습니다."))
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("오류", str(e)))
                self._log(f"[오류] {e}")

        threading.Thread(target=_run, daemon=True).start()


def main():
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
