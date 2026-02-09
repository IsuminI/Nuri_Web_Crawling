from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from playwright.async_api import Page, async_playwright

from src.models import ListItem, NoticeRecord, SourceMeta
from src.storage.jsonl import JsonlWriter
from src.storage.state import StateStore, sha256_text
from src.utils.retry import default_retry

log = logging.getLogger("nuri.crawler")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def safe_filename(s: str) -> str:
    return re.sub(r"[^0-9A-Za-z._-]+", "_", s)[:180]


@dataclass
class CrawlConfig:
    base_url: str
    search_button_text: Optional[str]
    search_button_selector: Optional[str]
    bid_list_ready_selectors: List[str]
    list_columns: List[str]
    row_selector_candidates: List[str]
    link_selector_candidates: List[str]
    next_button_name_candidates: List[str]
    next_button_selector_candidates: List[str]
    detail_ready_selectors: List[str]
    detail_sections: List[str]
    detail_table_sections: List[str]
    detail_field_sections: Dict[str, List[str]]
    detail_table_columns: Dict[str, List[str]]
    mega_menu: Optional[Dict[str, str]] = None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "CrawlConfig":
        nav = d.get("navigation", {})
        lst = d.get("list", {})
        det = d.get("detail", {})
        mega = nav.get("mega_menu")
        return CrawlConfig(
            base_url=d.get("base_url", "https://nuri.g2b.go.kr/"),
            search_button_text=nav.get("search_button_text"),
            search_button_selector=nav.get("search_button_selector"),
            bid_list_ready_selectors=list(nav.get("bid_list_ready_selectors", ["tbody tr"])),
            list_columns=list(lst.get("columns", [])),
            row_selector_candidates=list(lst.get("row_selector_candidates", ["tbody tr"])),
            link_selector_candidates=list(lst.get("link_selector_candidates", ["a"])),
            next_button_name_candidates=list(lst.get("next_button_name_candidates", ["다음", ">"])),
            next_button_selector_candidates=list(lst.get("next_button_selector_candidates", [])),
            detail_ready_selectors=list(det.get("ready_selectors", ["body"])),
            detail_sections=list(det.get("sections", [])),
            detail_table_sections=list(det.get("table_sections", [])),
            detail_field_sections=dict(det.get("field_sections", {}) or {}),
            detail_table_columns=dict(det.get("table_columns", {}) or {}),
            mega_menu=dict(mega) if isinstance(mega, dict) else None,
        )


async def _wait_any_selector(page: Page, selectors: List[str], timeout_ms: int) -> str:
    last_err: Optional[Exception] = None
    for sel in selectors:
        try:
            await page.wait_for_selector(sel, timeout=timeout_ms)
            return sel
        except Exception as e:
            last_err = e
    raise RuntimeError(f"ready selector not found: {selectors}") from last_err


async def _click_by_text(page: Page, text: str, timeout_ms: int = 5000) -> None:
    # link/button 우선, 실패 시 텍스트 locator
    errors: List[Exception] = []
    for role in ("link", "button"):
        try:
            loc = page.get_by_role(role, name=text)
            await loc.first.click(timeout=timeout_ms)
            return
        except Exception as e:
            errors.append(e)
    try:
        await page.locator(f"text={text}").first.click(timeout=timeout_ms)
        return
    except Exception as e:
        errors.append(e)
    raise RuntimeError(f"failed clicking by text={text}") from errors[-1]


async def _navigate_mega_menu(page: Page, cfg: CrawlConfig) -> None:
    """메가메뉴: depth1에 hover 후 depth3(하위메뉴) 클릭."""
    mm = cfg.mega_menu
    if not mm:
        raise ValueError("mega_menu config required")
    d1_sel = mm.get("depth1_selector", "a.depth1")
    d3_sel = mm.get("depth3_selector", "a.depth3")
    hover_text = mm.get("hover_text", "")
    click_text = mm.get("click_text", "")

    # depth1 요소에 hover (서브메뉴 display: none -> block)
    hover_loc = page.locator(d1_sel).filter(has_text=hover_text).first
    await hover_loc.wait_for(state="visible", timeout=5000)
    await hover_loc.hover()
    await page.wait_for_timeout(100)

    # depth3(입찰공고목록) 클릭
    click_loc = page.locator(d3_sel).filter(has_text=click_text).first
    await click_loc.wait_for(state="visible", timeout=3000)
    await click_loc.click(timeout=3000)


@default_retry()
async def navigate_to_bid_list(page: Page, cfg: CrawlConfig) -> None:
    await page.goto(cfg.base_url, wait_until="domcontentloaded", timeout=20000)
    await page.wait_for_timeout(100)

    if cfg.mega_menu:
        await _navigate_mega_menu(page, cfg)
    else:
        raise ValueError("navigation requires mega_menu config")

    # 입찰공고목록 페이지에서 필터창 "검색" 버튼 클릭 → 조회 결과 로드 (통합검색 X)
    if cfg.search_button_text or cfg.search_button_selector:
        await page.wait_for_timeout(300)
        # 필터 영역(btn_shbox) 검색 버튼 우선 사용
        if cfg.search_button_selector:
            try:
                await page.locator(cfg.search_button_selector).first.click(timeout=8000)
            except Exception:
                if cfg.search_button_text:
                    await _click_by_text(page, cfg.search_button_text, timeout_ms=8000)
        elif cfg.search_button_text:
            try:
                await page.locator(f'div.btn_shbox input[value="{cfg.search_button_text}"]').first.click(timeout=8000)
            except Exception:
                await _click_by_text(page, cfg.search_button_text, timeout_ms=8000)
        # 그리드 비동기 로드 대기 (검색 요청 → 응답 → 렌더)
        await page.wait_for_timeout(1500)

    # 목록 화면 준비 대기 (데이터 행 tr.grid_body_row)
    sel = await _wait_any_selector(page, cfg.bid_list_ready_selectors, timeout_ms=20000)
    log.info("목록 준비 완료 (selector=%s)", sel)


@default_retry()
async def _goto_or_recover_list(page: Page, cfg: CrawlConfig) -> None:
    try:
        await _wait_any_selector(page, cfg.bid_list_ready_selectors, timeout_ms=8000)
    except Exception:
        await navigate_to_bid_list(page, cfg)


def _guess_notice_id(detail_url: Optional[str], row_text: str) -> str:
    if detail_url:
        return sha256_text(detail_url)[:24]
    # 공고번호 패턴을 모를 때의 안전망
    return sha256_text(row_text)[:24]


async def extract_list_items(page: Page, cfg: CrawlConfig, limit: int) -> List[ListItem]:
    row_sel = await _wait_any_selector(page, cfg.row_selector_candidates, timeout_ms=15000)
    rows = page.locator(row_sel)
    count = await rows.count()
    n = min(count, limit)
    log.info("목록 행 %d개 발견 (selector=%s, 추출 limit=%d)", count, row_sel, n)
    if count > 0:
        await rows.first.scroll_into_view_if_needed()
        await page.wait_for_timeout(200)

    # page.evaluate로 한 번에 추출 (가상스크롤/요소 detach 회피)
    raw_data: List[Dict[str, Any]] = await page.evaluate(
        """
        ([selector, limit]) => {
            const rows = document.querySelectorAll(selector);
            const result = [];
            const n = Math.min(rows.length, limit);
            for (let i = 0; i < n; i++) {
                const row = rows[i];
                const text = (row.innerText || '').trim();
                let href = null;
                const a = row.querySelector('a');
                if (a) href = a.getAttribute('href');
                result.push({ text, href, i });
            }
            return result;
        }
        """,
        [row_sel, n],
    )
    items: List[ListItem] = []
    cols = cfg.list_columns
    for d in raw_data:
        text = d.get("text") or ""
        detail_url = d.get("href")
        notice_id = _guess_notice_id(detail_url, text)
        parts = [p.strip() for p in text.replace("\n", "\t").split("\t") if p.strip()]
        parsed: Dict[str, str] = {}
        if cols and parts:
            for i, name in enumerate(cols[:10]):
                if i < len(parts):
                    parsed[name] = parts[i]
            if len(cols) >= 13 and len(parts) >= 3:
                parsed[cols[10]] = parts[-3]
                parsed[cols[11]] = parts[-2]
                parsed[cols[12]] = parts[-1]
        title = parsed.get("입찰공고명") or (parts[2] if len(parts) > 2 else None) or text.split("\n")[0].strip()
        items.append(
            ListItem(
                notice_id=notice_id,
                title=title,
                organization=parsed.get("기관명"),
                posted_at=parsed.get("공고게시일시"),
                deadline_at=parsed.get("입찰마감일시"),
                detail_url=detail_url,
                raw_text=text,
                raw={"row_selector": row_sel, "row_index": d.get("i", 0), "parsed": parsed},
            )
        )
    return items


def _is_navigable_url(href: Optional[str]) -> bool:
    if not href or not href.strip():
        return False
    h = href.strip().lower()
    if h.startswith("javascript:") or h == "#" or h.startswith("#"):
        return False
    return True


async def _open_detail(page: Page, item: ListItem) -> None:
    # 1) 실제 URL인 경우 goto 시도
    if _is_navigable_url(item.detail_url):
        try:
            await page.goto(item.detail_url, wait_until="domcontentloaded", timeout=20000)
            return
        except Exception:
            pass
    # 2) row 내 링크를 evaluate로 클릭 (가상스크롤/긴텍스트 회피)
    row_sel = item.raw.get("row_selector")
    row_idx = item.raw.get("row_index", 0)
    if row_sel is not None:
        clicked = await page.evaluate(
            """
            ([selector, idx]) => {
                const rows = document.querySelectorAll(selector);
                const row = rows[idx];
                if (row) {
                    const a = row.querySelector('a');
                    if (a) { a.click(); return true; }
                }
                return false;
            }
            """,
            [row_sel, row_idx],
        )
        if clicked:
            await page.wait_for_timeout(300)
            return
    raise RuntimeError("상세 페이지 진입 실패: 링크를 찾을 수 없음")


def _normalize_key(k: str) -> str:
    """줄바꿈/공백 정규화 (입찰보증서\\n접수마감일시 -> 입찰보증서접수마감일시)"""
    if not k:
        return ""
    return "".join(k.split())


async def _extract_detail_raw(page: Page) -> Dict[str, Any]:
    """th/td, dt/dd를 순서 유지하며 추출. textContent 폴백으로 동적 렌더 값 수집."""
    r: Dict[str, Any] = await page.evaluate(
        """
        () => {
            const trim = (s) => (s ?? '').toString().trim();
            const cellText = (el) => {
                if (!el) return '';
                return trim((el.textContent || el.innerText || '').toString());
            };
            const ordered = [];
            const tables = [];

            document.querySelectorAll('table tr').forEach(tr => {
                const ths = tr.querySelectorAll('th');
                const tds = tr.querySelectorAll('td');
                if (ths.length >= 1 && tds.length >= 1) {
                    for (let j = 0; j < Math.min(ths.length, tds.length); j++) {
                        ordered.push([cellText(ths[j]), cellText(tds[j])]);
                    }
                } else if (ths.length >= 2 && tds.length === 0) {
                    tables.push({ headers: Array.from(ths).map(h => cellText(h)), rows: [] });
                } else if (ths.length === 0 && tds.length >= 1 && tables.length) {
                    const last = tables[tables.length - 1];
                    if (last.headers) last.rows.push(Array.from(tds).map(c => cellText(c)));
                } else if (ths.length === 1 && tds.length === 0) {
                    ordered.push([cellText(ths[0]), '']);
                }
            });
            document.querySelectorAll('dl').forEach(dl => {
                const dts = dl.querySelectorAll('dt');
                const dds = dl.querySelectorAll('dd');
                for (let j = 0; j < Math.min(dts.length, dds.length); j++) {
                    ordered.push([cellText(dts[j]), cellText(dds[j])]);
                }
            });
            return { ordered, tables };
        }
        """
    )
    return r or {"ordered": [], "tables": []}


async def extract_detail_kv(page: Page, cfg: CrawlConfig) -> Dict[str, Any]:
    """입찰공고진행상세 등에서 대분류→소분류 구조로 추출 (공백값 포함)"""
    return await _extract_detail_kv_sectioned(page, cfg)


async def _extract_detail_kv_sectioned(page: Page, cfg: CrawlConfig) -> Dict[str, Any]:
    """필드→섹션 매핑으로 대분류→소분류 구조 생성."""
    sections = cfg.detail_sections or []
    field_sections = cfg.detail_field_sections or {}
    table_columns = cfg.detail_table_columns or {}
    table_section_names = set(cfg.detail_table_sections or [])

    if not sections and not field_sections:
        raw = await _extract_detail_raw(page)
        return {k: v for k, v in raw.get("ordered", []) if k}

    raw = await _extract_detail_raw(page)
    ordered: List[List[str]] = raw.get("ordered", [])
    tables: List[Dict[str, Any]] = raw.get("tables", [])

    section_set = set(sections)
    out: Dict[str, Any] = {}
    current = sections[0] if sections else None
    if current and current not in table_section_names:
        out[current] = {}

    # 필드->섹션 매핑 (섹션 제목 없이 필드로 전환). 중복필드(지역제한 등)는 섹션 순서 유지.
    key_to_section_order: Dict[str, List[str]] = {}
    for sec in sections:
        if sec in table_section_names:
            continue
        for f in (field_sections.get(sec) or []):
            nf = _normalize_key(f)
            if nf:
                key_to_section_order.setdefault(nf, []).append(sec)
    section_idx = {s: i for i, s in enumerate(sections)}

    def _section_for_key(nk: str) -> Optional[str]:
        cands = key_to_section_order.get(nk)
        if not cands:
            return None
        cur_i = section_idx.get(current, -1)
        for sec in cands:
            if section_idx.get(sec, 999) > cur_i:
                return sec
        return cands[-1]

    for k, v in ordered:
        nk = _normalize_key(k)
        if not nk:
            continue
        if nk in section_set:
            current = nk
            if current not in out:
                out[current] = [] if current in table_section_names else {}
            if v and current not in table_section_names and isinstance(out.get(current), dict):
                out[current][k] = v
            continue
        cur_fields = {_normalize_key(f) for f in (field_sections.get(current) or [])}
        target = _section_for_key(nk)
        already_in_current = nk in cur_fields and isinstance(out.get(current), dict) and any(
            _normalize_key(key) == nk for key in out.get(current, {})
        )
        if target and target != current and target not in table_section_names and (
            nk not in cur_fields or already_in_current
        ):
            current = target
            if current not in out:
                out[current] = {}
        if not current or current not in out:
            continue
        if isinstance(out[current], list):
            continue
        fields = field_sections.get(current, [])
        norm_fields = {_normalize_key(f): f for f in fields}
        if fields and nk not in norm_fields and nk not in fields:
            if current != "공고일반":
                continue
        out[current][k] = v

    col_aliases: Dict[str, str] = {"번호": "재입찰번호", "공고": "공고명"}
    for tbl in tables:
        headers = [_normalize_key(h) for h in tbl.get("headers", [])]
        header_set = set(headers)
        header_canonical = {h: _normalize_key(col_aliases.get(h, h)) for h in headers}
        rows = tbl.get("rows", [])
        for sec, cols in table_columns.items():
            if sec in out and isinstance(out.get(sec), list) and out[sec]:
                continue
            want = [_normalize_key(c) for c in cols]
            want_set = set(want)
            orig_col = {_normalize_key(c): c for c in cols}
            if not want_set:
                continue
            expanded = set(header_canonical.values())
            overlap = want_set & expanded
            if not (want_set <= expanded or len(overlap) >= max(2, len(want_set) - 1)):
                continue
            use_cols = overlap if overlap else want_set
            recs = []
            for row in rows:
                rec = {}
                for i, h in enumerate(headers):
                    canon = header_canonical.get(h, h)
                    if i < len(row) and canon in use_cols:
                        key = orig_col.get(canon, col_aliases.get(h, h))
                        rec[key] = (row[i] or "").strip()
                if any(rec.values()):
                    recs.append(rec)
            if recs:
                out[sec] = recs
            break

    for sec in table_section_names:
        if sec not in out:
            out[sec] = []

    return out


async def save_evidence(errors_dir: Path, notice_id: str, page: Page, err: Exception) -> None:
    errors_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base = errors_dir / f"{safe_filename(notice_id)}_{ts}"
    meta_path = base.with_suffix(".json")
    html_path = base.with_suffix(".html")
    png_path = base.with_suffix(".png")

    try:
        html = await page.content()
        html_path.write_text(html, encoding="utf-8", errors="ignore")
    except Exception:
        pass
    try:
        await page.screenshot(path=str(png_path), full_page=True)
    except Exception:
        pass
    meta = {
        "notice_id": notice_id,
        "error": repr(err),
        "url": page.url,
        "collected_at_utc": utc_now_iso(),
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


async def crawl_once(
    cfg: CrawlConfig,
    run_id: str,
    out_raw_list: Path,
    out_normalized: Path,
    errors_dir: Path,
    state_db: Path,
    headless: bool,
    max_pages: int,
    max_items: int,
    keywords: List[str],
    list_only: bool = False,
    debug_slowmo_ms: int = 0,
) -> None:
    state = StateStore(state_db)
    raw_writer = JsonlWriter(out_raw_list)
    norm_writer = JsonlWriter(out_normalized)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless, slow_mo=debug_slowmo_ms)
        context = await browser.new_context()
        page = await context.new_page()

        await navigate_to_bid_list(page, cfg)

        start_page = int(state.get_checkpoint("bid_list.page") or "1")
        current_page = start_page
        processed_this_run = 0

        log.info("시작: page=%d, processed=0/%d", current_page, max_items)

        while current_page <= max_pages and processed_this_run < max_items:
            state.set_checkpoint("bid_list.page", str(current_page))
            items = await extract_list_items(page, cfg, limit=max_items - processed_this_run)
            if not items:
                log.warning("페이지 %d에서 추출된 항목 없음", current_page)

            # 키워드 필터(제목/row_text 기준)
            if keywords:
                def _match(it: ListItem) -> bool:
                    hay = (it.title or "") + "\n" + (it.raw_text or "")
                    return any(k in hay for k in keywords)

                items = [it for it in items if _match(it)]

            # raw 목록 저장
            for it in items:
                raw_writer.write_one(it.model_dump())
                state.mark_seen(it.notice_id, utc_now_iso())

            if list_only:
                processed_this_run += len(items)
                # 상세 수집은 생략하고 다음 페이지로만 진행
                # (아래 페이지네이션 로직을 그대로 사용)
            else:
                for it in items:
                    if processed_this_run >= max_items:
                        break
                    if state.is_processed(it.notice_id):
                        continue

                    try:
                        # 상세 진입/추출은 실패가 잦을 수 있어 개별 재시도
                        await _open_detail(page, it)
                        await _wait_any_selector(page, cfg.detail_ready_selectors, timeout_ms=15000)
                        await page.wait_for_timeout(1500)
                        kv = await extract_detail_kv(page, cfg)
                        html = await page.content()
                        content_hash = sha256_text(html)

                        prev_hash = state.get_content_hash(it.notice_id)
                        state.upsert_processed(it.notice_id, "ok", utc_now_iso(), content_hash=content_hash)

                        rec = NoticeRecord(
                            source=SourceMeta(collected_at_utc=utc_now_iso(), run_id=run_id),
                            notice={
                                "notice_id": it.notice_id,
                                "title": it.title,
                                "detail_url": page.url,
                                "list_parsed": it.raw.get("parsed", {}),
                                "detail_fields": kv,
                                "updated": bool(prev_hash and prev_hash != content_hash),
                            },
                            raw={
                                "list_item": it.model_dump(),
                                "html_sha256": content_hash,
                            },
                        )
                        norm_writer.write_one(rec.model_dump())
                        processed_this_run += 1
                    except Exception as e:
                        log.exception("detail failed: %s", it.notice_id)
                        state.upsert_processed(it.notice_id, "error", utc_now_iso())
                        await save_evidence(errors_dir, it.notice_id, page, e)
                    finally:
                        # 목록으로 복귀(뒤로가기), 실패하면 다시 목록 네비게이션
                        try:
                            await page.go_back(wait_until="domcontentloaded", timeout=15000)
                            await _goto_or_recover_list(page, cfg)
                        except Exception:
                            await navigate_to_bid_list(page, cfg)

            # 다음 페이지로 이동 (가능한 경우)
            moved = False
            await page.wait_for_timeout(500)  # 목록 복귀 후 안정화

            # 0) 페이지 번호 링크 직접 클릭 (next/nextPage 버튼 혼동 방지)
            next_page_num = current_page + 1
            try:
                page_link = page.locator(
                    f"a[id*='pagelist_page_'][index='{next_page_num}']"
                )
                if await page_link.count() > 0:
                    await page_link.first.scroll_into_view_if_needed(timeout=5000)
                    await page_link.first.click(timeout=8000)
                    await _goto_or_recover_list(page, cfg)
                    moved = True
                    log.info("다음 페이지로 이동 (page link): %d", next_page_num)
            except Exception:
                pass
            if moved:
                current_page += 1
                continue

            # 1) CSS 선택자 우선 (누리장터 w2pageList 등 커스텀 페이지네이션)
            for sel in cfg.next_button_selector_candidates:
                try:
                    loc = page.locator(sel)
                    if await loc.count() > 0:
                        await loc.first.scroll_into_view_if_needed(timeout=5000)
                        await loc.first.click(timeout=8000)
                        await _goto_or_recover_list(page, cfg)
                        moved = True
                        log.info("다음 페이지로 이동 (selector): %s", sel)
                        break
                except Exception:
                    continue
            if moved:
                current_page += 1
                continue

            # 2) role/text 기반 폴백
            for name in cfg.next_button_name_candidates:
                for strategy in [
                    lambda n: page.get_by_role("button", name=n),
                    lambda n: page.get_by_role("link", name=n),
                    lambda n: page.get_by_text(n, exact=True),
                ]:
                    try:
                        loc = strategy(name)
                        if await loc.count() > 0:
                            await loc.first.scroll_into_view_if_needed(timeout=5000)
                            await loc.first.click(timeout=8000)
                            await _goto_or_recover_list(page, cfg)
                            moved = True
                            log.info("다음 페이지로 이동: %s", name)
                            break
                    except Exception:
                        continue
                if moved:
                    break

            if not moved:
                log.info("다음 페이지 없음, 종료")
                break

            current_page += 1

        await context.close()
        await browser.close()

        log.info(
            "크롤링 완료: raw=%s, normalized=%s, processed=%d",
            out_raw_list,
            out_normalized,
            processed_this_run,
        )

