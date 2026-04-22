from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

# Reuse existing processing pipeline (download logo -> upload s3 -> put-logo -> cleanup).
from sync_no_logo_batch import process_task, send_lark_text

MATCH_SEARCH_API_URL = os.getenv(
    "MATCH_SEARCH_API_URL",
    "https://xpbet-service-api.helix.city/v1/match/search",
)
MATCH_SEARCH_SPORT_IDS = [x.strip() for x in os.getenv("MATCH_SEARCH_SPORT_IDS", "6046,48242").split(",") if x.strip()]
MATCH_SEARCH_TZ = os.getenv("MATCH_SEARCH_TIMEZONE", "Asia/Shanghai")
MATCH_SEARCH_SOURCE = os.getenv("MATCH_SEARCH_X_SOURCE", "ls")
MATCH_SEARCH_HEADERS = {
    "accept": "*/*",
    "accept-language": "en",
    "cache-control": "no-cache, no-store, must-revalidate",
    "content-type": "application/json",
    "origin": "https://match-pc.helix.city",
    "referer": "https://match-pc.helix.city/",
    "user-agent": (
        "Mozilla/5.0 (Linux; Android 8.0.0; SM-G955U Build/R16NW) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 "
        "Mobile Safari/537.36 Edg/147.0.0.0"
    ),
    "x-source": MATCH_SEARCH_SOURCE,
    "x-timezone": MATCH_SEARCH_TZ,
}

PROCESS_BATCH_SIZE = int(os.getenv("PROCESS_BATCH_SIZE", "10"))
MAX_NEW_PER_CYCLE = int(os.getenv("MAX_NEW_PER_CYCLE", "0"))  # 0=unlimited
POLL_INTERVAL_SECONDS = int(os.getenv("MATCH_SEARCH_POLL_INTERVAL_SECONDS", "30"))
CONTINUOUS_RUN = os.getenv("CONTINUOUS_RUN", "true").lower() in {"1", "true", "yes"}
STATE_FILE = Path(os.getenv("MATCH_SEARCH_STATE_FILE", str(Path(__file__).resolve().parent / "match_search_state.json")))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("MATCH_SEARCH_REQUEST_TIMEOUT_SECONDS", "30"))
MAX_CURSOR_PAGES = int(os.getenv("MATCH_SEARCH_MAX_CURSOR_PAGES", "50"))
RETRY_FAILED = os.getenv("MATCH_SEARCH_RETRY_FAILED", "false").lower() in {"1", "true", "yes"}
RETRY_RETRYABLE = os.getenv("MATCH_SEARCH_RETRY_RETRYABLE", "true").lower() in {"1", "true", "yes"}


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def chunked(items: List[Dict[str, Any]], size: int) -> List[List[Dict[str, Any]]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def default_state() -> Dict[str, Any]:
    return {
        "version": 1,
        "updated_at": now_text(),
        "processed": {},
        "stats": {"success": 0, "fail": 0, "retryable_fail": 0},
    }


def load_state() -> Dict[str, Any]:
    if not STATE_FILE.exists():
        return default_state()
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return default_state()
        data.setdefault("processed", {})
        data.setdefault("stats", {"success": 0, "fail": 0, "retryable_fail": 0})
        return data
    except Exception:
        return default_state()


def save_state(state: Dict[str, Any]) -> None:
    state["updated_at"] = now_text()
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def should_process_match(state: Dict[str, Any], match_id: str) -> bool:
    item = (state.get("processed") or {}).get(match_id)
    if not item:
        return True
    status = item.get("status")
    if RETRY_RETRYABLE and status == "retryable_fail":
        return True
    if RETRY_RETRYABLE and status == "fail":
        err_text = " ".join([str(x) for x in (item.get("errors") or [])]).lower()
        if (
            "playwright抓取异常" in err_text
            or "status=429" in err_text
            or "rate limit exceeded per hour" in err_text
            or "timed out" in err_text
            or "connection reset" in err_text
        ):
            return True
    if RETRY_FAILED and status in {"fail", "retryable_fail"}:
        return True
    return False


def normalize_task(source: str, sport_id: str, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    match_id = str(event.get("event_id") or "").strip()
    if not match_id:
        return None
    home = event.get("home_competitor") or {}
    away = event.get("away_competitor") or {}
    return {
        "match_id": match_id,
        "sport_id": str(sport_id),
        "home_id": str(home.get("competitor_id") or "").strip() or None,
        "away_id": str(away.get("competitor_id") or "").strip() or None,
        "home_name": home.get("name"),
        "away_name": away.get("name"),
        "match_time": event.get("start_time"),
        "source": source,
    }


def fetch_tasks_for_source(sport_id: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    source_name = f"sport_id={sport_id}"
    tasks: List[Dict[str, Any]] = []
    errors: List[str] = []
    cursor = "0"
    visited_cursor = set()

    for _ in range(MAX_CURSOR_PAGES):
        if cursor in visited_cursor:
            break
        visited_cursor.add(cursor)
        # 按需求仅使用 sport_id + cursor 获取列表
        params = {"sport_id": sport_id, "cursor": cursor}
        try:
            resp = requests.get(
                MATCH_SEARCH_API_URL,
                params=params,
                headers=MATCH_SEARCH_HEADERS,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            resp.raise_for_status()
            body = resp.json()
        except Exception as exc:
            errors.append(f"{source_name} fetch失败 cursor={cursor}: {exc}")
            break

        if str(body.get("code")) != "0":
            errors.append(f"{source_name} 返回code异常 cursor={cursor}: {body}")
            break

        data = body.get("data") or {}
        groups = data.get("list") or []
        for g in groups:
            g_sport_id = str(g.get("sport_id") or sport_id)
            events = g.get("events") or []
            for e in events:
                task = normalize_task(source_name, g_sport_id, e)
                if task:
                    tasks.append(task)

        next_cursor = str(data.get("next_cursor") or "").strip()
        if not next_cursor:
            break
        cursor = next_cursor

    return tasks, errors


def fetch_all_tasks() -> Tuple[List[Dict[str, Any]], List[str]]:
    all_tasks: List[Dict[str, Any]] = []
    all_errors: List[str] = []
    for sport_id in MATCH_SEARCH_SPORT_IDS:
        tasks, errors = fetch_tasks_for_source(sport_id=sport_id)
        all_tasks.extend(tasks)
        all_errors.extend(errors)

    # Deduplicate by match_id, keep first.
    dedup: Dict[str, Dict[str, Any]] = {}
    for t in all_tasks:
        mid = str(t.get("match_id") or "").strip()
        if mid and mid not in dedup:
            dedup[mid] = t
    return list(dedup.values()), all_errors


def build_cycle_summary(
    cycle: int,
    started_at: datetime,
    fetched_total: int,
    new_total: int,
    processed_records: List[Dict[str, Any]],
    fetch_errors: List[str],
    state: Dict[str, Any],
) -> str:
    ended_at = datetime.now()
    match_success = sum(1 for r in processed_records if r.get("ok"))
    match_fail = len(processed_records) - match_success
    post_total = sum(int(r.get("post_total") or 0) for r in processed_records)
    post_success = sum(int(r.get("post_success") or 0) for r in processed_records)
    post_fail = sum(int(r.get("post_fail") or 0) for r in processed_records)
    state_stats = state.get("stats") or {}
    lines = [
        "logo_spd match-search轮询汇总",
        f"cycle: {cycle}",
        f"start: {started_at.strftime('%Y-%m-%d %H:%M:%S')}",
        f"end: {ended_at.strftime('%Y-%m-%d %H:%M:%S')}",
        f"fetched_match_ids: {fetched_total}",
        f"new_match_ids: {new_total}",
        f"processed_success: {match_success}",
        f"processed_fail: {match_fail}",
        f"post_success: {post_success}/{post_total}",
        f"post_fail: {post_fail}",
        f"state_file: {STATE_FILE}",
        f"state_total_success: {state_stats.get('success', 0)}",
        f"state_total_fail: {state_stats.get('fail', 0)}",
        f"state_total_retryable_fail: {state_stats.get('retryable_fail', 0)}",
    ]
    if fetch_errors:
        lines.append("fetch_errors:")
        for e in fetch_errors[:10]:
            lines.append(f"- {e}")
    failed = [r for r in processed_records if not r.get("ok")]
    if failed:
        lines.append("failed_samples:")
        for r in failed[:12]:
            reason = "; ".join((r.get("errors") or [])[:2]) or "unknown"
            lines.append(f"- match_id={r.get('match_id')}, reason={reason}")
    return "\n".join(lines)


def main():
    state = load_state()
    cycle = 0

    while True:
        cycle += 1
        started_at = datetime.now()
        print(f"\n=== match-search cycle {cycle} start: {started_at.strftime('%Y-%m-%d %H:%M:%S')} ===")

        tasks, fetch_errors = fetch_all_tasks()
        fetched_total = len(tasks)
        new_tasks = [t for t in tasks if should_process_match(state, str(t.get("match_id")))]

        if MAX_NEW_PER_CYCLE > 0:
            new_tasks = new_tasks[:MAX_NEW_PER_CYCLE]

        if not new_tasks:
            idle_text = (
                "logo_spd match-search轮询状态\n"
                f"cycle: {cycle}\n"
                f"fetched_match_ids: {fetched_total}\n"
                "no new match_id to process"
            )
            if fetch_errors:
                idle_text += "\nfetch_errors:\n" + "\n".join([f"- {x}" for x in fetch_errors[:10]])
            print(idle_text)
            send_lark_text(idle_text)
            save_state(state)

            if not CONTINUOUS_RUN:
                break
            print(f"sleep {POLL_INTERVAL_SECONDS}s then next cycle...")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        total = len(new_tasks)
        records: List[Dict[str, Any]] = []

        for idx, task in enumerate(new_tasks, start=1):
            rec = process_task(task, idx, total)
            records.append(rec)

            match_id = str(task.get("match_id"))
            ok = bool(rec.get("ok"))
            retryable = bool(rec.get("retryable"))
            status = "success" if ok else ("retryable_fail" if retryable else "fail")
            state_item = {
                "match_id": match_id,
                "sport_id": str(task.get("sport_id") or ""),
                "home_id": str(task.get("home_id") or ""),
                "away_id": str(task.get("away_id") or ""),
                "home_name": task.get("home_name"),
                "away_name": task.get("away_name"),
                "source": task.get("source"),
                "status": status,
                "processed_at": now_text(),
                "post_total": rec.get("post_total", 0),
                "post_success": rec.get("post_success", 0),
                "post_fail": rec.get("post_fail", 0),
                "errors": rec.get("errors", []),
            }
            state.setdefault("processed", {})[match_id] = state_item
            stats = state.setdefault("stats", {"success": 0, "fail": 0, "retryable_fail": 0})
            if ok:
                stats["success"] = int(stats.get("success", 0)) + 1
            elif retryable:
                stats["retryable_fail"] = int(stats.get("retryable_fail", 0)) + 1
            else:
                stats["fail"] = int(stats.get("fail", 0)) + 1
            save_state(state)

        summary = build_cycle_summary(
            cycle=cycle,
            started_at=started_at,
            fetched_total=fetched_total,
            new_total=len(new_tasks),
            processed_records=records,
            fetch_errors=fetch_errors,
            state=state,
        )
        notified = send_lark_text(summary)
        print(summary)
        print(f"\nLark summary notified: {notified}")

        if not CONTINUOUS_RUN:
            break
        print(f"sleep {POLL_INTERVAL_SECONDS}s then next cycle...")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
