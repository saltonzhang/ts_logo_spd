from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from athena_sdk import AthenaClient

BASE_URL = "https://match-pc.helix.city/"
MATCH_PAGE_PREFIX = "https://match-pc.helix.city/en/matches/"
OUTPUT_DIR = Path(__file__).resolve().parent

NO_LOGO_API_URL = os.getenv(
    "NO_LOGO_API_URL",
    "https://ls-sportdata-syncer-api.helix.city/api/v1/competitors/no-logo",
)
PUT_LOGO_API_URL = os.getenv(
    "PUT_LOGO_API_URL",
    "https://ls-sportdata-syncer-api.helix.city/api/v1/competitors/put-logo",
)
PUT_LOGO_VALUE_FIELD = os.getenv("PUT_LOGO_VALUE_FIELD", "cdn_url").strip().lower()

LARK_WEBHOOK_URL = os.getenv(
    "LARK_WEBHOOK_URL",
    "https://open.larksuite.com/open-apis/bot/v2/hook/f4af0cbe-27e9-445a-8a02-960522d4905a",
)

ATHENA_BASE_URL = os.getenv("ATHENA_BASE_URL", "https://xp-athena-test1-api.helix.city")
ATHENA_ACCESS_KEY = os.getenv("ATHENA_ACCESS_KEY", "8KPsuFGFyrfz")
ATHENA_SECRET_KEY = os.getenv("ATHENA_SECRET_KEY", "C0JIsSNKNBJYFOuG6Evu")
ATHENA_REGION_NAME = os.getenv("ATHENA_REGION_NAME", "us-west-2")
S3_KEY_PREFIX = os.getenv("S3_KEY_PREFIX", "images").strip("/")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "")
UPLOAD_JSON_TO_S3 = os.getenv("UPLOAD_JSON_TO_S3", "false").lower() in {"1", "true", "yes"}

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
MAX_MATCHES = int(os.getenv("MAX_MATCHES", str(BATCH_SIZE)))
DRY_RUN = os.getenv("DRY_RUN", "false").lower() in {"1", "true", "yes"}
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "3600"))
CONTINUOUS_RUN = os.getenv("CONTINUOUS_RUN", "true").lower() in {"1", "true", "yes"}
PROCESSING_LOOP_DELAY_SECONDS = int(os.getenv("PROCESSING_LOOP_DELAY_SECONDS", "30"))

MOBILE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 "
        "Mobile/15E148 Safari/604.1"
    ),
    "Referer": BASE_URL,
}


def send_lark_text(text: str) -> bool:
    if not LARK_WEBHOOK_URL:
        return False
    payload = {"msg_type": "text", "content": {"text": text}}
    try:
        resp = requests.post(LARK_WEBHOOK_URL, json=payload, timeout=10)
        resp.raise_for_status()
        body = resp.json()
        return body.get("StatusCode") == 0
    except Exception:
        return False


def chunked(items: List[Dict[str, Any]], size: int) -> List[List[Dict[str, Any]]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def abs_url(src: Optional[str]) -> Optional[str]:
    if not src:
        return None
    return urljoin(BASE_URL, src)


def sanitize_filename(value: str) -> str:
    value = value.strip()
    value = re.sub(r'[\\/:*?"<>|]+', "_", value)
    value = re.sub(r"\s+", " ", value)
    return value or "unknown"


def build_logo_filename(team: dict, default_name: str) -> str:
    team_name = sanitize_filename(team.get("name") or team.get("short_name") or default_name)
    team_id = sanitize_filename(str(team.get("id") or "unknown"))
    return f"{team_name}_{team_id}.png"


def fetch_html(url: str, headers: Dict[str, str]) -> str:
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.text


def fetch_no_logo_tasks(size: int) -> List[Dict[str, Any]]:
    resp = requests.get(NO_LOGO_API_URL, params={"size": size}, timeout=30)
    resp.raise_for_status()
    body = resp.json()
    if body.get("code") != 0:
        raise RuntimeError(f"no-logo接口返回异常: {body}")
    data = body.get("data")
    if not isinstance(data, list):
        raise RuntimeError(f"no-logo接口data类型异常: {type(data).__name__}")
    return data


def parse_dom(html: str, match_url: str, match_id: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text("\n", strip=True)

    home_name = None
    away_name = None
    title_text = soup.title.get_text(" ", strip=True) if soup.title else ""
    m = re.search(r"^(.*?)\s+vs\s+(.*?)\s+-", title_text, re.IGNORECASE)
    if m:
        home_name = m.group(1).strip()
        away_name = m.group(2).strip()

    img_srcs = [img.get("src") for img in soup.find_all("img") if img.get("src")]
    png_srcs = [src for src in img_srcs if src.lower().endswith(".png")]
    team_pngs = []
    for src in png_srcs:
        lower = src.lower()
        if any(x in lower for x in ["seal", "stamp", "license", "anjouan", "eighteen-plus"]):
            continue
        team_pngs.append(src)
    team_pngs = list(dict.fromkeys(team_pngs))

    short_names = []
    for candidate in ["Jamtland", "Boras"]:
        if re.search(rf"\b{re.escape(candidate)}\b", page_text, re.IGNORECASE):
            short_names.append(candidate)

    return {
        "match_url": match_url,
        "match_id": match_id,
        "sport_id": None,
        "sport_ids": [],
        "source_mode": "dom",
        "home": {
            "id": None,
            "name": home_name,
            "short_name": short_names[0] if len(short_names) > 0 else None,
            "abbr": None,
            "logo": abs_url(team_pngs[0]) if len(team_pngs) > 0 else None,
        },
        "away": {
            "id": None,
            "name": away_name,
            "short_name": short_names[1] if len(short_names) > 1 else None,
            "abbr": None,
            "logo": abs_url(team_pngs[1]) if len(team_pngs) > 1 else None,
        },
        "api_hits": [],
        "errors": [],
    }


def deep_find_sport_ids(obj, found=None):
    if found is None:
        found = []

    def add_value(value):
        if value is None:
            return
        if isinstance(value, (list, tuple, set)):
            for item in value:
                add_value(item)
            return
        value_str = str(value).strip()
        if not value_str:
            return
        if value_str not in found:
            found.append(value_str)

    if isinstance(obj, dict):
        for k, v in obj.items():
            key_l = str(k).lower()
            if key_l in {"sportid", "sport_id", "sportids", "sport_ids"}:
                add_value(v)
            elif "sport" in key_l and key_l.endswith("id"):
                add_value(v)
            deep_find_sport_ids(v, found)
    elif isinstance(obj, list):
        for item in obj:
            deep_find_sport_ids(item, found)

    return found


def deep_find_team_info(obj, found=None):
    if found is None:
        found = {
            "home": {"id": None, "name": None, "abbr": None, "logo": None},
            "away": {"id": None, "name": None, "abbr": None, "logo": None},
        }

    if isinstance(obj, dict):
        lower_to_real = {str(k).lower(): k for k in obj.keys()}
        for side in ("home", "away"):
            for key in (
                f"{side}team",
                f"{side}_team",
                f"{side}competitor",
                f"{side}_competitor",
                side,
            ):
                if key in lower_to_real:
                    val = obj[lower_to_real[key]]
                    if isinstance(val, dict):
                        found[side]["id"] = (
                            found[side]["id"]
                            or val.get("id")
                            or val.get("teamId")
                            or val.get("team_id")
                            or val.get("competitorId")
                            or val.get("competitor_id")
                        )
                        found[side]["name"] = found[side]["name"] or val.get("name")
                        found[side]["abbr"] = (
                            found[side]["abbr"]
                            or val.get("abbr")
                            or val.get("shortName")
                            or val.get("short_name")
                            or val.get("code")
                        )
                        found[side]["logo"] = (
                            found[side]["logo"]
                            or val.get("logo")
                            or val.get("logoUrl")
                            or val.get("logo_url")
                            or val.get("image")
                            or val.get("icon")
                        )

            for id_key in (
                f"{side}TeamId",
                f"{side}_team_id",
                f"{side}CompetitorId",
                f"{side}_competitor_id",
            ):
                if id_key in obj and not found[side]["id"]:
                    found[side]["id"] = obj[id_key]

            for name_key in (f"{side}TeamName", f"{side}_team_name", f"{side}Name", f"{side}_name"):
                if name_key in obj and not found[side]["name"]:
                    found[side]["name"] = obj[name_key]

            for logo_key in (f"{side}TeamLogo", f"{side}_team_logo", f"{side}Logo", f"{side}_logo"):
                if logo_key in obj and not found[side]["logo"]:
                    found[side]["logo"] = obj[logo_key]

        for v in obj.values():
            deep_find_team_info(v, found)
    elif isinstance(obj, list):
        for item in obj:
            deep_find_team_info(item, found)

    return found


def parse_match_api_error(api_url: str, payload) -> Optional[str]:
    if "/v1/match/" not in api_url.lower():
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("data") is not None:
        return None
    message = str(payload.get("message") or "")
    if "MatchHandler.MatchListRowHandler error" in message:
        return f"match接口异常: {api_url} | {message}"
    return None


def parse_statscore_widget_eventid_error(api_url: str, payload) -> Optional[str]:
    if "widgets.statscore.com/api/ssr/render-widget" not in api_url.lower():
        return None
    if not isinstance(payload, dict):
        return None
    message = str(payload.get("message") or "")
    if 'inputData parameter "eventId" is required' in message:
        return f"statscore eventId异常: {api_url} | {message}"
    return None


def enrich_with_playwright(result: dict, match_url: str, match_id: str) -> dict:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        result["errors"].append(f"playwright不可用: {exc}")
        return result

    keywords = [
        "match",
        "matches",
        "event",
        "fixture",
        "team",
        "competitor",
        "prematch",
        "summary",
        "stat",
        "score",
        "breadcrumb",
    ]

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(**p.devices["iPhone 13"])
            page = context.new_page()
            captured = []

            def on_response(response):
                if response.request.resource_type not in {"xhr", "fetch"}:
                    return
                url_l = response.url.lower()
                is_match_related = match_id in url_l or f"event_id={match_id}" in url_l
                is_statscore_widget = "widgets.statscore.com/api/ssr/render-widget" in url_l
                if not (is_match_related or is_statscore_widget):
                    return
                if (not is_statscore_widget) and (not any(k in url_l for k in keywords)):
                    return
                try:
                    ctype = response.headers.get("content-type", "").lower()
                    if "json" not in ctype:
                        return
                    data = response.json()
                    captured.append((response.url, data))
                except Exception:
                    return

            page.on("response", on_response)
            page.goto(match_url, wait_until="domcontentloaded", timeout=45000)
            page.wait_for_timeout(5000)

            img_srcs = page.eval_on_selector_all(
                "img",
                "els => els.map(e => e.getAttribute('src')).filter(Boolean)",
            )
            pngs = [x for x in img_srcs if x.lower().endswith(".png")]
            pngs = [x for x in pngs if not re.search(r"seal|stamp|license|anjouan|eighteen-plus", x, re.I)]
            if pngs and not result["home"].get("logo"):
                result["home"]["logo"] = abs_url(pngs[0])
            if len(pngs) > 1 and not result["away"].get("logo"):
                result["away"]["logo"] = abs_url(pngs[1])

            hit_urls = set()
            for api_url, data in captured:
                hit_urls.add(api_url)
                err = parse_match_api_error(api_url, data)
                if err:
                    result["errors"].append(err)
                err = parse_statscore_widget_eventid_error(api_url, data)
                if err:
                    result["errors"].append(err)

                info = deep_find_team_info(data)
                sport_ids = deep_find_sport_ids(data)
                for sport_id in sport_ids:
                    if sport_id not in result["sport_ids"]:
                        result["sport_ids"].append(sport_id)
                if not result.get("sport_id") and result["sport_ids"]:
                    result["sport_id"] = result["sport_ids"][0]

                useful = any(
                    [
                        info["home"]["id"],
                        info["away"]["id"],
                        info["home"]["name"],
                        info["away"]["name"],
                        info["home"]["logo"],
                        info["away"]["logo"],
                    ]
                )
                if not useful:
                    continue

                for side in ("home", "away"):
                    result[side]["id"] = result[side]["id"] or info[side]["id"]
                    result[side]["name"] = result[side]["name"] or info[side]["name"]
                    result[side]["abbr"] = result[side]["abbr"] or info[side]["abbr"]
                    result[side]["logo"] = result[side]["logo"] or abs_url(info[side]["logo"])

            result["api_hits"] = [{"mode": "mobile", "url": u} for u in sorted(hit_urls)]
            result["source_mode"] = "mobile"
            context.close()
            browser.close()
    except Exception as exc:
        result["errors"].append(f"playwright抓取异常: {exc}")

    # 去重，避免同类错误刷太多
    result["errors"] = list(dict.fromkeys(result["errors"]))
    return result


def download_file(url: str, save_path: Path, headers: Dict[str, str]):
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    save_path.write_bytes(resp.content)


def save_outputs(result: dict):
    output_root = OUTPUT_DIR
    output_root.mkdir(parents=True, exist_ok=True)
    sport_id = str(result.get("sport_id") or (result.get("sport_ids") or [None])[0] or "unknown_sport")
    sport_dir = output_root / sanitize_filename(sport_id)
    sport_dir.mkdir(parents=True, exist_ok=True)

    home_filename = None
    away_filename = None
    issues = []

    if result["home"].get("logo"):
        home_filename = build_logo_filename(result["home"], "home")
        try:
            download_file(result["home"]["logo"], sport_dir / home_filename, MOBILE_HEADERS)
        except Exception as exc:
            issues.append(f"home logo 下载失败: {exc}")
    else:
        issues.append("home logo 链接缺失")

    if result["away"].get("logo"):
        away_filename = build_logo_filename(result["away"], "away")
        try:
            download_file(result["away"]["logo"], sport_dir / away_filename, MOBILE_HEADERS)
        except Exception as exc:
            issues.append(f"away logo 下载失败: {exc}")
    else:
        issues.append("away logo 链接缺失")

    result["home"]["abbr"] = None
    result["away"]["abbr"] = None
    result["home"]["logo_file"] = home_filename
    result["away"]["logo_file"] = away_filename
    result["output_dir"] = str(sport_dir)
    result["logo_download_issues"] = issues

    json_path = sport_dir / f"match_{result.get('match_id')}.json"
    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return json_path, sport_dir, issues


def build_s3_key(*parts: str) -> str:
    cleaned = [str(p).strip("/") for p in parts if str(p).strip("/")]
    return "/".join(cleaned)


def upload_outputs_to_s3(result: dict, sport_dir: Path, json_path: Path):
    client = AthenaClient(
        ATHENA_BASE_URL,
        access_key=ATHENA_ACCESS_KEY,
        secret_key=ATHENA_SECRET_KEY,
        region_name=ATHENA_REGION_NAME,
    )

    upload_targets = []
    for side in ("home", "away"):
        logo_file = result.get(side, {}).get("logo_file")
        if logo_file:
            logo_path = sport_dir / logo_file
            key = build_s3_key(S3_KEY_PREFIX, str(result.get("sport_id")), logo_file)
            upload_targets.append((f"{side}_logo", logo_path, key))

    if UPLOAD_JSON_TO_S3:
        json_key = build_s3_key(S3_KEY_PREFIX, str(result.get("sport_id")), json_path.name)
        upload_targets.append(("metadata_json", json_path, json_key))

    uploaded = []
    issues = []
    for label, file_path, key in upload_targets:
        if not file_path.exists():
            issues.append(f"{label} 文件不存在: {file_path}")
            continue
        resolved = None
        try:
            resolved = client.resolve_upload_target(Key=key, Bucket=S3_BUCKET_NAME)
            if not DRY_RUN:
                client.upload_file(Filename=str(file_path), Bucket=S3_BUCKET_NAME, Key=key)
            uploaded.append(
                {
                    "label": label,
                    "file": file_path.name,
                    "requested_key": key,
                    "bucket": (resolved or {}).get("bucket"),
                    "final_key": (resolved or {}).get("final_key"),
                    "cdn_url": (resolved or {}).get("cdn_url"),
                }
            )
        except Exception as exc:
            final_key = (resolved or {}).get("final_key", key)
            issues.append(f"{label} 上传失败: key={final_key}, err={exc}")

    result["s3_uploads"] = uploaded
    result["s3_upload_issues"] = issues
    result["s3_key_prefix"] = S3_KEY_PREFIX
    return uploaded, issues


def put_logo(competitor_id: str, logo_value: str) -> Dict[str, Any]:
    payload = {"competitor_id": str(competitor_id), "logo": logo_value}
    if DRY_RUN:
        return {"success": True, "code": 0, "body": {"msg": "dry_run"}, "payload": payload}

    try:
        resp = requests.post(PUT_LOGO_API_URL, json=payload, timeout=30)
        body = resp.json()
        success = resp.status_code == 200 and body.get("code") == 0
        return {
            "success": success,
            "code": body.get("code"),
            "body": body,
            "payload": payload,
            "status_code": resp.status_code,
        }
    except Exception as exc:
        return {"success": False, "code": None, "body": str(exc), "payload": payload}


def select_logo_value(upload_item: Dict[str, Any]) -> Optional[str]:
    if PUT_LOGO_VALUE_FIELD == "final_key":
        return upload_item.get("final_key") or upload_item.get("cdn_url") or upload_item.get("requested_key")
    if PUT_LOGO_VALUE_FIELD == "requested_key":
        return upload_item.get("requested_key") or upload_item.get("final_key") or upload_item.get("cdn_url")
    # default: cdn_url
    return upload_item.get("cdn_url") or upload_item.get("final_key") or upload_item.get("requested_key")


def cleanup_local_files(result: dict, sport_dir: Path, json_path: Path) -> List[str]:
    removed = []
    for side in ("home", "away"):
        logo_file = result.get(side, {}).get("logo_file")
        if not logo_file:
            continue
        p = sport_dir / logo_file
        if p.exists():
            p.unlink()
            removed.append(str(p))
    if json_path.exists():
        json_path.unlink()
        removed.append(str(json_path))
    try:
        if sport_dir.exists() and not any(sport_dir.iterdir()):
            sport_dir.rmdir()
            removed.append(str(sport_dir))
    except Exception:
        pass
    return removed


def process_task(task: Dict[str, Any], index: int, total: int) -> Dict[str, Any]:
    match_id = str(task.get("match_id") or "").strip()
    if not match_id:
        return {
            "match_id": None,
            "sport_id": None,
            "ok": False,
            "post_total": 0,
            "post_success": 0,
            "post_fail": 0,
            "errors": ["任务缺少match_id"],
        }

    match_url = f"{MATCH_PAGE_PREFIX}{match_id}"
    print(f"[{index}/{total}] Processing match_id={match_id}")

    result = {
        "match_url": match_url,
        "match_id": match_id,
        "sport_id": str(task.get("sport_id")) if task.get("sport_id") is not None else None,
        "sport_ids": [str(task["sport_id"])] if task.get("sport_id") is not None else [],
        "source_mode": "task_seed",
        "home": {
            "id": str(task.get("home_id")) if task.get("home_id") else None,
            "name": task.get("home_name"),
            "short_name": None,
            "abbr": None,
            "logo": None,
        },
        "away": {
            "id": str(task.get("away_id")) if task.get("away_id") else None,
            "name": task.get("away_name"),
            "short_name": None,
            "abbr": None,
            "logo": None,
        },
        "api_hits": [],
        "errors": [],
    }

    try:
        html = fetch_html(match_url, MOBILE_HEADERS)
        dom_result = parse_dom(html, match_url, match_id)
        for side in ("home", "away"):
            result[side]["name"] = result[side]["name"] or dom_result[side].get("name")
            result[side]["short_name"] = result[side]["short_name"] or dom_result[side].get("short_name")
            result[side]["logo"] = result[side]["logo"] or dom_result[side].get("logo")
        result["source_mode"] = "mobile_html"
    except Exception as exc:
        result["errors"].append(f"获取页面HTML失败: {exc}")

    result = enrich_with_playwright(result, match_url, match_id)

    if not result.get("sport_id") and result.get("sport_ids"):
        result["sport_id"] = str(result["sport_ids"][0])
    if not result.get("sport_id") and task.get("sport_id") is not None:
        result["sport_id"] = str(task["sport_id"])
        if str(task["sport_id"]) not in result["sport_ids"]:
            result["sport_ids"].append(str(task["sport_id"]))

    for side in ("home", "away"):
        if not result[side].get("id") and task.get(f"{side}_id"):
            result[side]["id"] = str(task[f"{side}_id"])
        if not result[side].get("name") and task.get(f"{side}_name"):
            result[side]["name"] = task[f"{side}_name"]

    if not result.get("sport_id"):
        result["errors"].append("未获取到 sport_id")
        return {
            "match_id": match_id,
            "sport_id": None,
            "ok": False,
            "post_total": 0,
            "post_success": 0,
            "post_fail": 0,
            "errors": result["errors"],
        }

    json_path, sport_dir, logo_issues = save_outputs(result)
    if logo_issues:
        result["errors"].extend(logo_issues)
        return {
            "match_id": match_id,
            "sport_id": result["sport_id"],
            "ok": False,
            "post_total": 0,
            "post_success": 0,
            "post_fail": 0,
            "errors": result["errors"],
        }

    uploads, upload_issues = upload_outputs_to_s3(result, sport_dir, json_path)
    if upload_issues:
        result["errors"].extend(upload_issues)
        json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return {
            "match_id": match_id,
            "sport_id": result["sport_id"],
            "ok": False,
            "post_total": 0,
            "post_success": 0,
            "post_fail": 0,
            "errors": result["errors"],
        }

    upload_by_label = {item["label"]: item for item in uploads}
    post_results = []
    for side in ("home", "away"):
        competitor_id = str(task.get(f"{side}_id") or result[side].get("id") or "").strip()
        upload_item = upload_by_label.get(f"{side}_logo")
        if not competitor_id:
            post_results.append({"side": side, "success": False, "error": f"{side} competitor_id缺失"})
            continue
        if not upload_item:
            post_results.append({"side": side, "success": False, "error": f"{side} 缺少上传结果"})
            continue
        logo_value = select_logo_value(upload_item)
        if not logo_value:
            post_results.append({"side": side, "success": False, "error": f"{side} logo值为空"})
            continue
        resp = put_logo(competitor_id, logo_value)
        post_results.append(
            {
                "side": side,
                "competitor_id": competitor_id,
                "logo": logo_value,
                "success": bool(resp.get("success")),
                "response": resp,
            }
        )

    post_total = len(post_results)
    post_success = sum(1 for x in post_results if x.get("success"))
    post_fail = post_total - post_success
    ok = post_total == 2 and post_fail == 0

    for x in post_results:
        if not x.get("success"):
            result["errors"].append(f"put-logo失败: {x.get('side')} | {x.get('error') or x.get('response')}")

    result["put_logo_results"] = post_results
    if ok and not DRY_RUN:
        result["cleanup_removed"] = cleanup_local_files(result, sport_dir, json_path)
    else:
        json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "match_id": match_id,
        "sport_id": result["sport_id"],
        "ok": ok,
        "post_total": post_total,
        "post_success": post_success,
        "post_fail": post_fail,
        "errors": result["errors"],
    }


def build_summary_text(
    tasks_total: int,
    batch_size: int,
    group_stats: List[Dict[str, Any]],
    records: List[Dict[str, Any]],
    started_at: datetime,
) -> str:
    finished_at = datetime.now()
    match_success = sum(1 for r in records if r.get("ok"))
    match_fail = len(records) - match_success
    post_total = sum(int(r.get("post_total") or 0) for r in records)
    post_success = sum(int(r.get("post_success") or 0) for r in records)
    post_fail = sum(int(r.get("post_fail") or 0) for r in records)

    lines = [
        "logo_spd批处理汇总",
        f"start: {started_at.strftime('%Y-%m-%d %H:%M:%S')}",
        f"end: {finished_at.strftime('%Y-%m-%d %H:%M:%S')}",
        f"dry_run: {DRY_RUN}",
        f"tasks_total: {tasks_total}",
        f"batch_size: {batch_size}",
        f"match_success: {match_success}",
        f"match_fail: {match_fail}",
        f"post_success: {post_success}/{post_total}",
        f"post_fail: {post_fail}",
        "groups:",
    ]

    for g in group_stats:
        lines.append(
            f"- group {g['group_index']}: matches={g['matches']}, "
            f"post_success={g['post_success']}/{g['post_total']}, post_fail={g['post_fail']}"
        )

    failed = [r for r in records if not r.get("ok")]
    if failed:
        lines.append("fail_samples:")
        for r in failed[:12]:
            reason = "; ".join((r.get("errors") or [])[:2]) or "unknown"
            lines.append(f"- match_id={r.get('match_id')}, reason={reason}")

    return "\n".join(lines)


def main():
    cycle = 0
    while True:
        cycle += 1
        started_at = datetime.now()
        print(f"\n=== cycle {cycle} start: {started_at.strftime('%Y-%m-%d %H:%M:%S')} ===")

        try:
            tasks = fetch_no_logo_tasks(size=BATCH_SIZE)
        except Exception as exc:
            text = (
                "logo_spd批处理汇总\n"
                f"cycle: {cycle}\n"
                f"fetch no-logo error: {exc}"
            )
            notified = send_lark_text(text)
            print(text)
            print(f"Lark summary notified: {notified}")
            if not CONTINUOUS_RUN:
                break
            print(f"sleep {POLL_INTERVAL_SECONDS}s then retry...")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        # 某些环境下接口 size 参数可能不稳定，兜底本地截断
        tasks = [x for x in tasks if str(x.get("match_id") or "").strip()]
        if MAX_MATCHES > 0:
            tasks = tasks[:MAX_MATCHES]

        if not tasks:
            idle_text = (
                "logo_spd轮询状态\n"
                f"cycle: {cycle}\n"
                "no match_id from no-logo api"
            )
            print(idle_text)
            if not CONTINUOUS_RUN:
                break
            print(f"sleep {POLL_INTERVAL_SECONDS}s then poll again...")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        groups = chunked(tasks, BATCH_SIZE)
        records = []
        group_stats = []
        total = len(tasks)
        current_idx = 0

        for gi, group in enumerate(groups, start=1):
            group_records = []
            for task in group:
                current_idx += 1
                rec = process_task(task, current_idx, total)
                records.append(rec)
                group_records.append(rec)

            g_post_total = sum(int(r.get("post_total") or 0) for r in group_records)
            g_post_success = sum(int(r.get("post_success") or 0) for r in group_records)
            g_post_fail = sum(int(r.get("post_fail") or 0) for r in group_records)
            group_stats.append(
                {
                    "group_index": gi,
                    "matches": len(group_records),
                    "post_total": g_post_total,
                    "post_success": g_post_success,
                    "post_fail": g_post_fail,
                }
            )

        summary_text = build_summary_text(
            tasks_total=len(tasks),
            batch_size=BATCH_SIZE,
            group_stats=group_stats,
            records=records,
            started_at=started_at,
        )
        summary_text = f"cycle: {cycle}\n" + summary_text
        notified = send_lark_text(summary_text)
        print(summary_text)
        print(f"\nLark summary notified: {notified}")

        if not CONTINUOUS_RUN:
            break
        # 有任务时每轮之间短暂停顿，避免打满上游接口
        print(f"tasks processed, sleep {PROCESSING_LOOP_DELAY_SECONDS}s then next cycle...")
        time.sleep(PROCESSING_LOOP_DELAY_SECONDS)


if __name__ == "__main__":
    main()
