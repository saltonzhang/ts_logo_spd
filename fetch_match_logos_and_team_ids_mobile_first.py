import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

MATCH_URL = "https://match-pc.helix.city/en/matches/18625242"
BASE_URL = "https://match-pc.helix.city/"
MATCH_ID = MATCH_URL.rstrip("/").split("/")[-1]
OUTPUT_DIR = Path(__file__).resolve().parent
LARK_WEBHOOK_URL = os.getenv(
    "LARK_WEBHOOK_URL",
    "https://open.larksuite.com/open-apis/bot/v2/hook/f4af0cbe-27e9-445a-8a02-960522d4905a",
)
ATHENA_BASE_URL = os.getenv("ATHENA_BASE_URL", "https://xp-athena-test1-api.helix.city")
ATHENA_ACCESS_KEY = os.getenv("ATHENA_ACCESS_KEY", "8KPsuFGFyrfz")
ATHENA_SECRET_KEY = os.getenv("ATHENA_SECRET_KEY", "C0JIsSNKNBJYFOuG6Evu")
ATHENA_REGION_NAME = os.getenv("ATHENA_REGION_NAME", "us-west-2")
ATHENA_TOKEN_CACHE_FILE = os.getenv(
    "ATHENA_TOKEN_CACHE_FILE",
    str(Path(__file__).resolve().parent / "athena_token_cache.json"),
)
S3_KEY_PREFIX = os.getenv("S3_KEY_PREFIX", "images").strip("/")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "")
UPLOAD_JSON_TO_S3 = os.getenv("UPLOAD_JSON_TO_S3", "false").lower() in {"1", "true", "yes"}

MOBILE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 "
        "Mobile/15E148 Safari/604.1"
    ),
    "Referer": BASE_URL,
}
_ATHENA_CLIENT = None


def abs_url(src: Optional[str]) -> Optional[str]:
    if not src:
        return None
    return urljoin(BASE_URL, src)


def fetch_html(url: str, headers: dict) -> str:
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.text


def send_lark_text(text: str) -> bool:
    if not LARK_WEBHOOK_URL:
        return False

    payload = {
        "msg_type": "text",
        "content": {"text": text},
    }
    try:
        resp = requests.post(LARK_WEBHOOK_URL, json=payload, timeout=10)
        resp.raise_for_status()
        body = resp.json()
        # 飞书机器人成功通常是 {"StatusCode":0,"StatusMessage":"success"}
        if body.get("StatusCode") == 0:
            return True
    except Exception:
        return False
    return False


def send_logo_download_alert(result: dict, issues: list) -> bool:
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    issue_lines = "\n".join([f"- {item}" for item in issues])
    text = (
        "logo_spd告警\n"
        f"time: {now_text}\n"
        f"match_id: {result.get('match_id')}\n"
        f"sport_id: {result.get('sport_id')}\n"
        f"match_url: {result.get('match_url')}\n"
        "reason: logo下载失败\n"
        f"details:\n{issue_lines}"
    )
    return send_lark_text(text)


def send_s3_upload_alert(result: dict, issues: list) -> bool:
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    issue_lines = "\n".join([f"- {item}" for item in issues])
    text = (
        "logo_spd告警\n"
        f"time: {now_text}\n"
        f"match_id: {result.get('match_id')}\n"
        f"sport_id: {result.get('sport_id')}\n"
        f"match_url: {result.get('match_url')}\n"
        "reason: S3上传失败\n"
        f"details:\n{issue_lines}"
    )
    return send_lark_text(text)


def send_statscore_widget_alert(result: dict, error_info: dict) -> bool:
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    urls = error_info.get("urls") or []
    url_lines = "\n".join([f"- {u}" for u in urls]) if urls else "-"
    text = (
        "logo_spd告警\n"
        f"time: {now_text}\n"
        f"match_id: {result.get('match_id')}\n"
        f"sport_id: {result.get('sport_id')}\n"
        f"match_url: {result.get('match_url')}\n"
        "reason: Statscore接口eventId异常\n"
        f"message: {error_info.get('message')}\n"
        f"apis:\n{url_lines}"
    )
    return send_lark_text(text)


def send_missing_sport_id_alert(result: dict) -> bool:
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    hit_lines = "\n".join([f"- {x.get('url')}" for x in result.get("api_hits", [])]) or "-"
    text = (
        "logo_spd告警\n"
        f"time: {now_text}\n"
        f"match_id: {result.get('match_id')}\n"
        f"match_url: {result.get('match_url')}\n"
        "reason: 未获取到 sport_id\n"
        f"sport_ids: {result.get('sport_ids')}\n"
        f"source_mode: {result.get('source_mode')}\n"
        f"api_hits:\n{hit_lines}"
    )
    return send_lark_text(text)


def ordered_unique(values):
    seen = set()
    out = []
    for v in values:
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def parse_dom(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text("\n", strip=True)

    home_name = None
    away_name = None
    title_text = soup.title.get_text(" ", strip=True) if soup.title else ""
    m = re.search(r"^(.*?)\s+vs\s+(.*?)\s+-", title_text, re.IGNORECASE)
    if m:
        home_name = m.group(1).strip()
        away_name = m.group(2).strip()

    # 优先只取看起来像队徽的 png，避开站点底部 seal/license 图片
    img_srcs = [img.get("src") for img in soup.find_all("img") if img.get("src")]
    png_srcs = [src for src in img_srcs if src.lower().endswith(".png")]

    team_pngs = []
    for src in png_srcs:
        lower = src.lower()
        if any(x in lower for x in ["seal", "stamp", "license", "anjouan", "eighteen-plus"]):
            continue
        team_pngs.append(src)
    team_pngs = ordered_unique(team_pngs)

    short_names = []
    for candidate in ["Jamtland", "Boras"]:
        if re.search(rf"\b{re.escape(candidate)}\b", page_text, re.IGNORECASE):
            short_names.append(candidate)

    result = {
        "match_url": MATCH_URL,
        "match_id": MATCH_ID,
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
    }
    return result


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
                f"{side}team", f"{side}_team", f"{side}competitor",
                f"{side}_competitor", side
            ):
                if key in lower_to_real:
                    real_key = lower_to_real[key]
                    val = obj[real_key]
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
                f"{side}TeamId", f"{side}_team_id", f"{side}CompetitorId", f"{side}_competitor_id"
            ):
                if id_key in obj and not found[side]["id"]:
                    found[side]["id"] = obj[id_key]

            for name_key in (
                f"{side}TeamName", f"{side}_team_name", f"{side}Name", f"{side}_name"
            ):
                if name_key in obj and not found[side]["name"]:
                    found[side]["name"] = obj[name_key]

            for logo_key in (
                f"{side}TeamLogo", f"{side}_team_logo", f"{side}Logo", f"{side}_logo"
            ):
                if logo_key in obj and not found[side]["logo"]:
                    found[side]["logo"] = obj[logo_key]

        for v in obj.values():
            deep_find_team_info(v, found)

    elif isinstance(obj, list):
        for item in obj:
            deep_find_team_info(item, found)

    return found


def parse_match_api_error(api_url: str, payload) -> Optional[dict]:
    if "/v1/match/" not in api_url.lower():
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("data") is not None:
        return None
    message = str(payload.get("message") or "")
    if "MatchHandler.MatchListRowHandler error" not in message:
        return None
    return {
        "url": api_url,
        "code": payload.get("code"),
        "message": message,
    }


def parse_statscore_widget_eventid_error(api_url: str, payload) -> Optional[dict]:
    if "widgets.statscore.com/api/ssr/render-widget" not in api_url.lower():
        return None
    if not isinstance(payload, dict):
        return None
    message = str(payload.get("message") or "")
    if 'inputData parameter "eventId" is required' not in message:
        return None
    return {
        "url": api_url,
        "message": message,
        "details": payload.get("details"),
    }


def enrich_with_playwright(result: dict) -> dict:
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return result

    keywords = [
        "match", "matches", "event", "fixture", "team", "competitor",
        "prematch", "summary", "stat", "score", "breadcrumb"
    ]

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)

            device_profiles = [("mobile", p.devices["iPhone 13"])]
            notified_match_api_error = False
            notified_widget_api_error = False

            for mode, context_kwargs in device_profiles:
                context = browser.new_context(**context_kwargs)
                page = context.new_page()
                captured = []

                def on_response(response):
                    if response.request.resource_type not in {"xhr", "fetch"}:
                        return
                    url_l = response.url.lower()
                    is_match_related = MATCH_ID in url_l or f"event_id={MATCH_ID}" in url_l
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
                try:
                    # 某些站点长连接较多，networkidle 容易超时；先等 DOM 就绪再补短等待
                    page.goto(MATCH_URL, wait_until="domcontentloaded", timeout=45000)
                    page.wait_for_timeout(4000)
                except Exception:
                    context.close()
                    continue

                img_srcs = page.eval_on_selector_all(
                    "img",
                    "els => els.map(e => e.getAttribute('src')).filter(Boolean)"
                )
                pngs = [x for x in img_srcs if x.lower().endswith('.png')]
                pngs = [x for x in pngs if not re.search(r'seal|stamp|license|anjouan|eighteen-plus', x, re.I)]

                if pngs and not result["home"].get("logo"):
                    result["home"]["logo"] = abs_url(pngs[0])
                if len(pngs) > 1 and not result["away"].get("logo"):
                    result["away"]["logo"] = abs_url(pngs[1])

                for api_url, data in captured:
                    widget_api_error = parse_statscore_widget_eventid_error(api_url, data)
                    if widget_api_error:
                        existing = result.get("statscore_widget_error")
                        if not existing:
                            existing = {
                                "message": widget_api_error["message"],
                                "urls": [],
                            }
                            result["statscore_widget_error"] = existing
                        if widget_api_error["url"] not in existing["urls"]:
                            existing["urls"].append(widget_api_error["url"])
                        if not notified_widget_api_error:
                            notified_widget_api_error = send_statscore_widget_alert(result, existing)
                            result["statscore_widget_error_notified"] = notified_widget_api_error

                    match_api_error = parse_match_api_error(api_url, data)
                    if match_api_error:
                        result["match_api_error"] = match_api_error
                        if not notified_match_api_error:
                            now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            text = (
                                "logo_spd告警\n"
                                f"time: {now_text}\n"
                                f"match_id: {MATCH_ID}\n"
                                f"api: {match_api_error['url']}\n"
                                f"message: {match_api_error['message']}\n"
                                f"code: {match_api_error.get('code')}"
                            )
                            notified_match_api_error = send_lark_text(text)
                            result["match_api_error_notified"] = notified_match_api_error

                    info = deep_find_team_info(data)
                    sport_ids = deep_find_sport_ids(data)
                    for sport_id in sport_ids:
                        if sport_id not in result["sport_ids"]:
                            result["sport_ids"].append(sport_id)
                    if not result.get("sport_id") and result["sport_ids"]:
                        result["sport_id"] = result["sport_ids"][0]

                    useful = any([
                        info["home"]["id"], info["away"]["id"],
                        info["home"]["name"], info["away"]["name"],
                        info["home"]["logo"], info["away"]["logo"],
                    ])
                    if not useful:
                        continue

                    result["api_hits"].append({"mode": mode, "url": api_url})
                    for side in ("home", "away"):
                        result[side]["id"] = result[side]["id"] or info[side]["id"]
                        result[side]["name"] = result[side]["name"] or info[side]["name"]
                        result[side]["abbr"] = result[side]["abbr"] or info[side]["abbr"]
                        result[side]["logo"] = result[side]["logo"] or abs_url(info[side]["logo"])

                has_enough = all([
                    result["home"].get("logo"),
                    result["away"].get("logo"),
                    result["home"].get("id") or result["away"].get("id"),
                    result.get("sport_id") or result.get("sport_ids"),
                ])
                result["source_mode"] = mode
                context.close()

                if has_enough:
                    break

            browser.close()
    except Exception:
        return result

    return result


def download_file(url: str, save_path: Path, headers: dict):
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    save_path.write_bytes(resp.content)


def sanitize_filename(value: str) -> str:
    value = value.strip()
    value = re.sub(r'[\\/:*?"<>|]+', "_", value)
    value = re.sub(r"\s+", " ", value)
    return value or "unknown"


def build_logo_filename(team: dict, default_name: str) -> str:
    team_id = sanitize_filename(str(team.get("id") or "").strip())
    if not team_id:
        team_id = f"{sanitize_filename(default_name)}_unknown"
    return f"{team_id}.png"


def save_outputs(result: dict):
    output_root = OUTPUT_DIR
    output_root.mkdir(parents=True, exist_ok=True)
    sport_id = str(result.get("sport_id") or (result.get("sport_ids") or [None])[0] or "unknown_sport")
    sport_dir = output_root / sanitize_filename(sport_id)
    sport_dir.mkdir(parents=True, exist_ok=True)

    home_filename = None
    away_filename = None
    logo_issues = []
    if result["home"].get("logo"):
        home_filename = build_logo_filename(result["home"], "home")
        try:
            download_file(result["home"]["logo"], sport_dir / home_filename, MOBILE_HEADERS)
        except Exception as exc:
            logo_issues.append(f"home logo 下载失败: {exc}")
    else:
        logo_issues.append("home logo 链接缺失")

    if result["away"].get("logo"):
        away_filename = build_logo_filename(result["away"], "away")
        try:
            download_file(result["away"]["logo"], sport_dir / away_filename, MOBILE_HEADERS)
        except Exception as exc:
            logo_issues.append(f"away logo 下载失败: {exc}")
    else:
        logo_issues.append("away logo 链接缺失")

    # 不再输出/使用 abbr，避免 DOM 噪声缩写混入结果
    result["home"]["abbr"] = None
    result["away"]["abbr"] = None
    result["home"]["logo_file"] = home_filename
    result["away"]["logo_file"] = away_filename
    result["output_dir"] = str(sport_dir)
    result["logo_download_issues"] = logo_issues

    json_path = sport_dir / f"match_{result.get('match_id', MATCH_ID)}.json"
    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    notified = False
    if logo_issues:
        notified = send_logo_download_alert(result, logo_issues)
    return json_path, sport_dir, logo_issues, notified


def build_s3_key(*parts: str) -> str:
    cleaned = [str(p).strip("/") for p in parts if str(p).strip("/")]
    return "/".join(cleaned)


def get_athena_client():
    global _ATHENA_CLIENT
    if _ATHENA_CLIENT is None:
        from athena_sdk import AthenaClient

        _ATHENA_CLIENT = AthenaClient(
            ATHENA_BASE_URL,
            access_key=ATHENA_ACCESS_KEY,
            secret_key=ATHENA_SECRET_KEY,
            region_name=ATHENA_REGION_NAME,
            token_cache_file=ATHENA_TOKEN_CACHE_FILE,
        )
    return _ATHENA_CLIENT


def upload_outputs_to_s3(result: dict, sport_dir: Path, json_path: Path):
    client = get_athena_client()

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
            resolved = client.resolve_upload_target(
                Key=key,
                Bucket=S3_BUCKET_NAME,
            )
            client.upload_file(
                Filename=str(file_path),
                Bucket=S3_BUCKET_NAME,
                Key=key,
            )
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

    notified = False
    if issues:
        notified = send_s3_upload_alert(result, issues)
    return uploaded, issues, notified


def main():
    # 只按移动端 UA 抓取
    result = None
    try:
        html = fetch_html(MATCH_URL, MOBILE_HEADERS)
        result = parse_dom(html)
        if result["home"].get("logo") and result["away"].get("logo"):
            result["source_mode"] = "mobile_html"
    except Exception as exc:
        raise RuntimeError(f"无法获取移动端比赛页面 HTML: {exc}")

    if not result["home"].get("id") or not result["away"].get("id"):
        result = enrich_with_playwright(result)

    if not result.get("sport_id") and result.get("sport_ids"):
        result["sport_id"] = result["sport_ids"][0]
    if not result.get("sport_id"):
        missing_sport_notified = False
        if not result.get("match_api_error_notified"):
            missing_sport_notified = send_missing_sport_id_alert(result)

        if result.get("match_api_error"):
            notified = result.get("match_api_error_notified")
            if missing_sport_notified and not notified:
                notified_text = "已发送飞书通知"
            else:
                notified_text = "已发送飞书通知" if notified else "飞书通知发送失败"
            message = result["match_api_error"].get("message")
            raise RuntimeError(
                f"未获取到 sport_id，已停止写入，避免生成 unknown_sport 目录。"
                f" 检测到接口异常: {message}（{notified_text}）"
            )
        notified_text = "已发送飞书通知" if missing_sport_notified else "飞书通知发送失败"
        raise RuntimeError(
            f"未获取到 sport_id，已停止写入，避免生成 unknown_sport 目录。"
            f"（{notified_text}）"
        )

    json_path, sport_dir, logo_issues, logo_notified = save_outputs(result)
    if logo_issues:
        notified_text = "已发送飞书通知" if logo_notified else "飞书通知发送失败"
        raise RuntimeError(f"logo 下载失败: {'; '.join(logo_issues)}（{notified_text}）")

    _, s3_issues, s3_notified = upload_outputs_to_s3(result, sport_dir, json_path)
    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    if s3_issues:
        notified_text = "已发送飞书通知" if s3_notified else "飞书通知发送失败"
        raise RuntimeError(f"S3 上传失败: {'; '.join(s3_issues)}（{notified_text}）")

    print(json.dumps(result, ensure_ascii=False, indent=2))
    print(f"\nSaved to: {sport_dir}")
    print(f"Metadata JSON: {json_path}")


if __name__ == "__main__":
    main()
