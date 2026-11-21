#!/usr/bin/env python3
"""News Alpha Radar — 경량 자체 관리 웹 대시보드 (외부 패키지 0개, Python 3.11+ 표준 라이브러리 전용).

우분투 상시 서버(또는 로컬 PC)에서 `radar.py --daemon`과 함께 실행하여:
- 실시간 데몬 생존 여부(Heartbeat), 오늘 알림 수, PT 기준 Gemini RPD 소모율 모니터링
- 촉매 점수·일일 한도·쿨다운·PR-3 모드(shadow/enforce)·RSS 피드 ON/OFF 실시간 변경
- 원클릭 즉시 스캔, 일시정지/재개, 텔레그램 테스트 발송, 전체 RSS 피드 실시간 점검(Probe)
- 최근 발송 알림(네이버 금융 바로가기), Deferred 재시도 큐, trace.jsonl 타임라인 조회
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import hmac
import io
import json
import logging
import os
import subprocess
import sys
import threading
import time
import urllib.parse
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import radar
import telegram_listener

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("news_alpha_dashboard")

REPO_DIR = Path(__file__).resolve().parent
DEFAULT_DASHBOARD_HOST = "0.0.0.0"
DEFAULT_DASHBOARD_PORT = 8787
MAX_REQUEST_BODY_BYTES = 64 * 1024


def _effective_config(data_dir: Path) -> dict[str, Any]:
    raw = radar.load_runtime_config(data_dir)
    limits = radar.resolve_alert_limits(raw, daemon_mode=True)
    default_feeds = [
        f["name"]
        for f in (radar.resolve_active_feeds(None, daemon_mode=True) or radar.RSS_FEEDS)
    ]
    enabled_raw = raw.get("enabled_feeds")
    if isinstance(enabled_raw, list) and enabled_raw:
        valid_names = {f["name"] for f in radar.ALL_FEEDS}
        enabled_feeds = [str(n) for n in enabled_raw if str(n) in valid_names] or default_feeds
    else:
        enabled_feeds = default_feeds

    dedup_mode = str(raw.get("dedup_mode") or os.getenv("DEDUP_MODE") or "enforce").strip().lower()
    if dedup_mode not in ("shadow", "enforce"):
        dedup_mode = "enforce"
    trigger_mode = str(raw.get("trigger_mode") or os.getenv("TRIGGER_MODE") or "enforce").strip().lower()
    if trigger_mode not in ("shadow", "enforce"):
        trigger_mode = "enforce"

    if raw.get("instant_flash_alert") is not None:
        instant_flash = bool(raw["instant_flash_alert"])
    else:
        fe = (os.getenv("INSTANT_FLASH_ALERT") or "true").strip().lower()
        instant_flash = fe not in ("0", "false", "no", "off")

    if isinstance(raw.get("telegram_source_channels"), list):
        tg_channels = telegram_listener.parse_channel_list(raw["telegram_source_channels"])
    else:
        tg_channels = telegram_listener.parse_channel_list(os.getenv("TELEGRAM_SOURCE_CHANNELS", ""))

    return {
        "paused": bool(raw.get("paused", False)),
        "instant_flash_alert": instant_flash,
        "telegram_source_channels": tg_channels,
        "min_catalyst_score": float(
            raw["min_catalyst_score"]
            if raw.get("min_catalyst_score") is not None
            else radar._env_float("MIN_CATALYST_SCORE", 7.5)
        ),
        "max_market_alerts": int(limits["max_market_alerts"]),
        "max_offhours_alerts": int(limits["max_offhours_alerts"]),
        "max_daily_alerts": int(limits["max_daily_alerts"]),
        "offhours_digest_enabled": bool(limits["offhours_digest_enabled"]),
        "offhours_digest_time": str(limits["offhours_digest_time"]),
        "stock_dedup_hours": max(
            0,
            int(
                raw["stock_dedup_hours"]
                if raw.get("stock_dedup_hours") is not None
                else radar._env_int("STOCK_DEDUP_HOURS", radar.DEFAULT_STOCK_DEDUP_HOURS)
            ),
        ),
        "max_daily_per_sector": max(
            0,
            int(
                raw["max_daily_per_sector"]
                if raw.get("max_daily_per_sector") is not None
                else radar._env_int("MAX_DAILY_PER_SECTOR", radar.DEFAULT_MAX_DAILY_PER_SECTOR)
            ),
        ),
        "max_bullish_prepriced_pct": max(
            0.0,
            float(
                raw["max_bullish_prepriced_pct"]
                if raw.get("max_bullish_prepriced_pct") is not None
                else radar._env_float("MAX_BULLISH_PREPRICED_PCT", radar.DEFAULT_MAX_BULLISH_PREPRICED_PCT)
            ),
        ),
        "max_alerts_per_window": int(
            raw["max_alerts_per_window"]
            if raw.get("max_alerts_per_window") is not None
            else radar._env_int("MAX_ALERTS_PER_RUN", 2)
        ),
        "theme_cooldown_hours": int(
            raw["theme_cooldown_hours"]
            if raw.get("theme_cooldown_hours") is not None
            else radar._env_int("THEME_COOLDOWN_HOURS", 12)
        ),
        "dedup_mode": dedup_mode,
        "trigger_mode": trigger_mode,
        "stage1_daily_rpd_limit": int(
            raw["stage1_daily_rpd_limit"]
            if raw.get("stage1_daily_rpd_limit") is not None
            else radar._env_int("STAGE1_DAILY_RPD_LIMIT", radar.DEFAULT_STAGE1_DAILY_RPD_LIMIT)
        ),
        "daemon_interval_market_sec": int(
            raw["daemon_interval_market_sec"]
            if raw.get("daemon_interval_market_sec") is not None
            else radar._env_int("DAEMON_INTERVAL_MARKET_SEC", radar.DEFAULT_DAEMON_INTERVAL_MARKET_SEC)
        ),
        "daemon_interval_extended_sec": int(
            raw["daemon_interval_extended_sec"]
            if raw.get("daemon_interval_extended_sec") is not None
            else radar._env_int("DAEMON_INTERVAL_EXTENDED_SEC", radar.DEFAULT_DAEMON_INTERVAL_EXTENDED_SEC)
        ),
        "daemon_interval_offhours_sec": int(
            raw["daemon_interval_offhours_sec"]
            if raw.get("daemon_interval_offhours_sec") is not None
            else radar._env_int("DAEMON_INTERVAL_OFFHOURS_SEC", radar.DEFAULT_DAEMON_INTERVAL_OFFHOURS_SEC)
        ),
        "enabled_feeds": enabled_feeds,
        "updated_at": raw.get("updated_at"),
    }


def _read_status(data_dir: Path, now: datetime) -> dict[str, Any]:
    path = data_dir / "status.json"
    if not path.exists():
        return {"state": "offline", "alive": False}
    try:
        st = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(st, dict):
            return {"state": "offline", "alive": False}
    except Exception:
        return {"state": "offline", "alive": False}

    hb = radar.parse_iso_kst(st.get("heartbeat_at"))
    poll_sec = int(st.get("poll_interval_sec") or 60)
    stale_after = max(poll_sec * 2 + 45, 150)
    age_sec = int((now - hb).total_seconds()) if hb is not None else 999999
    alive = st.get("state") in ("running", "paused", "error") and age_sec <= stale_after
    st["heartbeat_age_sec"] = age_sec if hb is not None else None
    st["alive"] = alive
    if not alive and st.get("state") != "stopped":
        st["state"] = "offline"
    return st


def _read_listener_status(data_dir: Path, now: datetime) -> dict[str, Any]:
    path = data_dir / "listener_status.json"
    if not path.exists():
        return {"state": "offline", "alive": False, "channels": [], "received_count": 0}
    try:
        st = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(st, dict):
            return {"state": "offline", "alive": False, "channels": [], "received_count": 0}
    except Exception:
        return {"state": "offline", "alive": False, "channels": [], "received_count": 0}

    hb = radar.parse_iso_kst(st.get("heartbeat_at"))
    age_sec = int((now - hb).total_seconds()) if hb is not None else 999999
    alive = st.get("state") in ("running", "waiting_channels") and age_sec <= 90
    st["heartbeat_age_sec"] = age_sec if hb is not None else None
    st["alive"] = alive
    if not alive and st.get("state") not in ("stopped", "needs_login"):
        st["state"] = "offline"
    return st


def _read_git_version(repo_dir: Path = REPO_DIR) -> dict[str, Any]:
    try:
        proc = subprocess.run(
            ["git", "log", "-1", "--format=%h|%s|%ci"],
            cwd=str(repo_dir),
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
        if proc.returncode != 0 or not proc.stdout.strip():
            return {}
        parts = proc.stdout.strip().split("|", 2)
        return {
            "commit": parts[0] if len(parts) > 0 else "",
            "subject": parts[1][:80] if len(parts) > 1 else "",
            "date": parts[2] if len(parts) > 2 else "",
        }
    except Exception:
        return {}


def run_self_update(repo_dir: Path = REPO_DIR) -> dict[str, Any]:
    """deploy/ubuntu/update.sh --defer-dashboard 실행 결과를 구조화된 JSON으로 반환."""
    script = repo_dir / "deploy" / "ubuntu" / "update.sh"
    if not script.exists():
        return {"ok": False, "status": "MISSING_SCRIPT", "message": "update.sh 스크립트를 찾을 수 없습니다."}
    env = dict(os.environ)
    if sys.executable:
        env["PYTHON_BIN"] = sys.executable
    try:
        proc = subprocess.run(
            ["bash", str(script), "--defer-dashboard"],
            cwd=str(repo_dir),
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
    except Exception as exc:
        return {"ok": False, "status": "ERROR", "message": str(exc), "error": str(exc)}

    kv: dict[str, str] = {}
    for line in (proc.stdout or "").splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            kv[k.strip()] = v.strip()

    status = kv.get("STATUS") or ("OK" if proc.returncode == 0 else "ERROR")
    if status == "UP_TO_DATE":
        msg = f"이미 최신 버전({kv.get('COMMIT', '')})입니다."
    elif status == "UPDATED":
        msg = f"업데이트 완료 ({kv.get('BEFORE', '')} → {kv.get('AFTER', '')}). 서비스가 1초 뒤 자동 재시작됩니다."
    elif status == "DIRTY_SKIP":
        msg = "우분투 로컬 파일에 미커밋 변경이 있어 업데이트를 건너뛰었습니다."
    elif status == "NON_MAIN_SKIP":
        msg = "현재 브랜치가 main이 아니어서 업데이트를 건너뛰었습니다."
    elif status == "ROLLBACK":
        detail = kv.get("TEST_ERROR", "").strip()
        msg = (
            f"테스트 실패로 이전 버전으로 자동 롤백했습니다. ({detail})"
            if detail
            else "테스트 실패로 이전 버전으로 자동 롤백했습니다."
        )
    elif status == "BUSY":
        msg = "이미 다른 업데이트 작업이 진행 중입니다."
    else:
        msg = (proc.stderr or proc.stdout or f"exit={proc.returncode}").strip()[:200]

    ok = proc.returncode == 0 and status not in ("ROLLBACK", "ERROR")
    out: dict[str, Any] = {
        "ok": ok,
        "status": status,
        "updated": status == "UPDATED",
        "before": kv.get("BEFORE") or kv.get("COMMIT"),
        "after": kv.get("AFTER") or kv.get("COMMIT"),
        "message": msg,
    }
    if kv.get("PYTHON_BIN"):
        out["python_bin"] = kv["PYTHON_BIN"]
    if not ok:
        out["error"] = msg
    return out


def build_overview_payload(data_dir: Path, now: datetime | None = None) -> dict[str, Any]:
    ref = radar.StateStore._now_kst(now)
    cfg = _effective_config(data_dir)
    state = radar.StateStore(data_dir / "state.json")
    daemon_status = _read_status(data_dir, ref)
    listener_status = _read_listener_status(data_dir, ref)
    poll_sec, window_label = radar.resolve_daemon_interval_sec(now=ref, runtime_cfg=cfg)

    pt_today = radar.pt_date_str(ref)
    ai_by_model = dict((state.daily_ai_calls.get(pt_today) or {}))
    screen_models = [
        m.strip()
        for m in radar._env_str("GEMINI_SCREEN_MODELS", radar.DEFAULT_SCREEN_MODELS).split(",")
        if m.strip()
    ]
    primary_screen = screen_models[0] if screen_models else "gemini-3.5-flash-lite"
    primary_used = state.ai_requests_today(primary_screen, now=ref)
    rpd_limit = max(10, cfg["stage1_daily_rpd_limit"])
    ratio = primary_used / rpd_limit
    if ratio >= 0.90:
        rpd_tier = "h_only"
    elif ratio >= 0.80:
        rpd_tier = "slowdown"
    elif ratio >= 0.70:
        rpd_tier = "caution"
    else:
        rpd_tier = "normal"

    exhausted_active = sorted(m for m, d in state.exhausted_models.items() if d == pt_today)
    enabled_set = set(cfg["enabled_feeds"])
    feeds_info = []
    for f in radar.ALL_FEEDS:
        name = str(f["name"])
        fs = dict(state.feed_stats.get(name) or {})
        feeds_info.append({
            "name": name,
            "category": str(f["category"]),
            "url": str(f["url"]),
            "group": str(f.get("group") or "direct"),
            "is_candidate": f in radar.PROBE_CANDIDATE_FEEDS,
            "enabled": name in enabled_set,
            "ok": int(fs.get("ok", 0)),
            "fail": int(fs.get("fail", 0)),
            "not_modified_304": int(fs.get("not_modified_304", 0)),
            "last_status": fs.get("last_status"),
            "last_fetched_at": fs.get("last_fetched_at"),
            "last_min_pub_lag_sec": fs.get("last_min_pub_lag_sec"),
            "last_error": fs.get("last_error"),
        })

    recent_alerts = sorted(
        list(state.recent_alerts),
        key=lambda a: str(a.get("ts") or ""),
        reverse=True,
    )[:40]

    deferred_summary = [
        {
            "kind": d.get("kind"),
            "theme_key": d.get("theme_key"),
            "headline": (d.get("cluster") or {}).get("headline") or d.get("deep_title") or "",
            "catalyst_score": d.get("catalyst_score"),
            "retries": d.get("retries", 0),
            "reason": d.get("reason"),
            "created_at": d.get("created_at"),
            "expires_at": d.get("expires_at"),
        }
        for d in state.deferred
    ]

    overnight_summary = [
        {
            "theme_key": q.get("theme_key"),
            "sector": q.get("sector"),
            "headline": (q.get("cluster") or {}).get("headline") or "",
            "catalyst_score": q.get("catalyst_score"),
            "created_at": q.get("created_at"),
        }
        for q in state.overnight_queue
    ]

    perf_records = radar.PerformanceTracker(data_dir / "performance.jsonl").load_records()
    perf_summary = radar.summarize_performance(perf_records, recent_limit=50)
    today_str = ref.strftime("%Y-%m-%d")

    return {
        "now_kst": ref.isoformat(),
        "pt_date": pt_today,
        "git": _read_git_version(),
        "daemon": daemon_status,
        "listener": listener_status,
        "config": cfg,
        "metrics": {
            "sent_today": state.sent_today(now=ref),
            "sent_market_today": state.sent_market_today(now=ref),
            "sent_offhours_today": state.sent_offhours_today(now=ref),
            "max_market_alerts": cfg["max_market_alerts"],
            "max_offhours_alerts": cfg["max_offhours_alerts"],
            "max_daily_alerts": cfg["max_daily_alerts"],
            "overnight_queue_count": len(state.overnight_queue),
            "last_digest_date": state.last_digest_date,
            "daily_sector_counts_today": dict(state.daily_sector_counts.get(today_str) or {}),
            "ai_requests_pt_total": state.ai_requests_today(now=ref),
            "ai_requests_by_model": ai_by_model,
            "primary_screen_model": primary_screen,
            "primary_screen_used": primary_used,
            "rpd_limit": rpd_limit,
            "rpd_ratio": round(ratio, 3),
            "rpd_tier": rpd_tier,
            "exhausted_models": exhausted_active,
            "pending_count": len(state.pending),
            "deferred_count": len(state.deferred),
            "evaluated_count": len(state.evaluated),
            "last_stage1_at": state.last_stage1_at,
            "window_label": window_label,
            "poll_interval_sec": poll_sec,
        },
        "recent_alerts": recent_alerts,
        "deferred": deferred_summary,
        "overnight_queue": overnight_summary,
        "performance": perf_summary,
        "feeds": feeds_info,
    }


def read_recent_traces(data_dir: Path, *, limit: int = 80, event_type: str = "all") -> list[dict[str, Any]]:
    path = data_dir / "trace.jsonl"
    if not path.exists():
        return []
    lim = max(1, min(300, int(limit)))
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    out: list[dict[str, Any]] = []
    for raw in reversed(lines):
        s = raw.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception:
            continue
        if event_type and event_type != "all" and obj.get("type") != event_type:
            continue
        out.append(obj)
        if len(out) >= lim:
            break
    return out


def validate_and_merge_config(data_dir: Path, patch: dict[str, Any]) -> dict[str, Any]:
    cur = _effective_config(data_dir)
    if "paused" in patch:
        cur["paused"] = bool(patch["paused"])
    if "instant_flash_alert" in patch:
        cur["instant_flash_alert"] = bool(patch["instant_flash_alert"])
    if "telegram_source_channels" in patch:
        raw_ch = patch["telegram_source_channels"]
        if not isinstance(raw_ch, (list, str)):
            raise ValueError("telegram_source_channels는 문자열 또는 배열이어야 합니다.")
        cur["telegram_source_channels"] = telegram_listener.parse_channel_list(raw_ch)
    if "min_catalyst_score" in patch:
        val = float(patch["min_catalyst_score"])
        cur["min_catalyst_score"] = round(max(1.0, min(10.0, val)), 1)
    split_updated = False
    if "max_market_alerts" in patch:
        cur["max_market_alerts"] = max(0, min(50, int(patch["max_market_alerts"])))
        split_updated = True
    if "max_offhours_alerts" in patch:
        cur["max_offhours_alerts"] = max(0, min(30, int(patch["max_offhours_alerts"])))
        split_updated = True
    if "max_daily_alerts" in patch:
        cur["max_daily_alerts"] = max(1, min(80, int(patch["max_daily_alerts"])))
    elif split_updated:
        cur["max_daily_alerts"] = max(1, cur["max_market_alerts"] + cur["max_offhours_alerts"])
    if "offhours_digest_enabled" in patch:
        cur["offhours_digest_enabled"] = bool(patch["offhours_digest_enabled"])
    if "offhours_digest_time" in patch:
        dh, dm = radar._parse_hhmm(str(patch["offhours_digest_time"]), default=(7, 50))
        cur["offhours_digest_time"] = f"{dh:02d}:{dm:02d}"
    if "stock_dedup_hours" in patch:
        cur["stock_dedup_hours"] = max(0, min(168, int(patch["stock_dedup_hours"])))
    if "max_daily_per_sector" in patch:
        cur["max_daily_per_sector"] = max(0, min(20, int(patch["max_daily_per_sector"])))
    if "max_bullish_prepriced_pct" in patch:
        cur["max_bullish_prepriced_pct"] = round(max(0.0, min(30.0, float(patch["max_bullish_prepriced_pct"]))), 1)
    if "max_alerts_per_window" in patch:
        cur["max_alerts_per_window"] = max(1, min(10, int(patch["max_alerts_per_window"])))
    if "theme_cooldown_hours" in patch:
        cur["theme_cooldown_hours"] = max(1, min(72, int(patch["theme_cooldown_hours"])))
    if "dedup_mode" in patch:
        dm = str(patch["dedup_mode"]).strip().lower()
        if dm not in ("shadow", "enforce"):
            raise ValueError("dedup_mode는 'shadow' 또는 'enforce'만 허용됩니다.")
        cur["dedup_mode"] = dm
    if "trigger_mode" in patch:
        tm = str(patch["trigger_mode"]).strip().lower()
        if tm not in ("shadow", "enforce"):
            raise ValueError("trigger_mode는 'shadow' 또는 'enforce'만 허용됩니다.")
        cur["trigger_mode"] = tm
    if "stage1_daily_rpd_limit" in patch:
        cur["stage1_daily_rpd_limit"] = max(10, min(10000, int(patch["stage1_daily_rpd_limit"])))
    if "daemon_interval_market_sec" in patch:
        cur["daemon_interval_market_sec"] = max(15, min(600, int(patch["daemon_interval_market_sec"])))
    if "daemon_interval_extended_sec" in patch:
        cur["daemon_interval_extended_sec"] = max(30, min(1800, int(patch["daemon_interval_extended_sec"])))
    if "daemon_interval_offhours_sec" in patch:
        cur["daemon_interval_offhours_sec"] = max(60, min(3600, int(patch["daemon_interval_offhours_sec"])))
    if "enabled_feeds" in patch:
        raw_feeds = patch["enabled_feeds"]
        if not isinstance(raw_feeds, list):
            raise ValueError("enabled_feeds는 피드 이름 배열이어야 합니다.")
        valid_names = {f["name"] for f in radar.ALL_FEEDS}
        filtered = [str(n) for n in raw_feeds if str(n) in valid_names]
        if not filtered:
            raise ValueError("최소 1개 이상의 RSS 피드를 활성화해야 합니다.")
        cur["enabled_feeds"] = list(dict.fromkeys(filtered))

    return radar.save_runtime_config(data_dir, cur)


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="description" content="News Alpha Radar 24/7 국내주식 일상뉴스 2차 파급효과 실시간 관제 콘솔" />
  <title>News Alpha Radar — 실시간 관제 콘솔</title>
  <link rel="preconnect" href="https://cdn.jsdelivr.net" crossorigin />
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/orioncactus/pretendard@v1.3.9/dist/web/variable/pretendardvariable-dynamic-subset.min.css" />
  <style>
    :root {
      --bg: #f6f7f9;
      --surface: #ffffff;
      --surface-subtle: #f9fafb;
      --border: #e5e7eb;
      --border-strong: #d1d5db;
      --ink: #0f172a;
      --ink-secondary: #475569;
      --ink-muted: #64748b;
      --accent: #2563eb;
      --accent-hover: #1d4ed8;
      --accent-soft: #eff6ff;
      --green: #059669;
      --green-soft: #ecfdf5;
      --amber: #d97706;
      --amber-soft: #fffbeb;
      --red: #dc2626;
      --red-soft: #fef2f2;
      --radius: 12px;
      --radius-sm: 8px;
      --shadow: 0 1px 2px rgba(15, 23, 42, 0.04), 0 4px 12px rgba(15, 23, 42, 0.03);
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      --sans: "Pretendard Variable", Pretendard, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: var(--sans);
      font-size: 14px;
      line-height: 1.5;
      -webkit-font-smoothing: antialiased;
    }

    .topbar {
      position: sticky;
      top: 0;
      z-index: 20;
      background: rgba(255, 255, 255, 0.92);
      backdrop-filter: blur(10px);
      border-bottom: 1px solid var(--border);
    }

    .topbar-inner {
      max-width: 1120px;
      margin: 0 auto;
      padding: 14px 20px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      flex-wrap: wrap;
    }

    .brand {
      display: flex;
      align-items: center;
      gap: 12px;
    }

    .brand h1 {
      margin: 0;
      font-size: 17px;
      font-weight: 700;
      letter-spacing: -0.02em;
      color: var(--ink);
    }

    .status-chip {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 600;
      background: var(--surface-subtle);
      border: 1px solid var(--border);
      color: var(--ink-secondary);
    }

    .status-dot {
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background: var(--ink-muted);
    }

    .status-chip.running { background: var(--green-soft); border-color: #a7f3d0; color: var(--green); }
    .status-chip.running .status-dot { background: var(--green); }
    .status-chip.paused { background: var(--amber-soft); border-color: #fde68a; color: var(--amber); }
    .status-chip.paused .status-dot { background: var(--amber); }
    .status-chip.offline, .status-chip.error { background: var(--red-soft); border-color: #fecaca; color: var(--red); }
    .status-chip.offline .status-dot, .status-chip.error .status-dot { background: var(--red); }

    .actions {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }

    button {
      font-family: inherit;
      font-size: 13px;
      font-weight: 600;
      border-radius: var(--radius-sm);
      padding: 8px 13px;
      cursor: pointer;
      border: 1px solid var(--border);
      background: var(--surface);
      color: var(--ink);
      transition: background 0.15s ease, border-color 0.15s ease, transform 0.08s ease;
    }

    button:hover:not(:disabled) {
      background: var(--surface-subtle);
      border-color: var(--border-strong);
    }

    button:active:not(:disabled) {
      transform: translateY(1px);
    }

    button:disabled {
      opacity: 0.55;
      cursor: not-allowed;
    }

    button.primary {
      background: var(--accent);
      border-color: var(--accent);
      color: #ffffff;
    }

    button.primary:hover:not(:disabled) {
      background: var(--accent-hover);
      border-color: var(--accent-hover);
    }

    button.danger {
      color: var(--red);
      border-color: #fecaca;
      background: var(--red-soft);
    }

    main {
      max-width: 1120px;
      margin: 0 auto;
      padding: 20px;
    }

    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 14px;
      margin-bottom: 20px;
    }

    .card {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 16px 18px;
    }

    .kpi-label {
      font-size: 12px;
      font-weight: 600;
      color: var(--ink-muted);
      margin-bottom: 6px;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }

    .kpi-value {
      font-size: 24px;
      font-weight: 700;
      letter-spacing: -0.025em;
      font-variant-numeric: tabular-nums;
      color: var(--ink);
      line-height: 1.2;
    }

    .kpi-sub {
      margin-top: 6px;
      font-size: 12px;
      color: var(--ink-secondary);
      font-variant-numeric: tabular-nums;
    }

    .progress-track {
      margin-top: 8px;
      height: 6px;
      border-radius: 999px;
      background: #e5e7eb;
      overflow: hidden;
    }

    .progress-fill {
      height: 100%;
      background: var(--accent);
      border-radius: 999px;
      transition: width 0.25s ease;
    }

    .progress-fill.caution { background: var(--amber); }
    .progress-fill.danger { background: var(--red); }

    .tabs {
      display: flex;
      gap: 4px;
      border-bottom: 1px solid var(--border);
      margin-bottom: 18px;
      overflow-x: auto;
    }

    .tab-btn {
      border: none;
      background: transparent;
      border-radius: 0;
      padding: 10px 14px;
      font-size: 13.5px;
      font-weight: 600;
      color: var(--ink-muted);
      border-bottom: 2px solid transparent;
      white-space: nowrap;
    }

    .tab-btn:hover {
      background: transparent;
      color: var(--ink);
    }

    .tab-btn.active {
      color: var(--accent);
      border-bottom-color: var(--accent);
    }

    .tab-panel { display: none; }
    .tab-panel.active { display: block; }

    .section-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 14px;
      flex-wrap: wrap;
    }

    .section-head h2 {
      margin: 0;
      font-size: 15px;
      font-weight: 700;
      letter-spacing: -0.015em;
    }

    .muted { color: var(--ink-muted); font-size: 12.5px; }

    .alert-list {
      display: flex;
      flex-direction: column;
      gap: 10px;
    }

    .alert-row {
      padding: 14px 16px;
      border: 1px solid var(--border);
      border-radius: var(--radius-sm);
      background: var(--surface);
      display: flex;
      flex-direction: column;
      gap: 8px;
    }

    .alert-top {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 10px;
      flex-wrap: wrap;
    }

    .alert-title {
      font-weight: 700;
      font-size: 14.5px;
      color: var(--ink);
    }

    .badge {
      display: inline-flex;
      align-items: center;
      padding: 2px 8px;
      border-radius: 999px;
      font-size: 11.5px;
      font-weight: 600;
      font-variant-numeric: tabular-nums;
      background: var(--surface-subtle);
      color: var(--ink-secondary);
      border: 1px solid var(--border);
    }

    .badge.score { background: var(--accent-soft); border-color: #bfdbfe; color: var(--accent); }
    .badge.ok { background: var(--green-soft); border-color: #a7f3d0; color: var(--green); }
    .badge.warn { background: var(--amber-soft); border-color: #fde68a; color: var(--amber); }
    .badge.err { background: var(--red-soft); border-color: #fecaca; color: var(--red); }

    .stock-links {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
    }

    .stock-link {
      text-decoration: none;
      font-size: 12px;
      font-weight: 600;
      color: var(--ink);
      background: var(--surface-subtle);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 3px 8px;
      font-variant-numeric: tabular-nums;
    }

    .stock-link:hover {
      border-color: var(--accent);
      color: var(--accent);
      background: var(--accent-soft);
    }

    .settings-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 16px;
    }

    .field {
      display: flex;
      flex-direction: column;
      gap: 6px;
      margin-bottom: 14px;
    }

    .field label {
      font-size: 12.5px;
      font-weight: 600;
      color: var(--ink-secondary);
    }

    .field input[type="number"],
    .field input[type="text"],
    .field select,
    .field textarea {
      width: 100%;
      padding: 9px 11px;
      border: 1px solid var(--border-strong);
      border-radius: var(--radius-sm);
      font-family: inherit;
      font-size: 13.5px;
      background: var(--surface);
      color: var(--ink);
      font-variant-numeric: tabular-nums;
    }

    .field-row {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }

    .perf-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 14px;
      margin-bottom: 16px;
    }

    .perf-card {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 15px 16px;
    }

    .perf-card-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 10px;
    }

    .perf-card-title {
      font-weight: 700;
      font-size: 14px;
    }

    .perf-metrics {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px 12px;
    }

    .perf-cell-label {
      font-size: 11.5px;
      color: var(--ink-muted);
    }

    .perf-cell-val {
      font-size: 14px;
      font-weight: 700;
      font-variant-numeric: tabular-nums;
    }

    .ret-pos { color: var(--green); font-weight: 700; }
    .ret-neg { color: var(--red); font-weight: 700; }

    .field textarea {
      min-height: 78px;
      resize: vertical;
      line-height: 1.45;
    }

    .field input:focus, .field select:focus, .field textarea:focus {
      outline: 2px solid var(--accent-soft);
      border-color: var(--accent);
    }

    .feed-toggle-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px;
    }

    .feed-toggle {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      padding: 9px 12px;
      border: 1px solid var(--border);
      border-radius: var(--radius-sm);
      background: var(--surface-subtle);
      cursor: pointer;
    }

    .feed-toggle input {
      width: 16px;
      height: 16px;
      accent-color: var(--accent);
      cursor: pointer;
    }

    .table-wrap {
      overflow-x: auto;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
      font-variant-numeric: tabular-nums;
    }

    th, td {
      text-align: left;
      padding: 10px 10px;
      border-bottom: 1px solid var(--border);
      vertical-align: middle;
    }

    th {
      font-size: 12px;
      font-weight: 600;
      color: var(--ink-muted);
      background: var(--surface-subtle);
      white-space: nowrap;
    }

    .mono { font-family: var(--mono); font-size: 12px; }

    .empty {
      text-align: center;
      padding: 36px 16px;
      color: var(--ink-muted);
      font-size: 13px;
    }

    .toast {
      position: fixed;
      right: 20px;
      bottom: 20px;
      z-index: 50;
      padding: 11px 16px;
      border-radius: var(--radius-sm);
      background: var(--ink);
      color: #ffffff;
      font-size: 13px;
      font-weight: 600;
      box-shadow: 0 8px 24px rgba(15, 23, 42, 0.18);
      opacity: 0;
      transform: translateY(8px);
      pointer-events: none;
      transition: opacity 0.2s ease, transform 0.2s ease;
    }

    .toast.show { opacity: 1; transform: translateY(0); }

    @media (max-width: 900px) {
      .kpi-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .settings-grid { grid-template-columns: 1fr; }
      .perf-grid { grid-template-columns: 1fr; }
    }

    @media (max-width: 560px) {
      .topbar-inner { padding: 12px 14px; }
      main { padding: 14px; }
      .kpi-grid { grid-template-columns: 1fr; }
      .feed-toggle-grid { grid-template-columns: 1fr; }
      .field-row { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <header class="topbar">
    <div class="topbar-inner">
      <div class="brand">
        <h1>News Alpha Radar</h1>
        <span id="daemon-chip" class="status-chip"><span class="status-dot"></span><span id="daemon-label">연결 중...</span></span>
        <span id="listener-chip" class="status-chip"><span class="status-dot"></span><span id="listener-label">콜백 확인 중</span></span>
      </div>
      <div class="actions">
        <button id="btn-scan-now" class="primary" type="button">지금 즉시 스캔</button>
        <button id="btn-toggle-pause" type="button">일시정지</button>
        <button id="btn-demo-alert" type="button">텔레그램 테스트</button>
        <button id="btn-self-update" type="button">최신 코드 업데이트</button>
        <button id="btn-refresh" type="button">새로고침</button>
      </div>
    </div>
  </header>

  <main>
    <section class="kpi-grid" aria-label="핵심 운영 지표">
      <article class="card">
        <div class="kpi-label"><span>오늘 발송 알림 (KST)</span><span id="kpi-alert-badge" class="badge">-</span></div>
        <div id="kpi-alerts" class="kpi-value">-</div>
        <div id="kpi-alerts-sub" class="kpi-sub">일일 한도 대비</div>
      </article>
      <article class="card">
        <div class="kpi-label"><span>금일 AI 요청 (태평양 자정 리셋)</span><span id="kpi-rpd-tier" class="badge">-</span></div>
        <div id="kpi-rpd" class="kpi-value">-</div>
        <div class="progress-track"><div id="kpi-rpd-bar" class="progress-fill" style="width: 0%"></div></div>
        <div id="kpi-rpd-sub" class="kpi-sub">소진 모델 확인 중</div>
      </article>
      <article class="card">
        <div class="kpi-label"><span>대기열 (Pending / Deferred)</span><span id="kpi-modes-badge" class="badge">-</span></div>
        <div id="kpi-queues" class="kpi-value">-</div>
        <div id="kpi-queues-sub" class="kpi-sub">누적 평가 기사 확인 중</div>
      </article>
      <article class="card">
        <div class="kpi-label"><span>현재 감시 주기</span><span id="kpi-window-badge" class="badge">-</span></div>
        <div id="kpi-interval" class="kpi-value">-</div>
        <div id="kpi-interval-sub" class="kpi-sub">마지막 스캔 확인 중</div>
      </article>
    </section>

    <nav class="tabs" role="tablist">
      <button class="tab-btn active" data-tab="alerts" role="tab" type="button">최근 알림 &amp; 재시도 큐</button>
      <button class="tab-btn" data-tab="performance" role="tab" type="button">알림 성과 (수익률)</button>
      <button class="tab-btn" data-tab="settings" role="tab" type="button">운영 설정 &amp; 피드 관리</button>
      <button class="tab-btn" data-tab="feeds" role="tab" type="button">피드 상태 &amp; 실시간 점검</button>
      <button class="tab-btn" data-tab="traces" role="tab" type="button">추적 로그 (trace.jsonl)</button>
    </nav>

    <section id="tab-alerts" class="tab-panel active">
      <div class="card" style="margin-bottom: 16px;">
        <div class="section-head">
          <h2>최근 발송 알림</h2>
          <span id="alerts-count-label" class="muted">최근 7일 보관</span>
        </div>
        <div id="alerts-container" class="alert-list"></div>
      </div>

      <div id="overnight-card" class="card" style="margin-bottom: 16px; display: none;">
        <div class="section-head">
          <h2>야간 장전 요약 대기열</h2>
          <span id="overnight-count-label" class="muted">평일 장전 요약 시각에 1건으로 묶어 발송</span>
        </div>
        <div id="overnight-container"></div>
      </div>

      <div class="card">
        <div class="section-head">
          <h2>Deferred 재시도 보관함</h2>
          <span class="muted">Stage 2 실패·텔레그램 부분 실패·한도 초과 시 자동 재시도</span>
        </div>
        <div id="deferred-container"></div>
      </div>
    </section>

    <section id="tab-performance" class="tab-panel">
      <div class="section-head" style="margin-bottom: 12px;">
        <h2>방향별 알림 성과 요약 (data/performance.jsonl)</h2>
        <span id="perf-meta-label" class="muted">상승수혜: 수익률 &gt; 0% 적중 · 하락/옥석가리기: 하락(&lt; 0%) 적중</span>
      </div>
      <div id="perf-summary-grid" class="perf-grid"></div>

      <div class="card">
        <div class="section-head">
          <h2>최근 종목별 자동 수익률 추적</h2>
          <span class="muted">알림가 대비 +5분 · +30분 · 당일 종가 · 익일 종가 자동 기록</span>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>알림 시각</th>
                <th>종목</th>
                <th>방향</th>
                <th>섹터</th>
                <th>알림가</th>
                <th>+5분</th>
                <th>+30분</th>
                <th>당일 종가</th>
                <th>익일 종가</th>
              </tr>
            </thead>
            <tbody id="perf-records-body"></tbody>
          </table>
        </div>
      </div>
    </section>

    <section id="tab-settings" class="tab-panel">
      <form id="config-form">
        <div class="settings-grid">
          <div class="card">
            <div class="section-head">
              <h2>감시 기준 &amp; 시간대·편중 제어</h2>
              <span class="muted">저장 즉시 다음 사이클부터 반영</span>
            </div>
            <div class="field-row">
              <div class="field">
                <label for="cfg-min-score">최소 촉매 점수 (1.0 ~ 10.0)</label>
                <input id="cfg-min-score" type="number" step="0.1" min="1" max="10" />
              </div>
              <div class="field">
                <label for="cfg-max-window">15분당 최대 연속 발송 건수</label>
                <input id="cfg-max-window" type="number" step="1" min="1" max="10" />
              </div>
            </div>
            <div class="field-row">
              <div class="field">
                <label for="cfg-max-market">장중 최대 알림 (평일 08:00~15:30)</label>
                <input id="cfg-max-market" type="number" step="1" min="0" max="50" />
              </div>
              <div class="field">
                <label for="cfg-max-offhours">야간·주말 최대 알림</label>
                <input id="cfg-max-offhours" type="number" step="1" min="0" max="30" />
              </div>
            </div>
            <div class="field-row">
              <div class="field">
                <label for="cfg-offhours-digest">야간 후보 장전 요약 묶음 발송</label>
                <select id="cfg-offhours-digest">
                  <option value="true">켜짐 — 야간 후보 모아서 평일 장전 1건 요약 (권장)</option>
                  <option value="false">꺼짐 — 야간에도 한도 내 즉시 발송</option>
                </select>
              </div>
              <div class="field">
                <label for="cfg-digest-time">장전 요약 발송 시각 (KST HH:MM)</label>
                <input id="cfg-digest-time" type="text" placeholder="07:50" />
              </div>
            </div>
            <div class="field-row">
              <div class="field">
                <label for="cfg-stock-dedup-hours">동일 종목코드 중복 차단 (시간, 0=해제)</label>
                <input id="cfg-stock-dedup-hours" type="number" step="1" min="0" max="168" />
              </div>
              <div class="field">
                <label for="cfg-max-per-sector">동일 섹터 하루 최대 건수 (0=무제한)</label>
                <input id="cfg-max-per-sector" type="number" step="1" min="0" max="20" />
              </div>
            </div>
            <div class="field-row">
              <div class="field">
                <label for="cfg-cooldown">동일 이슈 쿨다운 시간 (시간)</label>
                <input id="cfg-cooldown" type="number" step="1" min="1" max="72" />
              </div>
              <div class="field">
                <label for="cfg-max-daily">일일 전체 상한 (MAX_DAILY_ALERTS 호환)</label>
                <input id="cfg-max-daily" type="number" step="1" min="1" max="80" />
              </div>
            </div>
            <div class="field-row">
              <div class="field">
                <label for="cfg-max-prepriced">정규장 급등 추격 차단 기준 (%, 0=해제)</label>
                <input id="cfg-max-prepriced" type="number" step="0.5" min="0" max="30" />
              </div>
              <div class="field">
                <label for="cfg-rpd-limit">Stage 1 일일 RPD 기준 한도</label>
                <input id="cfg-rpd-limit" type="number" step="10" min="10" max="10000" />
              </div>
            </div>
            <div class="field">
              <label for="cfg-dedup-mode">PR-3 유사도 중복 차단 모드</label>
              <select id="cfg-dedup-mode">
                <option value="enforce">enforce — 실제 차단 + 1.0점↑ 급등 시 [후속] 통과 (권장)</option>
                <option value="shadow">shadow — 로그만 기록 (차단 안 함)</option>
              </select>
            </div>
            <div class="field">
              <label for="cfg-trigger-mode">PR-3 Stage 1 스마트 트리거 모드</label>
              <select id="cfg-trigger-mode">
                <option value="enforce">enforce — 속보 즉시 / 일반 기사 묶음 호출 (상시 서버 필수)</option>
                <option value="shadow">shadow — 로그만 기록 (매번 AI 호출)</option>
              </select>
            </div>
            <div class="field">
              <label for="cfg-flash-alert">2단 초고속 1보 푸시 (3초 내 1보 → 15초 뒤 2보 심층)</label>
              <select id="cfg-flash-alert">
                <option value="true">켜짐 — Stage 1 통과 즉시 1보 속보 먼저 푸시 (추천)</option>
                <option value="false">꺼짐 — Stage 2 심층 리포트만 1회 발송</option>
              </select>
            </div>
          </div>

          <div class="card">
            <div class="section-head">
              <h2>0초 텔레그램 콜백 &amp; 백업 RSS 주기</h2>
              <span class="muted">채널 저장 시 20초 내 리스너에 자동 반영</span>
            </div>
            <div class="field">
              <label for="cfg-tg-channels">구독할 텔레그램 속보 채널 (@아이디 또는 t.me 링크, 줄바꿈·쉼표 구분)</label>
              <textarea id="cfg-tg-channels" placeholder="예:&#10;@yonhap_news&#10;https://t.me/some_breaking_channel"></textarea>
            </div>
            <div class="field">
              <label for="cfg-int-market">정규장/장전 (08:00~15:30) 백업 RSS 주기 (초)</label>
              <input id="cfg-int-market" type="number" step="5" min="15" max="600" />
            </div>
            <div class="field">
              <label for="cfg-int-ext">장전초/장후 (07~08, 15:30~21:00) 주기 (초)</label>
              <input id="cfg-int-ext" type="number" step="10" min="30" max="1800" />
            </div>
            <div class="field">
              <label for="cfg-int-off">심야/주말 주기 (초)</label>
              <input id="cfg-int-off" type="number" step="30" min="60" max="3600" />
            </div>
            <div class="field">
              <label>활성 RSS 피드 선택</label>
              <div id="feed-toggles" class="feed-toggle-grid"></div>
            </div>
            <div style="display:flex; justify-content:flex-end; gap:8px; margin-top:16px;">
              <button id="btn-save-config" class="primary" type="submit">설정 저장</button>
            </div>
          </div>
        </div>
      </form>
    </section>

    <section id="tab-feeds" class="tab-panel">
      <div class="card" style="margin-bottom: 16px;">
        <div class="section-head">
          <h2>누적 피드 수집 통계</h2>
          <button id="btn-probe-feeds" class="primary" type="button">지금 전체 피드 실시간 점검 (Probe)</button>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>피드</th>
                <th>구분</th>
                <th>상태</th>
                <th>성공 / 304 / 실패</th>
                <th>최근 상태코드</th>
                <th>최소 출고지연</th>
                <th>최근 오류</th>
              </tr>
            </thead>
            <tbody id="feed-stats-body"></tbody>
          </table>
        </div>
      </div>

      <div id="probe-card" class="card" style="display:none;">
        <div class="section-head">
          <h2>실시간 Probe 점검 결과</h2>
          <span id="probe-time" class="muted"></span>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>피드</th>
                <th>HTTP</th>
                <th>기사 수</th>
                <th>응답시간</th>
                <th>최신 기사 지연</th>
                <th>최신 헤드라인 / 오류</th>
              </tr>
            </thead>
            <tbody id="probe-body"></tbody>
          </table>
        </div>
      </div>
    </section>

    <section id="tab-traces" class="tab-panel">
      <div class="card">
        <div class="section-head">
          <h2>추적 로그 (trace.jsonl)</h2>
          <div style="display:flex; gap:8px; align-items:center;">
            <select id="trace-filter" style="padding:6px 10px; border:1px solid var(--border-strong); border-radius:8px; font-size:12.5px;">
              <option value="all">전체 이벤트</option>
              <option value="flash_alert_sent">초고속 1보 푸시</option>
              <option value="premarket_digest_sent">장전 요약 발송</option>
              <option value="overnight_queued">야간 요약 대기열</option>
              <option value="prepriced_skip">장중 급등 추격 차단</option>
              <option value="stock_dedup_decision">24h 종목 중복 판정</option>
              <option value="sector_limit_decision">섹터 일일 한도 판정</option>
              <option value="cycle_summary">사이클 요약</option>
              <option value="trigger_decision">PR-3 트리거 판정</option>
              <option value="dedup_decision">PR-3 중복 판정</option>
              <option value="deferred_event">Deferred 재시도</option>
              <option value="article_decision">기사별 판정</option>
            </select>
            <button id="btn-reload-traces" type="button">로그 갱신</button>
          </div>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>시각 (KST)</th>
                <th>유형</th>
                <th>요약 내용</th>
              </tr>
            </thead>
            <tbody id="traces-body"></tbody>
          </table>
        </div>
      </div>
    </section>
  </main>

  <div id="toast" class="toast" role="status" aria-live="polite"></div>

  <script>
    let latestOverview = null;
    let formDirty = false;

    function toast(msg) {
      const el = document.getElementById("toast");
      el.textContent = msg;
      el.classList.add("show");
      clearTimeout(toast._t);
      toast._t = setTimeout(() => el.classList.remove("show"), 2800);
    }

    function fmtTime(iso) {
      if (!iso) return "-";
      const d = new Date(iso);
      if (isNaN(d.getTime())) return iso;
      return d.toLocaleString("ko-KR", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
    }

    function fmtLag(sec) {
      if (sec === null || sec === undefined) return "-";
      if (sec < 60) return sec + "초";
      return (sec / 60).toFixed(1) + "분";
    }

    function fmtPct(val) {
      if (val === null || val === undefined || isNaN(Number(val))) return "-";
      const n = Number(val);
      return (n > 0 ? "+" : "") + n.toFixed(2) + "%";
    }

    function makePctSpan(val) {
      const sp = document.createElement("span");
      if (val === null || val === undefined || isNaN(Number(val))) {
        sp.className = "muted";
        sp.textContent = "-";
        return sp;
      }
      const n = Number(val);
      sp.className = n > 0 ? "ret-pos" : (n < 0 ? "ret-neg" : "muted");
      sp.textContent = fmtPct(n);
      return sp;
    }

    async function api(url, opts = {}) {
      const res = await fetch(url, {
        headers: { "Content-Type": "application/json", ...(opts.headers || {}) },
        ...opts,
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.error || data.message || ("HTTP " + res.status));
      return data;
    }

    function renderOverview(data) {
      latestOverview = data;
      const d = data.daemon || {};
      const l = data.listener || {};
      const g = data.git || {};
      const m = data.metrics || {};
      const cfg = data.config || {};

      const chip = document.getElementById("daemon-chip");
      const label = document.getElementById("daemon-label");
      chip.className = "status-chip " + (d.state || "offline");
      if (d.state === "running" && d.alive) {
        label.textContent = "가동 중 · PID " + (d.pid || "-");
      } else if (d.state === "paused" && d.alive) {
        label.textContent = "일시정지됨";
      } else if (d.state === "error") {
        label.textContent = "일시 오류 복구 대기";
      } else {
        label.textContent = "데몬 오프라인";
      }

      const lChip = document.getElementById("listener-chip");
      const lLabel = document.getElementById("listener-label");
      if (l.alive && l.state === "running") {
        lChip.className = "status-chip running";
        lLabel.textContent = "0초 콜백 · " + (l.connected_count || (l.channels || []).length) + "채널 (수신 " + (l.received_count || 0) + ")";
      } else if (l.alive && l.state === "waiting_channels") {
        lChip.className = "status-chip paused";
        lLabel.textContent = "콜백 대기 (채널 미지정)";
      } else if (l.state === "needs_login") {
        lChip.className = "status-chip paused";
        lLabel.textContent = "콜백 1회 로그인 필요";
      } else {
        lChip.className = "status-chip";
        lLabel.textContent = "콜백 미연결 (RSS 전용)";
      }

      const updBtn = document.getElementById("btn-self-update");
      if (g.commit) {
        updBtn.title = "현재 버전: " + g.commit + " — " + (g.subject || "");
      }

      const pauseBtn = document.getElementById("btn-toggle-pause");
      pauseBtn.textContent = cfg.paused ? "감시 재개" : "일시정지";
      pauseBtn.className = cfg.paused ? "primary" : "";

      const mSent = m.sent_market_today ?? 0;
      const mMax = m.max_market_alerts ?? cfg.max_market_alerts ?? 8;
      const oSent = m.sent_offhours_today ?? 0;
      const oMax = m.max_offhours_alerts ?? cfg.max_offhours_alerts ?? 3;
      document.getElementById("kpi-alerts").textContent = m.sent_today + " / " + m.max_daily_alerts + "건";
      document.getElementById("kpi-alert-badge").textContent = m.sent_today >= m.max_daily_alerts ? "한도 도달" : "장중 " + mSent + "/" + mMax;
      document.getElementById("kpi-alerts-sub").textContent =
        "장중 " + mSent + "/" + mMax + " · 야간 " + oSent + "/" + oMax +
        (cfg.offhours_digest_enabled ? " (요약 " + (cfg.offhours_digest_time || "07:50") + ", 대기 " + (m.overnight_queue_count || 0) + ")" : "") +
        " · 촉매 " + Number(cfg.min_catalyst_score || 7.5).toFixed(1) + "+";

      const pct = Math.min(100, Math.round((m.rpd_ratio || 0) * 100));
      document.getElementById("kpi-rpd").textContent = m.primary_screen_used + " / " + m.rpd_limit;
      const bar = document.getElementById("kpi-rpd-bar");
      bar.style.width = pct + "%";
      bar.className = "progress-fill" + (pct >= 90 ? " danger" : pct >= 70 ? " caution" : "");
      const tierBadge = document.getElementById("kpi-rpd-tier");
      const tierMap = { normal: ["정상", "ok"], caution: ["70% 주의", "warn"], slowdown: ["80% 감속", "warn"], h_only: ["90% 긴급전용", "err"] };
      const [tTxt, tCls] = tierMap[m.rpd_tier] || ["정상", "ok"];
      tierBadge.textContent = tTxt;
      tierBadge.className = "badge " + tCls;
      const exh = (m.exhausted_models || []).length ? "소진: " + m.exhausted_models.join(", ") : "전체 총 " + m.ai_requests_pt_total + "회 · 소진 모델 없음";
      document.getElementById("kpi-rpd-sub").textContent = exh;

      document.getElementById("kpi-queues").textContent = m.pending_count + " / " + m.deferred_count;
      document.getElementById("kpi-modes-badge").textContent = "D:" + cfg.dedup_mode + " · T:" + cfg.trigger_mode;
      document.getElementById("kpi-queues-sub").textContent = "누적 평가 " + m.evaluated_count + "건 · 마지막 S1 " + fmtTime(m.last_stage1_at);

      document.getElementById("kpi-interval").textContent = m.poll_interval_sec + "초";
      document.getElementById("kpi-window-badge").textContent = m.window_label || "-";
      document.getElementById("kpi-interval-sub").textContent = d.last_cycle_at
        ? "최근 " + fmtTime(d.last_cycle_at) + " (" + (d.last_cycle_sec ?? "-") + "s)"
        : (d.last_error ? "오류: " + d.last_error.slice(0, 40) : "실행 기록 대기 중");

      renderAlerts(data.recent_alerts || []);
      renderOvernightQueue(data.overnight_queue || [], cfg);
      renderDeferred(data.deferred || []);
      renderPerformance(data.performance || {});
      renderFeedStats(data.feeds || []);
      if (!formDirty) populateSettingsForm(cfg, data.feeds || []);
    }

    function renderAlerts(alerts) {
      const box = document.getElementById("alerts-container");
      box.innerHTML = "";
      document.getElementById("alerts-count-label").textContent = "최근 7일 " + alerts.length + "건";
      if (!alerts.length) {
        box.innerHTML = '<div class="empty">아직 발송된 알림이 없습니다.</div>';
        return;
      }
      for (const a of alerts) {
        const row = document.createElement("div");
        row.className = "alert-row";

        const top = document.createElement("div");
        top.className = "alert-top";
        const title = document.createElement("div");
        title.className = "alert-title";
        title.textContent = a.title || a.theme_key || "(제목 없음)";
        const meta = document.createElement("div");
        meta.style.display = "flex";
        meta.style.gap = "6px";
        meta.style.alignItems = "center";
        meta.style.flexWrap = "wrap";

        if (a.sector) {
          const secBadge = document.createElement("span");
          secBadge.className = "badge";
          secBadge.textContent = a.sector;
          meta.appendChild(secBadge);
        }
        if (a.window) {
          const wBadge = document.createElement("span");
          wBadge.className = "badge " + (a.window === "market" ? "ok" : "warn");
          wBadge.textContent = a.window === "market" ? "장중" : "야간";
          meta.appendChild(wBadge);
        }
        const scoreBadge = document.createElement("span");
        scoreBadge.className = "badge score";
        scoreBadge.textContent = "촉매 " + Number(a.catalyst_score || 0).toFixed(1);
        const timeSpan = document.createElement("span");
        timeSpan.className = "muted";
        timeSpan.textContent = fmtTime(a.ts);
        meta.append(scoreBadge, timeSpan);
        top.append(title, meta);
        row.appendChild(top);

        const stocks = (a.stocks && a.stocks.length)
          ? a.stocks
          : (a.final_codes || []).map(c => ({ code: c, name: c }));
        if (stocks.length) {
          const links = document.createElement("div");
          links.className = "stock-links";
          for (const s of stocks) {
            const link = document.createElement("a");
            link.className = "stock-link";
            link.href = "https://m.stock.naver.com/domestic/stock/" + encodeURIComponent(s.code) + "/total";
            link.target = "_blank";
            link.rel = "noopener noreferrer";
            link.textContent = (s.name && s.name !== s.code ? s.name + " (" + s.code + ")" : s.code);
            links.appendChild(link);
          }
          row.appendChild(links);
        }

        if (a.theme_key) {
          const sub = document.createElement("div");
          sub.className = "muted";
          sub.textContent = "테마키: " + a.theme_key + ((a.keywords || []).length ? " · 키워드: " + a.keywords.slice(0, 5).join(", ") : "");
          row.appendChild(sub);
        }
        box.appendChild(row);
      }
    }

    function renderOvernightQueue(items, cfg) {
      const card = document.getElementById("overnight-card");
      const box = document.getElementById("overnight-container");
      if (!items.length) {
        card.style.display = "none";
        box.innerHTML = "";
        return;
      }
      card.style.display = "block";
      document.getElementById("overnight-count-label").textContent =
        "대기 " + items.length + "건 · 평일 " + (cfg.offhours_digest_time || "07:50") + " 장전 요약 발송";
      box.innerHTML = "";
      const wrap = document.createElement("div");
      wrap.className = "table-wrap";
      const table = document.createElement("table");
      table.innerHTML = "<thead><tr><th>점수</th><th>섹터</th><th>헤드라인 / 테마</th><th>적재 시각</th></tr></thead>";
      const tb = document.createElement("tbody");
      for (const q of items) {
        const tr = document.createElement("tr");
        const cells = [
          Number(q.catalyst_score || 0).toFixed(1),
          q.sector || "-",
          q.headline || q.theme_key || "-",
          fmtTime(q.created_at),
        ];
        for (const c of cells) {
          const td = document.createElement("td");
          td.textContent = c;
          tr.appendChild(td);
        }
        tb.appendChild(tr);
      }
      table.appendChild(tb);
      wrap.appendChild(table);
      box.appendChild(wrap);
    }

    function renderDeferred(items) {
      const box = document.getElementById("deferred-container");
      box.innerHTML = "";
      if (!items.length) {
        box.innerHTML = '<div class="empty">재시도 대기 중인 항목이 없습니다.</div>';
        return;
      }
      const wrap = document.createElement("div");
      wrap.className = "table-wrap";
      const table = document.createElement("table");
      table.innerHTML = "<thead><tr><th>유형</th><th>테마</th><th>점수</th><th>재시도</th><th>사유</th><th>만료 시각</th></tr></thead>";
      const tb = document.createElement("tbody");
      for (const d of items) {
        const tr = document.createElement("tr");
        const cells = [d.kind || "-", d.theme_key || d.headline || "-", Number(d.catalyst_score || 0).toFixed(1), String(d.retries || 0), d.reason || "-", fmtTime(d.expires_at)];
        for (const c of cells) {
          const td = document.createElement("td");
          td.textContent = c;
          tr.appendChild(td);
        }
        tb.appendChild(tr);
      }
      table.appendChild(tb);
      wrap.appendChild(table);
      box.appendChild(wrap);
    }

    function renderPerformance(perf) {
      const grid = document.getElementById("perf-summary-grid");
      const tb = document.getElementById("perf-records-body");
      grid.innerHTML = "";
      tb.innerHTML = "";

      const total = perf.total_records || 0;
      const done = perf.completed_records || 0;
      const costPct = perf.round_trip_cost_pct ?? 0.23;
      document.getElementById("perf-meta-label").textContent =
        "총 " + total + "건 추적 (완료 " + done + "건) · 상승수혜: >0% 적중 (순수익은 왕복비용 " + costPct + "% 차감) / 하락·옥석가리기: <0% 적중";

      const byDir = perf.by_direction || {};
      const dirMeta = [
        ["상승수혜", "📈 상승수혜", "ok"],
        ["하락주의", "📉 하락주의", "err"],
        ["옥석가리기주의", "⚠️ 옥석가리기주의", "warn"],
      ];
      const horizons = [
        ["ret_5m", "+5분"],
        ["ret_30m", "+30분"],
        ["ret_close", "당일 종가"],
        ["ret_next_close", "익일 종가"],
      ];

      for (const [key, titleText, badgeCls] of dirMeta) {
        const info = byDir[key] || { total: 0 };
        const card = document.createElement("article");
        card.className = "perf-card";

        const head = document.createElement("div");
        head.className = "perf-card-head";
        const ttl = document.createElement("div");
        ttl.className = "perf-card-title";
        ttl.textContent = titleText;
        const cntBadge = document.createElement("span");
        cntBadge.className = "badge " + badgeCls;
        cntBadge.textContent = (info.total || 0) + "건";
        head.append(ttl, cntBadge);
        card.appendChild(head);

        const metrics = document.createElement("div");
        metrics.className = "perf-metrics";
        for (const [hk, hlabel] of horizons) {
          const hs = info[hk] || {};
          const cell = document.createElement("div");
          const lbl = document.createElement("div");
          lbl.className = "perf-cell-label";
          lbl.textContent = hlabel + (hs.count ? " (" + hs.count + ")" : "");
          const val = document.createElement("div");
          val.className = "perf-cell-val";
          if (hs.count > 0 && hs.avg_pct !== null && hs.avg_pct !== undefined) {
            val.appendChild(makePctSpan(hs.avg_pct));
            const sub = document.createElement("div");
            sub.className = "muted";
            sub.style.fontSize = "11.5px";
            sub.textContent = (hs.net_avg_pct !== null && hs.net_avg_pct !== undefined)
              ? "순 " + fmtPct(hs.net_avg_pct) + " · 승률 " + Number(hs.win_rate_pct || 0).toFixed(1) + "%"
              : "승률 " + Number(hs.win_rate_pct || 0).toFixed(1) + "%";
            cell.append(lbl, val, sub);
          } else {
            val.textContent = "-";
            val.className = "perf-cell-val muted";
            cell.append(lbl, val);
          }
          metrics.appendChild(cell);
        }
        card.appendChild(metrics);
        grid.appendChild(card);
      }

      const rows = perf.recent_records || [];
      if (!rows.length) {
        tb.innerHTML = '<tr><td colspan="9" class="empty">아직 자동 기록된 알림 성과가 없습니다.</td></tr>';
        return;
      }
      for (const r of rows) {
        const tr = document.createElement("tr");
        const tdTs = document.createElement("td");
        tdTs.className = "mono";
        tdTs.style.whiteSpace = "nowrap";
        tdTs.textContent = fmtTime(r.alert_ts);

        const tdStock = document.createElement("td");
        const link = document.createElement("a");
        link.className = "stock-link";
        link.href = "https://m.stock.naver.com/domestic/stock/" + encodeURIComponent(r.code || "") + "/total";
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        link.textContent = (r.name || r.code || "-") + " (" + (r.code || "-") + ")";
        tdStock.appendChild(link);

        const tdDir = document.createElement("td");
        const db = document.createElement("span");
        const dCls = r.direction === "상승수혜" ? "ok" : (r.direction === "하락주의" ? "err" : "warn");
        db.className = "badge " + dCls;
        db.textContent = r.direction || "-";
        tdDir.appendChild(db);

        const tdSec = document.createElement("td");
        tdSec.className = "muted";
        tdSec.textContent = r.sector || "-";

        const tdPx = document.createElement("td");
        tdPx.textContent = r.alert_price ? Number(r.alert_price).toLocaleString("ko-KR") + "원" : "-";

        const td5 = document.createElement("td");
        td5.appendChild(makePctSpan(r.ret_5m));
        const td30 = document.createElement("td");
        td30.appendChild(makePctSpan(r.ret_30m));
        const tdClose = document.createElement("td");
        tdClose.appendChild(makePctSpan(r.ret_close));
        const tdNext = document.createElement("td");
        tdNext.appendChild(makePctSpan(r.ret_next_close));

        tr.append(tdTs, tdStock, tdDir, tdSec, tdPx, td5, td30, tdClose, tdNext);
        tb.appendChild(tr);
      }
    }

    function renderFeedStats(feeds) {
      const tb = document.getElementById("feed-stats-body");
      tb.innerHTML = "";
      for (const f of feeds) {
        const tr = document.createElement("tr");
        const tdName = document.createElement("td");
        tdName.style.fontWeight = "600";
        tdName.textContent = f.name;
        const tdCat = document.createElement("td");
        tdCat.textContent = f.category + (f.group === "google" ? " (구글)" : "");
        const tdOn = document.createElement("td");
        const b = document.createElement("span");
        b.className = "badge " + (f.enabled ? "ok" : "");
        b.textContent = f.enabled ? "활성" : "꺼짐";
        tdOn.appendChild(b);
        const tdCnt = document.createElement("td");
        tdCnt.textContent = f.ok + " / " + f.not_modified_304 + " / " + f.fail;
        const tdCode = document.createElement("td");
        tdCode.className = "mono";
        tdCode.textContent = f.last_status ? "HTTP " + f.last_status : "-";
        const tdLag = document.createElement("td");
        tdLag.textContent = fmtLag(f.last_min_pub_lag_sec);
        const tdErr = document.createElement("td");
        tdErr.className = "muted";
        tdErr.textContent = f.last_error || "-";
        tr.append(tdName, tdCat, tdOn, tdCnt, tdCode, tdLag, tdErr);
        tb.appendChild(tr);
      }
    }

    function populateSettingsForm(cfg, feeds) {
      document.getElementById("cfg-min-score").value = cfg.min_catalyst_score;
      document.getElementById("cfg-max-market").value = cfg.max_market_alerts;
      document.getElementById("cfg-max-offhours").value = cfg.max_offhours_alerts;
      document.getElementById("cfg-offhours-digest").value = cfg.offhours_digest_enabled ? "true" : "false";
      document.getElementById("cfg-digest-time").value = cfg.offhours_digest_time || "07:50";
      document.getElementById("cfg-stock-dedup-hours").value = cfg.stock_dedup_hours;
      document.getElementById("cfg-max-per-sector").value = cfg.max_daily_per_sector;
      document.getElementById("cfg-max-prepriced").value = cfg.max_bullish_prepriced_pct ?? 15;
      document.getElementById("cfg-max-daily").value = cfg.max_daily_alerts;
      document.getElementById("cfg-max-window").value = cfg.max_alerts_per_window;
      document.getElementById("cfg-cooldown").value = cfg.theme_cooldown_hours;
      document.getElementById("cfg-dedup-mode").value = cfg.dedup_mode;
      document.getElementById("cfg-trigger-mode").value = cfg.trigger_mode;
      document.getElementById("cfg-rpd-limit").value = cfg.stage1_daily_rpd_limit;
      document.getElementById("cfg-flash-alert").value = cfg.instant_flash_alert ? "true" : "false";
      document.getElementById("cfg-tg-channels").value = (cfg.telegram_source_channels || []).join("\\n");
      document.getElementById("cfg-int-market").value = cfg.daemon_interval_market_sec;
      document.getElementById("cfg-int-ext").value = cfg.daemon_interval_extended_sec;
      document.getElementById("cfg-int-off").value = cfg.daemon_interval_offhours_sec;

      const wrap = document.getElementById("feed-toggles");
      wrap.innerHTML = "";
      for (const f of feeds) {
        const lbl = document.createElement("label");
        lbl.className = "feed-toggle";
        const left = document.createElement("span");
        left.textContent = f.name;
        left.style.fontSize = "12.5px";
        left.style.fontWeight = "600";
        const chk = document.createElement("input");
        chk.type = "checkbox";
        chk.name = "enabled_feed";
        chk.value = f.name;
        chk.checked = !!f.enabled;
        chk.addEventListener("change", () => { formDirty = true; });
        lbl.append(left, chk);
        wrap.appendChild(lbl);
      }
    }

    async function loadOverview() {
      try {
        const data = await api("/api/overview");
        renderOverview(data);
      } catch (e) {
        toast("상태 불러오기 실패: " + e.message);
      }
    }

    async function loadTraces() {
      const f = document.getElementById("trace-filter").value;
      try {
        const data = await api("/api/traces?limit=80&type=" + encodeURIComponent(f));
        const tb = document.getElementById("traces-body");
        tb.innerHTML = "";
        const rows = data.traces || [];
        if (!rows.length) {
          tb.innerHTML = '<tr><td colspan="3" class="empty">기록된 로그가 없습니다.</td></tr>';
          return;
        }
        for (const r of rows) {
          const tr = document.createElement("tr");
          const tdTs = document.createElement("td");
          tdTs.className = "mono";
          tdTs.style.whiteSpace = "nowrap";
          tdTs.textContent = fmtTime(r.ts);

          const tdType = document.createElement("td");
          const b = document.createElement("span");
          b.className = "badge";
          b.textContent = r.type || "-";
          tdType.appendChild(b);

          const tdDesc = document.createElement("td");
          if (r.type === "flash_alert_sent") {
            tdDesc.textContent = "⚡ 1보 속보 발송 · " + (r.headline || r.theme_key || "") + " (촉매 " + Number(r.catalyst_score || 0).toFixed(1) + ")";
          } else if (r.type === "premarket_digest_sent") {
            tdDesc.textContent = "🌅 장전 요약 발송 · " + (r.count || 0) + "건 (" + (r.themes || []).join(", ") + ")";
          } else if (r.type === "overnight_queued") {
            tdDesc.textContent = "🌙 장전 요약 대기열 추가 · " + (r.theme_key || "") + " [" + (r.sector || "-") + "] (대기 " + (r.queue_size || 0) + "건)";
          } else if (r.type === "prepriced_skip") {
            tdDesc.textContent = "🛑 장중 급등 추격 차단 (≥+" + Number(r.max_pct || 15).toFixed(1) + "%) · " + (r.theme_key || "") + " · " + (r.details || []).join(", ");
          } else if (r.type === "stock_dedup_decision") {
            tdDesc.textContent = "[" + r.phase + "] " + (r.theme_key || "") + " · 제외=" + (r.excluded_codes || []).join(",") + " · 유지=" + (r.kept_codes || []).join(",") + (r.only_weak_leftovers ? " · 단기테마 잔여 차단" : (r.blocked ? " · 차단됨" : ""));
          } else if (r.type === "sector_limit_decision") {
            tdDesc.textContent = "[" + r.phase + "] " + (r.sector || "-") + " (" + r.used_today + "/" + r.limit + ") · " + (r.theme_key || "") + (r.blocked ? " · 차단됨" : "");
          } else if (r.type === "cycle_summary") {
            tdDesc.textContent = "신규 " + (r.fresh_count || 0) + "건 · 클러스터 " + (r.cluster_count || 0) + " · 발송 " + (r.alerts_sent || 0) + "건 · pending " + (r.pending_remaining ?? "-") + (r.note ? " (" + r.note + ")" : "");
          } else if (r.type === "trigger_decision") {
            tdDesc.textContent = "[" + r.mode + "] 실행=" + r.executed + " · 사유=" + r.reason + " · tier=" + r.rpd_tier + " · RPD " + r.rpd_used + "/" + r.rpd_limit + " · pending=" + r.pending_count;
          } else if (r.type === "dedup_decision") {
            tdDesc.textContent = "[" + r.phase + "/" + r.mode + "] " + (r.theme_key || "") + " · 차단=" + r.blocked + " (would=" + r.would_block + ", escalated=" + r.escalated + ") · " + r.reason;
          } else if (r.type === "deferred_event") {
            tdDesc.textContent = "[" + r.action + "] " + (r.theme_key || r.cluster_id) + " (" + r.kind + ", retries=" + r.retries + ") · " + (r.reason || "");
          } else if (r.type === "article_decision") {
            tdDesc.textContent = "[" + (r.source || "-") + "] " + (r.title || "") + " · lag=" + fmtLag(r.pub_lag_sec) + (r.deferred_reason ? " · " + r.deferred_reason : "");
          } else {
            tdDesc.className = "mono";
            tdDesc.textContent = JSON.stringify(r).slice(0, 180);
          }
          tr.append(tdTs, tdType, tdDesc);
          tb.appendChild(tr);
        }
      } catch (e) {
        toast("로그 불러오기 실패: " + e.message);
      }
    }

    document.querySelectorAll(".tab-btn").forEach(btn => {
      btn.addEventListener("click", () => {
        document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
        document.querySelectorAll(".tab-panel").forEach(p => p.classList.remove("active"));
        btn.classList.add("active");
        document.getElementById("tab-" + btn.dataset.tab).classList.add("active");
        if (btn.dataset.tab === "traces") loadTraces();
      });
    });

    document.getElementById("config-form").addEventListener("input", () => { formDirty = true; });
    document.getElementById("config-form").addEventListener("submit", async (e) => {
      e.preventDefault();
      const checkedFeeds = Array.from(document.querySelectorAll('input[name="enabled_feed"]:checked')).map(i => i.value);
      const maxMkt = parseInt(document.getElementById("cfg-max-market").value, 10);
      const maxOff = parseInt(document.getElementById("cfg-max-offhours").value, 10);
      const maxDailyRaw = parseInt(document.getElementById("cfg-max-daily").value, 10);
      const prevCfg = (latestOverview && latestOverview.config) || {};
      const splitChanged = (maxMkt !== prevCfg.max_market_alerts) || (maxOff !== prevCfg.max_offhours_alerts);
      const dailyUnchanged = maxDailyRaw === prevCfg.max_daily_alerts;
      const effDaily = (isNaN(maxDailyRaw) || (splitChanged && dailyUnchanged))
        ? Math.max(1, (isNaN(maxMkt) ? 8 : maxMkt) + (isNaN(maxOff) ? 3 : maxOff))
        : maxDailyRaw;
      const payload = {
        min_catalyst_score: parseFloat(document.getElementById("cfg-min-score").value),
        max_market_alerts: maxMkt,
        max_offhours_alerts: maxOff,
        max_daily_alerts: effDaily,
        offhours_digest_enabled: document.getElementById("cfg-offhours-digest").value === "true",
        offhours_digest_time: document.getElementById("cfg-digest-time").value.trim() || "07:50",
        stock_dedup_hours: parseInt(document.getElementById("cfg-stock-dedup-hours").value, 10),
        max_daily_per_sector: parseInt(document.getElementById("cfg-max-per-sector").value, 10),
        max_bullish_prepriced_pct: parseFloat(document.getElementById("cfg-max-prepriced").value),
        max_alerts_per_window: parseInt(document.getElementById("cfg-max-window").value, 10),
        theme_cooldown_hours: parseInt(document.getElementById("cfg-cooldown").value, 10),
        dedup_mode: document.getElementById("cfg-dedup-mode").value,
        trigger_mode: document.getElementById("cfg-trigger-mode").value,
        stage1_daily_rpd_limit: parseInt(document.getElementById("cfg-rpd-limit").value, 10),
        instant_flash_alert: document.getElementById("cfg-flash-alert").value === "true",
        telegram_source_channels: document.getElementById("cfg-tg-channels").value,
        daemon_interval_market_sec: parseInt(document.getElementById("cfg-int-market").value, 10),
        daemon_interval_extended_sec: parseInt(document.getElementById("cfg-int-ext").value, 10),
        daemon_interval_offhours_sec: parseInt(document.getElementById("cfg-int-off").value, 10),
        enabled_feeds: checkedFeeds,
      };
      try {
        await api("/api/config", { method: "POST", body: JSON.stringify(payload) });
        formDirty = false;
        toast("설정을 저장했습니다. 다음 사이클부터 즉시 적용됩니다.");
        loadOverview();
      } catch (err) {
        toast("저장 실패: " + err.message);
      }
    });

    document.getElementById("btn-toggle-pause").addEventListener("click", async () => {
      if (!latestOverview) return;
      const nextPaused = !latestOverview.config.paused;
      try {
        await api("/api/config", { method: "POST", body: JSON.stringify({ paused: nextPaused }) });
        toast(nextPaused ? "감시를 일시정지했습니다." : "감시를 재개했습니다.");
        loadOverview();
      } catch (err) {
        toast("상태 변경 실패: " + err.message);
      }
    });

    document.getElementById("btn-scan-now").addEventListener("click", async (ev) => {
      const btn = ev.currentTarget;
      btn.disabled = true;
      try {
        const res = await api("/api/actions/scan-now", { method: "POST", body: "{}" });
        toast(res.mode === "woke_daemon" ? "데몬에 즉시 스캔 신호를 보냈습니다." : "단발 백그라운드 스캔을 시작했습니다.");
        setTimeout(loadOverview, 2500);
      } catch (err) {
        toast("즉시 스캔 실패: " + err.message);
      } finally {
        btn.disabled = false;
      }
    });

    document.getElementById("btn-demo-alert").addEventListener("click", async (ev) => {
      const btn = ev.currentTarget;
      btn.disabled = true;
      try {
        await api("/api/actions/demo-alert", { method: "POST", body: "{}" });
        toast("텔레그램 테스트 알림을 발송했습니다.");
      } catch (err) {
        toast("테스트 발송 실패: " + err.message);
      } finally {
        btn.disabled = false;
      }
    });

    document.getElementById("btn-self-update").addEventListener("click", async (ev) => {
      const btn = ev.currentTarget;
      btn.disabled = true;
      btn.textContent = "업데이트 확인 중...";
      try {
        const res = await api("/api/actions/self-update", { method: "POST", body: "{}" });
        toast(res.message || "업데이트 확인 완료");
        if (res.updated) {
          setTimeout(() => window.location.reload(), 3500);
        }
      } catch (err) {
        toast("업데이트 실패: " + err.message);
      } finally {
        btn.disabled = false;
        btn.textContent = "최신 코드 업데이트";
      }
    });

    document.getElementById("btn-probe-feeds").addEventListener("click", async (ev) => {
      const btn = ev.currentTarget;
      btn.disabled = true;
      btn.textContent = "점검 중...";
      try {
        const res = await api("/api/actions/probe-feeds", { method: "POST", body: "{}" });
        const card = document.getElementById("probe-card");
        card.style.display = "block";
        document.getElementById("probe-time").textContent = fmtTime(res.checked_at);
        const tb = document.getElementById("probe-body");
        tb.innerHTML = "";
        for (const r of (res.rows || [])) {
          const tr = document.createElement("tr");
          const ok = !r.error;
          const cells = [
            r.name,
            "HTTP " + r.status,
            r.items + "건",
            Number(r.elapsed || 0).toFixed(2) + "s",
            r.lag_min !== null && r.lag_min !== undefined ? r.lag_min + "분" : "N/A",
            r.error || r.sample_title || "-",
          ];
          cells.forEach((txt, idx) => {
            const td = document.createElement("td");
            if (idx === 1) {
              const sp = document.createElement("span");
              sp.className = "badge " + (ok ? "ok" : "err");
              sp.textContent = txt;
              td.appendChild(sp);
            } else {
              if (idx === 0) td.style.fontWeight = "600";
              td.textContent = txt;
            }
            tr.appendChild(td);
          });
          tb.appendChild(tr);
        }
        toast("전체 피드 실시간 점검 완료");
      } catch (err) {
        toast("피드 점검 실패: " + err.message);
      } finally {
        btn.disabled = false;
        btn.textContent = "지금 전체 피드 실시간 점검 (Probe)";
      }
    });

    document.getElementById("btn-refresh").addEventListener("click", () => { loadOverview(); loadTraces(); });
    document.getElementById("btn-reload-traces").addEventListener("click", loadTraces);
    document.getElementById("trace-filter").addEventListener("change", loadTraces);

    loadOverview();
    setInterval(loadOverview, 15000);
  </script>
</body>
</html>
"""


def create_dashboard_server(
    host: str = DEFAULT_DASHBOARD_HOST,
    port: int = DEFAULT_DASHBOARD_PORT,
    data_dir: Path | None = None,
    password: str = "",
) -> ThreadingHTTPServer:
    base_dir = data_dir if data_dir is not None else Path("data")
    base_dir.mkdir(parents=True, exist_ok=True)
    scan_lock = threading.Lock()

    class DashboardHandler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:
            logger.info("%s - %s", self.address_string(), fmt % args)

        def _check_auth(self) -> bool:
            if not password:
                return True
            hdr = self.headers.get("Authorization") or ""
            if not hdr.startswith("Basic "):
                self._send_unauthorized()
                return False
            try:
                decoded = base64.b64decode(hdr[6:].strip()).decode("utf-8", errors="ignore")
                _, _, supplied = decoded.partition(":")
            except Exception:
                self._send_unauthorized()
                return False
            if not hmac.compare_digest(supplied, password):
                self._send_unauthorized()
                return False
            return True

        def _send_unauthorized(self) -> None:
            body = json.dumps({"error": "인증이 필요합니다."}, ensure_ascii=False).encode("utf-8")
            self.send_response(HTTPStatus.UNAUTHORIZED)
            self.send_header("WWW-Authenticate", 'Basic realm="News Alpha Radar"')
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, html_text: str) -> None:
            body = html_text.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json_body(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length") or 0)
            if length > MAX_REQUEST_BODY_BYTES:
                raise ValueError("요청 본문이 너무 큽니다.")
            raw = self.rfile.read(length) if length > 0 else b"{}"
            parsed = json.loads(raw.decode("utf-8") or "{}")
            if not isinstance(parsed, dict):
                raise ValueError("JSON 객체 형식이 필요합니다.")
            return parsed

        def do_GET(self) -> None:
            if not self._check_auth():
                return
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            qs = urllib.parse.parse_qs(parsed.query)
            if path in ("/", "/index.html"):
                self._send_html(DASHBOARD_HTML)
            elif path == "/api/overview":
                self._send_json(build_overview_payload(base_dir))
            elif path == "/api/performance":
                try:
                    limit = int((qs.get("limit") or ["100"])[0])
                except ValueError:
                    limit = 100
                recs = radar.PerformanceTracker(base_dir / "performance.jsonl").load_records()
                self._send_json(radar.summarize_performance(recs, recent_limit=max(1, min(500, limit))))
            elif path == "/api/traces":
                try:
                    limit = int((qs.get("limit") or ["80"])[0])
                except ValueError:
                    limit = 80
                ev_type = str((qs.get("type") or ["all"])[0]).strip()
                self._send_json({"traces": read_recent_traces(base_dir, limit=limit, event_type=ev_type)})
            else:
                self._send_json({"error": "Not Found"}, HTTPStatus.NOT_FOUND)

        def do_POST(self) -> None:
            if not self._check_auth():
                return
            path = urllib.parse.urlparse(self.path).path
            try:
                body = self._read_json_body()
            except ValueError as exc:
                self._send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
                return
            except Exception:
                self._send_json({"error": "잘못된 JSON 본문입니다."}, HTTPStatus.BAD_REQUEST)
                return

            if path == "/api/config":
                try:
                    updated = validate_and_merge_config(base_dir, body)
                    self._send_json({"ok": True, "config": updated})
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
                return

            if path == "/api/actions/scan-now":
                now = datetime.now(radar.KST)
                st = _read_status(base_dir, now)
                if st.get("alive"):
                    radar.atomic_write_text(base_dir / "trigger_now", now.isoformat())
                    self._send_json({"ok": True, "mode": "woke_daemon"})
                else:
                    if not scan_lock.acquire(blocking=False):
                        self._send_json({"ok": True, "mode": "already_running"})
                        return

                    def _bg_scan() -> None:
                        try:
                            radar.run_live(dry_run=False, cycles=1, interval_sec=0, data_dir=base_dir, daemon_mode=True)
                        except Exception as exc:
                            logger.warning("대시보드 수동 단발 스캔 오류: %s", exc)
                        finally:
                            scan_lock.release()

                    threading.Thread(target=_bg_scan, daemon=True).start()
                    self._send_json({"ok": True, "mode": "triggered_oneshot"})
                return

            if path == "/api/actions/demo-alert":
                token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
                chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
                if not (token and chat_id):
                    self._send_json(
                        {"error": "TELEGRAM_BOT_TOKEN 또는 TELEGRAM_CHAT_ID가 설정되지 않았습니다."},
                        HTTPStatus.BAD_REQUEST,
                    )
                    return
                try:
                    radar.run_demo(False, token, chat_id)
                    self._send_json({"ok": True})
                except Exception as exc:
                    self._send_json({"error": str(exc)}, HTTPStatus.INTERNAL_SERVER_ERROR)
                return

            if path == "/api/actions/probe-feeds":
                try:
                    buf = io.StringIO()
                    with contextlib.redirect_stdout(buf):
                        rows = radar.probe_feeds()
                    self._send_json({"ok": True, "checked_at": datetime.now(radar.KST).isoformat(), "rows": rows})
                except Exception as exc:
                    self._send_json({"error": str(exc)}, HTTPStatus.INTERNAL_SERVER_ERROR)
                return

            if path == "/api/actions/self-update":
                res = run_self_update()
                status = HTTPStatus.OK if res.get("ok") else HTTPStatus.INTERNAL_SERVER_ERROR
                self._send_json(res, status)
                return

            self._send_json({"error": "Not Found"}, HTTPStatus.NOT_FOUND)

    return ThreadingHTTPServer((host, port), DashboardHandler)


def main() -> int:
    radar.load_dotenv_file()
    parser = argparse.ArgumentParser(description="News Alpha Radar Management Web Dashboard")
    parser.add_argument("--host", default=os.getenv("DASHBOARD_HOST", DEFAULT_DASHBOARD_HOST))
    parser.add_argument("--port", type=int, default=radar._env_int("DASHBOARD_PORT", DEFAULT_DASHBOARD_PORT))
    parser.add_argument("--data-dir", default="data")
    args = parser.parse_args()

    password = os.getenv("DASHBOARD_PASSWORD", "").strip()
    server = create_dashboard_server(
        host=args.host,
        port=args.port,
        data_dir=Path(args.data_dir),
        password=password,
    )
    auth_state = "활성(Basic Auth)" if password else "미설정(로컬 전용 권장)"
    logger.info("News Alpha Radar 대시보드 시작: http://%s:%d (인증=%s)", args.host, args.port, auth_state)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("대시보드 종료 중...")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
