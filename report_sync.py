#!/usr/bin/env python3
"""News Alpha Radar — 일별 수익률·체결·알림 대조 통합 리포트 생성 및 비공개 GitHub 자동 백업 모듈.

특징:
- 외부 패키지 0개 (Python 3.11+ 표준 라이브러리 전용)
- 민감정보 0% 보장: API 키, 비밀번호, 계좌번호, 채팅 ID, 터널 주소 등은 절대 포함하지 않음
- 4종 통합 분석 번들:
  1) 모의 자동매매 체결 내역 + 보유 중 최고점(peak_ret_pct) 대비 반납폭 + 버전(commit)별 성적표
  2) 알림 발송 전 종목 vs 실제 자동매수 종목 사후 수익률(+5분/+30분/당일종가/익일종가) 및 미진입 사유 대조
  3) 당일 필터 차단 통계 (유동성 미달, 장중 급등 추격 차단, 종목/섹터 중복 차단 등)
  4) 현재 적용 중인 매매·감시 수치 스냅샷
"""

from __future__ import annotations

import base64
import html
import json
import logging
import os
import threading
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from collections.abc import Callable
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import radar
import tunnel_manager

logger = logging.getLogger("news_alpha_report_sync")

DEFAULT_GITHUB_REPORT_REPO = "jjangna0817-hue/news-alpha-radar-logs"
DEFAULT_DAILY_REPORT_TIME = "15:35"
GITHUB_API_BASE = "https://api.github.com"

_BG_SYNC_LOCK = threading.Lock()


def previous_trading_date(d: date) -> date:
    """주말을 건너뛰어 직전 평일(거래일 기준) 날짜 반환."""
    cur = d - timedelta(days=1)
    while cur.weekday() >= 5:
        cur -= timedelta(days=1)
    return cur


def _load_sync_state(data_dir: Path) -> dict[str, Any]:
    path = data_dir / "report_sync_state.json"
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _save_sync_state(data_dir: Path, state: dict[str, Any]) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / "report_sync_state.json"
    radar.atomic_write_text(path, json.dumps(state, ensure_ascii=False, indent=2))


def resolve_github_sync_config(repo_dir: Path | None = None) -> dict[str, Any]:
    """환경변수 또는 .env에서 GITHUB_REPORT_TOKEN 및 GITHUB_REPORT_REPO 조회."""
    env_file = (repo_dir / ".env") if repo_dir is not None else Path(".env")
    token = (
        os.getenv("GITHUB_REPORT_TOKEN")
        or tunnel_manager.read_dotenv_value(env_file, "GITHUB_REPORT_TOKEN")
        or ""
    ).strip()
    if "여기에_" in token:
        token = ""

    repo = (
        os.getenv("GITHUB_REPORT_REPO")
        or tunnel_manager.read_dotenv_value(env_file, "GITHUB_REPORT_REPO")
        or DEFAULT_GITHUB_REPORT_REPO
    ).strip()
    if repo.startswith("https://github.com/"):
        repo = repo[len("https://github.com/") :].strip("/")
    if repo.endswith(".git"):
        repo = repo[:-4]
    if not repo or "/" not in repo:
        repo = DEFAULT_GITHUB_REPORT_REPO

    masked = ""
    if token:
        masked = (token[:7] + "****" + token[-4:]) if len(token) > 12 else "****"

    return {
        "configured": bool(token and "/" in repo),
        "token": token,
        "token_masked": masked,
        "repo": repo,
    }


def save_github_sync_settings(
    env_path: Path,
    *,
    token: str | None = None,
    repo: str | None = None,
) -> dict[str, Any]:
    """웹 대시보드에서 입력한 GitHub 토큰·저장소명을 주석 보존하며 .env(0600)에 저장."""
    cur_cfg = resolve_github_sync_config(env_path.parent)
    new_token = cur_cfg["token"] if (token is None or not str(token).strip()) else str(token).strip()
    raw_repo = str(repo).strip() if (repo is not None and str(repo).strip()) else cur_cfg["repo"]
    if raw_repo.startswith("https://github.com/"):
        raw_repo = raw_repo[len("https://github.com/") :].strip("/")
    if raw_repo.endswith(".git"):
        raw_repo = raw_repo[:-4]
    if "/" not in raw_repo:
        raise ValueError("저장소 형식은 '아이디/저장소이름' 이어야 합니다 (예: jjangna0817-hue/news-alpha-radar-logs).")

    updates = {
        "GITHUB_REPORT_REPO": raw_repo,
    }
    if new_token:
        updates["GITHUB_REPORT_TOKEN"] = new_token

    lines: list[str] = []
    seen_keys: set[str] = set()
    if env_path.exists():
        for ln in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            stripped = ln.strip()
            if stripped and not stripped.startswith("#") and "=" in stripped:
                k, _ = stripped.split("=", 1)
                k_clean = k.strip()
                if k_clean.startswith("export "):
                    k_clean = k_clean[7:].strip()
                if k_clean in updates:
                    lines.append(f"{k_clean}={updates[k_clean]}")
                    seen_keys.add(k_clean)
                    continue
            lines.append(ln)

    missing = [k for k in updates if k not in seen_keys]
    if missing:
        if lines and lines[-1].strip():
            lines.append("")
        lines.append("# 비공개 GitHub 일별 수익률·성적표 자동 백업 설정")
        for k in missing:
            lines.append(f"{k}={updates[k]}")

    env_path.parent.mkdir(parents=True, exist_ok=True)
    radar.atomic_write_text(env_path, "\n".join(lines) + "\n")
    try:
        os.chmod(env_path, 0o600)
    except OSError:
        pass

    os.environ["GITHUB_REPORT_REPO"] = raw_repo
    if new_token:
        os.environ["GITHUB_REPORT_TOKEN"] = new_token
    return resolve_github_sync_config(env_path.parent)


def get_sync_status_summary(data_dir: Path, repo_dir: Path | None = None) -> dict[str, Any]:
    cfg = resolve_github_sync_config(repo_dir)
    st = _load_sync_state(data_dir)
    return {
        "github_configured": cfg["configured"],
        "github_repo": cfg["repo"],
        "github_token_masked": cfg["token_masked"],
        "last_synced_at": st.get("last_synced_at"),
        "last_synced_date": st.get("last_synced_date"),
        "last_sync_error": st.get("last_sync_error"),
        "last_daily_telegram_date": st.get("last_daily_telegram_date"),
    }


def _avg_or_none(vals: list[float]) -> float | None:
    return round(sum(vals) / len(vals), 2) if vals else None


def _infer_skip_reason(
    rec: dict[str, Any],
    at_cfg: dict[str, Any],
) -> str:
    import paper_trader

    direction = radar._normalize_perf_direction(rec.get("direction"))
    if direction != "상승수혜":
        return f"경고종목({direction})"

    tier = str(rec.get("order_tier") or "").strip()
    rev = str(rec.get("revenue_link") or "").strip()
    if at_cfg.get("autotrade_only_second_order", True):
        if not paper_trader.is_second_order_candidate(rec):
            return f"1차/단기테마 제외({tier or '-'}·{rev or '-'})"
    elif rev == "단기테마성":
        return "단기테마성 제외"

    if rec.get("buyable") is False:
        return f"매수불가({rec.get('unbuyable_reason') or '체결제한'})"

    chg = rec.get("alert_change_pct")
    if chg is not None:
        chg_f = float(chg)
        max_chg = float(at_cfg.get("autotrade_max_change_pct", 2.0))
        min_chg = float(at_cfg.get("autotrade_min_change_pct", -2.0))
        if chg_f > max_chg:
            return f"진입상한 초과({chg_f:+.2f}% > +{max_chg:.1f}%)"
        if chg_f < min_chg:
            return f"진입하한 미달({chg_f:+.2f}% < {min_chg:+.1f}%)"

    tv = rec.get("eff_trading_value_eok")
    min_tv = float(at_cfg.get("min_trading_value_eok", 5.0))
    if tv is not None and float(tv) < min_tv:
        return f"유동성 미달({float(tv):.1f}억 < {min_tv:.1f}억)"

    open_str = str(at_cfg.get("autotrade_open_entry_time") or "09:08")
    cutoff_str = str(at_cfg.get("autotrade_entry_cutoff_time") or "14:00")
    alert_dt = radar.parse_iso_kst(rec.get("alert_ts"))
    if alert_dt is not None and alert_dt.weekday() < 5:
        ch, cm = radar._parse_hhmm(cutoff_str, default=(14, 0))
        now_min = alert_dt.hour * 60 + alert_dt.minute
        if (ch * 60 + cm) < now_min <= (15 * 60 + 30):
            return f"오후 진입마감 이후({cutoff_str}~)"

    win = str(rec.get("window") or "")
    if win == "offhours":
        return f"장전/야간 알림({open_str} 시초가 과열 또는 한도 스킵)"
    if "확산" in tier:
        return "대리확산주 차단·동시보유 한도·당일 중복 스킵"
    return "동시보유 한도·당일 중복·선도주 과열 스킵"


def _summarize_traces_for_date(data_dir: Path, target_date: str) -> dict[str, Any]:
    path = data_dir / "trace.jsonl"
    summary: dict[str, Any] = {
        "cycles_run": 0,
        "fresh_articles": 0,
        "clusters_evaluated": 0,
        "alerts_sent_in_cycles": 0,
        "flash_alerts_sent": 0,
        "premarket_digest_sent": 0,
        "overnight_queued": 0,
        "illiquid_blocked": 0,
        "illiquid_samples": [],
        "prepriced_blocked": 0,
        "prepriced_samples": [],
        "caution_only_blocked": 0,
        "caution_only_samples": [],
        "stock_dedup_blocked": 0,
        "sector_limit_blocked": 0,
        "dedup_blocked": 0,
        "substitute_buy_blocked": 0,
        "cutoff_buy_blocked": 0,
        "premarket_crash_blocked": 0,
        "paper_buys": 0,
        "paper_sells": 0,
        "kis_errors": 0,
    }
    if not path.exists():
        return summary

    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = raw.strip()
        if not s or target_date not in s:
            continue
        try:
            ev = json.loads(s)
        except Exception:
            continue
        ts_dt = radar.parse_iso_kst(ev.get("ts"))
        ev_date = ts_dt.strftime("%Y-%m-%d") if ts_dt is not None else str(ev.get("ts") or "")[:10]
        if ev_date != target_date:
            continue

        etype = str(ev.get("type") or "")
        if etype == "cycle_summary":
            summary["cycles_run"] += 1
            summary["fresh_articles"] += int(ev.get("fresh_count") or 0)
            summary["clusters_evaluated"] += int(ev.get("cluster_count") or 0)
            summary["alerts_sent_in_cycles"] += int(ev.get("alerts_sent") or 0)
        elif etype == "flash_alert_sent":
            summary["flash_alerts_sent"] += 1
        elif etype == "premarket_digest_sent":
            summary["premarket_digest_sent"] += 1
        elif etype == "overnight_queued":
            summary["overnight_queued"] += 1
        elif etype == "illiquid_skip" and ev.get("blocked", True):
            summary["illiquid_blocked"] += 1
            if len(summary["illiquid_samples"]) < 5:
                summary["illiquid_samples"].append(
                    f"{ev.get('theme_key') or ''}: {', '.join((ev.get('details') or [])[:2])}"
                )
        elif etype == "prepriced_skip":
            summary["prepriced_blocked"] += 1
            if len(summary["prepriced_samples"]) < 5:
                summary["prepriced_samples"].append(
                    f"{ev.get('theme_key') or ''}: {', '.join((ev.get('details') or [])[:2])}"
                )
        elif etype == "caution_only_skip":
            summary["caution_only_blocked"] += 1
            if len(summary["caution_only_samples"]) < 5:
                summary["caution_only_samples"].append(
                    f"{ev.get('theme_key') or ''}: {', '.join((ev.get('details') or [])[:2])}"
                )
        elif etype == "stock_dedup_decision" and ev.get("blocked"):
            summary["stock_dedup_blocked"] += 1
        elif etype == "sector_limit_decision" and ev.get("blocked"):
            summary["sector_limit_blocked"] += 1
        elif etype == "dedup_decision" and ev.get("blocked"):
            summary["dedup_blocked"] += 1
        elif etype == "paper_trade_event":
            act = str(ev.get("action") or "")
            if act == "buy":
                summary["paper_buys"] += 1
            elif act == "sell":
                summary["paper_sells"] += 1
            elif act == "skip_substitute":
                summary["substitute_buy_blocked"] += 1
            elif act == "skip_cutoff":
                summary["cutoff_buy_blocked"] += 1
            elif act == "skip_premarket_crash":
                summary["premarket_crash_blocked"] += 1
            elif "error" in act:
                summary["kis_errors"] += 1

    return summary


def build_analytics_bundle(
    data_dir: Path,
    repo_dir: Path | None = None,
    *,
    target_date: str | None = None,
    days: int = 7,
    now: datetime | None = None,
) -> dict[str, Any]:
    """특정 일자(기본 오늘 KST) 및 최근 N일간의 모의자동매매·알림 성과·필터 통계를 민감정보 없이 통합."""
    import paper_trader

    ref = radar.StateStore._now_kst(now)
    eff_repo = repo_dir if repo_dir is not None else Path(__file__).resolve().parent
    t_date = (target_date or ref.strftime("%Y-%m-%d")).strip()

    runtime_cfg = radar.load_runtime_config(data_dir)
    at_cfg = paper_trader.resolve_autotrade_config(runtime_cfg, daemon_mode=True)
    alert_limits = radar.resolve_alert_limits(runtime_cfg, daemon_mode=True)

    trader = paper_trader.PaperAutoTrader(data_dir, eff_repo)
    ov = trader.build_overview(now=ref, runtime_cfg=runtime_cfg)
    all_trades = trader.load_trades(limit=500)

    def _trade_exit_date(tr: dict[str, Any]) -> str:
        dt = radar.parse_iso_kst(tr.get("exit_ts"))
        return dt.strftime("%Y-%m-%d") if dt is not None else str(tr.get("exit_ts") or "")[:10]

    def _trade_entry_date(tr: dict[str, Any]) -> str:
        dt = radar.parse_iso_kst(tr.get("entry_ts"))
        return dt.strftime("%Y-%m-%d") if dt is not None else str(tr.get("entry_ts") or "")[:10]

    today_closed_trades: list[dict[str, Any]] = []
    for tr in reversed(all_trades):
        if _trade_exit_date(tr) == t_date:
            peak_r = float(tr.get("peak_ret_pct") or 0.0)
            gross_r = float(tr.get("gross_ret_pct") or 0.0)
            clean_tr = {
                "code": tr.get("code"),
                "name": tr.get("name"),
                "qty": tr.get("qty"),
                "buy_price": tr.get("buy_price"),
                "sell_price": tr.get("sell_price"),
                "highest_price": tr.get("highest_price"),
                "peak_ret_pct": peak_r,
                "gross_ret_pct": gross_r,
                "missed_from_peak_pct": round(max(0.0, peak_r - gross_r), 2),
                "net_pnl_krw": tr.get("net_pnl_krw"),
                "net_pnl_pct": tr.get("net_pnl_pct"),
                "fees_and_tax_krw": tr.get("fees_and_tax_krw"),
                "exit_reason": tr.get("exit_reason"),
                "entry_ts": tr.get("entry_ts"),
                "exit_ts": tr.get("exit_ts"),
                "theme_key": tr.get("theme_key"),
                "sector": tr.get("sector"),
                "title": tr.get("title"),
                "order_tier": tr.get("order_tier"),
                "git_commit": tr.get("git_commit") or None,
            }
            today_closed_trades.append(clean_tr)

    # 버전(commit)별 누적 성적표
    commit_buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for tr in all_trades:
        c_key = str(tr.get("git_commit") or "unknown")
        commit_buckets[c_key].append(tr)

    by_commit: dict[str, dict[str, Any]] = {}
    for c_key, rows in commit_buckets.items():
        wins = sum(1 for r in rows if int(r.get("net_pnl_krw") or 0) > 0)
        losses = len(rows) - wins
        net_sum = sum(int(r.get("net_pnl_krw") or 0) for r in rows)
        pcts = [float(r.get("net_pnl_pct") or 0.0) for r in rows]
        by_commit[c_key] = {
            "trades": len(rows),
            "wins": wins,
            "losses": losses,
            "win_rate_pct": round((wins / len(rows)) * 100.0, 1) if rows else None,
            "net_pnl_krw": net_sum,
            "avg_net_pnl_pct": _avg_or_none(pcts),
        }

    open_positions_clean = [
        {
            "code": p.get("code"),
            "name": p.get("name"),
            "qty": p.get("qty"),
            "avg_buy_price": p.get("avg_buy_price"),
            "last_price": p.get("last_price"),
            "highest_price": p.get("highest_price"),
            "peak_ret_pct": p.get("peak_ret_pct"),
            "breakeven_armed": bool(p.get("breakeven_armed")),
            "trailing_armed": bool(p.get("trailing_armed")),
            "unrealized_pnl_krw": p.get("unrealized_pnl_krw"),
            "unrealized_pnl_pct": p.get("unrealized_pnl_pct"),
            "entry_change_pct": p.get("entry_change_pct"),
            "entry_ts": p.get("entry_ts"),
            "entry_date": p.get("entry_date"),
            "theme_key": p.get("theme_key"),
            "sector": p.get("sector"),
            "title": p.get("title"),
            "order_tier": p.get("order_tier"),
            "revenue_link": p.get("revenue_link"),
            "git_commit": p.get("git_commit") or None,
        }
        for p in (ov.get("positions") or [])
    ]

    traded_codes_on_date: set[str] = set()
    for tr in all_trades:
        if _trade_entry_date(tr) == t_date or _trade_exit_date(tr) == t_date:
            traded_codes_on_date.add(str(tr.get("code") or "").zfill(6))
    for p in open_positions_clean:
        if str(p.get("entry_date") or "") == t_date or t_date == ref.strftime("%Y-%m-%d"):
            traded_codes_on_date.add(str(p.get("code") or "").zfill(6))

    perf_records = radar.PerformanceTracker(data_dir / "performance.jsonl").load_records()
    today_perf_rows: list[dict[str, Any]] = []
    traded_close_rets: list[float] = []
    skipped_bullish_close_rets: list[float] = []
    skip_reason_counts: Counter[str] = Counter()

    for r in perf_records:
        r_date = str(r.get("target_close_date") or r.get("alert_date") or "")
        if r_date != t_date and str(r.get("alert_date") or "") != t_date:
            continue
        code = str(r.get("code") or "").zfill(6)
        was_traded = code in traded_codes_on_date
        skip_hint = None if was_traded else _infer_skip_reason(r, at_cfg)
        if skip_hint and radar._normalize_perf_direction(r.get("direction")) == "상승수혜":
            skip_reason_counts[skip_hint.split("(")[0]] += 1

        rc = r.get("ret_close")
        if rc is not None:
            if was_traded:
                traded_close_rets.append(float(rc))
            elif radar._normalize_perf_direction(r.get("direction")) == "상승수혜" and r.get("buyable") is not False:
                skipped_bullish_close_rets.append(float(rc))

        today_perf_rows.append({
            "alert_ts": r.get("alert_ts"),
            "code": code,
            "name": r.get("name"),
            "direction": r.get("direction"),
            "order_tier": r.get("order_tier") or None,
            "revenue_link": r.get("revenue_link") or None,
            "sector": r.get("sector"),
            "theme_key": r.get("theme_key"),
            "title": r.get("title"),
            "catalyst_score": r.get("catalyst_score"),
            "alert_price": r.get("alert_price"),
            "alert_change_pct": r.get("alert_change_pct"),
            "eff_trading_value_eok": r.get("eff_trading_value_eok"),
            "buyable": r.get("buyable", True),
            "unbuyable_reason": r.get("unbuyable_reason"),
            "was_traded": was_traded,
            "skip_reason_hint": skip_hint,
            "ret_5m": r.get("ret_5m"),
            "ret_30m": r.get("ret_30m"),
            "ret_close": r.get("ret_close"),
            "ret_next_close": r.get("ret_next_close"),
            "git_commit": r.get("git_commit") or None,
        })

    cutoff_dt = ref - timedelta(days=max(1, int(days)))
    recent_perf = [
        r for r in perf_records
        if (radar.parse_iso_kst(r.get("alert_ts")) or ref) >= cutoff_dt
    ]
    rolling_perf_summary = radar.summarize_performance(recent_perf, recent_limit=20)
    rolling_perf_summary.pop("recent_records", None)

    today_realized_krw = sum(int(t.get("net_pnl_krw") or 0) for t in today_closed_trades)
    today_wins = sum(1 for t in today_closed_trades if int(t.get("net_pnl_krw") or 0) > 0)
    today_losses = len(today_closed_trades) - today_wins

    return {
        "schema_version": 1,
        "generated_at": ref.isoformat(),
        "target_date": t_date,
        "git_commit": radar.current_git_commit(eff_repo) or None,
        "broker_mode": ov.get("broker_mode"),
        "account": {
            "initial_capital_krw": ov.get("initial_capital_krw"),
            "cash_krw": ov.get("cash_krw"),
            "positions_value_krw": ov.get("positions_value_krw"),
            "total_equity_krw": ov.get("total_equity_krw"),
            "total_pnl_krw": ov.get("total_pnl_krw"),
            "total_pnl_pct": ov.get("total_pnl_pct"),
            "cumulative_realized_pnl_krw": ov.get("realized_pnl_krw"),
            "unrealized_pnl_krw": ov.get("unrealized_pnl_krw"),
            "total_fees_krw": ov.get("total_fees_krw"),
            "cumulative_trades": ov.get("trade_count"),
            "cumulative_wins": ov.get("win_count"),
            "cumulative_losses": ov.get("loss_count"),
            "cumulative_win_rate_pct": ov.get("win_rate_pct"),
        },
        "today_trade_summary": {
            "date": t_date,
            "closed_count": len(today_closed_trades),
            "wins": today_wins,
            "losses": today_losses,
            "realized_net_pnl_krw": today_realized_krw,
            "avg_net_pnl_pct": _avg_or_none([float(t.get("net_pnl_pct") or 0.0) for t in today_closed_trades]),
            "avg_peak_ret_pct": _avg_or_none([float(t.get("peak_ret_pct") or 0.0) for t in today_closed_trades]),
            "avg_missed_from_peak_pct": _avg_or_none(
                [float(t.get("missed_from_peak_pct") or 0.0) for t in today_closed_trades]
            ),
            "open_positions_count": len(open_positions_clean),
        },
        "today_closed_trades": today_closed_trades,
        "open_positions": open_positions_clean,
        "pending_open_entries": [
            {
                "code": e.get("code"),
                "name": (e.get("stock") or {}).get("name"),
                "theme_key": e.get("theme_key"),
                "catalyst_score": e.get("catalyst_score"),
                "queued_at": e.get("queued_at"),
            }
            for e in (ov.get("pending_open_entries") or [])
        ],
        "by_commit": by_commit,
        "traded_vs_skipped_summary": {
            "today_alert_stocks_total": len(today_perf_rows),
            "traded_alert_count": len(traded_close_rets),
            "traded_alert_avg_close_ret_pct": _avg_or_none(traded_close_rets),
            "skipped_buyable_bullish_count": len(skipped_bullish_close_rets),
            "skipped_buyable_bullish_avg_close_ret_pct": _avg_or_none(skipped_bullish_close_rets),
            "skip_reason_breakdown": dict(skip_reason_counts),
        },
        "today_alert_vs_trade_matrix": today_perf_rows,
        "rolling_performance_summary": rolling_perf_summary,
        "today_funnel": _summarize_traces_for_date(data_dir, t_date),
        "config_snapshot": {
            "min_catalyst_score": float(runtime_cfg.get("min_catalyst_score") or radar._env_float("MIN_CATALYST_SCORE", 7.5)),
            "max_market_alerts": alert_limits["max_market_alerts"],
            "max_offhours_alerts": alert_limits["max_offhours_alerts"],
            "max_bullish_prepriced_pct": float(
                runtime_cfg.get("max_bullish_prepriced_pct")
                if runtime_cfg.get("max_bullish_prepriced_pct") is not None
                else radar._env_float("MAX_BULLISH_PREPRICED_PCT", radar.DEFAULT_MAX_BULLISH_PREPRICED_PCT)
            ),
            "min_trading_value_eok": at_cfg["min_trading_value_eok"],
            "autotrade_enabled": at_cfg["autotrade_enabled"],
            "autotrade_only_second_order": at_cfg["autotrade_only_second_order"],
            "autotrade_order_amount_krw": at_cfg["autotrade_order_amount_krw"],
            "autotrade_max_positions": at_cfg["autotrade_max_positions"],
            "autotrade_min_change_pct": at_cfg["autotrade_min_change_pct"],
            "autotrade_max_change_pct": at_cfg["autotrade_max_change_pct"],
            "autotrade_take_profit_pct": at_cfg["autotrade_take_profit_pct"],
            "autotrade_trailing_stop_pct": at_cfg["autotrade_trailing_stop_pct"],
            "autotrade_stop_loss_pct": at_cfg["autotrade_stop_loss_pct"],
            "autotrade_breakeven_trigger_pct": at_cfg["autotrade_breakeven_trigger_pct"],
            "autotrade_breakeven_floor_pct": at_cfg["autotrade_breakeven_floor_pct"],
            "autotrade_open_entry_time": at_cfg["autotrade_open_entry_time"],
            "autotrade_entry_cutoff_time": at_cfg["autotrade_entry_cutoff_time"],
            "autotrade_eod_exit_enabled": at_cfg["autotrade_eod_exit_enabled"],
            "autotrade_eod_exit_time": at_cfg["autotrade_eod_exit_time"],
            "autotrade_premarket_max_drop_pct": at_cfg["autotrade_premarket_max_drop_pct"],
        },
    }


def format_ai_feedback_markdown(bundle: dict[str, Any]) -> str:
    """채팅창 복붙 및 GitHub .md 저장용 고밀도 마크다운 성적표 생성."""
    t_date = bundle.get("target_date") or "-"
    commit = bundle.get("git_commit") or "-"
    acc = bundle.get("account") or {}
    ts = bundle.get("today_trade_summary") or {}
    vs = bundle.get("traded_vs_skipped_summary") or {}
    fn = bundle.get("today_funnel") or {}
    cfg = bundle.get("config_snapshot") or {}

    def _f_pct(v: Any) -> str:
        return f"{float(v):+.2f}%" if v is not None else "-"

    be_trig = float(cfg.get("autotrade_breakeven_trigger_pct") or 0.0)
    be_flr = float(cfg.get("autotrade_breakeven_floor_pct") or 0.0)
    be_txt = f" · 본전보호 +{be_trig:.1f}%→{be_flr:+.1f}%" if be_trig > 0.0 else ""

    lines = [
        f"# 📊 News Alpha Radar 일일 통합 성적표 ({t_date} · commit `{commit}`)",
        "",
        "## 1. 계좌 & 당일 자동매매 요약",
        f"- **총 평가자산**: {int(acc.get('total_equity_krw') or 0):,}원 ({_f_pct(acc.get('total_pnl_pct'))}, 누적 실현 {int(acc.get('cumulative_realized_pnl_krw') or 0):+,}원)",
        f"- **당일 청산 성적**: {ts.get('closed_count', 0)}건 ({ts.get('wins', 0)}승 {ts.get('losses', 0)}패) | 당일 순손익 **{int(ts.get('realized_net_pnl_krw') or 0):+,}원** (평균 {_f_pct(ts.get('avg_net_pnl_pct'))})",
        f"- **보유 중 최고점 대비 반납폭**: 평균 고점 {_f_pct(ts.get('avg_peak_ret_pct'))} / 평균 반납 -{(ts.get('avg_missed_from_peak_pct') or 0.0):.2f}%p",
        f"- **현재 적용 규칙**: 주문 {int(cfg.get('autotrade_order_amount_krw') or 0)//10000}만원 · 진입창 {cfg.get('autotrade_open_entry_time') or '09:08'}~{cfg.get('autotrade_entry_cutoff_time') or '14:00'} [{cfg.get('autotrade_min_change_pct')}% ~ +{cfg.get('autotrade_max_change_pct')}%] · 익절 +{cfg.get('autotrade_take_profit_pct')}%(트레일링 -{cfg.get('autotrade_trailing_stop_pct')}%){be_txt} · 손절 -{cfg.get('autotrade_stop_loss_pct')}% · 유동성 ≥{cfg.get('min_trading_value_eok')}억",
        "",
        "## 2. 당일 청산 내역 (`paper_trades.jsonl`)",
    ]

    closed = bundle.get("today_closed_trades") or []
    if not closed:
        lines.append("- 당일 청산된 모의매매 내역 없음")
    else:
        lines.append("| 종목 | 매수가→매도가 | 순손익 | 보유중 고점 | 청산 사유 | 테마 |")
        lines.append("|---|---:|---:|---:|---|---|")
        for t in closed:
            lines.append(
                f"| {t.get('name')}({t.get('code')}) | {int(t.get('buy_price') or 0):,}→{int(t.get('sell_price') or 0):,}원 | "
                f"**{int(t.get('net_pnl_krw') or 0):+,}원 ({_f_pct(t.get('net_pnl_pct'))})** | {_f_pct(t.get('peak_ret_pct'))} | "
                f"{t.get('exit_reason')} | {t.get('theme_key') or t.get('title') or '-'} |"
            )

    open_pos = bundle.get("open_positions") or []
    if open_pos:
        lines.append("")
        lines.append("## 2-1. 현재 보유 중인 포지션")
        for p in open_pos:
            lines.append(
                f"- **{p.get('name')}({p.get('code')})**: {p.get('qty')}주 · 매수 {int(p.get('avg_buy_price') or 0):,}원 → 현재 {int(p.get('last_price') or 0):,}원 "
                f"({_f_pct(p.get('unrealized_pnl_pct'))}, 고점 {_f_pct(p.get('peak_ret_pct'))})"
            )

    lines.extend([
        "",
        "## 3. 알림 종목 전체 vs 실제 매수 대조 (`performance.jsonl`)",
        f"- **매수 진입 종목 종가 평균**: {_f_pct(vs.get('traded_alert_avg_close_ret_pct'))} ({vs.get('traded_alert_count', 0)}건)  vs  "
        f"**미진입 상승후보 종가 평균**: {_f_pct(vs.get('skipped_buyable_bullish_avg_close_ret_pct'))} ({vs.get('skipped_buyable_bullish_count', 0)}건)",
    ])

    matrix = bundle.get("today_alert_vs_trade_matrix") or []
    if not matrix:
        lines.append("- 당일 발송된 알림 종목 기록 없음")
    else:
        lines.append("| 종목 | 구분 | 알림시점 등락 | 매매여부 / 미진입 사유 | +5분 | +30분 | 종가 | 익일종가 |")
        lines.append("|---|---|---:|---|---:|---:|---:|---:|")
        for r in matrix[:25]:
            tier_txt = f"{r.get('direction') or ''}/{r.get('order_tier') or '-'}"
            status_txt = "✅ **자동매수**" if r.get("was_traded") else (r.get("skip_reason_hint") or "미진입")
            lines.append(
                f"| {r.get('name')}({r.get('code')}) | {tier_txt} | {_f_pct(r.get('alert_change_pct'))} | {status_txt} | "
                f"{_f_pct(r.get('ret_5m'))} | {_f_pct(r.get('ret_30m'))} | {_f_pct(r.get('ret_close'))} | {_f_pct(r.get('ret_next_close'))} |"
            )

    lines.extend([
        "",
        "## 4. 당일 필터 차단 통계 (`trace.jsonl`)",
        f"- 수집 기사 {fn.get('fresh_articles', 0)}건 / 평가 클러스터 {fn.get('clusters_evaluated', 0)}건 / 발송 알림 {fn.get('alerts_sent_in_cycles', 0)}건",
        f"- 옥석가리기 노이즈 차단 {fn.get('caution_only_blocked', 0)}건 · 대리소외주 매수 차단 {fn.get('substitute_buy_blocked', 0)}건 · 유동성 미달 {fn.get('illiquid_blocked', 0)}건 · 급등 추격 {fn.get('prepriced_blocked', 0)}건 · 24h 종목중복 {fn.get('stock_dedup_blocked', 0)}건 · 섹터한도 {fn.get('sector_limit_blocked', 0)}건",
    ])

    by_c = bundle.get("by_commit") or {}
    if by_c:
        lines.append("")
        lines.append("## 5. 버전(commit)별 누적 성적 비교")
        for ck, cv in by_c.items():
            lines.append(
                f"- `{ck}`: {cv.get('trades')}전 {cv.get('wins')}승 {cv.get('losses')}패 "
                f"(승률 {cv.get('win_rate_pct') if cv.get('win_rate_pct') is not None else '-'}%, 순손익 {int(cv.get('net_pnl_krw') or 0):+,}원, 평균 {_f_pct(cv.get('avg_net_pnl_pct'))})"
            )

    return "\n".join(lines) + "\n"


def format_daily_telegram_summary_html(
    bundle: dict[str, Any],
    *,
    sync_status: dict[str, Any] | None = None,
) -> str:
    """평일 15:35 장 마감 후 및 텔레그램 `리포트` 명령 입력 시 발송할 요약 메시지."""
    esc = lambda s: html.escape(str(s or ""), quote=False)
    t_date = bundle.get("target_date") or "-"
    acc = bundle.get("account") or {}
    ts = bundle.get("today_trade_summary") or {}
    vs = bundle.get("traded_vs_skipped_summary") or {}
    fn = bundle.get("today_funnel") or {}

    def _f_pct(v: Any) -> str:
        return f"{float(v):+.2f}%" if v is not None else "-"

    today_pnl = int(ts.get("realized_net_pnl_krw") or 0)
    pnl_icon = "📈" if today_pnl > 0 else ("📉" if today_pnl < 0 else "📊")
    lines = [
        f"{pnl_icon} <b>[일일 자동매매 &amp; 알림 성적표 · {esc(t_date)}]</b>",
        f"💰 <b>총 평가자산</b>: {int(acc.get('total_equity_krw') or 0):,}원 (<b>{_f_pct(acc.get('total_pnl_pct'))}</b>)",
        f"• <b>오늘 실현 순손익</b>: <b>{today_pnl:+,}원</b> ({ts.get('closed_count', 0)}전 {ts.get('wins', 0)}승 {ts.get('losses', 0)}패 · 수수료·세금 차감)",
        f"• <b>누적 실현 순손익</b>: {int(acc.get('cumulative_realized_pnl_krw') or 0):+,}원 (통산 승률 {acc.get('cumulative_win_rate_pct') if acc.get('cumulative_win_rate_pct') is not None else '-'}%)",
    ]

    closed = bundle.get("today_closed_trades") or []
    if closed:
        lines.append("")
        lines.append("<b>[오늘 청산 내역]</b>")
        for t in closed[:6]:
            lines.append(
                f"• <b>{esc(t.get('name'))}({esc(t.get('code'))})</b>: "
                f"{int(t.get('net_pnl_krw') or 0):+,}원 ({_f_pct(t.get('net_pnl_pct'))}, 고점 {_f_pct(t.get('peak_ret_pct'))}) — {esc(t.get('exit_reason'))}"
            )

    open_pos = bundle.get("open_positions") or []
    if open_pos:
        lines.append("")
        lines.append("<b>[현재 보유 중]</b>")
        for p in open_pos[:5]:
            lines.append(
                f"• <b>{esc(p.get('name'))}({esc(p.get('code'))})</b>: "
                f"{int(p.get('unrealized_pnl_krw') or 0):+,}원 ({_f_pct(p.get('unrealized_pnl_pct'))})"
            )

    lines.extend([
        "",
        "<b>[알림 전체 vs 자동매수 대조]</b>",
        f"• 오늘 알림 종목 {vs.get('today_alert_stocks_total', 0)}건 중 매수 종가평균 <b>{_f_pct(vs.get('traded_alert_avg_close_ret_pct'))}</b> / 미진입 상승후보 종가평균 <b>{_f_pct(vs.get('skipped_buyable_bullish_avg_close_ret_pct'))}</b>",
        f"• 필터 차단: 옥석노이즈 {fn.get('caution_only_blocked', 0)}건 · 대리소외주 {fn.get('substitute_buy_blocked', 0)}건 · 유동성 {fn.get('illiquid_blocked', 0)}건 · 급등추격 {fn.get('prepriced_blocked', 0)}건 · 중복/섹터 {int(fn.get('stock_dedup_blocked') or 0) + int(fn.get('sector_limit_blocked') or 0)}건",
    ])

    if sync_status is not None:
        if sync_status.get("ok"):
            lines.append(f"☁️ 비공개 GitHub 백업 완료: <code>{esc(sync_status.get('repo'))}</code>")
        elif sync_status.get("configured") is False:
            lines.append("💡 <i>대시보드 [모의 자동매매] 탭에서 GitHub 토큰을 1회 등록하면 성적표가 비공개 저장소에 자동 백업됩니다.</i>")

    return "\n".join(lines)


def _github_put_file(
    *,
    owner_repo: str,
    rel_path: str,
    content_bytes: bytes,
    commit_message: str,
    token: str,
    timeout: int = 15,
) -> bool:
    """GitHub Contents REST API로 단일 파일 생성/업데이트 (내용 동일 시 PUT 생략)."""
    encoded_path = "/".join(urllib.parse.quote(seg, safe="") for seg in rel_path.strip("/").split("/"))
    api_url = f"{GITHUB_API_BASE}/repos/{owner_repo}/contents/{encoded_path}"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": radar.USER_AGENT,
    }

    existing_sha: str | None = None
    get_req = urllib.request.Request(api_url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(get_req, timeout=timeout) as resp:
            cur_obj = json.loads(resp.read().decode("utf-8"))
            existing_sha = cur_obj.get("sha")
            raw_b64 = str(cur_obj.get("content") or "").replace("\n", "")
            if raw_b64:
                try:
                    if base64.b64decode(raw_b64) == content_bytes:
                        return False
                except Exception:
                    pass
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            detail = exc.read().decode("utf-8", errors="ignore")[:200]
            raise RuntimeError(f"GitHub 조회 오류 (HTTP {exc.code}): {detail}") from exc

    body_dict: dict[str, Any] = {
        "message": commit_message,
        "content": base64.b64encode(content_bytes).decode("ascii"),
    }
    if existing_sha:
        body_dict["sha"] = existing_sha

    put_req = urllib.request.Request(
        api_url,
        data=json.dumps(body_dict).encode("utf-8"),
        headers={**headers, "Content-Type": "application/json; charset=utf-8"},
        method="PUT",
    )
    try:
        with urllib.request.urlopen(put_req, timeout=timeout) as resp:
            _ = resp.read()
        return True
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")[:220]
        raise RuntimeError(f"GitHub 업로드 오류 (HTTP {exc.code}): {detail}") from exc


def _daily_report_cache_path(data_dir: Path, target_date: str) -> Path:
    return data_dir / "daily_reports" / f"{target_date}.json"


def _save_local_daily_bundle(data_dir: Path, target_date: str, bundle: dict[str, Any]) -> None:
    try:
        path = _daily_report_cache_path(data_dir, target_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        radar.atomic_write_text(path, json.dumps(bundle, ensure_ascii=False, indent=2) + "\n")
    except Exception:
        pass


def _load_local_daily_bundle(data_dir: Path, target_date: str) -> dict[str, Any] | None:
    path = _daily_report_cache_path(data_dir, target_date)
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else None
    except Exception:
        return None


def _github_get_file_bytes(
    *,
    owner_repo: str,
    rel_path: str,
    token: str,
    timeout: int = 15,
) -> bytes | None:
    """GitHub Contents REST API로 단일 파일 원본 바이트 조회 (없으면 None)."""
    encoded_path = "/".join(urllib.parse.quote(seg, safe="") for seg in rel_path.strip("/").split("/"))
    api_url = f"{GITHUB_API_BASE}/repos/{owner_repo}/contents/{encoded_path}"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": radar.USER_AGENT,
    }
    get_req = urllib.request.Request(api_url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(get_req, timeout=timeout) as resp:
            cur_obj = json.loads(resp.read().decode("utf-8"))
            raw_b64 = str(cur_obj.get("content") or "").replace("\n", "")
            return base64.b64decode(raw_b64) if raw_b64 else None
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        return None
    except Exception:
        return None


def _merge_backfill_bundle(existing: dict[str, Any], fresh: dict[str, Any]) -> dict[str, Any]:
    """직전 거래일 익일종가(ret_next_close) 백필 시 당시 계좌·보유포지션·설정·필터 통계를 보존하고 측정값만 병합."""
    merged = json.loads(json.dumps(existing, ensure_ascii=False))

    existing_matrix = merged.get("today_alert_vs_trade_matrix") or []
    fresh_matrix = fresh.get("today_alert_vs_trade_matrix") or []
    if existing_matrix and fresh_matrix:
        fresh_by_key = {
            (str(r.get("alert_ts") or ""), str(r.get("code") or "").zfill(6)): r
            for r in fresh_matrix
        }
        seen_keys: set[tuple[str, str]] = set()
        updated_matrix: list[dict[str, Any]] = []
        for row in existing_matrix:
            key = (str(row.get("alert_ts") or ""), str(row.get("code") or "").zfill(6))
            seen_keys.add(key)
            nrow = dict(row)
            f_row = fresh_by_key.get(key)
            if f_row is not None:
                for fld in ("ret_5m", "ret_30m", "ret_close", "ret_next_close"):
                    if f_row.get(fld) is not None:
                        nrow[fld] = f_row[fld]
            updated_matrix.append(nrow)
        for f_row in fresh_matrix:
            key = (str(f_row.get("alert_ts") or ""), str(f_row.get("code") or "").zfill(6))
            if key not in seen_keys:
                updated_matrix.append(dict(f_row))
        merged["today_alert_vs_trade_matrix"] = updated_matrix
        if not merged.get("traded_vs_skipped_summary") and fresh.get("traded_vs_skipped_summary"):
            merged["traded_vs_skipped_summary"] = fresh["traded_vs_skipped_summary"]
    elif fresh_matrix:
        merged["today_alert_vs_trade_matrix"] = fresh_matrix
        if fresh.get("traded_vs_skipped_summary"):
            merged["traded_vs_skipped_summary"] = fresh["traded_vs_skipped_summary"]

    if fresh.get("rolling_performance_summary"):
        merged["rolling_performance_summary"] = fresh["rolling_performance_summary"]
    if fresh.get("by_commit"):
        merged["by_commit"] = fresh["by_commit"]

    old_closed = merged.get("today_closed_trades") or []
    new_closed = fresh.get("today_closed_trades") or []
    if len(new_closed) > len(old_closed):
        merged["today_closed_trades"] = new_closed
        old_ts = dict(merged.get("today_trade_summary") or {})
        new_ts = dict(fresh.get("today_trade_summary") or {})
        if "open_positions_count" in old_ts:
            new_ts["open_positions_count"] = old_ts["open_positions_count"]
        merged["today_trade_summary"] = new_ts

    # trace.jsonl 로테이션으로 과거 일자 필터 통계가 0으로 유실되는 덮어쓰기 방지
    old_fn = merged.get("today_funnel") or {}
    new_fn = fresh.get("today_funnel") or {}
    old_score = (
        int(old_fn.get("fresh_articles") or 0)
        + int(old_fn.get("alerts_sent_in_cycles") or 0) * 100
        + int(old_fn.get("cycles_run") or 0)
    )
    new_score = (
        int(new_fn.get("fresh_articles") or 0)
        + int(new_fn.get("alerts_sent_in_cycles") or 0) * 100
        + int(new_fn.get("cycles_run") or 0)
    )
    if new_score > old_score:
        merged["today_funnel"] = new_fn

    if fresh.get("generated_at"):
        merged["backfilled_at"] = fresh["generated_at"]
    return merged


def sync_report_to_github(
    data_dir: Path,
    repo_dir: Path | None = None,
    *,
    target_date: str | None = None,
    include_prev_trading_day: bool = True,
    now: datetime | None = None,
) -> dict[str, Any]:
    """비공개 GitHub 로그 저장소(reports/YYYY-MM-DD.json, .md, latest.json)로 성적표 업로드."""
    ref = radar.StateStore._now_kst(now)
    eff_repo = repo_dir if repo_dir is not None else Path(__file__).resolve().parent
    cfg = resolve_github_sync_config(eff_repo)
    if not cfg["configured"]:
        return {"ok": False, "configured": False, "repo": cfg["repo"], "error": "GITHUB_REPORT_TOKEN이 설정되지 않았습니다."}

    t_date = (target_date or ref.strftime("%Y-%m-%d")).strip()
    st = _load_sync_state(data_dir)
    updated_files: list[str] = []

    try:
        bundle_today = build_analytics_bundle(data_dir, eff_repo, target_date=t_date, now=ref)
        _save_local_daily_bundle(data_dir, t_date, bundle_today)
        md_today = format_ai_feedback_markdown(bundle_today)
        json_bytes = (json.dumps(bundle_today, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        md_bytes = md_today.encode("utf-8")

        if _github_put_file(
            owner_repo=cfg["repo"],
            rel_path=f"reports/{t_date}.json",
            content_bytes=json_bytes,
            commit_message=f"report: update {t_date}.json",
            token=cfg["token"],
        ):
            updated_files.append(f"reports/{t_date}.json")

        if _github_put_file(
            owner_repo=cfg["repo"],
            rel_path=f"reports/{t_date}.md",
            content_bytes=md_bytes,
            commit_message=f"report: update {t_date}.md",
            token=cfg["token"],
        ):
            updated_files.append(f"reports/{t_date}.md")

        if _github_put_file(
            owner_repo=cfg["repo"],
            rel_path="reports/latest.json",
            content_bytes=json_bytes,
            commit_message=f"report: update latest.json ({t_date})",
            token=cfg["token"],
        ):
            updated_files.append("reports/latest.json")

        # 직전 거래일의 익일 종가(ret_next_close) 측정값만 최신화 반영 (당시 계좌·보유·설정·필터 통계 보존)
        if include_prev_trading_day:
            try:
                cur_d = date.fromisoformat(t_date)
                prev_d = previous_trading_date(cur_d).isoformat()
                bundle_prev_fresh = build_analytics_bundle(data_dir, eff_repo, target_date=prev_d, now=ref)
                existing_prev = _load_local_daily_bundle(data_dir, prev_d)
                if existing_prev is None:
                    remote_prev_bytes = _github_get_file_bytes(
                        owner_repo=cfg["repo"],
                        rel_path=f"reports/{prev_d}.json",
                        token=cfg["token"],
                    )
                    if remote_prev_bytes:
                        try:
                            parsed_prev = json.loads(remote_prev_bytes.decode("utf-8"))
                            if isinstance(parsed_prev, dict):
                                existing_prev = parsed_prev
                        except Exception:
                            existing_prev = None
                if existing_prev is not None and str(existing_prev.get("target_date") or "") == prev_d:
                    bundle_prev = _merge_backfill_bundle(existing_prev, bundle_prev_fresh)
                else:
                    bundle_prev = bundle_prev_fresh
                if bundle_prev.get("today_closed_trades") or bundle_prev.get("today_alert_vs_trade_matrix"):
                    _save_local_daily_bundle(data_dir, prev_d, bundle_prev)
                    prev_json = (json.dumps(bundle_prev, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
                    prev_md = format_ai_feedback_markdown(bundle_prev).encode("utf-8")
                    if _github_put_file(
                        owner_repo=cfg["repo"],
                        rel_path=f"reports/{prev_d}.json",
                        content_bytes=prev_json,
                        commit_message=f"report: backfill next-close {prev_d}.json",
                        token=cfg["token"],
                    ):
                        updated_files.append(f"reports/{prev_d}.json")
                    if _github_put_file(
                        owner_repo=cfg["repo"],
                        rel_path=f"reports/{prev_d}.md",
                        content_bytes=prev_md,
                        commit_message=f"report: backfill next-close {prev_d}.md",
                        token=cfg["token"],
                    ):
                        updated_files.append(f"reports/{prev_d}.md")
            except Exception as prev_exc:
                logger.warning("직전 거래일 성적표 보정 업로드 스킵: %s", prev_exc)

        st["last_synced_at"] = ref.isoformat()
        st["last_synced_date"] = t_date
        st["last_sync_error"] = None
        st["repo"] = cfg["repo"]
        _save_sync_state(data_dir, st)
        return {
            "ok": True,
            "configured": True,
            "repo": cfg["repo"],
            "target_date": t_date,
            "updated_files": updated_files,
            "last_synced_at": st["last_synced_at"],
        }
    except Exception as exc:
        err_str = str(exc)[:220]
        st["last_sync_error"] = err_str
        _save_sync_state(data_dir, st)
        logger.warning("GitHub 성적표 자동 백업 실패: %s", err_str)
        return {
            "ok": False,
            "configured": True,
            "repo": cfg["repo"],
            "error": err_str,
        }


def try_background_github_sync(
    data_dir: Path,
    repo_dir: Path | None = None,
    *,
    now: datetime | None = None,
) -> None:
    """매수·매도 체결 직후 메인 루프를 블로킹하지 않고 비공개 GitHub 최신 스냅샷을 백그라운드 갱신."""
    eff_repo = repo_dir if repo_dir is not None else Path(__file__).resolve().parent
    cfg = resolve_github_sync_config(eff_repo)
    if not cfg["configured"]:
        return
    if not _BG_SYNC_LOCK.acquire(blocking=False):
        return
    ref = radar.StateStore._now_kst(now)

    def _worker() -> None:
        try:
            sync_report_to_github(
                data_dir,
                eff_repo,
                include_prev_trading_day=False,
                now=ref,
            )
        finally:
            _BG_SYNC_LOCK.release()

    threading.Thread(target=_worker, daemon=True).start()


def maybe_run_daily_report_and_sync(
    data_dir: Path,
    repo_dir: Path | None = None,
    *,
    now: datetime | None = None,
    daemon_mode: bool = False,
    send_fn: Callable[[str], Any] | None = None,
) -> dict[str, bool]:
    """평일 장 마감 후(15:35 KST~) 일일 텔레그램 성적표 1회 발송 및 비공개 GitHub 자동 백업."""
    ref = radar.StateStore._now_kst(now)
    out = {"telegram_sent": False, "github_synced": False}
    if not daemon_mode or ref.weekday() >= 5:
        return out

    eff_repo = repo_dir if repo_dir is not None else Path(__file__).resolve().parent
    today_str = ref.strftime("%Y-%m-%d")
    hm = (ref.hour, ref.minute)
    if hm < (15, 35):
        return out

    st = _load_sync_state(data_dir)
    sync_cfg = resolve_github_sync_config(eff_repo)

    # 1) 비공개 GitHub 장마감 정기 백업 (15:35 이후 하루 1회 보장)
    sync_res: dict[str, Any] | None = None
    if sync_cfg["configured"] and st.get("last_eod_sync_date") != today_str:
        sync_res = sync_report_to_github(
            data_dir,
            eff_repo,
            target_date=today_str,
            include_prev_trading_day=True,
            now=ref,
        )
        if sync_res.get("ok"):
            st = _load_sync_state(data_dir)
            st["last_eod_sync_date"] = today_str
            _save_sync_state(data_dir, st)
            out["github_synced"] = True

    # 2) 평일 15:35~16:30 장 마감 요약 텔레그램 브리핑 (당일 실제 알림/매매가 1건 이상 있을 때만 1회 발송)
    if (
        send_fn is not None
        and (15, 35) <= hm <= (16, 30)
        and st.get("last_daily_telegram_date") != today_str
    ):
        bundle = build_analytics_bundle(data_dir, eff_repo, target_date=today_str, now=ref)
        has_activity = bool(
            bundle.get("today_closed_trades")
            or bundle.get("open_positions")
            or bundle.get("today_alert_vs_trade_matrix")
        )
        st = _load_sync_state(data_dir)
        st["last_daily_telegram_date"] = today_str
        _save_sync_state(data_dir, st)
        if has_activity:
            if sync_res is None:
                sync_res = {"ok": False, "configured": sync_cfg["configured"], "repo": sync_cfg["repo"]}
            try:
                send_fn(format_daily_telegram_summary_html(bundle, sync_status=sync_res))
                out["telegram_sent"] = True
            except Exception as exc:
                logger.warning("일일 마감 성적표 텔레그램 발송 실패: %s", exc)

    return out
