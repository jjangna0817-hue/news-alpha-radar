#!/usr/bin/env python3
"""News Alpha Radar — 모의투자 자동매매 엔진 (내장 실전형 가상계좌 + 한국투자증권 KIS 공식 모의투자 API 겸용).

특징:
- 외부 패키지 0개 (Python 3.11+ 표준 라이브러리 전용, 유지비 0원)
- 실전 안전 잠금: 한국투자증권 API 연결 시에도 모의투자 서버(openapivts.koreainvestment.com:29443) 전용으로 고정
- 1차 공시·단말기 뒷북 추격 원천 차단: '2차 파급' + 미반영 초입(-2% ~ +4%) + 유동성 기준 통과 종목만 자동 진입
- 단타 기본 청산 엔진: +5% 도달 시 트레일링 활성화(고점 대비 -2% 이탈 시 익절) / -3% 칼손절 / 15:15 장마감 전 전량 청산
- 현실적 체결 모델: 호가단위(KRX 틱) 슬리피지 + 매수·매도 수수료 및 증권거래세(약 0.21%) 차감 반영
"""

from __future__ import annotations

import contextlib
import html
import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

try:
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]

import radar

logger = logging.getLogger("news_alpha_trader")

KIS_VTS_BASE_URL = "https://openapivts.koreainvestment.com:29443"
KIS_TR_BUY_VTS = "VTTC0802U"
KIS_TR_SELL_VTS = "VTTC0801U"
KIS_TR_BALANCE_VTS = "VTTC8434R"

DEFAULT_INITIAL_CAPITAL_KRW = 10_000_000
DEFAULT_ORDER_AMOUNT_KRW = 1_000_000
DEFAULT_MAX_POSITIONS = 4
DEFAULT_ENTRY_MIN_CHANGE_PCT = -2.0
DEFAULT_ENTRY_MAX_CHANGE_PCT = 4.0
DEFAULT_TAKE_PROFIT_PCT = 5.0
DEFAULT_TRAILING_STOP_PCT = 2.0
DEFAULT_STOP_LOSS_PCT = 3.0
DEFAULT_OPEN_ENTRY_TIME = "09:08"
DEFAULT_ENTRY_CUTOFF_TIME = "14:00"
DEFAULT_EOD_EXIT_ENABLED = True
DEFAULT_EOD_EXIT_TIME = "15:15"
DEFAULT_DAILY_MAX_LOSS_PCT = 3.0

BUY_FEE_RATE = 0.00015
SELL_FEE_AND_TAX_RATE = 0.00195
MAX_TRADE_HISTORY = 2000


@contextlib.contextmanager
def _account_file_lock(data_dir: Path):
    data_dir.mkdir(parents=True, exist_ok=True)
    lock_path = data_dir / ".paper_account.lock"
    fp = lock_path.open("a+", encoding="utf-8")
    try:
        if fcntl is not None:
            fcntl.flock(fp.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        if fcntl is not None:
            try:
                fcntl.flock(fp.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        fp.close()


def krx_tick_size(price: int) -> int:
    """KRX 유가증권·코스닥 통합 호가단위."""
    px = max(1, int(price))
    if px < 2_000:
        return 1
    if px < 5_000:
        return 5
    if px < 20_000:
        return 10
    if px < 50_000:
        return 50
    if px < 200_000:
        return 100
    if px < 500_000:
        return 500
    return 1_000


def apply_execution_slippage(price: int, *, side: str, eff_trading_value_krw: int = 0) -> int:
    """내장 모의계좌 체결 시 유동성 구간별 현실적 호가 슬리피지(1~3틱) 적용."""
    px = max(1, int(price))
    tick = krx_tick_size(px)
    if eff_trading_value_krw >= 5_000_000_000:
        ticks = 1
    elif eff_trading_value_krw >= 1_500_000_000:
        ticks = 2
    else:
        ticks = 3 if side == "buy" else 2

    if side == "buy":
        raw = px + ticks * tick
        t2 = krx_tick_size(raw)
        return ((raw + t2 - 1) // t2) * t2
    raw = max(tick, px - ticks * tick)
    t2 = krx_tick_size(raw)
    return max(t2, (raw // t2) * t2)


def resolve_kis_credentials(repo_dir: Path | None = None) -> dict[str, str]:
    """환경변수 또는 .env에서 한국투자증권 모의투자 키(KIS_APP_KEY, KIS_APP_SECRET, KIS_CANO) 조회."""
    def _val(key: str) -> str:
        env_v = (os.getenv(key) or "").strip()
        if env_v:
            return env_v
        if repo_dir is not None:
            env_file = repo_dir / ".env"
            if env_file.exists():
                for raw_line in env_file.read_text(encoding="utf-8", errors="ignore").splitlines():
                    line = raw_line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    if k.strip() == key:
                        return v.strip().strip('"').strip("'")
        return ""

    app_key = _val("KIS_APP_KEY")
    app_secret = _val("KIS_APP_SECRET")
    raw_cano = _val("KIS_CANO").replace(" ", "")
    prdt = _val("KIS_ACNT_PRDT_CD") or "01"
    if "-" in raw_cano:
        parts = raw_cano.split("-", 1)
        cano = parts[0].strip()
        if parts[1].strip():
            prdt = parts[1].strip()
    elif len(raw_cano) == 10 and raw_cano.isdigit():
        cano = raw_cano[:8]
        prdt = raw_cano[8:]
    else:
        cano = raw_cano

    configured = bool(
        app_key
        and app_secret
        and len(cano) == 8
        and cano.isdigit()
        and "여기에_" not in app_key
        and "여기에_" not in app_secret
    )
    masked = f"{cano[:4]}****-{prdt}" if len(cano) == 8 else ""
    return {
        "configured": "true" if configured else "",
        "app_key": app_key if configured else "",
        "app_secret": app_secret if configured else "",
        "cano": cano if configured else "",
        "acnt_prdt_cd": prdt,
        "cano_masked": masked if configured else "",
    }


def resolve_autotrade_config(
    runtime_cfg: dict[str, Any] | None = None,
    *,
    daemon_mode: bool = True,
) -> dict[str, Any]:
    cfg = runtime_cfg or {}

    if cfg.get("autotrade_enabled") is not None:
        enabled = bool(cfg["autotrade_enabled"])
    else:
        raw_env = (os.getenv("AUTOTRADE_ENABLED") or "").strip().lower()
        if raw_env in ("1", "true", "yes", "on"):
            enabled = True
        elif raw_env in ("0", "false", "no", "off"):
            enabled = False
        else:
            enabled = bool(daemon_mode)

    if cfg.get("autotrade_only_second_order") is not None:
        only_2nd = bool(cfg["autotrade_only_second_order"])
    else:
        so_env = (os.getenv("AUTOTRADE_ONLY_SECOND_ORDER") or "true").strip().lower()
        only_2nd = so_env not in ("0", "false", "no", "off")

    if cfg.get("autotrade_eod_exit_enabled") is not None:
        eod_enabled = bool(cfg["autotrade_eod_exit_enabled"])
    else:
        eod_env = (os.getenv("AUTOTRADE_EOD_EXIT_ENABLED") or "true").strip().lower()
        eod_enabled = eod_env not in ("0", "false", "no", "off")

    oh, om = radar._parse_hhmm(
        str(cfg.get("autotrade_open_entry_time") or radar._env_str("AUTOTRADE_OPEN_ENTRY_TIME", DEFAULT_OPEN_ENTRY_TIME)),
        default=(9, 8),
    )
    ch, cm = radar._parse_hhmm(
        str(cfg.get("autotrade_entry_cutoff_time") or radar._env_str("AUTOTRADE_ENTRY_CUTOFF_TIME", DEFAULT_ENTRY_CUTOFF_TIME)),
        default=(14, 0),
    )
    open_min = max(9 * 60, min(14 * 60, oh * 60 + om))
    cutoff_min = max(open_min + 10, min(15 * 60 + 15, ch * 60 + cm))
    oh, om = divmod(open_min, 60)
    ch, cm = divmod(cutoff_min, 60)

    eh, em = radar._parse_hhmm(
        str(cfg.get("autotrade_eod_exit_time") or radar._env_str("AUTOTRADE_EOD_EXIT_TIME", DEFAULT_EOD_EXIT_TIME)),
        default=(15, 15),
    )

    order_amt = int(
        cfg["autotrade_order_amount_krw"]
        if cfg.get("autotrade_order_amount_krw") is not None
        else radar._env_int("AUTOTRADE_ORDER_AMOUNT_KRW", DEFAULT_ORDER_AMOUNT_KRW)
    )
    max_pos = int(
        cfg["autotrade_max_positions"]
        if cfg.get("autotrade_max_positions") is not None
        else radar._env_int("AUTOTRADE_MAX_POSITIONS", DEFAULT_MAX_POSITIONS)
    )
    min_chg = float(
        cfg["autotrade_min_change_pct"]
        if cfg.get("autotrade_min_change_pct") is not None
        else radar._env_float("AUTOTRADE_MIN_CHANGE_PCT", DEFAULT_ENTRY_MIN_CHANGE_PCT)
    )
    max_chg = float(
        cfg["autotrade_max_change_pct"]
        if cfg.get("autotrade_max_change_pct") is not None
        else radar._env_float("AUTOTRADE_MAX_CHANGE_PCT", DEFAULT_ENTRY_MAX_CHANGE_PCT)
    )
    if min_chg > max_chg:
        min_chg, max_chg = max_chg, min_chg

    tp_pct = abs(
        float(
            cfg["autotrade_take_profit_pct"]
            if cfg.get("autotrade_take_profit_pct") is not None
            else radar._env_float("AUTOTRADE_TAKE_PROFIT_PCT", DEFAULT_TAKE_PROFIT_PCT)
        )
    )
    trail_pct = max(
        0.0,
        abs(
            float(
                cfg["autotrade_trailing_stop_pct"]
                if cfg.get("autotrade_trailing_stop_pct") is not None
                else radar._env_float("AUTOTRADE_TRAILING_STOP_PCT", DEFAULT_TRAILING_STOP_PCT)
            )
        ),
    )
    sl_pct = abs(
        float(
            cfg["autotrade_stop_loss_pct"]
            if cfg.get("autotrade_stop_loss_pct") is not None
            else radar._env_float("AUTOTRADE_STOP_LOSS_PCT", DEFAULT_STOP_LOSS_PCT)
        )
    )
    min_tv_eok = max(
        0.0,
        float(
            cfg["min_trading_value_eok"]
            if cfg.get("min_trading_value_eok") is not None
            else radar._env_float("MIN_TRADING_VALUE_EOK", radar.DEFAULT_MIN_TRADING_VALUE_EOK)
        ),
    )

    return {
        "autotrade_enabled": enabled,
        "autotrade_only_second_order": only_2nd,
        "autotrade_order_amount_krw": max(100_000, min(50_000_000, order_amt)),
        "autotrade_max_positions": max(1, min(15, max_pos)),
        "autotrade_min_change_pct": round(max(-15.0, min(15.0, min_chg)), 1),
        "autotrade_max_change_pct": round(max(-10.0, min(25.0, max_chg)), 1),
        "autotrade_take_profit_pct": round(max(0.5, min(30.0, tp_pct)), 1),
        "autotrade_trailing_stop_pct": round(min(15.0, trail_pct), 1),
        "autotrade_stop_loss_pct": round(max(0.5, min(20.0, sl_pct)), 1),
        "autotrade_open_entry_time": f"{oh:02d}:{om:02d}",
        "autotrade_entry_cutoff_time": f"{ch:02d}:{cm:02d}",
        "autotrade_eod_exit_enabled": eod_enabled,
        "autotrade_eod_exit_time": f"{eh:02d}:{em:02d}",
        "autotrade_daily_max_loss_pct": DEFAULT_DAILY_MAX_LOSS_PCT,
        "min_trading_value_eok": min_tv_eok,
    }


class KISPaperClient:
    """한국투자증권(KIS Developers) 공식 모의투자 REST API 경량 클라이언트 (openapivts 전용)."""

    def __init__(self, creds: dict[str, str], data_dir: Path) -> None:
        self.app_key = creds["app_key"]
        self.app_secret = creds["app_secret"]
        self.cano = creds["cano"]
        self.acnt_prdt_cd = creds.get("acnt_prdt_cd") or "01"
        self.token_cache_path = data_dir / "kis_token_vts.json"

    def get_access_token(self, now: datetime | None = None) -> str:
        ref = radar.StateStore._now_kst(now)
        if self.token_cache_path.exists():
            try:
                cached = json.loads(self.token_cache_path.read_text(encoding="utf-8"))
                tok = str(cached.get("access_token") or "").strip()
                exp = radar.parse_iso_kst(cached.get("expires_at"))
                if tok and exp is not None and (exp - ref) > timedelta(minutes=30):
                    return tok
            except Exception:
                pass

        url = f"{KIS_VTS_BASE_URL}/oauth2/tokenP"
        payload = json.dumps(
            {
                "grant_type": "client_credentials",
                "appkey": self.app_key,
                "appsecret": self.app_secret,
            }
        ).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"content-type": "application/json; charset=utf-8"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        tok = str(data.get("access_token") or "").strip()
        if not tok:
            raise RuntimeError(f"KIS 모의투자 토큰 발급 실패: {data}")
        expires_in = max(3600, int(data.get("expires_in") or 86400))
        exp_at = ref + timedelta(seconds=expires_in)
        radar.atomic_write_text(
            self.token_cache_path,
            json.dumps({"access_token": tok, "expires_at": exp_at.isoformat()}, ensure_ascii=False),
        )
        try:
            self.token_cache_path.chmod(0o600)
        except OSError:
            pass
        return tok

    def place_market_order(self, *, side: str, code: str, qty: int, now: datetime | None = None) -> tuple[bool, str, str]:
        if qty <= 0:
            return False, "", "주문 수량이 0 이하입니다."
        tok = self.get_access_token(now=now)
        tr_id = KIS_TR_BUY_VTS if side == "buy" else KIS_TR_SELL_VTS
        url = f"{KIS_VTS_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
        body = json.dumps(
            {
                "CANO": self.cano,
                "ACNT_PRDT_CD": self.acnt_prdt_cd,
                "PDNO": code.zfill(6),
                "ORD_DVSN": "01",
                "ORD_QTY": str(int(qty)),
                "ORD_UNPR": "0",
            }
        ).encode("utf-8")
        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {tok}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
            "custtype": "P",
        }
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                res = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            err_txt = exc.read().decode("utf-8", errors="ignore")[:240]
            return False, "", f"HTTP {exc.code}: {err_txt}"
        except Exception as exc:
            return False, "", str(exc)

        if str(res.get("rt_cd")) == "0":
            out = res.get("output") or {}
            odno = str(out.get("ODNO") or out.get("KRX_FWDG_ORD_ORGNO") or "OK")
            return True, odno, str(res.get("msg1") or "정상 접수")
        return False, "", f"{res.get('msg_cd', '')} {res.get('msg1', '주문 거절')}".strip()

    def inquire_balance(self, now: datetime | None = None) -> dict[str, Any]:
        tok = self.get_access_token(now=now)
        qs = urllib.parse.urlencode(
            {
                "CANO": self.cano,
                "ACNT_PRDT_CD": self.acnt_prdt_cd,
                "AFHR_FLPR_YN": "N",
                "OFL_YN": "",
                "INQR_DVSN": "02",
                "UNPR_DVSN": "01",
                "FUND_STTL_ICLD_YN": "N",
                "FNCG_AMT_AUTO_RDPT_YN": "N",
                "PRCS_DVSN": "00",
                "CTX_AREA_FK100": "",
                "CTX_AREA_NK100": "",
            }
        )
        url = f"{KIS_VTS_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance?{qs}"
        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {tok}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": KIS_TR_BALANCE_VTS,
            "custtype": "P",
        }
        req = urllib.request.Request(url, headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=10) as resp:
            res = json.loads(resp.read().decode("utf-8"))
        if str(res.get("rt_cd")) != "0":
            raise RuntimeError(f"KIS 모의잔고 조회 실패: {res.get('msg1')}")
        out2 = (res.get("output2") or [{}])[0]
        return {
            "cash_krw": int(out2.get("dnca_tot_amt") or 0),
            "total_equity_krw": int(out2.get("tot_evlu_amt") or 0),
            "eval_pnl_krw": int(out2.get("evlu_pfls_smtl_amt") or 0),
            "holdings": res.get("output1") or [],
        }


def is_second_order_candidate(stock: dict[str, Any]) -> bool:
    """1차 공시·단말기 추격 종목 및 단기테마성을 배제하고 2차 파급 본업·부분연관 종목만 통과."""
    if radar._normalize_perf_direction(stock.get("direction")) != "상승수혜":
        return False
    tier = str(stock.get("order_tier") or "").strip()
    rev = str(stock.get("revenue_link") or "").strip()
    if "1차" in tier or "피해" in tier or "역풍" in tier:
        return False
    if rev == "단기테마성":
        return False
    return True


def check_substitute_laggard_guard(
    candidate: dict[str, Any],
    ranked_candidates: list[dict[str, Any]],
    quotes: dict[str, radar.StockQuote],
    *,
    max_change_pct: float = DEFAULT_ENTRY_MAX_CHANGE_PCT,
) -> tuple[bool, str]:
    """대리 소외주(확산·주변주) 추격 진입 차단 가드:
    1) 동일 알림 내 '핵심' 2차 후보가 1개 이상 존재하는데 모두 진입 불가였을 때,
       후순위 '확산' 주변주를 대리 매수하지 않는다.
    2) 동일 알림 내 2차 파급 선도주가 이미 진입 상한(> max_change_pct)을 돌파했거나 상한가 잠김일 때,
       비핵심 주변주('확산'·일반 '2차') 대리 진입 및 역행 하락(< 0%) 소외주 진입을 차단한다.
    - '1차 직접' 종목만 급등하고 '2차 파급(핵심)'이 미반영인 구간은 본 봇의 핵심 알파 구간이므로 허용한다.
    """
    cand_code = str(candidate.get("code") or "").strip().zfill(6)
    tier = str(candidate.get("order_tier") or "").strip()
    is_core = "핵심" in tier
    is_diffusion = "확산" in tier
    has_any_core = any(
        "핵심" in str(s.get("order_tier") or "") and is_second_order_candidate(s)
        for s in ranked_candidates
    )

    if is_diffusion and has_any_core:
        return True, f"핵심후보 미체결 시 확산주 대리매수 금지({tier})"

    overheated_peers: list[str] = []
    overheated_core_peers: list[str] = []
    for peer in ranked_candidates:
        p_code = str(peer.get("code") or "").strip().zfill(6)
        if not p_code or p_code == cand_code:
            continue
        if not is_second_order_candidate(peer):
            continue
        qp = quotes.get(p_code)
        if qp is None or int(qp.price) <= 0:
            continue
        if str(qp.market_status or "").strip().upper() in ("CLOSE", "CLOSED"):
            continue
        _, p_unbuyable = radar.assess_quote_executability(qp)
        if float(qp.change_pct) > float(max_change_pct) or p_unbuyable == "상한가잠김":
            label = f"{peer.get('name') or p_code}({qp.change_pct:+.1f}%)"
            overheated_peers.append(label)
            if "핵심" in str(peer.get("order_tier") or ""):
                overheated_core_peers.append(label)

    if not is_core and overheated_core_peers:
        return True, f"핵심 선도주 이미 급등({', '.join(overheated_core_peers[:2])}) — 후순위 대리매수 금지"
    if is_diffusion and overheated_peers:
        return True, f"2차 선도주 이미 급등({', '.join(overheated_peers[:2])}) — 확산주 대리매수 금지"

    q_cand = quotes.get(cand_code)
    if overheated_peers and q_cand is not None and int(q_cand.price) > 0:
        if str(q_cand.market_status or "").strip().upper() not in ("CLOSE", "CLOSED") and float(q_cand.change_pct) < 0.0:
            return True, f"동일테마 선도주 급등({', '.join(overheated_peers[:2])}) 중 역행 약세({q_cand.change_pct:+.2f}%) 소외주 진입 금지"

    return False, ""


class PaperAutoTrader:
    """내장 가상 모의계좌 + 한국투자증권 공식 모의투자(VTS) 자동매매 매니저."""

    def __init__(self, data_dir: Path, repo_dir: Path | None = None) -> None:
        self.data_dir = data_dir
        self.repo_dir = repo_dir
        self.account_path = data_dir / "paper_account.json"
        self.trades_path = data_dir / "paper_trades.jsonl"

    def _default_account(self, now: datetime) -> dict[str, Any]:
        initial = max(
            1_000_000,
            radar._env_int("AUTOTRADE_INITIAL_CAPITAL_KRW", DEFAULT_INITIAL_CAPITAL_KRW),
        )
        return {
            "initial_capital_krw": initial,
            "cash_krw": initial,
            "realized_pnl_krw": 0,
            "total_fees_krw": 0,
            "trade_count": 0,
            "win_count": 0,
            "loss_count": 0,
            "positions": {},
            "pending_open_entries": [],
            "daily_realized_pnl": {},
            "traded_codes_by_day": {},
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        }

    def load_account(self, now: datetime | None = None) -> dict[str, Any]:
        ref = radar.StateStore._now_kst(now)
        if not self.account_path.exists():
            return self._default_account(ref)
        try:
            raw = json.loads(self.account_path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                return self._default_account(ref)
        except Exception:
            return self._default_account(ref)

        base = self._default_account(ref)
        base.update(raw)
        if not isinstance(base.get("positions"), dict):
            base["positions"] = {}
        if not isinstance(base.get("pending_open_entries"), list):
            base["pending_open_entries"] = []
        if not isinstance(base.get("daily_realized_pnl"), dict):
            base["daily_realized_pnl"] = {}
        if not isinstance(base.get("traded_codes_by_day"), dict):
            base["traded_codes_by_day"] = {}
        return base

    def _save_account(self, account: dict[str, Any], now: datetime) -> None:
        day_cut = (now - timedelta(days=14)).strftime("%Y-%m-%d")
        account["daily_realized_pnl"] = {
            k: int(v) for k, v in (account.get("daily_realized_pnl") or {}).items() if k >= day_cut
        }
        account["traded_codes_by_day"] = {
            k: list(v) for k, v in (account.get("traded_codes_by_day") or {}).items() if k >= day_cut
        }
        account["updated_at"] = now.isoformat()
        radar.atomic_write_text(
            self.account_path,
            json.dumps(account, ensure_ascii=False, indent=2),
        )

    def load_trades(self, limit: int = 50) -> list[dict[str, Any]]:
        if not self.trades_path.exists():
            return []
        rows: list[dict[str, Any]] = []
        for raw in self.trades_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    rows.append(obj)
            except Exception:
                continue
        rows.reverse()
        return rows[: max(1, int(limit))]

    def _append_trade(self, trade: dict[str, Any]) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        existing: list[str] = []
        if self.trades_path.exists():
            existing = [
                ln for ln in self.trades_path.read_text(encoding="utf-8", errors="ignore").splitlines() if ln.strip()
            ]
        existing.append(json.dumps(trade, ensure_ascii=False))
        if len(existing) > MAX_TRADE_HISTORY:
            existing = existing[-MAX_TRADE_HISTORY:]
        radar.atomic_write_text(self.trades_path, "\n".join(existing) + "\n")

    def has_active_work(self, now: datetime | None = None) -> bool:
        ref = radar.StateStore._now_kst(now)
        if ref.weekday() >= 5:
            return False
        if not ((9, 0) <= (ref.hour, ref.minute) <= (15, 28)):
            return False
        acc = self.load_account(ref)
        return bool(acc.get("positions") or acc.get("pending_open_entries"))

    def reset_account(self, *, initial_capital_krw: int | None = None, now: datetime | None = None) -> dict[str, Any]:
        ref = radar.StateStore._now_kst(now)
        with _account_file_lock(self.data_dir):
            acc = self._default_account(ref)
            if initial_capital_krw is not None:
                cap = max(1_000_000, min(1_000_000_000, int(initial_capital_krw)))
                acc["initial_capital_krw"] = cap
                acc["cash_krw"] = cap
            self._save_account(acc, ref)
            if self.trades_path.exists():
                self.trades_path.unlink(missing_ok=True)
        return acc

    @staticmethod
    def _rank_candidates(stocks: list[dict[str, Any]], *, only_second_order: bool) -> list[dict[str, Any]]:
        eligible: list[dict[str, Any]] = []
        for s in stocks:
            if only_second_order:
                if not is_second_order_candidate(s):
                    continue
            else:
                if radar._normalize_perf_direction(s.get("direction")) != "상승수혜":
                    continue
                if str(s.get("revenue_link") or "").strip() == "단기테마성":
                    continue
            eligible.append(s)

        eligible.sort(
            key=lambda s: (
                0 if "핵심" in str(s.get("order_tier") or "") else (1 if "2차" in str(s.get("order_tier") or "") else 2),
                0 if str(s.get("revenue_link") or "") == "본업직결" else 1,
            )
        )
        return eligible

    @staticmethod
    def _is_entry_hours(
        now_dt: datetime,
        eod_exit_time: str,
        eod_enabled: bool,
        *,
        open_entry_time: str = DEFAULT_OPEN_ENTRY_TIME,
        entry_cutoff_time: str = DEFAULT_ENTRY_CUTOFF_TIME,
    ) -> bool:
        if now_dt.weekday() >= 5:
            return False
        now_min = now_dt.hour * 60 + now_dt.minute
        oh, om = radar._parse_hhmm(open_entry_time, default=(9, 8))
        open_entry_min = max(9 * 60, oh * 60 + om)
        ch, cm = radar._parse_hhmm(entry_cutoff_time, default=(14, 0))
        cutoff_min = ch * 60 + cm
        if eod_enabled:
            eh, em = radar._parse_hhmm(eod_exit_time, default=(15, 15))
            cutoff_min = min(cutoff_min, eh * 60 + em - 15)
        cutoff_min = max(open_entry_min + 5, cutoff_min)
        return open_entry_min <= now_min <= cutoff_min

    @staticmethod
    def _is_premarket_queue_hours(
        now_dt: datetime,
        *,
        open_entry_time: str = DEFAULT_OPEN_ENTRY_TIME,
    ) -> bool:
        if now_dt.weekday() >= 5:
            return False
        now_min = now_dt.hour * 60 + now_dt.minute
        oh, om = radar._parse_hhmm(open_entry_time, default=(9, 8))
        open_entry_min = max(9 * 60 + 1, oh * 60 + om)
        return (7 * 60) <= now_min < open_entry_min

    def _daily_loss_circuit_tripped(
        self,
        account: dict[str, Any],
        quotes: dict[str, radar.StockQuote],
        now_dt: datetime,
        max_loss_pct: float,
    ) -> bool:
        day = now_dt.strftime("%Y-%m-%d")
        realized_today = int((account.get("daily_realized_pnl") or {}).get(day, 0))
        unrealized = 0
        for code, pos in (account.get("positions") or {}).items():
            q = quotes.get(code)
            cur_px = int(q.price) if (q and int(q.price) > 0) else int(pos.get("last_price") or pos["avg_buy_price"])
            qty = int(pos["qty"])
            gross_sell = cur_px * qty
            sell_cost = int(gross_sell * SELL_FEE_AND_TAX_RATE)
            unrealized += (gross_sell - sell_cost) - int(pos["invested_krw"])
        init_cap = max(1, int(account.get("initial_capital_krw") or DEFAULT_INITIAL_CAPITAL_KRW))
        return ((realized_today + unrealized) / init_cap) * 100.0 <= -abs(max_loss_pct)

    def _execute_buy_locked(
        self,
        account: dict[str, Any],
        *,
        stock: dict[str, Any],
        quote: radar.StockQuote,
        theme_key: str,
        sector: str,
        title: str,
        catalyst_score: float,
        cfg: dict[str, Any],
        now_dt: datetime,
        kis_client: KISPaperClient | None,
        send_fn: Callable[[str], Any] | None,
        trace: radar.TraceLogger | None,
    ) -> dict[str, Any] | None:
        code = str(stock["code"]).strip().zfill(6)
        positions: dict[str, Any] = account.setdefault("positions", {})
        day = now_dt.strftime("%Y-%m-%d")
        traded_today = set((account.get("traded_codes_by_day") or {}).get(day) or [])

        if code in positions or code in traded_today:
            return None
        if len(positions) >= int(cfg["autotrade_max_positions"]):
            return None

        buyable, unbuyable_reason = radar.assess_quote_executability(quote)
        if not buyable or int(quote.price) <= 0:
            return None
        if str(quote.market_status or "").strip().upper() in ("CLOSE", "CLOSED"):
            return None

        chg = float(quote.change_pct)
        if chg < float(cfg["autotrade_min_change_pct"]) or chg > float(cfg["autotrade_max_change_pct"]):
            return None

        eff_tv = radar.effective_trading_value_krw(quote)
        min_tv_krw = int(float(cfg["min_trading_value_eok"]) * 100_000_000)
        if min_tv_krw > 0 and radar.has_known_liquidity(quote) and eff_tv < min_tv_krw:
            return None

        liq_cap = radar.recommend_position_cap_krw(quote)
        cash_avail = int(account.get("cash_krw") or 0)
        budget = min(int(cfg["autotrade_order_amount_krw"]), int(cash_avail * 0.98))
        if liq_cap > 0:
            budget = min(budget, liq_cap)

        fill_price = (
            int(quote.price)
            if kis_client is not None
            else apply_execution_slippage(int(quote.price), side="buy", eff_trading_value_krw=eff_tv)
        )
        qty = budget // max(1, fill_price)
        if qty <= 0:
            return None

        gross_buy = fill_price * qty
        buy_fee = int(gross_buy * BUY_FEE_RATE)
        total_cost = gross_buy + buy_fee
        if kis_client is None and total_cost > cash_avail:
            qty -= 1
            if qty <= 0:
                return None
            gross_buy = fill_price * qty
            buy_fee = int(gross_buy * BUY_FEE_RATE)
            total_cost = gross_buy + buy_fee

        broker_mode = "kis_vts" if kis_client is not None else "builtin_paper"
        kis_order_no = ""
        if kis_client is not None:
            ok, odno, msg = kis_client.place_market_order(side="buy", code=code, qty=qty, now=now_dt)
            if not ok:
                logger.warning("[모의자동매매] KIS 모의매수 주문 실패(%s %d주): %s", code, qty, msg)
                if trace is not None:
                    trace.emit({
                        "ts": now_dt.isoformat(),
                        "type": "paper_trade_event",
                        "action": "kis_buy_error",
                        "code": code,
                        "name": stock.get("name") or code,
                        "error": msg[:140],
                    })
                return None
            kis_order_no = odno

        account["cash_krw"] = max(0, cash_avail - total_cost)
        account["total_fees_krw"] = int(account.get("total_fees_krw") or 0) + buy_fee
        account.setdefault("traded_codes_by_day", {}).setdefault(day, []).append(code)

        pos = {
            "code": code,
            "name": str(stock.get("name") or code),
            "qty": qty,
            "signal_price": int(quote.price),
            "avg_buy_price": fill_price,
            "invested_krw": total_cost,
            "buy_fee_krw": buy_fee,
            "last_price": fill_price,
            "highest_price": fill_price,
            "peak_ret_pct": 0.0,
            "trailing_armed": False,
            "entry_change_pct": round(chg, 2),
            "entry_ts": now_dt.isoformat(),
            "entry_date": day,
            "theme_key": str(theme_key or ""),
            "sector": str(sector or ""),
            "title": str(title or ""),
            "catalyst_score": float(catalyst_score),
            "order_tier": str(stock.get("order_tier") or "2차 파급"),
            "revenue_link": str(stock.get("revenue_link") or ""),
            "broker_mode": broker_mode,
            "kis_order_no": kis_order_no or None,
            "git_commit": radar.current_git_commit(self.repo_dir) or None,
        }
        positions[code] = pos
        self._save_account(account, now_dt)

        logger.info(
            "[모의자동매매] 매수 체결(%s): %s(%s) %d주 @ %s원 (총 %s원, 등락 %+.2f%%)",
            broker_mode, pos["name"], code, qty, f"{fill_price:,}", f"{total_cost:,}", chg,
        )
        if trace is not None:
            trace.emit({
                "ts": now_dt.isoformat(),
                "type": "paper_trade_event",
                "action": "buy",
                "broker_mode": broker_mode,
                "code": code,
                "name": pos["name"],
                "qty": qty,
                "fill_price": fill_price,
                "invested_krw": total_cost,
                "entry_change_pct": pos["entry_change_pct"],
                "theme_key": theme_key,
                "git_commit": pos["git_commit"],
            })
        if send_fn is not None:
            try:
                send_fn(format_buy_telegram_html(pos, account, cfg, now=now_dt))
            except Exception as exc:
                logger.warning("[모의자동매매] 매수 텔레그램 알림 실패: %s", exc)
        with contextlib.suppress(Exception):
            import report_sync

            report_sync.try_background_github_sync(self.data_dir, self.repo_dir, now=now_dt)
        return pos

    def _execute_sell_locked(
        self,
        account: dict[str, Any],
        *,
        code: str,
        quote: radar.StockQuote | None,
        exit_reason: str,
        now_dt: datetime,
        kis_client: KISPaperClient | None,
        send_fn: Callable[[str], Any] | None,
        trace: radar.TraceLogger | None,
    ) -> dict[str, Any] | None:
        positions: dict[str, Any] = account.get("positions") or {}
        pos = positions.get(code)
        if not pos:
            return None

        qty = int(pos["qty"])
        raw_px = int(quote.price) if (quote is not None and int(quote.price) > 0) else int(pos.get("last_price") or pos["avg_buy_price"])
        eff_tv = radar.effective_trading_value_krw(quote) if quote is not None else 0
        use_kis = kis_client is not None and pos.get("broker_mode") == "kis_vts"
        fill_price = raw_px if use_kis else apply_execution_slippage(raw_px, side="sell", eff_trading_value_krw=eff_tv)

        kis_order_no = ""
        if use_kis and kis_client is not None:
            ok, odno, msg = kis_client.place_market_order(side="sell", code=code, qty=qty, now=now_dt)
            if not ok:
                logger.warning("[모의자동매매] KIS 모의매도 주문 실패(%s %d주): %s", code, qty, msg)
                if trace is not None:
                    trace.emit({
                        "ts": now_dt.isoformat(),
                        "type": "paper_trade_event",
                        "action": "kis_sell_error",
                        "code": code,
                        "name": pos.get("name") or code,
                        "error": msg[:140],
                    })
                return None
            kis_order_no = odno

        gross_sell = fill_price * qty
        sell_fee_tax = int(gross_sell * SELL_FEE_AND_TAX_RATE)
        net_proceeds = max(0, gross_sell - sell_fee_tax)
        invested = int(pos["invested_krw"])
        net_pnl_krw = net_proceeds - invested
        net_pnl_pct = round((net_pnl_krw / max(1, invested)) * 100.0, 2)
        gross_ret_pct = round(((fill_price - float(pos["avg_buy_price"])) / max(1.0, float(pos["avg_buy_price"]))) * 100.0, 2)

        day = now_dt.strftime("%Y-%m-%d")
        account["cash_krw"] = int(account.get("cash_krw") or 0) + net_proceeds
        account["realized_pnl_krw"] = int(account.get("realized_pnl_krw") or 0) + net_pnl_krw
        account["total_fees_krw"] = int(account.get("total_fees_krw") or 0) + sell_fee_tax
        account["trade_count"] = int(account.get("trade_count") or 0) + 1
        if net_pnl_krw > 0:
            account["win_count"] = int(account.get("win_count") or 0) + 1
        else:
            account["loss_count"] = int(account.get("loss_count") or 0) + 1
        daily_map = account.setdefault("daily_realized_pnl", {})
        daily_map[day] = int(daily_map.get(day, 0)) + net_pnl_krw
        positions.pop(code, None)

        trade_rec = {
            "code": code,
            "name": pos["name"],
            "qty": qty,
            "buy_price": int(pos["avg_buy_price"]),
            "sell_price": fill_price,
            "highest_price": int(pos.get("highest_price") or pos["avg_buy_price"]),
            "peak_ret_pct": float(pos.get("peak_ret_pct") or 0.0),
            "invested_krw": invested,
            "net_proceeds_krw": net_proceeds,
            "fees_and_tax_krw": int(pos.get("buy_fee_krw") or 0) + sell_fee_tax,
            "gross_ret_pct": gross_ret_pct,
            "net_pnl_krw": net_pnl_krw,
            "net_pnl_pct": net_pnl_pct,
            "exit_reason": exit_reason,
            "entry_ts": pos["entry_ts"],
            "exit_ts": now_dt.isoformat(),
            "theme_key": pos.get("theme_key") or "",
            "sector": pos.get("sector") or "",
            "title": pos.get("title") or "",
            "order_tier": pos.get("order_tier") or "",
            "broker_mode": pos.get("broker_mode") or "builtin_paper",
            "kis_sell_order_no": kis_order_no or None,
            "git_commit": pos.get("git_commit") or radar.current_git_commit(self.repo_dir) or None,
        }
        self._save_account(account, now_dt)
        self._append_trade(trade_rec)

        logger.info(
            "[모의자동매매] 매도 청산(%s): %s(%s) %d주 @ %s원 → 순손익 %+d원 (%+.2f%%)",
            exit_reason, trade_rec["name"], code, qty, f"{fill_price:,}", net_pnl_krw, net_pnl_pct,
        )
        if trace is not None:
            trace.emit({
                "ts": now_dt.isoformat(),
                "type": "paper_trade_event",
                "action": "sell",
                "code": code,
                "name": trade_rec["name"],
                "qty": qty,
                "buy_price": trade_rec["buy_price"],
                "sell_price": fill_price,
                "net_pnl_krw": net_pnl_krw,
                "net_pnl_pct": net_pnl_pct,
                "exit_reason": exit_reason,
                "git_commit": trade_rec["git_commit"],
            })
        if send_fn is not None:
            try:
                send_fn(format_sell_telegram_html(trade_rec, account, now=now_dt))
            except Exception as exc:
                logger.warning("[모의자동매매] 매도 텔레그램 알림 실패: %s", exc)
        with contextlib.suppress(Exception):
            import report_sync

            report_sync.try_background_github_sync(self.data_dir, self.repo_dir, now=now_dt)
        return trade_rec

    def on_alert_candidates(
        self,
        *,
        alert_ts: datetime,
        theme_key: str,
        sector: str,
        title: str,
        catalyst_score: float,
        stocks: list[dict[str, Any]],
        quotes: dict[str, radar.StockQuote],
        runtime_cfg: dict[str, Any] | None = None,
        daemon_mode: bool = True,
        send_fn: Callable[[str], Any] | None = None,
        trace: radar.TraceLogger | None = None,
    ) -> dict[str, Any] | None:
        ref = radar.StateStore._now_kst(alert_ts)
        cfg = resolve_autotrade_config(runtime_cfg, daemon_mode=daemon_mode)
        if not cfg["autotrade_enabled"] or not stocks:
            return None

        ranked = self._rank_candidates(stocks, only_second_order=cfg["autotrade_only_second_order"])
        if not ranked:
            return None

        creds = resolve_kis_credentials(self.repo_dir)
        kis_client = KISPaperClient(creds, self.data_dir) if creds["configured"] else None

        with _account_file_lock(self.data_dir):
            account = self.load_account(ref)
            if self._daily_loss_circuit_tripped(account, quotes, ref, cfg["autotrade_daily_max_loss_pct"]):
                logger.info("[모의자동매매] 일일 손실 한도(-%.1f%%) 도달 — 신규 매수를 중단합니다.", cfg["autotrade_daily_max_loss_pct"])
                return None

            # 1) 장중 진입 가능 시각(평일 기본 09:08 ~ 14:00): 조건 통과 1순위 종목 즉시 자동 매수
            if self._is_entry_hours(
                ref,
                cfg["autotrade_eod_exit_time"],
                cfg["autotrade_eod_exit_enabled"],
                open_entry_time=cfg["autotrade_open_entry_time"],
                entry_cutoff_time=cfg["autotrade_entry_cutoff_time"],
            ):
                for st in ranked:
                    code = str(st.get("code") or "").strip().zfill(6)
                    q = quotes.get(code)
                    if q is None:
                        continue
                    sub_blocked, sub_reason = check_substitute_laggard_guard(
                        st,
                        ranked,
                        quotes,
                        max_change_pct=float(cfg["autotrade_max_change_pct"]),
                    )
                    if sub_blocked:
                        logger.info(
                            "[모의자동매매] 대리 소외주 진입 차단: %s(%s) — %s",
                            st.get("name") or code, code, sub_reason,
                        )
                        if trace is not None:
                            trace.emit({
                                "ts": ref.isoformat(),
                                "type": "paper_trade_event",
                                "action": "skip_substitute",
                                "code": code,
                                "name": str(st.get("name") or code),
                                "theme_key": theme_key,
                                "reason": sub_reason,
                            })
                        continue
                    bought = self._execute_buy_locked(
                        account,
                        stock=st,
                        quote=q,
                        theme_key=theme_key,
                        sector=sector,
                        title=title,
                        catalyst_score=catalyst_score,
                        cfg=cfg,
                        now_dt=ref,
                        kis_client=kis_client,
                        send_fn=send_fn,
                        trace=trace,
                    )
                    if bought is not None:
                        return {"action": "bought", "position": bought}
                return None

            # 2) 평일 장전·시초 안정화 구간(07:00 ~ 09:07, 07:50 장전 요약 포함): 09:08 이후 안정화 시세 확인 후 진입하도록 대기열 적재
            if self._is_premarket_queue_hours(ref, open_entry_time=cfg["autotrade_open_entry_time"]):
                top = ranked[0]
                code = str(top.get("code") or "").strip().zfill(6)
                if not code or code in (account.get("positions") or {}):
                    return None
                q_list: list[dict[str, Any]] = [
                    e for e in (account.get("pending_open_entries") or []) if str(e.get("code")) != code
                ]
                oh, om = radar._parse_hhmm(cfg["autotrade_open_entry_time"], default=(9, 8))
                exp_total_min = min(15 * 60, oh * 60 + om + 17)
                exp_h, exp_m = divmod(exp_total_min, 60)
                exp_dt = datetime(ref.year, ref.month, ref.day, exp_h, exp_m, tzinfo=radar.KST)
                entry_item = {
                    "code": code,
                    "stock": dict(top),
                    "theme_key": theme_key,
                    "sector": sector,
                    "title": title,
                    "catalyst_score": float(catalyst_score),
                    "queued_at": ref.isoformat(),
                    "expires_at": exp_dt.isoformat(),
                }
                q_list.append(entry_item)
                q_list.sort(key=lambda x: -float(x.get("catalyst_score") or 0.0))
                account["pending_open_entries"] = q_list[:5]
                self._save_account(account, ref)
                return {"action": "queued_for_open", "entry": entry_item}

            # 3) 평일 오후 진입 마감(기본 14:00) 이후 정규장 알림은 관망 (보유 포지션 청산 감시는 계속 작동)
            if ref.weekday() < 5 and (9 * 60) <= (ref.hour * 60 + ref.minute) <= (15 * 60 + 30):
                top_st = ranked[0]
                logger.info(
                    "[모의자동매매] 신규 진입 마감 시각(%s) 경과 — 알림만 수신하고 매수는 생략합니다: %s",
                    cfg["autotrade_entry_cutoff_time"], theme_key,
                )
                if trace is not None:
                    trace.emit({
                        "ts": ref.isoformat(),
                        "type": "paper_trade_event",
                        "action": "skip_cutoff",
                        "code": str(top_st.get("code") or "").strip().zfill(6),
                        "name": str(top_st.get("name") or ""),
                        "theme_key": theme_key,
                        "cutoff_time": cfg["autotrade_entry_cutoff_time"],
                    })

        return None

    def tick_positions(
        self,
        *,
        now: datetime | None = None,
        runtime_cfg: dict[str, Any] | None = None,
        daemon_mode: bool = True,
        fetch_quotes_fn: Callable[[list[str]], dict[str, radar.StockQuote]] | None = None,
        send_fn: Callable[[str], Any] | None = None,
        trace: radar.TraceLogger | None = None,
    ) -> dict[str, int]:
        """보유 포지션 익절·트레일링·손절·15:15 시간청산 및 장전 대기 종목 09:08 시초 안정화 진입 점검."""
        ref = radar.StateStore._now_kst(now)
        if ref.weekday() >= 5:
            return {"bought": 0, "sold": 0}

        now_min = ref.hour * 60 + ref.minute
        if now_min < 9 * 60 or now_min > 15 * 60 + 30:
            return {"bought": 0, "sold": 0}

        cfg = resolve_autotrade_config(runtime_cfg, daemon_mode=daemon_mode)
        fetcher = fetch_quotes_fn if fetch_quotes_fn is not None else radar.fetch_naver_quotes

        with _account_file_lock(self.data_dir):
            account = self.load_account(ref)
            positions: dict[str, Any] = account.get("positions") or {}
            pending_open: list[dict[str, Any]] = list(account.get("pending_open_entries") or [])
            if not positions and not pending_open:
                return {"bought": 0, "sold": 0}

            codes_needed = list(
                dict.fromkeys(
                    list(positions.keys()) + [str(e.get("code") or "").zfill(6) for e in pending_open if e.get("code")]
                )
            )
            quotes = fetcher(codes_needed) if codes_needed else {}

            creds = resolve_kis_credentials(self.repo_dir)
            kis_client = KISPaperClient(creds, self.data_dir) if creds["configured"] else None

            bought_cnt = 0
            sold_cnt = 0
            dirty = False

            # A) 시초 안정화 시각(기본 09:08 ~ 09:25) 장전 대기 후보 진입 점검 (갭상승 과열·급락 종목은 자동 폐기)
            if pending_open:
                oh_open, om_open = radar._parse_hhmm(cfg["autotrade_open_entry_time"], default=(9, 8))
                open_entry_min = max(9 * 60, oh_open * 60 + om_open)
                kept_pending: list[dict[str, Any]] = []
                for item in pending_open:
                    exp = radar.parse_iso_kst(item.get("expires_at"))
                    if exp is not None and ref > exp:
                        dirty = True
                        continue
                    if now_min < open_entry_min:
                        kept_pending.append(item)
                        continue
                    if not cfg["autotrade_enabled"]:
                        dirty = True
                        continue
                    code = str(item.get("code") or "").zfill(6)
                    q = quotes.get(code)
                    if q is None or int(q.price) <= 0 or str(q.market_status or "").upper() in ("CLOSE", "CLOSED"):
                        kept_pending.append(item)
                        continue
                    # 시초가가 이미 과열(> max_change_pct)이거나 상한가/거래정지면 미련 없이 폐기
                    buyable, _ = radar.assess_quote_executability(q)
                    if not buyable or float(q.change_pct) > float(cfg["autotrade_max_change_pct"]):
                        logger.info(
                            "[모의자동매매] 장전 대기 종목 시초 과열/체결제한으로 진입 포기: %s(%+.2f%%)",
                            code, float(q.change_pct),
                        )
                        dirty = True
                        continue
                    if float(q.change_pct) < float(cfg["autotrade_min_change_pct"]):
                        dirty = True
                        continue
                    bought = self._execute_buy_locked(
                        account,
                        stock=dict(item.get("stock") or {"code": code, "name": code, "direction": "상승수혜"}),
                        quote=q,
                        theme_key=str(item.get("theme_key") or ""),
                        sector=str(item.get("sector") or ""),
                        title=str(item.get("title") or ""),
                        catalyst_score=float(item.get("catalyst_score") or 8.0),
                        cfg=cfg,
                        now_dt=ref,
                        kis_client=kis_client,
                        send_fn=send_fn,
                        trace=trace,
                    )
                    if bought is not None:
                        bought_cnt += 1
                    dirty = True
                account["pending_open_entries"] = kept_pending

            # B) 보유 포지션 실시간 가격 갱신 & 손절·트레일링 익절·15:15 시간청산
            eh, em = radar._parse_hhmm(cfg["autotrade_eod_exit_time"], default=(15, 15))
            eod_min = eh * 60 + em
            today_str = ref.strftime("%Y-%m-%d")
            tp_pct = float(cfg["autotrade_take_profit_pct"])
            trail_pct = float(cfg["autotrade_trailing_stop_pct"])
            sl_pct = float(cfg["autotrade_stop_loss_pct"])

            for code in list((account.get("positions") or {}).keys()):
                pos = account["positions"][code]
                q = quotes.get(code)
                if q is None or int(q.price) <= 0:
                    continue
                if str(q.market_status or "").strip().upper() in ("CLOSE", "CLOSED"):
                    continue
                _, unbuyable_reason = radar.assess_quote_executability(q)
                if unbuyable_reason == "거래정지":
                    continue

                cur_px = int(q.price)
                buy_px = max(1, int(pos["avg_buy_price"]))
                prev_high = max(buy_px, int(pos.get("highest_price") or buy_px))
                new_high = max(prev_high, cur_px)
                cur_ret_pct = ((cur_px - buy_px) / buy_px) * 100.0
                peak_ret_pct = ((new_high - buy_px) / buy_px) * 100.0

                if cur_px != pos.get("last_price") or new_high != pos.get("highest_price"):
                    pos["last_price"] = cur_px
                    pos["highest_price"] = new_high
                    pos["peak_ret_pct"] = round(peak_ret_pct, 2)
                    dirty = True

                if not pos.get("trailing_armed") and peak_ret_pct >= tp_pct:
                    pos["trailing_armed"] = True
                    dirty = True
                    logger.info(
                        "[모의자동매매] 트레일링 스탑 가동: %s(%s) 수익률 +%.2f%% (목표 +%.1f%% 도달)",
                        pos["name"], code, peak_ret_pct, tp_pct,
                    )

                exit_reason = ""
                if cur_ret_pct <= -sl_pct:
                    exit_reason = f"손절(-{sl_pct:.1f}%)"
                elif trail_pct <= 0 and cur_ret_pct >= tp_pct:
                    exit_reason = f"목표 익절(+{tp_pct:.1f}%)"
                elif pos.get("trailing_armed"):
                    pullback_pct = ((new_high - cur_px) / new_high) * 100.0
                    lock_floor = max(0.5, tp_pct - trail_pct)
                    if pullback_pct >= trail_pct or cur_ret_pct <= lock_floor:
                        exit_reason = f"트레일링 익절(고점 +{peak_ret_pct:.1f}% → {cur_ret_pct:+.1f}%)"

                if not exit_reason and cfg["autotrade_eod_exit_enabled"]:
                    if now_min >= eod_min or str(pos.get("entry_date") or today_str) < today_str:
                        exit_reason = f"장마감 시간청산({cfg['autotrade_eod_exit_time']})"

                if exit_reason:
                    sold = self._execute_sell_locked(
                        account,
                        code=code,
                        quote=q,
                        exit_reason=exit_reason,
                        now_dt=ref,
                        kis_client=kis_client,
                        send_fn=send_fn,
                        trace=trace,
                    )
                    if sold is not None:
                        sold_cnt += 1
                        dirty = False

            if dirty:
                self._save_account(account, ref)

        return {"bought": bought_cnt, "sold": sold_cnt}

    def close_position_manual(
        self,
        code: str,
        *,
        now: datetime | None = None,
        fetch_quotes_fn: Callable[[list[str]], dict[str, radar.StockQuote]] | None = None,
        send_fn: Callable[[str], Any] | None = None,
        trace: radar.TraceLogger | None = None,
    ) -> dict[str, Any] | None:
        ref = radar.StateStore._now_kst(now)
        norm_code = str(code or "").strip()
        if norm_code != "ALL":
            norm_code = norm_code.zfill(6)
        fetcher = fetch_quotes_fn if fetch_quotes_fn is not None else radar.fetch_naver_quotes

        with _account_file_lock(self.data_dir):
            account = self.load_account(ref)
            positions = account.get("positions") or {}
            target_codes = list(positions.keys()) if norm_code == "ALL" else ([norm_code] if norm_code in positions else [])
            if not target_codes:
                return None
            quotes = fetcher(target_codes)
            creds = resolve_kis_credentials(self.repo_dir)
            kis_client = KISPaperClient(creds, self.data_dir) if creds["configured"] else None
            closed: list[dict[str, Any]] = []
            for c in target_codes:
                rec = self._execute_sell_locked(
                    account,
                    code=c,
                    quote=quotes.get(c),
                    exit_reason="대시보드 수동 청산",
                    now_dt=ref,
                    kis_client=kis_client,
                    send_fn=send_fn,
                    trace=trace,
                )
                if rec is not None:
                    closed.append(rec)
            return {"closed_count": len(closed), "trades": closed}

    def build_overview(
        self,
        *,
        now: datetime | None = None,
        runtime_cfg: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        ref = radar.StateStore._now_kst(now)
        cfg = resolve_autotrade_config(runtime_cfg, daemon_mode=True)
        creds = resolve_kis_credentials(self.repo_dir)
        account = self.load_account(ref)
        positions_map = account.get("positions") or {}

        pos_rows: list[dict[str, Any]] = []
        pos_eval_krw = 0
        unrealized_pnl_krw = 0
        for code, p in positions_map.items():
            qty = int(p.get("qty") or 0)
            buy_px = int(p.get("avg_buy_price") or 0)
            last_px = int(p.get("last_price") or buy_px)
            gross_val = last_px * qty
            net_val = max(0, gross_val - int(gross_val * SELL_FEE_AND_TAX_RATE))
            invested = int(p.get("invested_krw") or (buy_px * qty))
            upnl = net_val - invested
            upct = round((upnl / max(1, invested)) * 100.0, 2)
            pos_eval_krw += net_val
            unrealized_pnl_krw += upnl
            pos_rows.append({
                **p,
                "code": code,
                "current_value_krw": net_val,
                "unrealized_pnl_krw": upnl,
                "unrealized_pnl_pct": upct,
            })

        cash = int(account.get("cash_krw") or 0)
        init_cap = max(1, int(account.get("initial_capital_krw") or DEFAULT_INITIAL_CAPITAL_KRW))
        total_equity = cash + pos_eval_krw
        total_pnl_krw = total_equity - init_cap
        total_pnl_pct = round((total_pnl_krw / init_cap) * 100.0, 2)
        today_str = ref.strftime("%Y-%m-%d")
        today_realized = int((account.get("daily_realized_pnl") or {}).get(today_str, 0))
        trades_cnt = int(account.get("trade_count") or 0)
        wins = int(account.get("win_count") or 0)
        losses = int(account.get("loss_count") or 0)
        win_rate = round((wins / trades_cnt) * 100.0, 1) if trades_cnt > 0 else None

        return {
            "enabled": cfg["autotrade_enabled"],
            "broker_mode": "kis_vts" if creds["configured"] else "builtin_paper",
            "broker_label": (
                f"한국투자증권 공식 모의투자 ({creds['cano_masked']})"
                if creds["configured"]
                else "내장 실전형 가상 모의계좌"
            ),
            "kis_configured": bool(creds["configured"]),
            "kis_cano_masked": creds["cano_masked"],
            "initial_capital_krw": init_cap,
            "cash_krw": cash,
            "positions_value_krw": pos_eval_krw,
            "total_equity_krw": total_equity,
            "realized_pnl_krw": int(account.get("realized_pnl_krw") or 0),
            "unrealized_pnl_krw": unrealized_pnl_krw,
            "total_pnl_krw": total_pnl_krw,
            "total_pnl_pct": total_pnl_pct,
            "today_realized_pnl_krw": today_realized,
            "total_fees_krw": int(account.get("total_fees_krw") or 0),
            "trade_count": trades_cnt,
            "win_count": wins,
            "loss_count": losses,
            "win_rate_pct": win_rate,
            "positions": pos_rows,
            "pending_open_entries": list(account.get("pending_open_entries") or []),
            "recent_trades": self.load_trades(limit=40),
            "report_sync": __import__("report_sync").get_sync_status_summary(self.data_dir, self.repo_dir),
            "config": cfg,
        }


def format_buy_telegram_html(
    pos: dict[str, Any],
    account: dict[str, Any],
    cfg: dict[str, Any],
    *,
    now: datetime | None = None,
) -> str:
    esc = lambda s: html.escape(str(s or ""), quote=False)
    ref = radar.StateStore._now_kst(now)
    mode_txt = "한투 공식 모의투자(VTS)" if pos.get("broker_mode") == "kis_vts" else "내장 가상 모의계좌"
    code = pos["code"]
    naver = f"https://m.stock.naver.com/domestic/stock/{code}/total"
    lines = [
        f"🤖📈 <b>[모의 자동매수 체결] <a href=\"{naver}\">{esc(pos['name'])}({code})</a></b>",
        f"🕒 {ref.strftime('%H:%M:%S KST')}  |  🏦 {esc(mode_txt)}",
        "",
        f"• <b>체결 수량·단가</b>: {int(pos['qty']):,}주 × {int(pos['avg_buy_price']):,}원 (진입 시점 {pos['entry_change_pct']:+.2f}%)",
        f"• <b>총 투입금액</b>: {int(pos['invested_krw']):,}원 (남은 예수금 {int(account.get('cash_krw') or 0):,}원)",
        f"• <b>진입 근거</b>: [{esc(pos.get('order_tier'))} / {esc(pos.get('revenue_link'))}] {esc(pos.get('title'))}",
        f"• <b>자동 청산 규칙</b>: +{cfg['autotrade_take_profit_pct']:.1f}% 트레일링(-{cfg['autotrade_trailing_stop_pct']:.1f}%) / "
        f"-{cfg['autotrade_stop_loss_pct']:.1f}% 손절"
        + (f" / {cfg['autotrade_eod_exit_time']} 당일청산" if cfg["autotrade_eod_exit_enabled"] else ""),
    ]
    return "\n".join(lines)


def format_sell_telegram_html(
    trade: dict[str, Any],
    account: dict[str, Any],
    *,
    now: datetime | None = None,
) -> str:
    esc = lambda s: html.escape(str(s or ""), quote=False)
    ref = radar.StateStore._now_kst(now)
    pnl_krw = int(trade["net_pnl_krw"])
    pnl_pct = float(trade["net_pnl_pct"])
    icon = "💰" if pnl_krw >= 0 else "✂️"
    code = trade["code"]
    naver = f"https://m.stock.naver.com/domestic/stock/{code}/total"
    trades_cnt = int(account.get("trade_count") or 0)
    wins = int(account.get("win_count") or 0)
    win_rate = f"{(wins / trades_cnt) * 100.0:.1f}%" if trades_cnt > 0 else "-"
    cum_pnl = int(account.get("realized_pnl_krw") or 0)

    lines = [
        f"🤖{icon} <b>[모의 자동매도 · {esc(trade['exit_reason'])}] <a href=\"{naver}\">{esc(trade['name'])}({code})</a></b>",
        f"🕒 {ref.strftime('%H:%M:%S KST')}",
        "",
        f"• <b>매수가 → 매도가</b>: {int(trade['buy_price']):,}원 → {int(trade['sell_price']):,}원 ({int(trade['qty']):,}주)",
        f"• <b>실현 순손익</b>: <b>{pnl_krw:+,}원 ({pnl_pct:+.2f}%)</b> <i>(수수료·거래세 {int(trade['fees_and_tax_krw']):,}원 차감)</i>",
        f"• <b>누적 실현손익</b>: {cum_pnl:+,}원  |  승률 {win_rate} ({wins}승/{trades_cnt}전)",
    ]
    return "\n".join(lines)
