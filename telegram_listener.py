#!/usr/bin/env python3
"""News Alpha Radar — 텔레그램 속보 채널 0초 실시간 콜백 리스너 (Telethon 기반, 선택 모듈).

동작 구조 (락 경합 없는 이벤트 인박스 패턴):
1. 구독 중인 텔레그램 속보 채널에 새 메시지가 올라오는 순간(0초 MTProto 푸시 콜백) 수신
2. 광고·단문 노이즈를 걸러낸 뒤 `data/inbox/<ts>_<hash>.json` 원자적 기록 + `data/trigger_now` 깨우기
3. `radar.py --daemon`이 0.5초 내 기상하여 RSS 대기 없이 즉시 Stage 1 → 3초 1보 속보 푸시 → 2보 심층 리포트 발송

최초 1회 로그인:
  .venv/bin/python telegram_listener.py --login
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import radar

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("news_alpha_telegram_listener")

URL_RE = re.compile(r"https?://[^\s<>()\"']+", re.IGNORECASE)
SPAM_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"(무료\s*리딩|유료\s*리딩|리딩방|추천인\s*코드|가입\s*이벤트|카톡방\s*입장)",
        r"(도박|토토|카지노|선물\s*리딩|코인\s*신호방|광고\s*문의)",
        r"^\[?광고\]?",
    ]
]


def parse_channel_list(raw: str | list[str] | None) -> list[str]:
    """쉼표/줄바꿈 구분 문자열 또는 리스트에서 텔레그램 채널 식별자 정규화."""
    if raw is None:
        return []
    tokens: list[str] = []
    if isinstance(raw, list):
        for item in raw:
            tokens.extend(re.split(r"[\s,]+", str(item or "").strip()))
    else:
        tokens.extend(re.split(r"[\s,]+", str(raw).strip()))

    cleaned: list[str] = []
    seen: set[str] = set()
    for tok in tokens:
        t = tok.strip()
        if not t:
            continue
        t = re.sub(r"^(?:https?://)?(?:t\.me|telegram\.me)/", "", t, flags=re.IGNORECASE)
        t = t.split("?", 1)[0].strip("/")
        if "/" in t and not t.startswith("+"):
            t = t.split("/", 1)[0]
        if t.startswith("@"):
            t = t[1:]
        if not t:
            continue
        key = t.lower()
        if key not in seen:
            seen.add(key)
            cleaned.append(t)
    return cleaned


def resolve_source_channels(data_dir: Path) -> list[str]:
    """runtime_config.json(웹 대시보드) > 환경변수(.env) 순으로 구독 채널 목록 결정."""
    cfg = radar.load_runtime_config(data_dir)
    if isinstance(cfg.get("telegram_source_channels"), list):
        ch = parse_channel_list(cfg["telegram_source_channels"])
        if ch:
            return ch
    return parse_channel_list(os.getenv("TELEGRAM_SOURCE_CHANNELS", ""))


def extract_message_payload(
    text: str,
    channel_name: str,
    msg_id: int,
    *,
    channel_username: str | None = None,
    msg_dt: datetime | None = None,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    """텔레그램 채널 원문 메시지를 News Alpha Radar 인박스 이벤트 dict로 변환."""
    raw = (text or "").strip()
    if not raw:
        return None
    if any(p.search(raw) for p in SPAM_PATTERNS):
        return None

    lines = [re.sub(r"\s+", " ", ln).strip() for ln in raw.splitlines() if ln.strip()]
    if not lines:
        return None

    # 첫 줄이 순수 URL뿐이면 다음 줄을 제목으로 사용
    title_line = lines[0]
    if URL_RE.fullmatch(title_line) and len(lines) > 1:
        title_line = lines[1]

    # 제목 끝의 URL·마크다운 강조 기호 제거
    title = URL_RE.sub("", title_line).replace("**", "").replace("__", "").strip(" -·•|[]")
    title = re.sub(r"\s+", " ", title).strip()[:140]
    if len(title) < 10:
        return None

    if any(p.search(title) for p in radar.NOISE_PATTERNS):
        return None

    url_match = URL_RE.search(raw)
    if url_match:
        url = url_match.group(0).rstrip(".,);]")
    elif channel_username:
        url = f"https://t.me/{channel_username.lstrip('@')}/{msg_id}"
    else:
        url = f"https://t.me/c/{abs(hash(channel_name)) % 10**9}/{msg_id}"

    ref = radar.StateStore._now_kst(now)
    pub_dt = radar.StateStore._now_kst(msg_dt) if msg_dt is not None else ref
    summary = re.sub(r"\s+", " ", URL_RE.sub("", raw)).strip()[:360]
    clean_ch = re.sub(r"\s+", "", channel_name or channel_username or "채널")[:30]

    return {
        "title": title,
        "url": url,
        "source": f"텔레그램_{clean_ch}",
        "category": "속보",
        "published_at": pub_dt.isoformat(),
        "summary": summary,
        "priority": 0,
        "first_seen": ref.isoformat(),
    }


def enqueue_inbox_event(data_dir: Path, event: dict[str, Any]) -> Path:
    """data/inbox/에 원자적 이벤트 파일 생성 후 데몬 기상 신호(data/trigger_now) 기록."""
    inbox_dir = data_dir / "inbox"
    inbox_dir.mkdir(parents=True, exist_ok=True)
    epoch_ms = int(time.time() * 1000)
    digest = hashlib.sha1(f"{event.get('url','')}|{event.get('title','')}".encode("utf-8")).hexdigest()[:8]
    target = inbox_dir / f"{epoch_ms}_{digest}.json"
    radar.atomic_write_text(target, json.dumps(event, ensure_ascii=False))
    radar.atomic_write_text(data_dir / "trigger_now", "inbox")
    return target


def write_listener_status(data_dir: Path, payload: dict[str, Any]) -> None:
    try:
        radar.atomic_write_text(
            data_dir / "listener_status.json",
            json.dumps(payload, ensure_ascii=False, indent=2),
        )
    except Exception as exc:
        logger.warning("listener_status.json 기록 실패: %s", exc)


def _require_telethon() -> tuple[Any, Any]:
    try:
        from telethon import TelegramClient, events  # type: ignore[import-not-found]
        return TelegramClient, events
    except ImportError as exc:
        raise RuntimeError(
            "telethon 패키지가 설치되지 않았습니다. "
            "우분투에서 `.venv/bin/pip install -r requirements-realtime.txt` 를 실행하세요."
        ) from exc


def _read_api_creds() -> tuple[int, str]:
    api_id_raw = (os.getenv("TELEGRAM_API_ID") or "").strip()
    api_hash = (os.getenv("TELEGRAM_API_HASH") or "").strip()
    if not (api_id_raw.isdigit() and api_hash):
        raise RuntimeError(
            "TELEGRAM_API_ID 또는 TELEGRAM_API_HASH가 비어 있습니다. "
            "https://my.telegram.org 에서 무료 발급 후 .env에 입력하세요."
        )
    return int(api_id_raw), api_hash


async def _run_interactive_login(data_dir: Path) -> None:
    TelegramClient, _ = _require_telethon()
    api_id, api_hash = _read_api_creds()
    data_dir.mkdir(parents=True, exist_ok=True)
    session_base = str(data_dir / "telegram_user")
    client = TelegramClient(session_base, api_id, api_hash)
    await client.start()
    me = await client.get_me()
    session_file = Path(session_base + ".session")
    if session_file.exists():
        try:
            os.chmod(session_file, 0o600)
        except OSError:
            pass
    logger.info("텔레그램 실시간 콜백 로그인 완료! 계정: %s (@%s)", getattr(me, "first_name", ""), getattr(me, "username", ""))
    await client.disconnect()


async def _run_listener_async(data_dir: Path) -> None:
    TelegramClient, events = _require_telethon()
    api_id, api_hash = _read_api_creds()
    data_dir.mkdir(parents=True, exist_ok=True)
    session_base = str(data_dir / "telegram_user")
    session_file = Path(session_base + ".session")
    if not session_file.exists():
        msg = "텔레그램 세션 파일이 없습니다. 먼저 `.venv/bin/python telegram_listener.py --login` 으로 1회 로그인하세요."
        write_listener_status(
            data_dir,
            {
                "pid": os.getpid(),
                "state": "needs_login",
                "heartbeat_at": datetime.now(radar.KST).isoformat(),
                "channels": resolve_source_channels(data_dir),
                "received_count": 0,
                "last_error": msg,
            },
        )
        raise RuntimeError(msg)

    client = TelegramClient(session_base, api_id, api_hash)
    await client.connect()
    if not await client.is_user_authorized():
        msg = "텔레그램 세션이 만료되었습니다. `--login` 으로 다시 인증하세요."
        write_listener_status(
            data_dir,
            {
                "pid": os.getpid(),
                "state": "needs_login",
                "heartbeat_at": datetime.now(radar.KST).isoformat(),
                "channels": resolve_source_channels(data_dir),
                "received_count": 0,
                "last_error": msg,
            },
        )
        await client.disconnect()
        raise RuntimeError(msg)

    started_at = datetime.now(radar.KST).isoformat()
    received_count = 0
    last_event_at: str | None = None
    last_error: str | None = None
    active_channels: list[str] = []
    active_chat_ids: set[int] = set()

    async def _refresh_channels() -> None:
        nonlocal active_channels, active_chat_ids, last_error
        wanted = resolve_source_channels(data_dir)
        if wanted == active_channels and (active_chat_ids or not wanted):
            return
        resolved_ids: set[int] = set()
        resolved_names: list[str] = []
        errs: list[str] = []
        for ch in wanted:
            try:
                ent = await client.get_entity(ch)
                cid = getattr(ent, "id", None)
                if cid is not None:
                    resolved_ids.add(int(cid))
                    resolved_names.append(ch)
            except Exception as exc:
                errs.append(f"{ch}: {str(exc)[:60]}")
        active_channels = wanted
        active_chat_ids = resolved_ids
        last_error = "; ".join(errs) if errs else None
        logger.info("텔레그램 콜백 구독 채널 갱신: %s (실제 연결 %d개)", resolved_names, len(resolved_ids))

    @client.on(events.NewMessage())
    async def _on_new_message(event: Any) -> None:
        nonlocal received_count, last_event_at
        if not active_chat_ids:
            return
        try:
            chat = await event.get_chat()
            cid = getattr(chat, "id", None)
            if cid is None or int(cid) not in active_chat_ids:
                return
            text = getattr(event.message, "message", "") or getattr(event, "raw_text", "") or ""
            ch_title = getattr(chat, "title", None) or getattr(chat, "username", None) or str(cid)
            ch_user = getattr(chat, "username", None)
            msg_dt = getattr(event.message, "date", None)
            payload = extract_message_payload(
                text,
                str(ch_title),
                int(getattr(event.message, "id", 0)),
                channel_username=str(ch_user) if ch_user else None,
                msg_dt=msg_dt,
            )
            if payload is None:
                return
            enqueue_inbox_event(data_dir, payload)
            received_count += 1
            last_event_at = datetime.now(radar.KST).isoformat()
            logger.info("⚡ 실시간 콜백 수신 [%s]: %s", payload["source"], payload["title"][:60])
            write_listener_status(
                data_dir,
                {
                    "pid": os.getpid(),
                    "state": "running",
                    "started_at": started_at,
                    "heartbeat_at": last_event_at,
                    "channels": active_channels,
                    "connected_count": len(active_chat_ids),
                    "received_count": received_count,
                    "last_event_at": last_event_at,
                    "last_error": last_error,
                },
            )
        except Exception as exc:
            logger.warning("텔레그램 콜백 메시지 처리 오류: %s", exc)

    await _refresh_channels()

    async def _heartbeat_loop() -> None:
        while True:
            await _refresh_channels()
            write_listener_status(
                data_dir,
                {
                    "pid": os.getpid(),
                    "state": "running" if active_chat_ids else "waiting_channels",
                    "started_at": started_at,
                    "heartbeat_at": datetime.now(radar.KST).isoformat(),
                    "channels": active_channels,
                    "connected_count": len(active_chat_ids),
                    "received_count": received_count,
                    "last_event_at": last_event_at,
                    "last_error": last_error,
                },
            )
            await asyncio.sleep(20)

    hb_task = asyncio.create_task(_heartbeat_loop())
    logger.info("News Alpha Radar 텔레그램 0초 콜백 리스너 가동 중...")
    try:
        await client.run_until_disconnected()
    finally:
        hb_task.cancel()
        write_listener_status(
            data_dir,
            {
                "pid": os.getpid(),
                "state": "stopped",
                "started_at": started_at,
                "heartbeat_at": datetime.now(radar.KST).isoformat(),
                "channels": active_channels,
                "received_count": received_count,
                "last_event_at": last_event_at,
                "last_error": last_error,
            },
        )
        await client.disconnect()


def main() -> int:
    radar.load_dotenv_file()
    parser = argparse.ArgumentParser(description="News Alpha Radar Telegram Realtime Callback Listener")
    parser.add_argument("--login", action="store_true", help="최초 1회 텔레그램 계정 로그인(.session 생성)")
    parser.add_argument("--data-dir", default="data")
    args = parser.parse_args()
    data_dir = Path(args.data_dir)
    try:
        if args.login:
            asyncio.run(_run_interactive_login(data_dir))
        else:
            asyncio.run(_run_listener_async(data_dir))
        return 0
    except KeyboardInterrupt:
        logger.info("텔레그램 리스너 종료.")
        return 0
    except Exception as exc:
        logger.error("텔레그램 리스너 오류: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
