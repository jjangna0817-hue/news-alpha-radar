#!/usr/bin/env python3
"""News Alpha Radar — 무료 외부 HTTPS 보안 터널 매니저 (Cloudflare Quick Tunnel, 외부 패키지 0개).

동작 구조:
1. 공유기 포트포워딩·고정IP·도메인·별도 앱 설치 없이 `cloudflared` 단일 정적 바이너리를 사용자 영역(`~/.local/bin`)에 자동 준비
2. 외부 노출 전 `.env`의 `DASHBOARD_PASSWORD`가 비어 있으면 강력한 12자리 비밀번호를 자동 생성·적용(무단 접속 원천 차단)
3. `http://127.0.0.1:<DASHBOARD_PORT>`를 `https://<random>.trycloudflare.com` 암호화 터널로 연결하고 `data/tunnel_status.json` 갱신
4. 새 접속 주소가 발급되면 텔레그램으로 클릭 한 번에 접속 가능한 링크 전송 + 텔레그램 채팅에 `대시보드` 또는 `/link` 입력 시 즉시 재안내
"""

from __future__ import annotations

import argparse
import html
import json
import logging
import os
import platform
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

import radar

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("news_alpha_tunnel")

REPO_DIR = Path(__file__).resolve().parent
DEFAULT_DASHBOARD_PORT = 8787
CLOUDFLARED_RELEASE_BASE = "https://github.com/cloudflare/cloudflared/releases/latest/download"
TRYCLOUDFLARE_URL_RE = re.compile(
    r"https://([a-z0-9][a-z0-9-]{2,})\.trycloudflare\.com\b",
    re.IGNORECASE,
)
PASSWORD_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789"
NOTIFY_MIN_INTERVAL_SEC = 180
HEARTBEAT_INTERVAL_SEC = 20

LINK_COMMANDS = frozenset({"/link", "/url", "/dashboard", "/대시보드", "대시보드", "링크", "접속주소"})
PW_COMMANDS = frozenset({"/pw", "/password", "비밀번호"})


def extract_trycloudflare_url(text: str) -> str | None:
    """cloudflared 로그 문자열에서 실제 발급된 trycloudflare.com 터널 HTTPS URL만 추출."""
    if not text:
        return None
    for match in TRYCLOUDFLARE_URL_RE.finditer(text):
        sub = match.group(1).lower()
        if sub in ("api", "www", "try"):
            continue
        return f"https://{sub}.trycloudflare.com"
    return None


def read_dotenv_value(env_path: Path, key: str) -> str:
    """주어진 .env 파일에서 특정 키 값을 직접 읽어 반환 (없으면 빈 문자열)."""
    if not env_path.exists():
        return ""
    value = ""
    try:
        for raw in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            s = raw.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = s.split("=", 1)
            k = k.strip()
            if k.startswith("export "):
                k = k[7:].strip()
            if k != key:
                continue
            val = v.strip()
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
                val = val[1:-1]
            value = val.strip()
    except OSError:
        return ""
    return value


def is_tunnel_enabled(data_dir: Path, env: Mapping[str, str] | None = None) -> bool:
    """runtime_config.json > 환경변수(DASHBOARD_TUNNEL_ENABLED) 순으로 외부 터널 활성 여부 판정 (기본 True)."""
    cfg = radar.load_runtime_config(data_dir)
    if cfg.get("tunnel_enabled") is not None:
        return bool(cfg["tunnel_enabled"])
    src = env if env is not None else os.environ
    raw = str(src.get("DASHBOARD_TUNNEL_ENABLED") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    if raw in ("1", "true", "yes", "on"):
        return True
    return True


def generate_strong_password(length: int = 12) -> str:
    """모바일 입력 시 헷갈리는 문자(0/O/1/I/l)를 제외한 안전한 무작위 비밀번호 생성."""
    return "".join(secrets.choice(PASSWORD_ALPHABET) for _ in range(max(10, length)))


def _load_tunnel_state(data_dir: Path) -> dict[str, Any]:
    path = data_dir / "tunnel_state.json"
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_tunnel_state(data_dir: Path, state: dict[str, Any]) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / "tunnel_state.json"
    radar.atomic_write_text(path, json.dumps(state, ensure_ascii=False, indent=2))
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def ensure_dashboard_password(
    env_path: Path,
    *,
    data_dir: Path | None = None,
) -> tuple[str, bool]:
    """외부 터널 활성화 시 대시보드 비밀번호가 비어 있으면 자동 생성해 .env(0600)에 기록."""
    file_pw = read_dotenv_value(env_path, "DASHBOARD_PASSWORD")
    if file_pw:
        os.environ["DASHBOARD_PASSWORD"] = file_pw
        return file_pw, False

    env_pw = (os.getenv("DASHBOARD_PASSWORD") or "").strip()
    if env_pw:
        return env_pw, False

    new_pw = generate_strong_password(12)
    lines: list[str] = []
    replaced = False
    if env_path.exists():
        raw_lines = env_path.read_text(encoding="utf-8", errors="ignore").splitlines()
        for ln in raw_lines:
            stripped = ln.strip()
            if not stripped.startswith("#") and "=" in stripped:
                k, _ = stripped.split("=", 1)
                k_clean = k.strip()
                if k_clean.startswith("export "):
                    k_clean = k_clean[7:].strip()
                if k_clean == "DASHBOARD_PASSWORD":
                    lines.append(f"DASHBOARD_PASSWORD={new_pw}")
                    replaced = True
                    continue
            lines.append(ln)

    if not replaced:
        if lines and lines[-1].strip():
            lines.append("")
        lines.append("# 외부 터널 보안 보호를 위해 자동 생성된 대시보드 비밀번호")
        lines.append(f"DASHBOARD_PASSWORD={new_pw}")

    env_path.parent.mkdir(parents=True, exist_ok=True)
    radar.atomic_write_text(env_path, "\n".join(lines) + "\n")
    try:
        os.chmod(env_path, 0o600)
    except OSError:
        pass
    os.environ["DASHBOARD_PASSWORD"] = new_pw

    if data_dir is not None:
        st = _load_tunnel_state(data_dir)
        st["pending_initial_password"] = new_pw
        _save_tunnel_state(data_dir, st)

    return new_pw, True


def detect_cloudflared_asset(machine: str | None = None) -> str:
    """현재 Linux CPU 아키텍처에 맞는 공식 cloudflared 바이너리 파일명 반환."""
    arch = (machine or platform.machine() or "").strip().lower()
    if arch in ("x86_64", "amd64"):
        return "cloudflared-linux-amd64"
    if arch in ("aarch64", "arm64"):
        return "cloudflared-linux-arm64"
    if arch.startswith("arm"):
        return "cloudflared-linux-arm"
    if arch in ("i386", "i686", "x86"):
        return "cloudflared-linux-386"
    raise RuntimeError(f"지원하지 않는 CPU 아키텍처입니다: {arch}")


def _is_working_cloudflared(path: Path) -> bool:
    if not path.exists() or not os.access(path, os.X_OK):
        return False
    try:
        proc = subprocess.run(
            [str(path), "--version"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        return proc.returncode == 0
    except Exception:
        return False


def ensure_cloudflared_binary(bin_dir: Path | None = None) -> Path:
    """설치된 cloudflared를 탐색하거나, 없으면 ~/.local/bin/cloudflared 에 무권한(0원·no sudo) 자동 다운로드."""
    env_bin = (os.getenv("CLOUDFLARED_BIN") or "").strip()
    if env_bin:
        p = Path(env_bin)
        if _is_working_cloudflared(p):
            return p

    which_bin = shutil.which("cloudflared")
    if which_bin:
        p = Path(which_bin)
        if _is_working_cloudflared(p):
            return p

    target_dir = bin_dir if bin_dir is not None else (Path.home() / ".local" / "bin")
    target = target_dir / "cloudflared"
    if _is_working_cloudflared(target):
        return target

    if sys.platform != "linux":
        raise RuntimeError(
            "cloudflared가 설치되어 있지 않습니다. (macOS: `brew install cloudflared` 후 다시 실행하세요)"
        )

    asset = detect_cloudflared_asset()
    download_url = f"{CLOUDFLARED_RELEASE_BASE}/{asset}"
    target_dir.mkdir(parents=True, exist_ok=True)
    tmp_target = target_dir / f".cloudflared.{os.getpid()}.tmp"

    logger.info("cloudflared 정적 바이너리 자동 다운로드 시작: %s -> %s", download_url, target)
    req = urllib.request.Request(download_url, headers={"User-Agent": radar.USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=90) as resp, tmp_target.open("wb") as out_fp:
            shutil.copyfileobj(resp, out_fp)
        os.chmod(tmp_target, 0o755)
        if not _is_working_cloudflared(tmp_target):
            raise RuntimeError("다운로드된 cloudflared 바이너리 실행 검증에 실패했습니다.")
        os.replace(tmp_target, target)
    except Exception:
        try:
            tmp_target.unlink(missing_ok=True)
        except OSError:
            pass
        raise

    logger.info("cloudflared 준비 완료: %s", target)
    return target


def detect_lan_ip() -> str:
    """공유기 내부 와이파이 사설 IP 감지 (외부 패킷 전송 없이 라우팅 인터페이스 조회)."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            if ip and not ip.startswith("127."):
                return ip
    except Exception:
        pass
    return "127.0.0.1"


def write_tunnel_status(data_dir: Path, payload: dict[str, Any]) -> None:
    try:
        data_dir.mkdir(parents=True, exist_ok=True)
        radar.atomic_write_text(
            data_dir / "tunnel_status.json",
            json.dumps(payload, ensure_ascii=False, indent=2),
        )
    except Exception as exc:
        logger.warning("tunnel_status.json 기록 실패: %s", exc)


def read_tunnel_status(data_dir: Path, now: datetime | None = None) -> dict[str, Any]:
    path = data_dir / "tunnel_status.json"
    if not path.exists():
        return {"state": "offline", "alive": False, "url": None}
    try:
        st = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(st, dict):
            return {"state": "offline", "alive": False, "url": None}
    except Exception:
        return {"state": "offline", "alive": False, "url": None}

    ref = radar.StateStore._now_kst(now)
    hb = radar.parse_iso_kst(st.get("heartbeat_at"))
    age_sec = int((ref - hb).total_seconds()) if hb is not None else 999999
    state_val = str(st.get("state") or "offline")
    alive = state_val in ("running", "starting", "installing") and age_sec <= 90
    st["heartbeat_age_sec"] = age_sec if hb is not None else None
    st["alive"] = alive
    if not alive and state_val not in ("stopped", "disabled"):
        st["state"] = "offline"
        st["url"] = None
    return st


def format_tunnel_telegram_html(
    url: str,
    *,
    port: int = DEFAULT_DASHBOARD_PORT,
    lan_ip: str = "",
    generated_password: str | None = None,
) -> str:
    """텔레그램에서 클릭 한 번으로 접속 가능한 외부 대시보드 안내 메시지 생성."""
    safe_url = html.escape(url, quote=True)
    lines = [
        "🌐 <b>[News Alpha Radar 외부 접속 주소]</b>",
        "LTE/5G 데이터·외부 와이파이에서 아래 링크를 눌러 바로 접속하세요:",
        "",
        f"👉 <a href=\"{safe_url}\">{safe_url}</a>",
        "",
    ]
    if generated_password:
        lines.append(f"🔑 자동 설정된 비밀번호: <code>{html.escape(generated_password)}</code>")
        lines.append("   <i>(한 번 로그인하면 브라우저에 30일간 자동 유지됩니다)</i>")
    else:
        lines.append("🔐 비밀번호: <code>.env</code>에 설정된 <code>DASHBOARD_PASSWORD</code> (1회 로그인 시 30일 유지)")
    if lan_ip and lan_ip != "127.0.0.1":
        lines.append(f"🏠 집 와이파이 내부 주소: <code>http://{html.escape(lan_ip)}:{port}</code>")
    lines.append("💡 <i>언제든 이 채팅창에</i> <code>대시보드</code> <i>또는</i> <code>/link</code><i>를 입력하면 현재 링크를 다시 보내드립니다.</i>")
    return "\n".join(lines)


def send_tunnel_link_telegram(
    data_dir: Path,
    repo_dir: Path = REPO_DIR,
    *,
    url_override: str | None = None,
    include_generated_password: bool = False,
) -> bool:
    """현재 활성 터널 URL을 텔레그램으로 전송."""
    env_path = repo_dir / ".env"
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or read_dotenv_value(env_path, "TELEGRAM_BOT_TOKEN")).strip()
    chat_id = (os.getenv("TELEGRAM_CHAT_ID") or read_dotenv_value(env_path, "TELEGRAM_CHAT_ID")).strip()
    if not (token and chat_id):
        logger.info("TELEGRAM_BOT_TOKEN/CHAT_ID 미설정으로 터널 링크 텔레그램 알림을 건너뜁니다.")
        return False

    st = read_tunnel_status(data_dir)
    url = (url_override or str(st.get("url") or "")).strip()
    if not url:
        raise RuntimeError("현재 활성화된 외부 접속 터널 주소가 없습니다.")

    port_str = os.getenv("DASHBOARD_PORT") or read_dotenv_value(env_path, "DASHBOARD_PORT") or str(DEFAULT_DASHBOARD_PORT)
    try:
        port = int(port_str)
    except ValueError:
        port = DEFAULT_DASHBOARD_PORT

    tstate = _load_tunnel_state(data_dir)
    gen_pw = str(tstate.get("pending_initial_password") or "").strip() if include_generated_password else ""

    msg = format_tunnel_telegram_html(
        url,
        port=port,
        lan_ip=detect_lan_ip(),
        generated_password=gen_pw or None,
    )
    radar.send_telegram(token, chat_id, msg)

    tstate["last_notified_url"] = url
    tstate["last_notified_at"] = datetime.now(radar.KST).isoformat()
    if gen_pw:
        tstate.pop("pending_initial_password", None)
    _save_tunnel_state(data_dir, tstate)
    return True


def bootstrap_systemd_tunnel_if_needed(
    repo_dir: Path = REPO_DIR,
    python_bin: str | None = None,
    data_dir: Path | None = None,
) -> bool:
    """기존 우분투 systemd 설치 환경이 자동 업데이트로 올라왔을 때 news-alpha-tunnel.service를 자동 등록·기동."""
    if sys.platform != "linux" or not shutil.which("systemctl"):
        return False
    base_dir = data_dir if data_dir is not None else (repo_dir / "data")
    if not is_tunnel_enabled(base_dir):
        return False

    unit_dir = Path.home() / ".config" / "systemd" / "user"
    dash_unit = unit_dir / "news-alpha-dashboard.service"
    tunnel_unit = unit_dir / "news-alpha-tunnel.service"
    template = repo_dir / "deploy" / "ubuntu" / "news-alpha-tunnel.service"

    # systemd user 배포 환경(install.sh로 설치된 서버)일 때만 동작
    if not dash_unit.exists() or not template.exists():
        return False

    ensure_dashboard_password(repo_dir / ".env", data_dir=base_dir)

    py_exec = python_bin or sys.executable
    if not tunnel_unit.exists():
        rendered = (
            template.read_text(encoding="utf-8")
            .replace("__WORKDIR__", str(repo_dir))
            .replace("__PYTHON__", str(py_exec))
        )
        radar.atomic_write_text(tunnel_unit, rendered)
        subprocess.run(["systemctl", "--user", "daemon-reload"], check=False, timeout=10)

    active = subprocess.run(
        ["systemctl", "--user", "is-active", "--quiet", "news-alpha-tunnel.service"],
        check=False,
        timeout=5,
    )
    if active.returncode != 0:
        subprocess.run(
            ["systemctl", "--user", "enable", "--now", "news-alpha-tunnel.service"],
            check=False,
            timeout=15,
        )
        logger.info("news-alpha-tunnel.service 자동 활성화 완료")
        return True
    return False


def _bot_command_responder_loop(
    stop_evt: threading.Event,
    data_dir: Path,
    repo_dir: Path,
) -> None:
    """텔레그램 봇 채팅에 `대시보드` / `/link` (또는 1:1 채팅에서 `/pw`) 입력 시 즉시 응답."""
    env_path = repo_dir / ".env"
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or read_dotenv_value(env_path, "TELEGRAM_BOT_TOKEN")).strip()
    target_chat = (os.getenv("TELEGRAM_CHAT_ID") or read_dotenv_value(env_path, "TELEGRAM_CHAT_ID")).strip()
    if not (token and target_chat):
        return

    api_base = f"https://api.telegram.org/bot{token}"
    offset = 0

    def _get_updates(cur_offset: int, poll_timeout: int) -> list[dict[str, Any]] | None:
        qs = urllib.parse.urlencode({
            "offset": cur_offset,
            "timeout": poll_timeout,
            "allowed_updates": json.dumps(["message"]),
        })
        req = urllib.request.Request(f"{api_base}/getUpdates?{qs}", headers={"User-Agent": radar.USER_AGENT})
        with urllib.request.urlopen(req, timeout=poll_timeout + 10) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if not payload.get("ok"):
            return []
        res = payload.get("result")
        return res if isinstance(res, list) else []

    # 시작 이전 과거 메시지는 건너뜀
    try:
        init_rows = _get_updates(-1, 0) or []
        if init_rows:
            offset = int(init_rows[-1].get("update_id", 0)) + 1
    except urllib.error.HTTPError as exc:
        if exc.code == 409:
            logger.info("텔레그램 봇이 다른 웹훅/폴러에서 사용 중이어서 명령어 자동응답만 비활성화합니다.")
            return
    except Exception:
        pass

    while not stop_evt.is_set():
        try:
            updates = _get_updates(offset, 20) or []
            for upd in updates:
                uid = int(upd.get("update_id", 0))
                offset = max(offset, uid + 1)
                msg = upd.get("message")
                if not isinstance(msg, dict):
                    continue
                chat = msg.get("chat") or {}
                chat_id_str = str(chat.get("id") or "")
                if chat_id_str != target_chat:
                    continue
                text = str(msg.get("text") or "").strip()
                if not text:
                    continue
                first_tok = text.split()[0].split("@")[0].strip().lower()
                if first_tok in LINK_COMMANDS:
                    st = read_tunnel_status(data_dir)
                    live_url = str(st.get("url") or "").strip()
                    if st.get("alive") and live_url:
                        send_tunnel_link_telegram(data_dir, repo_dir, url_override=live_url, include_generated_password=False)
                    elif not is_tunnel_enabled(data_dir):
                        radar.send_telegram(token, target_chat, "⏸ 현재 외부 접속 터널이 꺼져 있습니다. 집 와이파이 대시보드 설정에서 켤 수 있습니다.")
                    else:
                        radar.send_telegram(token, target_chat, "⏳ 외부 접속 보안 터널을 연결하고 있습니다. 5~10초 뒤 다시 입력해 주세요.")
                elif first_tok in PW_COMMANDS and str(chat.get("type") or "") == "private":
                    pw = (read_dotenv_value(env_path, "DASHBOARD_PASSWORD") or os.getenv("DASHBOARD_PASSWORD") or "").strip()
                    if pw:
                        radar.send_telegram(token, target_chat, f"🔑 현재 대시보드 접속 비밀번호: <code>{html.escape(pw)}</code>")
        except urllib.error.HTTPError as exc:
            if exc.code == 409:
                logger.info("텔레그램 봇 getUpdates 409 감지 — 명령어 자동응답 스레드 종료 (터널은 정상 유지).")
                return
            stop_evt.wait(5.0)
        except Exception:
            stop_evt.wait(5.0)


def run_tunnel_supervisor(
    *,
    data_dir: Path,
    repo_dir: Path = REPO_DIR,
    port: int | None = None,
    stop_event: threading.Event | None = None,
) -> None:
    """cloudflared Quick Tunnel 상시 실행·감시 및 텔레그램 주소 자동 안내 루프."""
    stop_evt = stop_event if stop_event is not None else threading.Event()
    proc_holder: dict[str, subprocess.Popen[str] | None] = {"proc": None}

    if stop_event is None and threading.current_thread() is threading.main_thread():
        def _on_sig(signum: int, _frame: Any) -> None:
            logger.info("터널 종료 시그널(%d) 수신 — 안전 종료합니다.", signum)
            stop_evt.set()
            p = proc_holder.get("proc")
            if p is not None and p.poll() is None:
                with contextlib.suppress(Exception):
                    p.terminate()

        import contextlib
        signal.signal(signal.SIGTERM, _on_sig)
        signal.signal(signal.SIGINT, _on_sig)

    env_path = repo_dir / ".env"
    eff_port = port or radar._env_int("DASHBOARD_PORT", DEFAULT_DASHBOARD_PORT)
    target_url = f"http://127.0.0.1:{eff_port}"

    responder = threading.Thread(
        target=_bot_command_responder_loop,
        args=(stop_evt, data_dir, repo_dir),
        daemon=True,
    )
    responder.start()

    backoff_sec = 3.0
    last_notify_mono = 0.0

    while not stop_evt.is_set():
        if not is_tunnel_enabled(data_dir):
            write_tunnel_status(
                data_dir,
                {
                    "pid": os.getpid(),
                    "state": "disabled",
                    "url": None,
                    "target": target_url,
                    "heartbeat_at": datetime.now(radar.KST).isoformat(),
                    "last_error": None,
                },
            )
            stop_evt.wait(5.0)
            continue

        # 1) 외부 노출 전 반드시 대시보드 비밀번호 보장
        _, created_pw = ensure_dashboard_password(env_path, data_dir=data_dir)
        if created_pw:
            logger.info("외부 접속 보안을 위해 .env에 DASHBOARD_PASSWORD를 자동 생성했습니다.")

        # 2) cloudflared 바이너리 확보
        try:
            write_tunnel_status(
                data_dir,
                {
                    "pid": os.getpid(),
                    "state": "installing",
                    "url": None,
                    "target": target_url,
                    "heartbeat_at": datetime.now(radar.KST).isoformat(),
                    "last_error": None,
                },
            )
            cf_bin = ensure_cloudflared_binary()
        except Exception as exc:
            err_msg = str(exc)[:200]
            logger.error("cloudflared 준비 실패: %s", err_msg)
            write_tunnel_status(
                data_dir,
                {
                    "pid": os.getpid(),
                    "state": "error",
                    "url": None,
                    "target": target_url,
                    "heartbeat_at": datetime.now(radar.KST).isoformat(),
                    "last_error": err_msg,
                },
            )
            stop_evt.wait(min(60.0, backoff_sec))
            backoff_sec = min(60.0, backoff_sec * 2)
            continue

        started_at = datetime.now(radar.KST).isoformat()
        started_mono = time.monotonic()
        current_url: str | None = None
        last_err_line: str | None = None
        url_lock = threading.Lock()

        write_tunnel_status(
            data_dir,
            {
                "pid": os.getpid(),
                "state": "starting",
                "url": None,
                "target": target_url,
                "started_at": started_at,
                "heartbeat_at": started_at,
                "last_error": None,
            },
        )

        cmd = [str(cf_bin), "tunnel", "--no-autoupdate", "--url", target_url]
        logger.info("Cloudflare 보안 터널 시작: %s", " ".join(cmd))
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            proc_holder["proc"] = proc
        except Exception as exc:
            err_msg = str(exc)[:200]
            logger.error("cloudflared 프로세스 실행 실패: %s", err_msg)
            write_tunnel_status(
                data_dir,
                {
                    "pid": os.getpid(),
                    "state": "error",
                    "url": None,
                    "target": target_url,
                    "heartbeat_at": datetime.now(radar.KST).isoformat(),
                    "last_error": err_msg,
                },
            )
            stop_evt.wait(min(60.0, backoff_sec))
            backoff_sec = min(60.0, backoff_sec * 2)
            continue

        def _reader() -> None:
            nonlocal current_url, last_err_line, last_notify_mono
            assert proc.stdout is not None
            for line in proc.stdout:
                s = line.strip()
                if not s:
                    continue
                found = extract_trycloudflare_url(s)
                if found and current_url is None:
                    with url_lock:
                        current_url = found
                    logger.info("🌐 외부 접속 HTTPS 주소 발급 완료: %s", found)
                    write_tunnel_status(
                        data_dir,
                        {
                            "pid": os.getpid(),
                            "tunnel_pid": proc.pid,
                            "state": "running",
                            "url": found,
                            "target": target_url,
                            "started_at": started_at,
                            "heartbeat_at": datetime.now(radar.KST).isoformat(),
                            "last_error": None,
                        },
                    )
                    # DNS 전파 대기(3초) 후 텔레그램 알림 발송
                    if not stop_evt.wait(3.0) and proc.poll() is None:
                        tstate = _load_tunnel_state(data_dir)
                        prev_url = str(tstate.get("last_notified_url") or "")
                        has_pending_pw = bool(tstate.get("pending_initial_password"))
                        now_m = time.monotonic()
                        if found != prev_url and (
                            has_pending_pw
                            or last_notify_mono == 0.0
                            or (now_m - last_notify_mono) >= NOTIFY_MIN_INTERVAL_SEC
                        ):
                            try:
                                if send_tunnel_link_telegram(
                                    data_dir,
                                    repo_dir,
                                    url_override=found,
                                    include_generated_password=True,
                                ):
                                    last_notify_mono = now_m
                                    logger.info("텔레그램으로 외부 접속 HTTPS 링크를 발송했습니다.")
                            except Exception as tg_exc:
                                logger.warning("터널 링크 텔레그램 발송 실패: %s", tg_exc)
                elif "ERR " in s or "error" in s.lower():
                    last_err_line = s[:180]

        reader_thread = threading.Thread(target=_reader, daemon=True)
        reader_thread.start()

        while not stop_evt.is_set() and proc.poll() is None:
            if not is_tunnel_enabled(data_dir):
                logger.info("설정에서 외부 접속 터널이 비활성화되어 터널을 중지합니다.")
                proc.terminate()
                break
            with url_lock:
                u = current_url
            write_tunnel_status(
                data_dir,
                {
                    "pid": os.getpid(),
                    "tunnel_pid": proc.pid,
                    "state": "running" if u else "starting",
                    "url": u,
                    "target": target_url,
                    "started_at": started_at,
                    "heartbeat_at": datetime.now(radar.KST).isoformat(),
                    "last_error": None if u else last_err_line,
                },
            )
            stop_evt.wait(HEARTBEAT_INTERVAL_SEC)

        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        proc_holder["proc"] = None
        reader_thread.join(timeout=2.0)

        if stop_evt.is_set():
            break

        ran_sec = time.monotonic() - started_mono
        if ran_sec >= 60.0:
            backoff_sec = 3.0
        if is_tunnel_enabled(data_dir):
            logger.warning("보안 터널 프로세스 종료됨 — %.0f초 후 자동 재연결합니다.", backoff_sec)
            stop_evt.wait(backoff_sec)
            backoff_sec = min(30.0, backoff_sec * 1.8)

    write_tunnel_status(
        data_dir,
        {
            "pid": os.getpid(),
            "state": "stopped",
            "url": None,
            "target": target_url,
            "heartbeat_at": datetime.now(radar.KST).isoformat(),
            "last_error": None,
        },
    )


def main() -> int:
    radar.load_dotenv_file()
    parser = argparse.ArgumentParser(description="News Alpha Radar External HTTPS Tunnel Manager")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument(
        "--ensure-password",
        action="store_true",
        help=".env에 DASHBOARD_PASSWORD가 비어 있으면 자동 생성 후 비밀번호와 생성 여부(NEW/EXISTING) 출력",
    )
    args = parser.parse_args()
    data_dir = Path(args.data_dir)
    if args.ensure_password:
        pw, created = ensure_dashboard_password(REPO_DIR / ".env", data_dir=data_dir)
        print(f"STATUS={'CREATED' if created else 'EXISTING'}")
        print(f"PASSWORD={pw}")
        return 0

    run_tunnel_supervisor(data_dir=data_dir, repo_dir=REPO_DIR, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
