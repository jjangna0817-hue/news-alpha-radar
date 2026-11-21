#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-$(command -v python3 || true)}"

if [ -z "${PYTHON_BIN}" ]; then
  echo "[오류] python3가 설치되어 있지 않습니다. 먼저 설치하세요: sudo apt update && sudo apt install -y python3"
  exit 1
fi

"${PYTHON_BIN}" -c 'import sys; assert sys.version_info >= (3, 11), f"Python 3.11+ 필요 (현재 {sys.version})"'
echo "[확인] Python: $(${PYTHON_BIN} --version)"

mkdir -p "${REPO_DIR}/data/inbox"

if [ ! -f "${REPO_DIR}/.env" ]; then
  cp "${REPO_DIR}/.env.example" "${REPO_DIR}/.env"
  chmod 600 "${REPO_DIR}/.env"
  echo ""
  echo "[안내] .env 파일을 새로 만들었습니다: ${REPO_DIR}/.env"
  echo "       먼저 아래 명령으로 API 키와 텔레그램 정보를 입력한 뒤 이 스크립트를 다시 실행하세요:"
  echo "       nano ${REPO_DIR}/.env"
  exit 0
fi

if grep -q "여기에_" "${REPO_DIR}/.env"; then
  echo "[오류] ${REPO_DIR}/.env 안에 아직 예시 문구(여기에_...)가 남아 있습니다."
  echo "       nano ${REPO_DIR}/.env 로 실제 키를 입력한 뒤 다시 실행하세요."
  exit 1
fi

chmod 600 "${REPO_DIR}/.env"
chmod +x "${SCRIPT_DIR}/update.sh" "${SCRIPT_DIR}/install.sh" || true

TUNNEL_RAW="$(grep -E '^DASHBOARD_TUNNEL_ENABLED=' "${REPO_DIR}/.env" | tail -n 1 | cut -d= -f2- | tr -d ' "\r' | tr '[:upper:]' '[:lower:]' || true)"
TUNNEL_ENABLED=1
if [ "${TUNNEL_RAW}" = "0" ] || [ "${TUNNEL_RAW}" = "false" ] || [ "${TUNNEL_RAW}" = "no" ] || [ "${TUNNEL_RAW}" = "off" ]; then
  TUNNEL_ENABLED=0
fi

PW_INFO=""
if [ "${TUNNEL_ENABLED}" = "1" ]; then
  PW_OUT="$("${PYTHON_BIN}" "${REPO_DIR}/tunnel_manager.py" --data-dir "${REPO_DIR}/data" --ensure-password || true)"
  PW_STATUS="$(printf '%s\n' "${PW_OUT}" | awk -F= '/^STATUS=/{print $2; exit}')"
  PW_VAL="$(printf '%s\n' "${PW_OUT}" | awk -F= '/^PASSWORD=/{print $2; exit}')"
  if [ "${PW_STATUS}" = "CREATED" ] && [ -n "${PW_VAL}" ]; then
    PW_INFO="${PW_VAL} (자동 생성됨 — 텔레그램으로도 안내됩니다)"
  elif [ -n "${PW_VAL}" ]; then
    PW_INFO="${PW_VAL}"
  fi
fi

UNIT_DIR="${HOME}/.config/systemd/user"
mkdir -p "${UNIT_DIR}"

for svc in news-alpha-radar.service news-alpha-dashboard.service news-alpha-updater.service news-alpha-updater.timer news-alpha-telegram.service news-alpha-tunnel.service; do
  sed \
    -e "s|__WORKDIR__|${REPO_DIR}|g" \
    -e "s|__PYTHON__|${PYTHON_BIN}|g" \
    "${SCRIPT_DIR}/${svc}" > "${UNIT_DIR}/${svc}"
  echo "[설치] ${UNIT_DIR}/${svc}"
done

systemctl --user daemon-reload
systemctl --user enable --now news-alpha-radar.service news-alpha-dashboard.service news-alpha-updater.timer
systemctl --user restart news-alpha-radar.service news-alpha-dashboard.service

TUNNEL_URL_MSG="비활성화됨 (DASHBOARD_TUNNEL_ENABLED=false)"
if [ "${TUNNEL_ENABLED}" = "1" ]; then
  systemctl --user enable --now news-alpha-tunnel.service
  systemctl --user restart news-alpha-tunnel.service
  TUNNEL_URL_MSG="발급 중... (수 초 내 텔레그램으로 자동 전송됩니다)"
  for _ in $(seq 1 18); do
    if [ -f "${REPO_DIR}/data/tunnel_status.json" ]; then
      FOUND_URL="$("${PYTHON_BIN}" -c "import json, pathlib; p=pathlib.Path('${REPO_DIR}/data/tunnel_status.json'); d=json.loads(p.read_text()) if p.exists() else {}; print(d.get('url') or '')" 2>/dev/null || true)"
      if [ -n "${FOUND_URL}" ]; then
        TUNNEL_URL_MSG="${FOUND_URL} (텔레그램으로도 자동 전송됨)"
        break
      fi
    fi
    sleep 0.5
  done
fi

# 선택 기능: TELEGRAM_API_ID & TELEGRAM_API_HASH가 설정되어 있으면 실시간 콜백용 .venv 준비
TG_API_ID="$(grep -E '^TELEGRAM_API_ID=' "${REPO_DIR}/.env" | tail -n 1 | cut -d= -f2- | tr -d ' "\r' || true)"
TG_API_HASH="$(grep -E '^TELEGRAM_API_HASH=' "${REPO_DIR}/.env" | tail -n 1 | cut -d= -f2- | tr -d ' "\r' || true)"
TG_STATE="미설정 (.env에 TELEGRAM_API_ID/HASH 입력 시 활성화)"

if [ -n "${TG_API_ID}" ] && [ -n "${TG_API_HASH}" ]; then
  echo "[설치] 텔레그램 0초 콜백용 가상환경(.venv) 및 telethon 설치 중..."
  if [ ! -x "${REPO_DIR}/.venv/bin/python" ]; then
    "${PYTHON_BIN}" -m venv "${REPO_DIR}/.venv"
  fi
  "${REPO_DIR}/.venv/bin/pip" install --quiet -r "${REPO_DIR}/requirements-realtime.txt"
  if [ -f "${REPO_DIR}/data/telegram_user.session" ]; then
    systemctl --user enable --now news-alpha-telegram.service
    systemctl --user restart news-alpha-telegram.service
    TG_STATE="가동 중 (0초 콜백 활성)"
  else
    TG_STATE="1회 로그인 대기 중 -> 아래 명령 실행 필요"
  fi
fi

if command -v loginctl >/dev/null 2>&1; then
  loginctl enable-linger "${USER}" 2>/dev/null || \
    echo "[참고] 재부팅 후 자동 시작을 위해 필요 시 실행: sudo loginctl enable-linger ${USER}"
fi

PORT="$(grep -E '^DASHBOARD_PORT=' "${REPO_DIR}/.env" | tail -n 1 | cut -d= -f2- | tr -d ' "\r' || true)"
PORT="${PORT:-8787}"
LAN_IP="$(hostname -I 2>/dev/null | awk '{print $1}' || echo '127.0.0.1')"

echo ""
echo "============================================================"
echo " News Alpha Radar 상시 서버 + 웹 대시보드 + 외부접속 설정 완료!"
echo "============================================================"
echo " • 집 와이파이 접속 : http://${LAN_IP}:${PORT}"
echo " • LTE/외부 접속    : ${TUNNEL_URL_MSG}"
if [ -n "${PW_INFO}" ]; then
  echo " • 대시보드 비밀번호: ${PW_INFO}"
fi
echo " • GitHub 자동반영 : 2분마다 main 체크 (news-alpha-updater.timer 활성)"
echo " • 텔레그램 콜백   : ${TG_STATE}"
if [ -n "${TG_API_ID}" ] && [ -n "${TG_API_HASH}" ] && [ ! -f "${REPO_DIR}/data/telegram_user.session" ]; then
  echo ""
  echo " [텔레그램 콜백 최초 1회 로그인 명령]"
  echo "   cd ${REPO_DIR} && .venv/bin/python telegram_listener.py --login"
  echo "   systemctl --user enable --now news-alpha-telegram.service"
fi
echo ""
echo " • 상태 확인       : systemctl --user status news-alpha-radar news-alpha-dashboard news-alpha-tunnel news-alpha-updater.timer"
echo " • 실시간 로그     : journalctl --user -u news-alpha-radar -f"
echo "============================================================"
