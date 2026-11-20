#!/usr/bin/env bash
set -euo pipefail

# News Alpha Radar — GitHub main 자동 동기화 및 무중단 안전 재시작 스크립트
# - 2분마다 systemd timer가 실행하거나, 웹 대시보드 '최신 코드 업데이트' 버튼이 호출합니다.
# - 새 커밋이 없으면 서비스를 재시작하지 않고 즉시 종료합니다.
# - 업데이트 후 단위 테스트가 실패하면 이전 커밋으로 자동 롤백합니다.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PYTHON_BIN="$(command -v python3 || true)"
LOCK_FILE="/tmp/news-alpha-radar-update.lock"

DEFER_DASHBOARD=0
for arg in "$@"; do
  case "$arg" in
    --defer-dashboard) DEFER_DASHBOARD=1 ;;
  esac
done

exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "STATUS=BUSY"
  exit 0
fi

cd "${REPO_DIR}"

if [ -n "$(git status --porcelain --untracked-files=no 2>/dev/null || true)" ]; then
  echo "STATUS=DIRTY_SKIP"
  echo "[경고] 로컬 수정 사항이 있어 자동 업데이트를 건너뜁니다."
  exit 0
fi

BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [ "${BRANCH}" != "main" ]; then
  echo "STATUS=NON_MAIN_SKIP"
  echo "[참고] 현재 브랜치가 main이 아닙니다 (${BRANCH})."
  exit 0
fi

git fetch --quiet origin main

BEFORE="$(git rev-parse --short HEAD)"
BEFORE_FULL="$(git rev-parse HEAD)"
REMOTE_FULL="$(git rev-parse origin/main)"

if [ "${BEFORE_FULL}" = "${REMOTE_FULL}" ]; then
  echo "STATUS=UP_TO_DATE"
  echo "COMMIT=${BEFORE}"
  exit 0
fi

echo "[업데이트] 새 커밋 감지: ${BEFORE} -> $(git rev-parse --short origin/main)"
git pull --ff-only origin main

AFTER="$(git rev-parse --short HEAD)"

# 안전 게이트: 오프라인 단위 테스트 통과 확인 (실패 시 즉시 자동 롤백)
if [ -n "${PYTHON_BIN}" ]; then
  if ! env -i PATH="${PATH}" HOME="${HOME}" "${PYTHON_BIN}" -m unittest discover -s tests >/dev/null 2>&1; then
    echo "[오류] 단위 테스트 실패 — 이전 버전(${BEFORE})으로 자동 롤백합니다!"
    git reset --hard "${BEFORE_FULL}"
    echo "STATUS=ROLLBACK"
    exit 1
  fi
fi

# 실시간 콜백용 .venv가 있으면 패키지 동기화
if [ -x "${REPO_DIR}/.venv/bin/pip" ] && [ -f "${REPO_DIR}/requirements-realtime.txt" ]; then
  "${REPO_DIR}/.venv/bin/pip" install --quiet -r "${REPO_DIR}/requirements-realtime.txt" || true
fi

# systemd 유저 유닛 템플릿 최신화
UNIT_DIR="${HOME}/.config/systemd/user"
if [ -d "${UNIT_DIR}" ] && [ -n "${PYTHON_BIN}" ]; then
  for svc in news-alpha-radar.service news-alpha-dashboard.service news-alpha-updater.service news-alpha-updater.timer news-alpha-telegram.service; do
    if [ -f "${SCRIPT_DIR}/${svc}" ]; then
      sed \
        -e "s|__WORKDIR__|${REPO_DIR}|g" \
        -e "s|__PYTHON__|${PYTHON_BIN}|g" \
        "${SCRIPT_DIR}/${svc}" > "${UNIT_DIR}/${svc}"
    fi
  done
  systemctl --user daemon-reload || true
fi

# 서비스 재시작 (대시보드 버튼에서 호출된 경우 응답 반환 후 1초 뒤 대시보드 재시작)
if command -v systemctl >/dev/null 2>&1; then
  systemctl --user is-active --quiet news-alpha-radar.service && systemctl --user restart news-alpha-radar.service || true
  systemctl --user is-active --quiet news-alpha-telegram.service && systemctl --user restart news-alpha-telegram.service || true
  if systemctl --user is-active --quiet news-alpha-dashboard.service; then
    if [ "${DEFER_DASHBOARD}" = "1" ]; then
      nohup bash -c 'sleep 1; systemctl --user restart news-alpha-dashboard.service' </dev/null >/dev/null 2>&1 9>&- &
    else
      systemctl --user restart news-alpha-dashboard.service || true
    fi
  fi
fi

echo "STATUS=UPDATED"
echo "BEFORE=${BEFORE}"
echo "AFTER=${AFTER}"
echo "[완료] 최신 코드(${AFTER}) 반영 및 서비스 재시작 완료!"
