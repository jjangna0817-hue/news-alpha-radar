# News Alpha Radar — 우분투 노트북 24/7 상시 서버 & 맥북 원격 개발 가이드

남는 우분투 노트북 1대로 **텔레그램 속보 0초 실시간 콜백 + 2단 초고속 1보 푸시(3~5초) + 백업 RSS 60초 감시 + 자체 관리 웹 대시보드 + GitHub 자동 업데이트**를 추가 비용 0원으로 운영하는 방법입니다.

---

## 1. 최초 1회 설치 (우분투 노트북)

```bash
# 1) 저장소 받기 (이미 있으면 cd news-alpha-radar && git pull)
git clone https://github.com/jjangna0817-hue/news-alpha-radar.git
cd news-alpha-radar

# 2) 최초 실행 (.env 파일 자동 생성)
bash deploy/ubuntu/install.sh

# 3) API 키·텔레그램 정보 입력 (저장: Ctrl+O -> Enter -> Ctrl+X)
nano .env

# 4) 상시 데몬 + 웹 대시보드 + 2분 자동 업데이트 타이머 일괄 등록
bash deploy/ubuntu/install.sh
```

> **맥북/스마트폰에서 대시보드 접속 시 방화벽 허용 (최초 1회)**:
> ```bash
> sudo ufw allow 8787/tcp
> echo "http://$(hostname -I | awk '{print $1}'):8787"
> ```

---

## 2. 텔레그램 0초 실시간 콜백 활성화 (최초 1회 로그인)

일반 뉴스 RSS(1~3분 지연)보다 훨씬 빠르게, **텔레그램 공개 속보 채널에 메시지가 뜨는 그 순간(0초 콜백)** 받아서 **3~5초 내 1보 속보 푸시 → 15초 뒤 2보 심층 리포트**를 보냅니다.

1. **무료 API 발급 (1분)**: https://my.telegram.org 로그인 → **API development tools** → 앱 이름 아무거나 입력 → **`App api_id`**(숫자)와 **`App api_hash`**(문자열) 복사
2. **`.env`에 추가**:
   ```bash
   nano ~/news-alpha-radar/.env
   ```
   ```env
   TELEGRAM_API_ID=12345678
   TELEGRAM_API_HASH=여기에_api_hash_입력
   TELEGRAM_SOURCE_CHANNELS=@구독할_속보채널1,@구독할_속보채널2
   INSTANT_FLASH_ALERT=true
   ```
   *(구독할 채널은 나중에 맥북 웹 대시보드 → `운영 설정` 탭에서 언제든 추가/삭제할 수 있습니다.)*
3. **가상환경 설치 및 최초 1회 로그인**:
   ```bash
   cd ~/news-alpha-radar
   bash deploy/ubuntu/install.sh
   .venv/bin/python telegram_listener.py --login
   # -> 휴대폰 번호(+8210...) + 텔레그램 앱으로 온 인증코드 입력 (딱 1번!)
   systemctl --user enable --now news-alpha-telegram.service
   ```
   이후에는 웹 대시보드 상단에 초록색 **`0초 콜백 · N채널`** 배지가 뜹니다!

---

## 3. 맥북에서 작업하고 우분투 손 안 대고 자동 반영하기

`install.sh`를 한 번 실행하면 **`news-alpha-updater.timer`** 가 자동으로 켜집니다.

- **자동 반영 (무조작)**: 맥북에서 수정 후 GitHub `main`에 머지/푸시하면 **최대 2분 이내**에 우분투가 자동으로:
  1. `git pull --ff-only origin main`
  2. 오프라인 단위 테스트(`unittest`) 통과 여부 검증 *(실패 시 자동 롤백!)*
  3. 데몬·콜백 리스너·웹 대시보드 자동 재시작
- **즉시 반영 (원클릭)**: 2분도 기다리기 싫을 때는 맥북 브라우저에서 웹 대시보드 오른쪽 위 **`최신 코드 업데이트`** 버튼만 클릭하면 바로 반영됩니다.

---

## 4. 노트북 24시간 운영 설정 (절전·덮개·재부팅)

```bash
# 1) 절전 모드 끄기
sudo systemctl mask sleep.target suspend.target hibernate.target hybrid-sleep.target

# 2) 덮개 닫아도 꺼지지 않게 설정
sudo mkdir -p /etc/systemd/logind.conf.d
sudo tee /etc/systemd/logind.conf.d/99-ignore-lid.conf <<'EOF'
[Login]
HandleLidSwitch=ignore
HandleLidSwitchExternalPower=ignore
HandleLidSwitchDocked=ignore
EOF

# 3) 재부팅 후 로그인 안 해도 자동 시작
sudo loginctl enable-linger "$USER"
```

---

## 5. 중복 알림 방지 — GitHub Actions 예약 끄기

우분투 노트북에서 정상 가동이 확인되면:
- GitHub 저장소 → **Actions** → **News Alpha Radar (24/7 Free)** → 오른쪽 위 **`...`** → **Disable workflow**

---

## 6. 자주 쓰는 확인 명령어

```bash
# 전체 서비스 상태 확인
systemctl --user status news-alpha-radar news-alpha-dashboard news-alpha-telegram news-alpha-updater.timer --no-pager

# 레이더 / 텔레그램 콜백 실시간 로그
journalctl --user -u news-alpha-radar -f
journalctl --user -u news-alpha-telegram -f
```
