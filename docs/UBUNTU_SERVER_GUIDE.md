# News Alpha Radar — 우분투 노트북 24/7 상시 서버 설치 가이드

남는 우분투 노트북 1대로 **장중 1분 단위 초실시간 감시 + 자체 관리 웹 대시보드**를 추가 비용 0원으로 운영하는 방법입니다.

---

## 1. 5분 만에 설치 및 실행

우분투 터미널을 열고 아래 명령을 순서대로 실행합니다.

```bash
# 1) 저장소 받기 (이미 있으면 git pull)
git clone https://github.com/jjangna0817-hue/news-alpha-radar.git
cd news-alpha-radar

# 2) 최초 1회 실행 (.env 파일 자동 생성)
bash deploy/ubuntu/install.sh

# 3) API 키·텔레그램 정보·대시보드 비밀번호 입력
nano .env
```

`.env`에서 아래 3가지를 입력하고 저장(`Ctrl+O` → `Enter` → `Ctrl+X`)합니다.
- `GEMINI_API_KEY` (Google AI Studio API 키)
- `TELEGRAM_BOT_TOKEN` (텔레그램 봇 토큰)
- `TELEGRAM_CHAT_ID` (텔레그램 채팅 ID)
- *(선택)* `DASHBOARD_PASSWORD` (외부 스마트폰 접속을 쓸 경우 반드시 비밀번호 설정!)

```bash
# 4) 상시 데몬 + 웹 대시보드 자동 등록 및 가동
bash deploy/ubuntu/install.sh
```

설치가 끝나면 브라우저에서 아래 주소로 접속할 수 있습니다.
- 노트북 본인: `http://127.0.0.1:8787`
- 같은 집 Wi-Fi의 스마트폰/PC: `http://<노트북_IP>:8787` *(설치 완료 화면에 주소 자동 표시)*

---

## 2. 노트북 24시간 운영 필수 설정

### ① 덮개를 닫아도 꺼지지 않게 설정
```bash
sudo nano /etc/systemd/logind.conf
```
아래 두 줄의 앞 `#`을 지우고 값을 `ignore`로 바꾼 뒤 저장합니다.
```ini
HandleLidSwitch=ignore
HandleLidSwitchExternalPower=ignore
```
적용:
```bash
sudo systemctl restart systemd-logind
```

### ② 재부팅 후 로그인 안 해도 자동 실행되게 설정
```bash
sudo loginctl enable-linger "$USER"
```

### ③ 배터리 수명 보호 (중요!)
24시간 충전기를 꽂아두면 배터리가 부풀 수 있습니다.
- 노트북 BIOS 설정(또는 TLP/제조사 도구)에서 **배터리 충전 상한을 60~80%** 로 제한하는 것을 권장합니다.

---

## 3. 중복 알림 방지 — GitHub Actions 자동 예약 끄기

노트북 대시보드에서 **`텔레그램 테스트`** 와 **`지금 즉시 스캔`** 이 정상 동작하는 것을 확인했다면, 기존 GitHub Actions와 중복 알림이 오지 않도록 Actions 예약 실행만 꺼두세요.

1. GitHub 저장소 상단 **Actions** 탭 클릭
2. 왼쪽 목록에서 **News Alpha Radar (24/7 Free)** 클릭
3. 오른쪽 위 **`...`** 버튼 클릭 → **Disable workflow** 선택

*(언제든 노트북을 끄고 싶을 때 같은 자리에서 **Enable workflow**를 누르면 바로 GitHub Actions로 복귀할 수 있습니다.)*

---

## 4. (선택) 밖에서 스마트폰으로 대시보드 접속하기 — Cloudflare Tunnel (무료)

공유기 포트포워딩 없이 안전하게 HTTPS로 접속하는 가장 쉬운 방법입니다.

1. 먼저 `.env`에 `DASHBOARD_PASSWORD=안전한비밀번호`를 넣고 대시보드를 재시작합니다:
   ```bash
   systemctl --user restart news-alpha-dashboard
   ```
2. `cloudflared` 설치 후 터널 실행:
   ```bash
   # 빠른 임시 테스트 URL 발급 (계정 불필요)
   cloudflared tunnel --url http://127.0.0.1:8787
   ```
   *(고정 도메인이 필요하면 Cloudflare Zero Trust 무료 계정에서 Tunnel을 생성해 서비스로 등록하면 됩니다.)*

---

## 5. 자주 쓰는 운영 명령어

```bash
# 실행 상태 확인
systemctl --user status news-alpha-radar news-alpha-dashboard

# 레이더 실시간 로그 보기 (종료: Ctrl+C)
journalctl --user -u news-alpha-radar -f

# 최신 코드 업데이트 후 재시작
cd ~/news-alpha-radar && git pull && systemctl --user restart news-alpha-radar news-alpha-dashboard

# 완전 중지
systemctl --user stop news-alpha-radar news-alpha-dashboard
```
